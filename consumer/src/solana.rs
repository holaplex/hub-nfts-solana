use anchor_lang::{prelude::AccountMeta, AnchorDeserialize, InstructionData};
use holaplex_hub_nfts_solana_core::proto::{
    treasury_events::SolanaTransactionResult, MasterEdition, MetaplexMasterEditionTransaction,
    MetaplexMetadata, MintMetaplexEditionTransaction, MintMetaplexMetadataTransaction,
    TransferMetaplexAssetTransaction, UpdateSolanaMintPayload,
};
use holaplex_hub_nfts_solana_entity::{
    collection_mints, collections, compression_leafs, update_revisions,
};
use hub_core::{
    anyhow::Result,
    backon::{BlockingRetryable, ExponentialBuilder},
    bs58, clap,
    prelude::*,
    thiserror,
    uuid::Uuid,
};
use mpl_bubblegum::state::metaplex_adapter::{
    Collection, Creator as BubblegumCreator, TokenProgramVersion,
};
use mpl_token_metadata::{
    instruction::{
        mint_new_edition_from_master_edition_via_token, set_and_verify_sized_collection_item,
        unverify_sized_collection_item, update_metadata_accounts_v2,
    },
    state::{Creator, DataV2, EDITION, PREFIX},
};
use solana_client::{client_error::ClientError, rpc_client::RpcClient as SolanaRpcClient};
use solana_program::{
    instruction::Instruction, program_pack::Pack, pubkey::Pubkey,
    system_instruction::create_account, system_program,
};
use solana_sdk::{
    signature::Signature,
    signer::{keypair::Keypair, Signer},
    transaction::Transaction,
};
use solana_transaction_status::{UiInnerInstructions, UiInstruction, UiTransactionEncoding};
use spl_account_compression::{
    events::{AccountCompressionEvent, ChangeLogEventV1},
    ChangeLogEvent,
};
use spl_associated_token_account::{
    get_associated_token_address, instruction::create_associated_token_account,
};
use spl_token::{
    instruction::{initialize_mint, mint_to},
    state,
};

use crate::{
    asset_api::RpcClient,
    backend::{
        CollectionBackend, MasterEditionAddresses, MintBackend, MintCompressedMintV1Addresses,
        MintEditionAddresses, MintMetaplexAddresses, SwitchCollectionAddresses,
        TransactionResponse, TransferAssetAddresses, TransferBackend,
        TransferCompressedMintV1Addresses, UpdateCollectionMintAddresses,
        UpdateMasterEditionAddresses,
    },
};

macro_rules! call_with_retry {
    ($expr:expr) => {{
        (|| $expr)
            .retry(
                &ExponentialBuilder::default()
                    .with_jitter()
                    .with_min_delay(Duration::from_millis(30))
                    .with_max_times(15),
            )
            .notify(|err: &ClientError, dur: Duration| {
                error!("retrying error {:?} in {:?}", err, dur);
            })
            .call()
    }};
}

const TOKEN_PROGRAM_PUBKEY: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

#[derive(Debug, clap::Args)]
pub struct SolanaArgs {
    #[arg(long, env)]
    pub solana_endpoint: String,

    #[arg(long, env)]
    pub solana_treasury_wallet_address: Pubkey,

    #[arg(long, env)]
    pub digital_asset_api_endpoint: String,

    #[arg(long, env)]
    pub tree_authority: Pubkey,
    #[arg(long, env)]
    pub merkle_tree: Pubkey,
}

#[derive(Clone)]
pub struct CreateEditionRequest {
    pub collection: Uuid,
    pub recipient: String,
    pub owner_address: String,
    pub edition: u64,
}

#[derive(Clone)]
pub struct UpdateEditionRequest {
    pub collection: Uuid,
    pub owner_address: String,
    pub seller_fee_basis_points: Option<u16>,
    pub name: String,
    pub symbol: String,
    pub uri: String,
    pub creators: Vec<Creator>,
}

#[derive(Clone)]
pub struct TransferAssetRequest {
    pub sender: String,
    pub recipient: String,
    pub mint_address: String,
}

#[derive(Debug, thiserror::Error)]
enum SolanaErrorNotFoundMessage {
    #[error("master edition message not found")]
    MasterEdition,
    #[error("serialized message message not found")]
    Serialized,
    #[error("metadata message not found")]
    Metadata,
}

#[derive(Debug, thiserror::Error, Triage)]
pub enum SolanaAssetIdError {
    #[error("The transaction has no meta field")]
    NoTransactionMeta,
    #[error("No inner instruction found")]
    NoInnerInstruction,
    #[error("Solana RPC error")]
    Rpc(#[from] solana_client::client_error::ClientError),
    #[error("No nonce found in log messages")]
    NoNonce,
    #[error("Parsing base58 string")]
    Base58(#[from] bs58::decode::Error),
    #[error("Borsh deserialization error")]
    BorshDeserialize(#[from] std::io::Error),
    #[error("Asset id not found")]
    NotFound,
}

#[derive(Clone)]
pub struct Solana {
    rpc_client: Arc<SolanaRpcClient>,
    treasury_wallet_address: Pubkey,
    bubblegum_tree_authority: Pubkey,
    bubblegum_merkle_tree: Pubkey,
    bubblegum_cpi_address: Pubkey,
    asset_rpc_client: jsonrpsee::http_client::HttpClient,
}

impl Solana {
    pub fn new(args: SolanaArgs) -> Result<Self> {
        let SolanaArgs {
            solana_endpoint,
            solana_treasury_wallet_address,
            digital_asset_api_endpoint,
            tree_authority,
            merkle_tree,
        } = args;
        let rpc_client = Arc::new(SolanaRpcClient::new(solana_endpoint));

        let (bubblegum_cpi_address, _) = Pubkey::find_program_address(
            &[mpl_bubblegum::state::COLLECTION_CPI_PREFIX.as_bytes()],
            &mpl_bubblegum::ID,
        );

        Ok(Self {
            rpc_client,
            treasury_wallet_address: solana_treasury_wallet_address,
            bubblegum_tree_authority: tree_authority,
            bubblegum_merkle_tree: merkle_tree,
            bubblegum_cpi_address,
            asset_rpc_client: jsonrpsee::http_client::HttpClientBuilder::default()
                .request_timeout(std::time::Duration::from_secs(15))
                .build(digital_asset_api_endpoint)
                .context("Failed to initialize asset API client")?,
        })
    }

    pub fn asset_rpc(&self) -> jsonrpsee::http_client::HttpClient {
        self.asset_rpc_client.clone()
    }

    #[must_use]
    pub fn rpc(&self) -> Arc<SolanaRpcClient> {
        self.rpc_client.clone()
    }

    /// Res
    ///
    /// # Errors
    /// This function fails if unable to query or pull the asset id from the instruction data of the transaction
    pub fn extract_compression_nonce(
        &self,
        signature: &Signature,
    ) -> Result<u32, SolanaAssetIdError> {
        let response = call_with_retry!(
            self.rpc()
                .get_transaction(signature, UiTransactionEncoding::Json)
        )?;

        let meta = response
            .transaction
            .meta
            .ok_or(SolanaAssetIdError::NoTransactionMeta)?;

        let inner_instructions: Option<Vec<UiInnerInstructions>> = meta.inner_instructions.into();
        let inner_instructions =
            inner_instructions.ok_or(SolanaAssetIdError::NoInnerInstruction)?;

        let instructions = inner_instructions
            .get(0)
            .ok_or(SolanaAssetIdError::NoInnerInstruction)?;
        let noop_change_log_instruction = instructions
            .instructions
            .get(3)
            .ok_or(SolanaAssetIdError::NoInnerInstruction)?;

        if let UiInstruction::Compiled(instruction) = noop_change_log_instruction {
            let data = bs58::decode(&instruction.data).into_vec()?;

            match AccountCompressionEvent::try_from_slice(&data)? {
                AccountCompressionEvent::ChangeLog(changelog_event) => {
                    let ChangeLogEvent::V1(changelog_event) = changelog_event;
                    let ChangeLogEventV1 { index, .. } = changelog_event;

                    Ok(index)
                },
                AccountCompressionEvent::ApplicationData(_) => Err(SolanaAssetIdError::NoNonce),
            }
        } else {
            Err(SolanaAssetIdError::NoNonce)
        }
    }

    /// Res
    ///
    /// # Errors
    /// This function fails if unable to submit transaction to Solana
    pub fn submit_transaction(&self, transaction: &SolanaTransactionResult) -> Result<String> {
        let signatures = transaction
            .signed_message_signatures
            .iter()
            .map(|s| Signature::from_str(s).context("failed to parse signature"))
            .collect::<Result<Vec<Signature>>>()?;

        let message = bincode::deserialize(
            &transaction
                .serialized_message
                .clone()
                .ok_or(SolanaErrorNotFoundMessage::Serialized)?,
        )?;

        let transaction = Transaction {
            signatures,
            message,
        };

        let signature = self.rpc().send_transaction(&transaction).map_err(|e| {
            let msg = format!("failed to send transaction: {e}");
            error!(msg);
            anyhow!(msg)
        })?;

        Ok(signature.to_string())
    }
}

#[repr(transparent)]
pub struct UncompressedRef<'a>(pub &'a Solana);
#[repr(transparent)]
pub struct CompressedRef<'a>(pub &'a Solana);
#[repr(transparent)]
pub struct EditionRef<'a>(pub &'a Solana);

impl<'a> CollectionBackend for UncompressedRef<'a> {
    fn create(
        &self,
        txn: MetaplexMasterEditionTransaction,
    ) -> hub_core::prelude::Result<TransactionResponse<MasterEditionAddresses>> {
        let MetaplexMasterEditionTransaction { master_edition, .. } = txn;
        let master_edition = master_edition.ok_or(SolanaErrorNotFoundMessage::MasterEdition)?;
        let payer: Pubkey = self.0.treasury_wallet_address;
        let rpc = &self.0.rpc_client;
        let mint = Keypair::new();
        let MasterEdition {
            name,
            symbol,
            seller_fee_basis_points,
            metadata_uri,
            creators,
            supply,
            owner_address,
        } = master_edition;
        let owner: Pubkey = owner_address.parse()?;

        let (metadata, _) = Pubkey::find_program_address(
            &[
                b"metadata",
                mpl_token_metadata::ID.as_ref(),
                mint.pubkey().as_ref(),
            ],
            &mpl_token_metadata::ID,
        );
        let associated_token_account = get_associated_token_address(&owner, &mint.pubkey());
        let (master_edition, _) = Pubkey::find_program_address(
            &[
                b"metadata",
                mpl_token_metadata::ID.as_ref(),
                mint.pubkey().as_ref(),
                b"edition",
            ],
            &mpl_token_metadata::ID,
        );
        let len = spl_token::state::Mint::LEN;

        let rent = call_with_retry!(rpc.get_minimum_balance_for_rent_exemption(len))?;
        let blockhash = call_with_retry!(rpc.get_latest_blockhash())?;

        let create_account_ins = solana_program::system_instruction::create_account(
            &payer,
            &mint.pubkey(),
            rent,
            len.try_into()?,
            &spl_token::ID,
        );
        let initialize_mint_ins = spl_token::instruction::initialize_mint(
            &spl_token::ID,
            &mint.pubkey(),
            &owner,
            Some(&owner),
            0,
        )?;
        let ata_ins = spl_associated_token_account::instruction::create_associated_token_account(
            &payer,
            &owner,
            &mint.pubkey(),
            &spl_token::ID,
        );
        let min_to_ins = spl_token::instruction::mint_to(
            &spl_token::ID,
            &mint.pubkey(),
            &associated_token_account,
            &owner,
            &[],
            1,
        )?;
        let create_metadata_account_ins =
            mpl_token_metadata::instruction::create_metadata_accounts_v3(
                mpl_token_metadata::ID,
                metadata,
                mint.pubkey(),
                owner,
                payer,
                owner,
                name,
                symbol,
                metadata_uri,
                Some(
                    creators
                        .into_iter()
                        .map(TryInto::try_into)
                        .collect::<Result<Vec<Creator>, _>>()?,
                ),
                seller_fee_basis_points.try_into()?,
                true,
                true,
                None,
                None,
                Some(mpl_token_metadata::state::CollectionDetails::V1 { size: 0 }),
            );
        let create_master_edition_ins = mpl_token_metadata::instruction::create_master_edition_v3(
            mpl_token_metadata::ID,
            master_edition,
            mint.pubkey(),
            owner,
            owner,
            metadata,
            payer,
            supply.map(TryInto::try_into).transpose()?,
        );
        let instructions = vec![
            create_account_ins,
            initialize_mint_ins,
            ata_ins,
            min_to_ins,
            create_metadata_account_ins,
            create_master_edition_ins,
        ];

        let message = solana_program::message::Message::new_with_blockhash(
            &instructions,
            Some(&payer),
            &blockhash,
        );

        let serialized_message = message.serialize();
        let mint_signature = mint.try_sign_message(&message.serialize())?;

        Ok(TransactionResponse {
            serialized_message,
            signatures_or_signers_public_keys: vec![
                payer.to_string(),
                mint_signature.to_string(),
                owner.to_string(),
            ],
            addresses: MasterEditionAddresses {
                master_edition,
                update_authority: owner,
                associated_token_account,
                mint: mint.pubkey(),
                owner,
                metadata,
            },
        })
    }

    fn update(
        &self,
        collection: &collections::Model,
        txn: MetaplexMasterEditionTransaction,
    ) -> hub_core::prelude::Result<TransactionResponse<UpdateMasterEditionAddresses>> {
        let rpc = &self.0.rpc_client;

        let MetaplexMasterEditionTransaction { master_edition, .. } = txn;

        let master_edition = master_edition.ok_or(SolanaErrorNotFoundMessage::MasterEdition)?;

        let MasterEdition {
            name,
            seller_fee_basis_points,
            symbol,
            creators,
            metadata_uri,
            ..
        } = master_edition;

        let payer: Pubkey = self.0.treasury_wallet_address;

        let program_pubkey = mpl_token_metadata::id();
        let update_authority: Pubkey = master_edition.owner_address.parse()?;
        let metadata: Pubkey = collection.metadata.parse()?;

        let ins = update_metadata_accounts_v2(
            program_pubkey,
            metadata,
            update_authority,
            None,
            Some(DataV2 {
                name,
                symbol,
                uri: metadata_uri,
                seller_fee_basis_points: seller_fee_basis_points.try_into()?,
                creators: Some(
                    creators
                        .into_iter()
                        .map(TryInto::try_into)
                        .collect::<Result<Vec<Creator>>>()?,
                ),
                collection: None,
                uses: None,
            }),
            None,
            None,
        );

        let blockhash = call_with_retry!(rpc.get_latest_blockhash())?;

        let message =
            solana_program::message::Message::new_with_blockhash(&[ins], Some(&payer), &blockhash);

        let serialized_message = message.serialize();

        Ok(TransactionResponse {
            serialized_message,
            signatures_or_signers_public_keys: vec![
                payer.to_string(),
                update_authority.to_string(),
            ],
            addresses: UpdateMasterEditionAddresses {
                metadata,
                update_authority,
            },
        })
    }

    fn update_mint(
        &self,
        collection: &collections::Model,
        collection_mint: &collection_mints::Model,
        payload: UpdateSolanaMintPayload,
    ) -> Result<TransactionResponse<UpdateCollectionMintAddresses>> {
        let metadata = payload
            .metadata
            .ok_or(SolanaErrorNotFoundMessage::Metadata)?;
        let payer: Pubkey = self.0.treasury_wallet_address;
        let rpc = &self.0.rpc_client;

        let MetaplexMetadata {
            name,
            symbol,
            seller_fee_basis_points,
            metadata_uri,
            creators,
            owner_address,
        } = metadata;
        let update_authority: Pubkey = owner_address.parse()?;
        let mint_pubkey: Pubkey = collection_mint.mint.parse()?;

        let (metadata, _) = Pubkey::find_program_address(
            &[
                b"metadata",
                mpl_token_metadata::ID.as_ref(),
                mint_pubkey.as_ref(),
            ],
            &mpl_token_metadata::ID,
        );

        let blockhash = call_with_retry!(rpc.get_latest_blockhash())?;

        let update_ins: Instruction = mpl_token_metadata::instruction::update_metadata_accounts_v2(
            mpl_token_metadata::ID,
            metadata,
            update_authority,
            None,
            Some(DataV2 {
                name,
                symbol,
                uri: metadata_uri,
                seller_fee_basis_points: seller_fee_basis_points.try_into()?,
                creators: Some(
                    creators
                        .into_iter()
                        .map(TryInto::try_into)
                        .collect::<Result<Vec<Creator>, _>>()?,
                ),
                collection: Some(mpl_token_metadata::state::Collection {
                    verified: true,
                    key: collection.mint.parse()?,
                }),
                uses: None,
            }),
            None,
            Some(true),
        );

        let message = solana_program::message::Message::new_with_blockhash(
            &[update_ins],
            Some(&payer),
            &blockhash,
        );

        let serialized_message = message.serialize();

        Ok(TransactionResponse {
            serialized_message,
            signatures_or_signers_public_keys: vec![
                payer.to_string(),
                update_authority.to_string(),
            ],
            addresses: UpdateCollectionMintAddresses {
                payer,
                metadata,
                update_authority,
            },
        })
    }

    fn retry_update_mint(
        &self,
        revision: &update_revisions::Model,
    ) -> Result<TransactionResponse<UpdateCollectionMintAddresses>> {
        let rpc = &self.0.rpc_client;

        let update_authority: Pubkey = revision.update_authority.parse()?;
        let metadata = revision.metadata.parse()?;
        let payer = Pubkey::from_str(&revision.payer)?;

        let mut message: solana_program::message::Message =
            bincode::deserialize(&revision.serialized_message)?;

        let blockhash = call_with_retry!(rpc.get_latest_blockhash())?;
        message.recent_blockhash = blockhash;

        Ok(TransactionResponse {
            serialized_message: message.serialize(),
            signatures_or_signers_public_keys: vec![
                payer.to_string(),
                update_authority.to_string(),
            ],
            addresses: UpdateCollectionMintAddresses {
                payer,
                metadata,
                update_authority,
            },
        })
    }

    fn switch(
        &self,
        mint: &collection_mints::Model,
        collection: &collections::Model,
        new_collection: &collections::Model,
    ) -> Result<TransactionResponse<SwitchCollectionAddresses>> {
        let rpc = &self.0.rpc_client;
        let payer = self.0.treasury_wallet_address;

        let mint_pubkey = Pubkey::from_str(&mint.mint)?;
        let program_pubkey = mpl_token_metadata::id();
        let metadata_seeds = &[
            PREFIX.as_bytes(),
            program_pubkey.as_ref(),
            mint_pubkey.as_ref(),
        ];
        let (metadata, _) = Pubkey::find_program_address(metadata_seeds, &program_pubkey);

        let collection_authority = collection.owner.parse()?;
        let collection_mint = Pubkey::from_str(&collection.mint)?;
        let collection_metadata_seeds = &[
            PREFIX.as_bytes(),
            program_pubkey.as_ref(),
            collection_mint.as_ref(),
        ];
        let (collection_metadata, _) =
            Pubkey::find_program_address(collection_metadata_seeds, &program_pubkey);

        let collection_master_edition = collection.master_edition.parse()?;

        let unverify_ins = unverify_sized_collection_item(
            program_pubkey,
            metadata,
            collection_authority,
            payer,
            collection_mint,
            collection_metadata,
            collection_master_edition,
            None,
        );

        let new_collection_mint = Pubkey::from_str(&new_collection.mint)?;

        let new_collection_metadata_seeds = &[
            PREFIX.as_bytes(),
            program_pubkey.as_ref(),
            new_collection_mint.as_ref(),
        ];
        let (new_collection_metadata, _) =
            Pubkey::find_program_address(new_collection_metadata_seeds, &program_pubkey);

        let new_collection_authority = Pubkey::from_str(&new_collection.owner)?;
        let new_collection_update_authority = Pubkey::from_str(&new_collection.update_authority)?;

        let verify_ins = set_and_verify_sized_collection_item(
            program_pubkey,
            metadata,
            new_collection_authority,
            payer,
            new_collection_update_authority,
            new_collection.mint.parse()?,
            new_collection_metadata,
            new_collection.master_edition.parse()?,
            None,
        );

        let instructions = vec![unverify_ins, verify_ins];

        let blockhash = call_with_retry!(rpc.get_latest_blockhash())?;

        let message = solana_program::message::Message::new_with_blockhash(
            &instructions,
            Some(&payer),
            &blockhash,
        );

        let serialized_message = message.serialize();

        Ok(TransactionResponse {
            serialized_message,
            signatures_or_signers_public_keys: vec![
                payer.to_string(),
                collection_authority.to_string(),
            ],
            addresses: SwitchCollectionAddresses {
                payer,
                new_collection_authority,
            },
        })
    }
}

impl<'a> MintBackend<MintMetaplexEditionTransaction, MintEditionAddresses> for EditionRef<'a> {
    fn mint(
        &self,
        collection: &collections::Model,
        txn: MintMetaplexEditionTransaction,
    ) -> hub_core::prelude::Result<TransactionResponse<MintEditionAddresses>> {
        let rpc = &self.0.rpc_client;
        let MintMetaplexEditionTransaction {
            recipient_address,
            owner_address,
            edition,
            ..
        } = txn;

        let payer: Pubkey = self.0.treasury_wallet_address;
        let owner = owner_address.parse()?;

        let program_pubkey = mpl_token_metadata::id();
        let master_edition_pubkey: Pubkey = collection.master_edition.parse()?;
        let master_edition_mint: Pubkey = collection.mint.parse()?;
        let existing_token_account: Pubkey = collection.associated_token_account.parse()?;
        let metadata: Pubkey = collection.metadata.parse()?;
        let recipient: Pubkey = recipient_address.parse()?;
        let edition = edition.try_into()?;

        let token_key = Pubkey::from_str(TOKEN_PROGRAM_PUBKEY)?;

        let new_mint_key = Keypair::new();
        let new_mint_pubkey = new_mint_key.pubkey();
        let added_token_account = get_associated_token_address(&recipient, &new_mint_key.pubkey());
        let new_mint_pub = new_mint_key.pubkey();
        let edition_seeds = &[
            PREFIX.as_bytes(),
            program_pubkey.as_ref(),
            new_mint_pub.as_ref(),
            EDITION.as_bytes(),
        ];
        let (edition_key, _) = Pubkey::find_program_address(edition_seeds, &program_pubkey);

        let metadata_seeds = &[
            PREFIX.as_bytes(),
            program_pubkey.as_ref(),
            new_mint_pub.as_ref(),
        ];
        let (metadata_key, _) = Pubkey::find_program_address(metadata_seeds, &program_pubkey);

        let rent = call_with_retry!(rpc.get_minimum_balance_for_rent_exemption(state::Mint::LEN))?;

        let mut instructions = vec![
            create_account(
                &payer,
                &new_mint_key.pubkey(),
                rent,
                state::Mint::LEN as u64,
                &token_key,
            ),
            initialize_mint(&token_key, &new_mint_key.pubkey(), &owner, Some(&owner), 0)?,
            create_associated_token_account(&payer, &recipient, &new_mint_pubkey, &spl_token::ID),
            mint_to(
                &token_key,
                &new_mint_pubkey,
                &added_token_account,
                &owner,
                &[&owner],
                1,
            )?,
        ];

        instructions.push(mint_new_edition_from_master_edition_via_token(
            program_pubkey,
            metadata_key,
            edition_key,
            master_edition_pubkey,
            new_mint_pubkey,
            owner,
            payer,
            owner,
            existing_token_account,
            owner,
            metadata,
            master_edition_mint,
            edition,
        ));

        let blockhash = call_with_retry!(rpc.get_latest_blockhash())?;

        let message = solana_program::message::Message::new_with_blockhash(
            &instructions,
            Some(&payer),
            &blockhash,
        );

        let serialized_message = message.serialize();
        let mint_signature = new_mint_key.try_sign_message(&message.serialize())?;

        Ok(TransactionResponse {
            serialized_message,
            signatures_or_signers_public_keys: vec![
                payer.to_string(),
                mint_signature.to_string(),
                owner.to_string(),
            ],
            addresses: MintEditionAddresses {
                owner,
                edition: edition_key,
                mint: new_mint_pubkey,
                metadata: metadata_key,
                associated_token_account: added_token_account,
                recipient,
            },
        })
    }
}

#[async_trait]
impl<'a> TransferBackend<collection_mints::Model, TransferAssetAddresses> for UncompressedRef<'a> {
    async fn transfer(
        &self,
        collection_mint: &collection_mints::Model,
        txn: TransferMetaplexAssetTransaction,
    ) -> hub_core::prelude::Result<TransactionResponse<TransferAssetAddresses>> {
        let rpc = &self.0.rpc_client;
        let TransferMetaplexAssetTransaction {
            owner_address,
            recipient_address,
            ..
        } = txn;

        let sender: Pubkey = owner_address.parse()?;
        let recipient: Pubkey = recipient_address.parse()?;
        let mint_address: Pubkey = collection_mint.mint.parse()?;
        let payer: Pubkey = self.0.treasury_wallet_address;
        let blockhash = call_with_retry!(rpc.get_latest_blockhash())?;
        let source_ata = get_associated_token_address(&sender, &mint_address);
        let destination_ata = get_associated_token_address(&recipient, &mint_address);

        let create_ata_token_account =
            create_associated_token_account(&payer, &recipient, &mint_address, &spl_token::ID);

        let transfer_instruction = spl_token::instruction::transfer(
            &spl_token::ID,
            &source_ata,
            &destination_ata,
            &sender,
            &[&sender],
            1,
        )
        .context("failed to create transfer instruction")?;

        let close_ata = spl_token::instruction::close_account(
            &spl_token::ID,
            &source_ata,
            &payer,
            &sender,
            &[&sender],
        )?;

        let message = solana_program::message::Message::new_with_blockhash(
            &[create_ata_token_account, transfer_instruction, close_ata],
            Some(&payer),
            &blockhash,
        );

        let serialized_message = message.serialize();

        Ok(TransactionResponse {
            serialized_message,
            signatures_or_signers_public_keys: vec![payer.to_string(), sender.to_string()],
            addresses: TransferAssetAddresses {
                owner: sender,
                recipient,
                recipient_associated_token_account: destination_ata,
                owner_associated_token_account: source_ata,
            },
        })
    }
}

#[async_trait]
impl<'a> TransferBackend<compression_leafs::Model, TransferCompressedMintV1Addresses>
    for CompressedRef<'a>
{
    async fn transfer(
        &self,
        compression_leaf: &compression_leafs::Model,
        txn: TransferMetaplexAssetTransaction,
    ) -> hub_core::prelude::Result<TransactionResponse<TransferCompressedMintV1Addresses>> {
        let TransferMetaplexAssetTransaction {
            recipient_address,
            owner_address,
            ..
        } = txn;
        let payer = self.0.treasury_wallet_address;
        let recipient = recipient_address.parse()?;
        let owner = owner_address.parse()?;

        let asset_api = &self.0.asset_rpc();

        let tree_authority_address = Pubkey::from_str(&compression_leaf.tree_authority)?;
        let merkle_tree_address = Pubkey::from_str(&compression_leaf.merkle_tree)?;

        let asset_id = compression_leaf
            .asset_id
            .clone()
            .ok_or(SolanaAssetIdError::NotFound)?;
        let asset = asset_api
            .get_asset(&asset_id)
            .await
            .context("fetching asset from DAA")?;
        let asset_proof = asset_api
            .get_asset_proof(&asset_id)
            .await
            .context("fetching asset proof from DAA")?;

        let root: Vec<u8> = asset_proof.root.into();
        let data_hash: Vec<u8> = asset.compression.data_hash.context("no data hash")?.into();
        let creator_hash: Vec<u8> = asset
            .compression
            .creator_hash
            .context("no creator hash")?
            .into();
        let leaf_id = asset.compression.leaf_id;
        let proofs = asset_proof
            .proof
            .into_iter()
            .map(|proof| Ok(AccountMeta::new_readonly(proof.try_into()?, false)))
            .collect::<Result<Vec<AccountMeta>>>()?;

        let mut accounts = vec![
            AccountMeta::new(tree_authority_address, false),
            AccountMeta::new_readonly(owner, true),
            AccountMeta::new_readonly(owner, false),
            AccountMeta::new_readonly(recipient, false),
            AccountMeta::new(merkle_tree_address, false),
            AccountMeta::new_readonly(spl_noop::ID, false),
            AccountMeta::new_readonly(spl_account_compression::ID, false),
            AccountMeta::new_readonly(system_program::ID, false),
        ];

        accounts.extend(proofs);

        let instructions = [Instruction {
            program_id: mpl_bubblegum::ID,
            accounts,
            data: mpl_bubblegum::instruction::Transfer {
                root: root.try_into().map_err(|_| anyhow!("Invalid root hash"))?,
                data_hash: data_hash
                    .try_into()
                    .map_err(|_| anyhow!("Invalid data hash"))?,
                creator_hash: creator_hash
                    .try_into()
                    .map_err(|_| anyhow!("Invalid creator hash"))?,
                nonce: leaf_id.into(),
                index: leaf_id,
            }
            .data(),
        }];

        let serialized_message = solana_program::message::Message::new_with_blockhash(
            &instructions,
            Some(&payer),
            &self.0.rpc_client.get_latest_blockhash()?,
        )
        .serialize();

        Ok(TransactionResponse {
            serialized_message,
            signatures_or_signers_public_keys: vec![payer.to_string(), owner.to_string()],
            addresses: TransferCompressedMintV1Addresses { owner, recipient },
        })
    }
}

impl<'a> MintBackend<MintMetaplexMetadataTransaction, MintCompressedMintV1Addresses>
    for CompressedRef<'a>
{
    fn mint(
        &self,
        collection: &collections::Model,
        txn: MintMetaplexMetadataTransaction,
    ) -> hub_core::prelude::Result<TransactionResponse<MintCompressedMintV1Addresses>> {
        let MintMetaplexMetadataTransaction {
            recipient_address,
            metadata,
            ..
        } = txn;

        let MetaplexMetadata {
            name,
            seller_fee_basis_points,
            symbol,
            creators,
            metadata_uri,
            owner_address,
        } = metadata.ok_or(SolanaErrorNotFoundMessage::Metadata)?;
        let payer = self.0.treasury_wallet_address;
        let recipient = recipient_address.parse()?;
        let owner = owner_address.parse()?;
        let merkle_tree = self.0.bubblegum_merkle_tree;
        let tree_authority = self.0.bubblegum_tree_authority;

        let mut accounts = vec![
            // Tree authority
            AccountMeta::new(self.0.bubblegum_tree_authority, false),
            // TODO: can we make the project treasury the leaf owner while keeping the tree authority the holaplex treasury wallet
            // Leaf owner
            AccountMeta::new_readonly(recipient, false),
            // Leaf delegate
            AccountMeta::new_readonly(recipient, false),
            // Merkle tree
            AccountMeta::new(merkle_tree, false),
            // Payer [signer]
            AccountMeta::new_readonly(payer, true),
            // Tree delegate [signer]
            AccountMeta::new_readonly(payer, true),
            // Collection authority [signer]
            AccountMeta::new_readonly(owner, true),
            // Collection authority pda
            AccountMeta::new_readonly(mpl_bubblegum::ID, false),
            // Collection mint
            AccountMeta::new_readonly(collection.mint.parse()?, false),
            // collection metadata [mutable]
            AccountMeta::new(collection.metadata.parse()?, false),
            // Edition account
            AccountMeta::new_readonly(collection.master_edition.parse()?, false),
            // Bubblegum Signer
            AccountMeta::new_readonly(self.0.bubblegum_cpi_address, false),
            AccountMeta::new_readonly(spl_noop::ID, false),
            AccountMeta::new_readonly(spl_account_compression::ID, false),
            AccountMeta::new_readonly(mpl_token_metadata::ID, false),
            AccountMeta::new_readonly(system_program::ID, false),
        ];

        if creators
            .iter()
            .any(|creator| creator.verified && creator.address == owner.to_string())
        {
            accounts.push(AccountMeta::new_readonly(owner, true));
        }

        let instructions = [Instruction {
            program_id: mpl_bubblegum::ID,
            accounts: accounts.into_iter().collect(),
            data: mpl_bubblegum::instruction::MintToCollectionV1 {
                metadata_args: mpl_bubblegum::state::metaplex_adapter::MetadataArgs {
                    name,
                    symbol,
                    uri: metadata_uri,
                    seller_fee_basis_points: seller_fee_basis_points.try_into()?,
                    primary_sale_happened: false,
                    is_mutable: true,
                    edition_nonce: None,
                    token_standard: None,
                    collection: Some(Collection {
                        verified: false,
                        key: collection.mint.parse()?,
                    }),
                    uses: None,
                    token_program_version: TokenProgramVersion::Original,
                    creators: creators
                        .into_iter()
                        .map(TryInto::try_into)
                        .collect::<Result<Vec<BubblegumCreator>>>()?,
                },
            }
            .data(),
        }];

        let serialized_message = solana_program::message::Message::new_with_blockhash(
            &instructions,
            Some(&payer),
            &self.0.rpc_client.get_latest_blockhash()?,
        )
        .serialize();

        Ok(TransactionResponse {
            serialized_message,
            signatures_or_signers_public_keys: vec![payer.to_string(), owner.to_string()],
            addresses: MintCompressedMintV1Addresses {
                leaf_owner: recipient,
                tree_delegate: payer,
                tree_authority,
                merkle_tree,
            },
        })
    }
}

impl<'a> MintBackend<MintMetaplexMetadataTransaction, MintMetaplexAddresses>
    for UncompressedRef<'a>
{
    fn mint(
        &self,
        collection: &collections::Model,
        txn: MintMetaplexMetadataTransaction,
    ) -> hub_core::prelude::Result<TransactionResponse<MintMetaplexAddresses>> {
        let MintMetaplexMetadataTransaction {
            recipient_address,
            metadata,
            ..
        } = txn;
        let metadata = metadata.ok_or(SolanaErrorNotFoundMessage::Metadata)?;
        let payer: Pubkey = self.0.treasury_wallet_address;
        let rpc = &self.0.rpc_client;
        let mint = Keypair::new();
        let MetaplexMetadata {
            name,
            symbol,
            seller_fee_basis_points,
            metadata_uri,
            creators,
            owner_address,
        } = metadata;
        let owner: Pubkey = owner_address.parse()?;
        let recipient: Pubkey = recipient_address.parse()?;
        let collection_mint: Pubkey = collection.mint.parse()?;
        let collection_metadata: Pubkey = collection.metadata.parse()?;
        let collection_master_edition_account: Pubkey = collection.master_edition.parse()?;

        let (metadata, _) = Pubkey::find_program_address(
            &[
                b"metadata",
                mpl_token_metadata::ID.as_ref(),
                mint.pubkey().as_ref(),
            ],
            &mpl_token_metadata::ID,
        );
        let associated_token_account = get_associated_token_address(&recipient, &mint.pubkey());
        let len = spl_token::state::Mint::LEN;
        let rent = call_with_retry!(rpc.get_minimum_balance_for_rent_exemption(len))?;
        let blockhash = call_with_retry!(rpc.get_latest_blockhash())?;

        let create_account_ins = solana_program::system_instruction::create_account(
            &payer,
            &mint.pubkey(),
            rent,
            len.try_into()?,
            &spl_token::ID,
        );
        let initialize_mint_ins = spl_token::instruction::initialize_mint(
            &spl_token::ID,
            &mint.pubkey(),
            &owner,
            Some(&owner),
            0,
        )?;
        let ata_ins = spl_associated_token_account::instruction::create_associated_token_account(
            &payer,
            &recipient,
            &mint.pubkey(),
            &spl_token::ID,
        );
        let min_to_ins = spl_token::instruction::mint_to(
            &spl_token::ID,
            &mint.pubkey(),
            &associated_token_account,
            &owner,
            &[],
            1,
        )?;
        let create_metadata_account_ins =
            mpl_token_metadata::instruction::create_metadata_accounts_v3(
                mpl_token_metadata::ID,
                metadata,
                mint.pubkey(),
                owner,
                payer,
                owner,
                name,
                symbol,
                metadata_uri,
                Some(
                    creators
                        .into_iter()
                        .map(TryInto::try_into)
                        .collect::<Result<Vec<Creator>, _>>()?,
                ),
                seller_fee_basis_points.try_into()?,
                true,
                true,
                Some(mpl_token_metadata::state::Collection {
                    verified: false,
                    key: collection_mint,
                }),
                None,
                None,
            );

        let verify_collection = mpl_token_metadata::instruction::verify_sized_collection_item(
            mpl_token_metadata::ID,
            metadata,
            owner,
            payer,
            collection_mint,
            collection_metadata,
            collection_master_edition_account,
            None,
        );

        let instructions = vec![
            create_account_ins,
            initialize_mint_ins,
            ata_ins,
            min_to_ins,
            create_metadata_account_ins,
            verify_collection,
        ];

        let message = solana_program::message::Message::new_with_blockhash(
            &instructions,
            Some(&payer),
            &blockhash,
        );

        let serialized_message = message.serialize();
        let mint_signature = mint.try_sign_message(&message.serialize())?;

        Ok(TransactionResponse {
            serialized_message,
            signatures_or_signers_public_keys: vec![
                payer.to_string(),
                mint_signature.to_string(),
                owner.to_string(),
            ],
            addresses: MintMetaplexAddresses {
                update_authority: owner,
                associated_token_account,
                mint: mint.pubkey(),
                owner,
                metadata,
                recipient,
            },
        })
    }
}
