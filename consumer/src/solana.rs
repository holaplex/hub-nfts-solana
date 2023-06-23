use holaplex_hub_nfts_solana_core::proto::{
    treasury_events::SolanaTransactionResult, MasterEdition, MetaplexMasterEditionTransaction,
    MintMetaplexEditionTransaction, TransferMetaplexAssetTransaction,
};
use holaplex_hub_nfts_solana_entity::{collection_mints, collections};
use hub_core::{anyhow::Result, clap, prelude::*, thiserror::Error, uuid::Uuid};
use mpl_token_metadata::{
    instruction::{mint_new_edition_from_master_edition_via_token, update_metadata_accounts_v2},
    state::{Creator, DataV2, EDITION, PREFIX},
};
use solana_client::rpc_client::RpcClient;
use solana_program::{program_pack::Pack, pubkey::Pubkey, system_instruction::create_account};
use solana_sdk::{
    signature::Signature,
    signer::{keypair::Keypair, Signer},
    transaction::Transaction,
};
use spl_associated_token_account::{
    get_associated_token_address, instruction::create_associated_token_account,
};
use spl_token::{
    instruction::{initialize_mint, mint_to},
    state,
};

use crate::backend::{
    Backend, MasterEditionAddresses, MintEditionAddresses, TransactionResponse,
    TransferAssetAddresses, UpdateMasterEditionAddresses,
};

const TOKEN_PROGRAM_PUBKEY: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

#[derive(Debug, clap::Args, Clone)]
pub struct SolanaArgs {
    #[arg(long, env)]
    pub solana_endpoint: String,

    #[arg(long, env)]
    pub solana_treasury_wallet_address: String,
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

#[derive(Debug, Error)]
enum SolanaError {
    #[error("master edition message not found")]
    MasterEditionMessageNotFound,
    #[error("serialized message message not found")]
    SerializedMessageNotFound,
}

#[derive(Clone)]
pub struct Solana {
    rpc_client: Arc<RpcClient>,
    treasury_wallet_address: String,
}

impl Solana {
    pub fn new(rpc_client: Arc<RpcClient>, treasury_wallet_address: String) -> Self {
        Self {
            rpc_client,
            treasury_wallet_address,
        }
    }

    #[must_use]
    pub fn rpc(&self) -> Arc<RpcClient> {
        self.rpc_client.clone()
    }

    /// Res
    ///
    /// # Errors
    /// This function fails if unable to submit transaction to Solana
    pub fn submit_transaction(&self, transaction: &SolanaTransactionResult) -> Result<String> {
        let signatures = transaction
            .signed_message_signatures
            .iter()
            .map(|s| {
                Signature::from_str(s)
                    .map_err(|e| anyhow!(format!("failed to parse signature: {e}")))
            })
            .collect::<Result<Vec<Signature>>>()?;

        let message = bincode::deserialize(
            &transaction
                .serialized_message
                .clone()
                .ok_or(SolanaError::SerializedMessageNotFound)?,
        )?;

        let transaction = Transaction {
            signatures,
            message,
        };

        let signature = self.rpc().send_and_confirm_transaction(&transaction)?;

        Ok(signature.to_string())
    }

    #[allow(clippy::too_many_lines)]
    fn master_edition_transaction(
        &self,
        payload: MasterEdition,
    ) -> Result<TransactionResponse<MasterEditionAddresses>> {
        let payer: Pubkey = self.treasury_wallet_address.parse()?;
        let rpc = &self.rpc_client;
        let mint = Keypair::new();
        let MasterEdition {
            name,
            symbol,
            seller_fee_basis_points,
            metadata_uri,
            creators,
            supply,
            owner_address,
        } = payload;
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
        let rent = rpc.get_minimum_balance_for_rent_exemption(len)?;
        let blockhash = rpc.get_latest_blockhash()?;

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
                None,
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
}

impl Backend for Solana {
    fn create(
        &self,
        payload: MetaplexMasterEditionTransaction,
    ) -> Result<TransactionResponse<MasterEditionAddresses>> {
        let MetaplexMasterEditionTransaction { master_edition, .. } = payload;
        let master_edition = master_edition.ok_or(SolanaError::MasterEditionMessageNotFound)?;

        let tx = self.master_edition_transaction(master_edition)?;

        Ok(tx)
    }

    fn mint(
        &self,
        collection: &collections::Model,
        payload: MintMetaplexEditionTransaction,
    ) -> Result<TransactionResponse<MintEditionAddresses>> {
        let rpc = &self.rpc_client;
        let MintMetaplexEditionTransaction {
            recipient_address,
            owner_address,
            edition,
            ..
        } = payload;

        let payer: Pubkey = self.treasury_wallet_address.parse()?;
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

        let mut instructions = vec![
            create_account(
                &payer,
                &new_mint_key.pubkey(),
                rpc.get_minimum_balance_for_rent_exemption(state::Mint::LEN)?,
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

        let blockhash = rpc.get_latest_blockhash()?;

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

    fn update(
        &self,
        collection: &collections::Model,
        payload: MetaplexMasterEditionTransaction,
    ) -> Result<TransactionResponse<UpdateMasterEditionAddresses>> {
        let rpc = &self.rpc_client;

        let MetaplexMasterEditionTransaction { master_edition, .. } = payload;

        let master_edition = master_edition.ok_or(SolanaError::MasterEditionMessageNotFound)?;

        let MasterEdition {
            name,
            seller_fee_basis_points,
            symbol,
            creators,
            metadata_uri,
            ..
        } = master_edition;

        let payer: Pubkey = self.treasury_wallet_address.parse()?;

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

        let blockhash = rpc.get_latest_blockhash()?;

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

    fn transfer(
        &self,
        collection_mint: &collection_mints::Model,
        payload: TransferMetaplexAssetTransaction,
    ) -> Result<TransactionResponse<TransferAssetAddresses>> {
        let rpc = &self.rpc_client;
        let TransferMetaplexAssetTransaction {
            owner_address,
            recipient_address,
            ..
        } = payload;

        let sender: Pubkey = owner_address.parse()?;
        let recipient: Pubkey = recipient_address.parse()?;
        let mint_address: Pubkey = collection_mint.mint.parse()?;
        let payer: Pubkey = self.treasury_wallet_address.parse()?;
        let blockhash = rpc.get_latest_blockhash()?;
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
