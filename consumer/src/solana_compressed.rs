use anchor_lang::InstructionData;
use holaplex_hub_nfts_solana_core::proto::{
    MasterEdition, MetaplexMasterEditionTransaction, MintMetaplexEditionTransaction,
    TransferMetaplexAssetTransaction,
};
use holaplex_hub_nfts_solana_entity::{collection_mints, collections};
use hub_core::{anyhow::Result, clap, prelude::*};
use mpl_token_metadata::state::Creator;
use solana_client::rpc_client::RpcClient;
use solana_program::{
    instruction::{AccountMeta, Instruction},
    program_pack::Pack,
    pubkey::Pubkey,
    system_program,
};
use solana_sdk::{signature::Keypair, signer::Signer};
use spl_associated_token_account::get_associated_token_address;

use crate::backend::{
    Backend, MasterEditionAddresses, MintEditionAddresses, TransactionResponse,
    TransferAssetAddresses, UpdateMasterEditionAddresses,
};

#[derive(Debug, clap::Args)]
pub struct SolanaCompressedArgs {
    #[arg(long, env)]
    pub tree_authority: Pubkey,
    #[arg(long, env)]
    pub merkle_tree: Pubkey,
}

#[derive(Clone)]
pub struct SolanaCompressed {
    rpc: Arc<RpcClient>,
    treasury: Pubkey,
    tree_authority: Pubkey,
    merkle_tree: Pubkey,
}

impl Backend for SolanaCompressed {
    fn create(
        &self,
        evt: MetaplexMasterEditionTransaction,
    ) -> Result<TransactionResponse<MasterEditionAddresses>> {
        let MetaplexMasterEditionTransaction { master_edition, .. } = evt;
        let MasterEdition {
            name,
            symbol,
            metadata_uri,
            creators,
            seller_fee_basis_points,
            supply, // TODO: ?
            owner_address,
        } = master_edition.context("Missing master edition message")?;
        let payer = self.treasury;
        let owner = owner_address.parse()?;
        let mint = Keypair::new();

        let (metadata, _) = Pubkey::find_program_address(
            &[
                b"metadata",
                mpl_token_metadata::ID.as_ref(),
                mint.pubkey().as_ref(),
            ],
            &mpl_token_metadata::ID,
        );
        let associated_token_account = get_associated_token_address(&owner, &mint.pubkey());

        let mint_len = spl_token::state::Mint::LEN;

        // TODO: this is the collection NFT, right?
        let instructions = [
            solana_program::system_instruction::create_account(
                &payer,
                &mint.pubkey(),
                self.rpc.get_minimum_balance_for_rent_exemption(mint_len)?,
                mint_len.try_into()?,
                &spl_token::ID,
            ),
            spl_token::instruction::initialize_mint(
                &spl_token::ID,
                &mint.pubkey(),
                &owner,
                Some(&owner),
                0,
            )?,
            spl_associated_token_account::instruction::create_associated_token_account(
                &payer,
                &owner,
                &mint.pubkey(),
                &spl_token::ID,
            ),
            spl_token::instruction::mint_to(
                &spl_token::ID,
                &mint.pubkey(),
                &associated_token_account,
                &owner,
                &[],
                1,
            )?,
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
            ),
        ];

        let serialized_message = solana_program::message::Message::new_with_blockhash(
            &instructions,
            Some(&payer),
            &self.rpc.get_latest_blockhash()?,
        )
        .serialize();
        let mint_signature = mint.try_sign_message(&serialized_message)?;

        Ok(TransactionResponse {
            serialized_message,
            signatures_or_signers_public_keys: vec![
                payer.to_string(),
                mint_signature.to_string(),
                owner.to_string(),
            ],
            addresses: MasterEditionAddresses {
                metadata,
                associated_token_account,
                owner,
                master_edition: todo!("what"),
                mint: mint.pubkey(),
                update_authority: owner,
            },
        })
    }

    fn mint(
        &self,
        collection: &collections::Model,
        evt: MintMetaplexEditionTransaction,
    ) -> Result<TransactionResponse<MintEditionAddresses>> {
        let MintMetaplexEditionTransaction {
            recipient_address,
            owner_address,
            edition,
            ..
        } = evt;
        let payer = self.treasury;
        let recipient = recipient_address.parse()?;
        let owner = owner_address.parse()?;

        let instructions = [Instruction {
            program_id: mpl_bubblegum::ID,
            accounts: [
                AccountMeta::new(self.tree_authority, false),
                AccountMeta::new_readonly(recipient, false),
                AccountMeta::new_readonly(recipient, false),
                AccountMeta::new(self.merkle_tree, false),
                AccountMeta::new_readonly(payer, true),
                AccountMeta::new_readonly(owner, true), // TODO: who will own the trees??
                AccountMeta::new_readonly(spl_noop::ID, false),
                AccountMeta::new_readonly(spl_account_compression::ID, false),
                AccountMeta::new_readonly(system_program::ID, false),
            ]
            .into_iter()
            .collect(),
            data: mpl_bubblegum::instruction::MintV1 {
                message: mpl_bubblegum::state::metaplex_adapter::MetadataArgs {
                    name: todo!(),
                    symbol: todo!(),
                    uri: todo!(),
                    seller_fee_basis_points: todo!(),
                    primary_sale_happened: todo!(),
                    is_mutable: todo!(),
                    edition_nonce: todo!(),
                    token_standard: todo!(),
                    collection: todo!(),
                    uses: todo!(),
                    token_program_version: todo!(),
                    creators: todo!(),
                },
            }
            .data(),
        }];

        let serialized_message = solana_program::message::Message::new_with_blockhash(
            &instructions,
            Some(&payer),
            &self.rpc.get_latest_blockhash()?,
        )
        .serialize();

        Ok(TransactionResponse {
            serialized_message,
            signatures_or_signers_public_keys: vec![payer.to_string(), owner.to_string()],
            addresses: MintEditionAddresses {
                edition: todo!("what"),
                mint: todo!("what"),
                metadata: todo!("what"),
                owner,
                associated_token_account: todo!("what"),
                recipient,
            },
        })
    }

    fn update(
        &self,
        collection: &collections::Model,
        evt: MetaplexMasterEditionTransaction,
    ) -> Result<TransactionResponse<UpdateMasterEditionAddresses>> {
        bail!("Cannot update a compressed NFT")
    }

    fn transfer(
        &self,
        mint: &collection_mints::Model,
        evt: TransferMetaplexAssetTransaction,
    ) -> Result<TransactionResponse<TransferAssetAddresses>> {
        let TransferMetaplexAssetTransaction {
            recipient_address,
            owner_address,
            collection_mint_id,
            ..
        } = evt;
        let payer = self.treasury;
        let recipient = recipient_address.parse()?;
        let owner = owner_address.parse()?;

        let instructions = [Instruction {
            program_id: mpl_bubblegum::ID,
            accounts: [
                AccountMeta::new(self.tree_authority, false),
                AccountMeta::new_readonly(owner, true),
                AccountMeta::new_readonly(owner, false),
                AccountMeta::new_readonly(recipient, false),
                AccountMeta::new(self.merkle_tree, false),
                AccountMeta::new_readonly(spl_noop::ID, false),
                AccountMeta::new_readonly(spl_account_compression::ID, false),
                AccountMeta::new_readonly(system_program::ID, false),
            ]
            .into_iter()
            .collect(),
            data: mpl_bubblegum::instruction::Transfer {
                root: todo!("set up DAA"),
                data_hash: todo!("set up DAA"),
                creator_hash: todo!("set up DAA"),
                nonce: todo!("set up DAA"),
                index: todo!("set up DAA"),
            }
            .data(),
        }];

        let serialized_message = solana_program::message::Message::new_with_blockhash(
            &instructions,
            Some(&payer),
            &self.rpc.get_latest_blockhash()?,
        )
        .serialize();

        Ok(TransactionResponse {
            serialized_message,
            signatures_or_signers_public_keys: vec![payer.to_string(), owner.to_string()],
            addresses: TransferAssetAddresses {
                owner,
                recipient,
                recipient_associated_token_account: todo!("what"),
                owner_associated_token_account: todo!("what"),
            },
        })
    }
}

impl SolanaCompressed {
    #[must_use]
    pub fn new(rpc: Arc<RpcClient>, treasury: Pubkey, args: SolanaCompressedArgs) -> Self {
        let SolanaCompressedArgs {
            tree_authority,
            merkle_tree,
        } = args;
        Self {
            rpc,
            treasury,
            tree_authority,
            merkle_tree,
        }
    }
}
