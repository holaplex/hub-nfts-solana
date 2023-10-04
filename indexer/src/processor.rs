use std::{convert::TryInto, sync::Arc};

use anchor_lang::AnchorDeserialize;
use backoff::ExponentialBackoff;
use holaplex_hub_nfts_solana_core::{
    db::Connection,
    proto::{
        solana_nft_events::Event::UpdateMintOwner, MintOwnershipUpdate, SolanaNftEventKey,
        SolanaNftEvents,
    },
    sea_orm::Set,
    CollectionMint, CompressionLeaf,
};
use holaplex_hub_nfts_solana_entity::compression_leafs;
use hub_core::{prelude::*, producer::Producer};
use mpl_bubblegum::utils::get_asset_id;
use solana_client::rpc_client::RpcClient;
use solana_program::program_pack::Pack;
use solana_sdk::{pubkey::Pubkey, signature::Signature};
use spl_token::{instruction::TokenInstruction, state::Account};
use yellowstone_grpc_proto::prelude::{Message, SubscribeUpdateTransaction};

#[derive(Clone)]
pub struct Processor {
    db: Connection,
    rpc: Arc<RpcClient>,
    producer: Producer<SolanaNftEvents>,
}

impl Processor {
    pub(crate) fn new(
        db: Connection,
        rpc: Arc<RpcClient>,
        producer: Producer<SolanaNftEvents>,
    ) -> Self {
        Self { db, rpc, producer }
    }

    pub(crate) async fn process_transaction(self, tx: SubscribeUpdateTransaction) -> Result<()> {
        let message = tx
            .transaction
            .clone()
            .context("SubscribeTransactionInfo not found")?
            .transaction
            .context("Transaction not found")?
            .message
            .context("Message not found")?;
        let sig = tx
            .transaction
            .as_ref()
            .ok_or_else(|| anyhow!("failed to get transaction"))?
            .signature
            .clone();

        let keys = message.clone().account_keys;

        for (idx, key) in message.clone().account_keys.iter().enumerate() {
            let key: &[u8] = key;
            let k = Pubkey::try_from(key)?;
            if k == spl_token::ID {
                self.process_spl_token_transaction(idx, &keys, &sig, &message)
                    .await?;
            } else if k == mpl_bubblegum::ID {
                self.process_mpl_bubblegum_transaction(idx, &keys, &sig, &message)
                    .await?;
            }
        }

        Ok(())
    }

    pub(crate) async fn process_mpl_bubblegum_transaction(
        &self,
        program_account_index: usize,
        keys: &[Vec<u8>],
        sig: &Vec<u8>,
        message: &Message,
    ) -> Result<()> {
        for ins in message.instructions.iter() {
            let account_indices = ins.accounts.clone();
            let program_idx: usize = ins.program_id_index.try_into()?;

            if program_idx == program_account_index {
                let conn = self.db.get();
                let data = ins.data.clone();
                let data = data.as_slice();

                let tkn_instruction =
                    mpl_bubblegum::instruction::Transfer::try_from_slice(&data[8..])?;
                let new_leaf_owner_account_index = account_indices[3];
                let merkle_tree_account_index = account_indices[4];
                let new_leaf_owner_bytes: &[u8] = &keys[new_leaf_owner_account_index as usize];
                let merkle_tree_bytes: &[u8] = &keys[merkle_tree_account_index as usize];
                let new_leaf_owner = Pubkey::try_from(new_leaf_owner_bytes)?;
                let merkle_tree = Pubkey::try_from(merkle_tree_bytes)?;

                let asset_id = get_asset_id(&merkle_tree, tkn_instruction.nonce);

                let compression_leaf =
                    CompressionLeaf::find_by_asset_id(conn, asset_id.to_string()).await?;

                if compression_leaf.is_none() {
                    return Ok(());
                }

                let compression_leaf = compression_leaf.context("Compression leaf not found")?;

                let collection_mint_id = compression_leaf.id;
                let leaf_owner = compression_leaf.leaf_owner.clone();
                let mut compression_leaf: compression_leafs::ActiveModel = compression_leaf.into();

                compression_leaf.leaf_owner = Set(new_leaf_owner.to_string());

                CompressionLeaf::update(conn, compression_leaf).await?;

                self.producer
                    .send(
                        Some(&SolanaNftEvents {
                            event: Some(UpdateMintOwner(MintOwnershipUpdate {
                                mint_address: asset_id.to_string(),
                                sender: leaf_owner,
                                recipient: new_leaf_owner.to_string(),
                                tx_signature: Signature::new(sig.as_slice()).to_string(),
                            })),
                        }),
                        Some(&SolanaNftEventKey {
                            id: collection_mint_id.to_string(),
                            ..Default::default()
                        }),
                    )
                    .await?;
            }
        }

        Ok(())
    }

    pub(crate) async fn process_spl_token_transaction(
        &self,
        program_account_index: usize,
        keys: &[Vec<u8>],
        sig: &Vec<u8>,
        message: &Message,
    ) -> Result<()> {
        let conn = self.db.get();
        for ins in message.instructions.iter() {
            let account_indices = ins.accounts.clone();
            let program_idx: usize = ins.program_id_index.try_into()?;

            if program_idx == program_account_index {
                let data = ins.data.clone();
                let data = data.as_slice();
                let tkn_instruction = spl_token::instruction::TokenInstruction::unpack(data)?;

                let transfer_info = match tkn_instruction {
                    TokenInstruction::TransferChecked { amount, .. } => Some((amount, 2)),
                    TokenInstruction::Transfer { amount } => Some((amount, 1)),
                    _ => None,
                };

                if transfer_info.is_none() {
                    continue;
                }

                if let Some((1, destination_ata_index)) = transfer_info {
                    let source_account_index = account_indices[0];
                    let source_bytes: &[u8] = &keys[source_account_index as usize];
                    let source = Pubkey::try_from(source_bytes)?;

                    let collection_mint =
                        CollectionMint::find_by_ata(conn, source.to_string()).await?;

                    if collection_mint.is_none() {
                        return Ok(());
                    }

                    let destination_account_index = account_indices[destination_ata_index];
                    let destination_bytes: &[u8] = &keys[destination_account_index as usize];
                    let destination = Pubkey::try_from(destination_bytes)?;

                    let acct = fetch_account(&self.rpc, &destination).await?;
                    let destination_tkn_act = Account::unpack(&acct.data)?;
                    let new_owner = destination_tkn_act.owner.to_string();
                    let mint = collection_mint.context("No mint found")?;

                    CollectionMint::update_owner_and_ata(
                        conn,
                        &mint,
                        new_owner.clone(),
                        destination.to_string(),
                    )
                    .await?;

                    self.producer
                        .send(
                            Some(&SolanaNftEvents {
                                event: Some(UpdateMintOwner(MintOwnershipUpdate {
                                    mint_address: destination_tkn_act.mint.to_string(),
                                    sender: mint.owner.to_string(),
                                    recipient: new_owner,
                                    tx_signature: Signature::new(sig.as_slice()).to_string(),
                                })),
                            }),
                            Some(&SolanaNftEventKey {
                                id: mint.id.to_string(),
                                ..Default::default()
                            }),
                        )
                        .await?;
                }
            }
        }

        Ok(())
    }
}

async fn fetch_account(
    rpc: &Arc<RpcClient>,
    address: &Pubkey,
) -> Result<solana_sdk::account::Account, solana_client::client_error::ClientError> {
    backoff::future::retry(ExponentialBackoff::default(), || async {
        let acct = rpc.get_account(address)?;

        Ok(acct)
    })
    .await
}
