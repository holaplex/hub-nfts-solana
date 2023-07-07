use std::{convert::TryInto, sync::Arc};

use backoff::ExponentialBackoff;
use dashmap::DashMap;
use futures::{sink::SinkExt, stream::StreamExt};
use holaplex_hub_nfts_solana_core::{
    db::Connection,
    proto::{
        solana_nft_events::Event::UpdateMintOwner, MintOwnershipUpdate, SolanaNftEventKey,
        SolanaNftEvents,
    },
    CollectionMint,
};
use hub_core::{prelude::*, producer::Producer, tokio::task};
use solana_client::rpc_client::RpcClient;
use solana_program::program_pack::Pack;
use solana_sdk::{pubkey::Pubkey, signature::Signature};
use spl_token::{instruction::TokenInstruction, state::Account};
use yellowstone_grpc_client::GeyserGrpcClientError;
use yellowstone_grpc_proto::{
    prelude::{subscribe_update::UpdateOneof, SubscribeUpdate, SubscribeUpdateTransaction},
    tonic::Status,
};

use crate::{Args, GeyserGrpcConnector};

#[derive(Clone)]
pub struct MessageHandler {
    db: Connection,
    dashmap: DashMap<u64, Vec<SubscribeUpdateTransaction>>,
    rpc: Arc<RpcClient>,
    connector: GeyserGrpcConnector,
    producer: Producer<SolanaNftEvents>,
}

impl MessageHandler {
    pub async fn new(args: Args, producer: Producer<SolanaNftEvents>) -> Result<Self> {
        let Args {
            dragon_mouth_endpoint,
            dragon_mouth_x_token,
            solana_endpoint,
            db,
        } = args;

        let db = Connection::new(db)
            .await
            .context("failed to get database connection")?;
        let dashmap: DashMap<u64, Vec<SubscribeUpdateTransaction>> = DashMap::new();
        let rpc = Arc::new(RpcClient::new(solana_endpoint));
        let connector = GeyserGrpcConnector::new(dragon_mouth_endpoint, dragon_mouth_x_token);

        Ok(Self {
            db,
            dashmap,
            rpc,
            connector,
            producer,
        })
    }

    pub async fn run(&self) -> Result<()> {
        let request = GeyserGrpcConnector::build_request();
        let (mut subscribe_tx, mut stream) = self.connector.subscribe().await?;

        loop {
            let request = request.clone();

            subscribe_tx
                .send(request)
                .await
                .map_err(GeyserGrpcClientError::SubscribeSendError)?;

            while let Some(message) = stream.next().await {
                self.handle_message(message).await?;
            }
        }
    }

    async fn handle_message(&self, message: Result<SubscribeUpdate, Status>) -> Result<()> {
        match message {
            Ok(msg) => match msg.update_oneof {
                Some(UpdateOneof::Transaction(tx)) => {
                    self.dashmap.entry(tx.slot).or_insert(Vec::new()).push(tx);
                },
                Some(UpdateOneof::Slot(slot)) => {
                    if let Some((_, transactions)) = self.dashmap.remove(&slot.slot) {
                        for tx in transactions {
                            task::spawn(self.clone().process_transaction(tx));
                        }
                    }
                },
                _ => {},
            },
            Err(error) => return Err(anyhow!("stream error: {:?}", error)),
        };

        Ok(())
    }

    async fn process_transaction(self, tx: SubscribeUpdateTransaction) -> Result<()> {
        let message = tx
            .transaction
            .clone()
            .context("SubscribeTransactionInfo not found")?
            .transaction
            .context("Transaction not found")?
            .message
            .context("Message not found")?;

        let mut i = 0;
        let keys = message.clone().account_keys;

        for (idx, key) in message.clone().account_keys.iter().enumerate() {
            let k = Pubkey::new(&key);
            if k == spl_token::ID {
                i = idx;
                break;
            }
        }

        for ins in message.instructions.iter() {
            let account_indices = ins.accounts.clone();
            let program_idx: usize = ins.program_id_index.try_into()?;

            if program_idx == i {
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
                    let sig = tx
                        .transaction
                        .as_ref()
                        .ok_or_else(|| anyhow!("failed to get transaction"))?
                        .signature
                        .clone();

                    let source_account_index = account_indices[0];
                    let source_bytes = &keys[source_account_index as usize];
                    let source = Pubkey::new(&source_bytes);

                    let collection_mint =
                        CollectionMint::find_by_ata(&self.db, source.to_string()).await?;

                    if collection_mint.is_none() {
                        return Ok(());
                    }

                    let destination_account_index = account_indices[destination_ata_index];
                    let destination_bytes = &keys[destination_account_index as usize];
                    let destination = Pubkey::new(&destination_bytes);

                    let acct = fetch_account(&self.rpc, &destination).await?;
                    let destination_tkn_act = Account::unpack(&acct.data)?;
                    let new_owner = destination_tkn_act.owner.to_string();
                    let mint = collection_mint.context("No mint found")?;

                    CollectionMint::update_owner_and_ata(
                        &self.db,
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
