use std::{convert::TryInto, sync::Arc};

use dashmap::DashMap;
use futures::{sink::SinkExt, stream::StreamExt};
use holaplex_hub_nfts_solana_core::{
    db::Connection,
    proto::{
        solana_nft_events::Event::UpdateMintOwner, MintOwnershipUpdate, SolanaNftEventKey,
        SolanaNftEvents,
    },
    sea_orm::{ColumnTrait, EntityTrait, QueryFilter},
};
use holaplex_hub_nfts_solana_entity::{collection_mints, prelude::CollectionMints};
use hub_core::{prelude::*, producer::Producer, tokio::task};
use solana_client::rpc_client::RpcClient;
use solana_program::program_pack::Pack;
use solana_sdk::pubkey::Pubkey;
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
            endpoint,
            x_token,
            solana_endpoint,
            db,
        } = args;

        let db = Connection::new(db)
            .await
            .context("failed to get database connection")?;
        let dashmap: DashMap<u64, Vec<SubscribeUpdateTransaction>> = DashMap::new();
        let rpc = Arc::new(RpcClient::new(solana_endpoint));
        let connector = GeyserGrpcConnector::new(endpoint, x_token);

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
            .context("SubsribeTransactionInfo not found")?
            .transaction
            .context("Transaction not found")?
            .message
            .context("Message not found")?;

        let mut i = 0;
        let keys = message.clone().account_keys;

        for (idx, key) in message.clone().account_keys.iter().enumerate() {
            let k = Pubkey::try_from(key.clone()).map_err(|_| anyhow!("failed to parse pubkey"))?;
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
                if let TokenInstruction::Transfer { amount } = tkn_instruction {
                    if amount == 1 {
                        let source_account_index = account_indices[0];
                        let source_bytes = &keys[source_account_index as usize];
                        let source = Pubkey::try_from(source_bytes.clone())
                            .map_err(|_| anyhow!("failed to parse pubkey"))?;
                        let destination_account_index = account_indices[1];
                        let destination_bytes = &keys[destination_account_index as usize];
                        let destination = Pubkey::try_from(destination_bytes.clone())
                            .map_err(|_| anyhow!("failed to parse pubkey"))?;

                        let collection_mint = CollectionMints::find()
                            .filter(
                                collection_mints::Column::AssociatedTokenAccount
                                    .eq(source.to_string()),
                            )
                            .one(self.db.get())
                            .await?;

                        if let Some(mint) = collection_mint {
                            let acct = &self.rpc.get_account(&destination)?;
                            let destination_tkn_act = Account::unpack(&acct.data)?;

                            self.producer
                                .send(
                                    Some(&SolanaNftEvents {
                                        event: Some(UpdateMintOwner(MintOwnershipUpdate {
                                            mint_address: destination_tkn_act.mint.to_string(),
                                            sender: mint.owner.to_string(),
                                            recipient: destination_tkn_act.owner.to_string(),
                                            source_ata: source.to_string(),
                                            destination_ata: destination.to_string(),
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
            }
        }

        Ok(())
    }
}
