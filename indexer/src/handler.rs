use std::sync::Arc;

use futures::{sink::SinkExt, stream::StreamExt};
use holaplex_hub_nfts_solana_core::{db::Connection, proto::SolanaNftEvents};
use hub_core::{
    backon::{ExponentialBuilder, Retryable},
    prelude::*,
    producer::Producer,
    tokio::{
        self,
        sync::{
            mpsc::{self, UnboundedReceiver, UnboundedSender},
            Mutex,
        },
        task::{self, JoinSet},
    },
};
use solana_client::rpc_client::RpcClient;
use yellowstone_grpc_client::GeyserGrpcClientError;
use yellowstone_grpc_proto::prelude::{
    subscribe_update::UpdateOneof, SubscribeRequest, SubscribeUpdateTransaction,
};

use crate::{processor::Processor, Args, GeyserGrpcConnector};

#[derive(Clone)]
pub struct MessageHandler {
    connector: GeyserGrpcConnector,
    processor: Processor,
    tx: UnboundedSender<SubscribeUpdateTransaction>,
    rx: Arc<Mutex<UnboundedReceiver<SubscribeUpdateTransaction>>>,
    parallelism: usize,
}

impl MessageHandler {
    pub async fn new(args: Args, producer: Producer<SolanaNftEvents>) -> Result<Self> {
        let Args {
            dragon_mouth_endpoint,
            dragon_mouth_x_token,
            solana_endpoint,
            parallelism,
            db,
        } = args;

        let db = Connection::new(db)
            .await
            .context("failed to get database connection")?;

        let rpc = Arc::new(RpcClient::new(solana_endpoint));
        let connector = GeyserGrpcConnector::new(dragon_mouth_endpoint, dragon_mouth_x_token);
        let (tx, rx) = mpsc::unbounded_channel();
        let processor = Processor::new(db, rpc, producer);

        Ok(Self {
            connector,
            processor,
            tx,
            rx: Arc::new(Mutex::new(rx)),
            parallelism,
        })
    }

    async fn connect(&self, request: SubscribeRequest) -> Result<()> {
        (|| async {
            let (mut subscribe_tx, mut stream) = self.connector.subscribe().await?;
            let mut hashmap = std::collections::HashMap::new();
            subscribe_tx
                .send(request.clone())
                .await
                .map_err(GeyserGrpcClientError::SubscribeSendError)?;

            while let Some(message) = stream.next().await {
                match message {
                    Ok(msg) => match msg.update_oneof {
                        Some(UpdateOneof::Transaction(tx)) => {
                            hashmap.entry(tx.slot).or_insert(Vec::new()).push(tx);
                        },
                        Some(UpdateOneof::Slot(slot)) => {
                            if let Some(transactions) = hashmap.remove(&slot.slot) {
                                for tx in transactions {
                                    self.tx.send(tx)?;
                                }
                            }
                        },
                        _ => {},
                    },
                    Err(error) => bail!("stream error: {:?}", error),
                };
            }
            Ok(())
        })
        .retry(
            &ExponentialBuilder::default()
                .with_max_times(10)
                .with_jitter(),
        )
        .notify(|err: &Error, dur: Duration| {
            error!("stream error: {:?} retrying in {:?}", err, dur);
        })
        .await
    }

    pub async fn run(self) -> Result<()> {
        let spl_token_stream = tokio::spawn({
            let handler = self.clone();
            async move {
                handler
                    .connect(GeyserGrpcConnector::build_request(spl_token::ID))
                    .await
            }
        });

        let mpl_bubblegum_stream = tokio::spawn({
            let handler = self.clone();
            async move {
                handler
                    .connect(GeyserGrpcConnector::build_request(mpl_bubblegum::ID))
                    .await
            }
        });

        let processor = self.processor;

        let process_task = task::spawn(async move {
            let mut set = JoinSet::new();

            loop {
                let processor = processor.clone();
                let mut rx = self.rx.lock().await;

                while set.len() >= self.parallelism {
                    match set.join_next().await {
                        Some(Err(e)) => {
                            return Result::<(), Error>::Err(anyhow!(
                                "failed to join task {:?}",
                                e
                            ));
                        },
                        Some(Ok(_)) | None => (),
                    }
                }

                if let Some(tx) = rx.recv().await {
                    set.spawn(processor.process_transaction(tx));
                }
            }
        });

        tokio::select! {
            Err(e) = spl_token_stream => {
                bail!("spl token stream error: {:?}", e)
            },
            Err(e) = mpl_bubblegum_stream => {
                bail!("mpl bumblegum stream error: {:?}", e)
            }
            Err(e) = process_task => {
                bail!("Receiver err: {:?}", e)
            }
        }
    }
}
