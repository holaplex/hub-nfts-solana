use std::sync::Arc;

use futures::{sink::SinkExt, stream::StreamExt};
use holaplex_hub_nfts_solana_core::{db::Connection, proto::SolanaNftEvents};
use hub_core::{
    backon::{ExponentialBuilder, Retryable},
    prelude::*,
    producer::Producer,
    tokio,
};
use solana_client::rpc_client::RpcClient;
use yellowstone_grpc_client::GeyserGrpcClientError;
use yellowstone_grpc_proto::prelude::SubscribeRequest;

use crate::{processor::Processor, Args, GeyserGrpcConnector};

#[derive(Clone)]
pub struct MessageHandler {
    connector: GeyserGrpcConnector,
    processor: Processor,
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

        let rpc = Arc::new(RpcClient::new(solana_endpoint));
        let connector = GeyserGrpcConnector::new(dragon_mouth_endpoint, dragon_mouth_x_token);

        let processor = Processor::new(db, rpc, producer);

        Ok(Self {
            connector,
            processor,
        })
    }

    async fn connect(&self, request: SubscribeRequest) -> Result<()> {
        (|| async {
            let (mut subscribe_tx, mut stream) = self.connector.subscribe().await?;

            subscribe_tx
                .send(request.clone())
                .await
                .map_err(GeyserGrpcClientError::SubscribeSendError)?;

            while let Some(message) = stream.next().await {
                self.processor.process(message).await?;
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
            async move {
                self.connect(GeyserGrpcConnector::build_request(mpl_bubblegum::ID))
                    .await
            }
        });

        tokio::select! {
            Err(e) = spl_token_stream => {
                bail!("spl token stream error: {:?}", e)
            },
            Err(e) = mpl_bubblegum_stream => {
                bail!("mpl bumblegum stream error: {:?}", e)
            }
        }
    }
}
