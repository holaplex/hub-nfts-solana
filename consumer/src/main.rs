use std::sync::Arc;

use holaplex_hub_nfts_solana::{
    events::Processor,
    solana::{Solana, SolanaArgs},
    solana_compressed::SolanaCompressed,
    Args,
};
use holaplex_hub_nfts_solana_core::{db::Connection, proto::SolanaNftEvents, Services};
use hub_core::{prelude::*, tokio};
use solana_client::rpc_client::RpcClient;

pub fn main() {
    let opts = hub_core::StartConfig {
        service_name: "hub-nfts-solana",
    };

    hub_core::run(opts, |common, args| {
        let Args {
            db,
            solana:
                SolanaArgs {
                    solana_endpoint,
                    solana_treasury_wallet_address,
                },
            solana_compressed,
        } = args;

        common.rt.block_on(async move {
            let connection = Connection::new(db)
                .await
                .context("failed to get database connection")?;

            let producer = common
                .producer_cfg
                .build::<SolanaNftEvents>()
                .await?;

            let solana_rpc = Arc::new(RpcClient::new(solana_endpoint));
            let solana = Solana::new(solana_rpc.clone(), solana_treasury_wallet_address.clone());
            let compressed = SolanaCompressed::new(
                solana_rpc,
                solana_treasury_wallet_address
                    .parse()
                    .context("Error parsing treasury wallet address")?,
                solana_compressed,
            );

            let cons = common.consumer_cfg.build::<Services>().await?;
            let event_processor = Processor::new(solana, compressed, connection, producer);

            let mut stream = cons.stream();
            loop {
                let event_processor = event_processor.clone();

                match stream.next().await {
                    Some(Ok(msg)) => {
                        info!(?msg, "message received");

                        tokio::spawn(async move { event_processor.process(msg).await });
                        tokio::task::yield_now().await;
                    },
                    None => (),
                    Some(Err(e)) => {
                        warn!("failed to get message {:?}", e);
                    },
                }
            }
        })
    });
}
