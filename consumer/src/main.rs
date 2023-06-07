use std::{fs::File, sync::Arc};

use holaplex_hub_nfts_solana::{events::Processor, proto, solana::Solana, Args, Services};
use holaplex_hub_nfts_solana_core::db::Connection;
use hub_core::{prelude::*, tokio};
use solana_client::rpc_client::RpcClient;

pub fn main() {
    let opts = hub_core::StartConfig {
        service_name: "hub-nfts-solana",
    };

    hub_core::run(opts, |common, args| {
        let Args { db, solana } = args;

        common.rt.block_on(async move {
            let connection = Connection::new(db)
                .await
                .context("failed to get database connection")?;

            let producer = common
                .producer_cfg
                .build::<proto::SolanaNftEvents>()
                .await?;

            let solana_rpc = Arc::new(RpcClient::new(solana.solana_endpoint));
            let f = File::open(solana.solana_keypair_path).expect("unable to locate keypair file");
            let solana_keypair: Vec<u8> =
                serde_json::from_reader(f).expect("unable to read keypair bytes from the file");

            let solana = Solana::new(solana_rpc, solana_keypair);

            let cons = common.consumer_cfg.build::<Services>().await?;
            let event_processor = Processor::new(solana, connection, producer);

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
