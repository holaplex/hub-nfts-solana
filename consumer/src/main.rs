use holaplex_hub_nfts_solana::{events, import, solana::Solana, Args};
use holaplex_hub_nfts_solana_core::{db::Connection, proto::SolanaNftEvents, Services};
use hub_core::{prelude::*, tokio};

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

            let producer = common.producer_cfg.build::<SolanaNftEvents>().await?;

            let solana = Solana::new(solana)?;

            let cons = common.consumer_cfg.build::<Services>().await?;
            // TODO: change these names once there are fewer in-flight feature branches
            let import_processor =
                import::Processor::new(solana.clone(), connection.clone(), producer.clone());
            let event_processor = events::Processor::new(solana, connection, producer);

            let mut stream = cons.stream();
            loop {
                let import_processor = import_processor.clone();
                let event_processor = event_processor.clone();

                match stream.next().await {
                    Some(Ok(msg)) => {
                        info!(?msg, "message received");

                        tokio::spawn(async move {
                            if let Some(()) = import_processor
                                .process(&msg)
                                .await
                                .map_err(|e| error!("Error processing import: {e:?}"))?
                            {
                                return Ok(());
                            }

                            event_processor
                                .process(msg)
                                .await
                                .map_err(|e| error!("Error processing event: {:?}", Error::new(e)))
                        });
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
