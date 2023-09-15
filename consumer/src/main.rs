use holaplex_hub_nfts_solana::{
    events,
    handlers::{health, metrics_handler},
    import,
    metrics::Metrics,
    solana::Solana,
    Args,
};
use holaplex_hub_nfts_solana_core::{db::Connection, proto::SolanaNftEvents, Services};
use hub_core::{prelude::*, tokio, triage};
use poem::{get, listener::TcpListener, middleware::AddData, EndpointExt, Route, Server};

pub fn main() {
    let opts = hub_core::StartConfig {
        service_name: "hub-nfts-solana",
    };

    hub_core::run(opts, |common, args| {
        let Args { db, solana, port } = args;

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

            let metrics = Metrics::new()?;
            let event_processor =
                events::Processor::new(solana, connection, producer, metrics.clone());

            tokio::spawn(async move {
                cons.consume::<_, _, _, triage::BoxedSync>(
                    |b| {
                        b.with_jitter()
                            .with_min_delay(Duration::from_millis(500))
                            .with_max_delay(Duration::from_secs(90))
                    },
                    move |e| async move {
                        if let Some(()) = import_processor
                            .process(&e)
                            .await
                            .map_err(|e| Box::new(e) as triage::BoxedSync)?
                        {
                            return Ok(());
                        }

                        event_processor
                            .process(e)
                            .await
                            .map_err(|e| Box::new(e) as triage::BoxedSync)
                    },
                )
                .await;
            });
            Server::new(TcpListener::bind(format!("0.0.0.0:{port}")))
                .run(
                    Route::new()
                        .at("/health", get(health))
                        .at("/metrics", get(metrics_handler).with(AddData::new(metrics))),
                )
                .await
                .context("failed to build graphql server")
        })
    });
}
