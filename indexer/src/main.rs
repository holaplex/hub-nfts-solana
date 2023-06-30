use holaplex_hub_nfts_solana_core::proto::SolanaNftEvents;
use solana_indexer::MessageHandler;

pub fn main() {
    let opts = hub_core::StartConfig {
        service_name: "hub-nfts-solana-indexer",
    };

    hub_core::run(opts, |common, args| {
        common.rt.block_on(async move {
            let producer = common.producer_cfg.build::<SolanaNftEvents>().await?;

            let handler = MessageHandler::new(args, producer).await?;
            handler.run().await
        })
    });
}
