mod connector;
mod handler;
use clap::{arg, command};
pub use connector::GeyserGrpcConnector;
pub use handler::MessageHandler;
use holaplex_hub_nfts_solana_core::db::{self};
use hub_core::clap;

#[derive(Debug, clap::Args)]
pub struct Args {
    #[arg(long, env)]
    pub dragon_mouth_endpoint: String,

    #[arg(long, env)]
    pub dragon_mouth_x_token: Option<String>,

    #[arg(long, env)]
    pub solana_endpoint: String,

    #[command(flatten)]
    pub db: db::DbArgs,
}
