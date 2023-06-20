mod connector;
mod handler;
use clap::{arg, command};
pub use connector::GeyserGrpcConnector;
pub use handler::MessageHandler;
use holaplex_hub_nfts_solana_core::db::{self};
use hub_core::clap;

#[derive(Debug, clap::Args)]
pub struct Args {
    #[arg(short, long, env)]
    pub endpoint: String,

    #[arg(short, long, env)]
    pub x_token: Option<String>,

    #[arg(short, long, env)]
    pub solana_endpoint: String,

    #[command(flatten)]
    pub db: db::DbArgs,
}
