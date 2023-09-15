#![deny(clippy::disallowed_methods, clippy::suspicious, clippy::style)]
#![warn(clippy::pedantic, clippy::cargo)]
#![allow(clippy::module_name_repetitions)]

pub(crate) mod asset_api;
mod backend;
pub mod events;
pub mod handlers;
pub mod import;
pub mod metrics;
pub mod solana;
use holaplex_hub_nfts_solana_core::db::DbArgs;
use hub_core::{clap, prelude::*};
use metrics::Metrics;
use solana::SolanaArgs;

#[derive(Debug, clap::Args)]
#[command(version, author, about)]
pub struct Args {
    #[arg(short, long, env, default_value_t = 3004)]
    pub port: u16,

    #[command(flatten)]
    pub db: DbArgs,

    #[command(flatten)]
    pub solana: SolanaArgs,
}
