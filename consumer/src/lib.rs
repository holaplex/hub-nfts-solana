#![deny(clippy::disallowed_methods, clippy::suspicious, clippy::style)]
#![warn(clippy::pedantic, clippy::cargo)]
#![allow(clippy::module_name_repetitions)]

mod backend;
pub mod events;
pub mod solana;
pub mod solana_compressed;

use holaplex_hub_nfts_solana_core::db::DbArgs;
use hub_core::{clap, prelude::*};
use solana::SolanaArgs;

#[derive(Debug, clap::Args)]
#[command(version, author, about)]
pub struct Args {
    #[command(flatten)]
    pub db: DbArgs,

    #[command(flatten)]
    pub solana: SolanaArgs,

    #[command(flatten)]
    pub solana_compressed: solana_compressed::SolanaCompressedArgs,
}
