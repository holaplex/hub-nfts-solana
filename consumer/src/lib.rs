#![deny(clippy::disallowed_methods, clippy::suspicious, clippy::style)]
#![warn(clippy::pedantic, clippy::cargo)]
#![allow(clippy::module_name_repetitions)]

pub mod events;
pub mod solana;

use holaplex_hub_nfts_solana_core::db::DbArgs;
use hub_core::{clap, consumer::RecvError, prelude::*};

#[allow(clippy::pedantic)]
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/nfts.proto.rs"));
    include!(concat!(env!("OUT_DIR"), "/solana_nfts.proto.rs"));
}

#[derive(Debug)]
pub enum Services {
    Nfts(proto::NftEventKey, proto::SolanaEvents),
}

impl hub_core::producer::Message for proto::SolanaNftEvents {
    type Key = proto::SolanaNftEventKey;
}

impl hub_core::consumer::MessageGroup for Services {
    const REQUESTED_TOPICS: &'static [&'static str] = &["hub-nfts"];

    fn from_message<M: hub_core::consumer::Message>(msg: &M) -> Result<Self, RecvError> {
        let topic = msg.topic();
        let key = msg.key().ok_or(RecvError::MissingKey)?;
        let val = msg.payload().ok_or(RecvError::MissingPayload)?;

        info!(topic, ?key, ?val);

        match topic {
            "hub-nfts" => {
                let key = proto::NftEventKey::decode(key)?;
                let val = proto::SolanaEvents::decode(val)?;

                Ok(Services::Nfts(key, val))
            },
            t => Err(RecvError::BadTopic(t.into())),
        }
    }
}

#[derive(Debug, clap::Args)]
#[command(version, author, about)]
pub struct Args {
    #[command(flatten)]
    pub db: DbArgs,
}
