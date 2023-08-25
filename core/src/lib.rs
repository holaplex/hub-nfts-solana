#![deny(clippy::disallowed_methods, clippy::suspicious, clippy::style)]
#![warn(clippy::pedantic, clippy::cargo)]
#![allow(clippy::module_name_repetitions)]

mod collection_mints;
mod collections;
mod compression_leafs;
pub mod db;

pub use collection_mints::CollectionMint;
pub use collections::Collection;
pub use compression_leafs::CompressionLeaf;
use hub_core::{consumer::RecvError, prelude::*};
use proto::{NftEventKey, SolanaNftEventKey, TreasuryEventKey};
pub use sea_orm;

#[allow(clippy::pedantic)]
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/nfts.proto.rs"));
    include!(concat!(env!("OUT_DIR"), "/solana_nfts.proto.rs"));
    include!(concat!(env!("OUT_DIR"), "/treasury.proto.rs"));
}

#[derive(Debug, Clone)]
pub enum Services {
    Nfts(proto::NftEventKey, proto::NftEvents),
    Treasury(proto::TreasuryEventKey, proto::TreasuryEvents),
}

impl hub_core::producer::Message for proto::SolanaNftEvents {
    type Key = proto::SolanaNftEventKey;
}

impl hub_core::consumer::MessageGroup for Services {
    const REQUESTED_TOPICS: &'static [&'static str] = &["hub-nfts", "hub-treasuries"];

    fn from_message<M: hub_core::consumer::Message>(msg: &M) -> Result<Self, RecvError> {
        let topic = msg.topic();
        let key = msg.key().ok_or(RecvError::MissingKey)?;
        let val = msg.payload().ok_or(RecvError::MissingPayload)?;

        info!(topic, ?key, ?val);

        match topic {
            "hub-nfts" => {
                let key = proto::NftEventKey::decode(key)?;
                let val = proto::NftEvents::decode(val)?;

                Ok(Services::Nfts(key, val))
            },
            "hub-treasuries" => {
                let key = proto::TreasuryEventKey::decode(key)?;
                let val = proto::TreasuryEvents::decode(val)?;

                Ok(Services::Treasury(key, val))
            },
            t => Err(RecvError::BadTopic(t.into())),
        }
    }
}

impl From<TreasuryEventKey> for SolanaNftEventKey {
    fn from(key: TreasuryEventKey) -> Self {
        let TreasuryEventKey {
            user_id,
            id,
            project_id,
        } = key;
        Self {
            id,
            user_id,
            project_id,
        }
    }
}

impl From<NftEventKey> for SolanaNftEventKey {
    fn from(key: NftEventKey) -> Self {
        let NftEventKey {
            user_id,
            project_id,
            id,
        } = key;

        Self {
            id,
            user_id,
            project_id,
        }
    }
}

use mpl_token_metadata::state::Creator;

use crate::proto::Creator as ProtoCreator;

impl TryFrom<ProtoCreator> for Creator {
    type Error = Error;

    fn try_from(
        ProtoCreator {
            address,
            verified,
            share,
        }: ProtoCreator,
    ) -> Result<Self> {
        Ok(Self {
            address: address.parse()?,
            verified,
            share: share.try_into()?,
        })
    }
}

use mpl_bubblegum::state::metaplex_adapter::Creator as BubblegumCreator;

impl TryFrom<ProtoCreator> for BubblegumCreator {
    type Error = Error;

    fn try_from(
        ProtoCreator {
            address,
            verified,
            share,
        }: ProtoCreator,
    ) -> Result<Self> {
        Ok(Self {
            address: address.parse()?,
            verified,
            share: share.try_into()?,
        })
    }
}
