use holaplex_hub_nfts_solana_core::{db::Connection, proto::SolanaNftEvents, Services};
use hub_core::{prelude::*, producer::Producer, thiserror::Error};

use self::{nfts::NftEventProcessor, treasury::TreasuryEventProcessor};
use crate::solana::Solana;
mod failures;
mod nfts;
mod treasury;

#[derive(Error, Debug)]
pub enum ProcessorError {
    #[error("record not found")]
    RecordNotFound,
    #[error("message not found")]
    MessageNotFound,
    #[error("transaction status not found")]
    TransactionStatusNotFound,
}

#[derive(Clone)]
pub struct Processor {
    solana: Solana,
    db: Connection,
    producer: Producer<SolanaNftEvents>,
}

impl Processor {
    #[must_use]
    pub fn new(solana: Solana, db: Connection, producer: Producer<SolanaNftEvents>) -> Self {
        Self {
            solana,
            db,
            producer,
        }
    }

    /// Process the given message for various services.
    ///
    /// # Errors
    /// This function can return an error if it fails to process any event
    pub async fn process(&self, msg: Services) -> Result<()> {
        // match topics
        match msg {
            Services::Nfts(key, e) => {
                NftEventProcessor::new(self.clone())
                    .process(key.into(), e)
                    .await
            },
            Services::Treasury(key, e) => {
                TreasuryEventProcessor::new(self.clone())
                    .process(key.into(), e)
                    .await
            },
        }
    }

    pub fn solana(&self) -> Solana {
        self.solana.clone()
    }

    pub fn producer(&self) -> Producer<SolanaNftEvents> {
        self.producer.clone()
    }

    pub fn db(&self) -> Connection {
        self.db.clone()
    }
}
