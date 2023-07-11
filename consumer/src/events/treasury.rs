use holaplex_hub_nfts_solana_core::{
    db::Connection,
    proto::{
        solana_nft_events::Event::{
            CreateDropSubmitted, MintDropSubmitted, RetryCreateDropSubmitted,
            RetryMintDropSubmitted, TransferAssetSubmitted, UpdateDropSubmitted,
        },
        treasury_events::{Event as TreasuryEvent, TransactionStatus},
        SolanaCompletedMintTransaction, SolanaCompletedTransferTransaction,
        SolanaCompletedUpdateTransaction, SolanaNftEventKey, SolanaNftEvents,
        SolanaTransactionFailureReason, TreasuryEvents,
    },
    Collection, CollectionMint,
};
use hub_core::{prelude::*, producer::Producer, uuid::Uuid};

use super::failures::FailedTransactions;
use crate::{
    events::{Processor, ProcessorError},
    solana::Solana,
};

#[derive(Clone)]
pub struct TreasuryEventProcessor(Processor);

impl TreasuryEventProcessor {
    pub fn new(processor: Processor) -> Self {
        Self(processor)
    }

    fn solana(&self) -> &Solana {
        &self.processor().solana
    }

    fn producer(&self) -> &Producer<SolanaNftEvents> {
        &self.processor().producer
    }

    fn db(&self) -> &Connection {
        &self.processor().db
    }

    fn processor(&self) -> &Processor {
        &self.0
    }

    pub async fn process(&self, key: SolanaNftEventKey, e: TreasuryEvents) -> Result<(), Error> {
        match e.event {
            Some(TreasuryEvent::SolanaCreateDropSigned(payload)) => {
                let status = TransactionStatus::from_i32(payload.status)
                    .ok_or(ProcessorError::TransactionStatusNotFound)?;

                if status == TransactionStatus::Failed {
                    self.processor()
                        .create_drop_failed(key, SolanaTransactionFailureReason::Sign)
                        .await?;

                    return Ok(());
                }

                let signature_result = self.solana().submit_transaction(&payload);

                match signature_result {
                    Ok(signature) => {
                        self.create_drop_submitted(key, signature).await?;
                    },
                    Err(_) => {
                        self.processor()
                            .create_drop_failed(key, SolanaTransactionFailureReason::Submit)
                            .await?;
                    },
                }

                Ok(())
            },
            Some(TreasuryEvent::SolanaUpdateDropSigned(payload)) => {
                let status = TransactionStatus::from_i32(payload.status)
                    .ok_or(ProcessorError::TransactionStatusNotFound)?;

                if status == TransactionStatus::Failed {
                    self.processor()
                        .update_drop_failed(key, SolanaTransactionFailureReason::Sign)
                        .await?;

                    return Ok(());
                }

                let signature_result = self.solana().submit_transaction(&payload);

                match signature_result {
                    Ok(signature) => {
                        self.update_drop_submitted(key, signature).await?;
                    },
                    Err(_) => {
                        self.processor()
                            .update_drop_failed(key, SolanaTransactionFailureReason::Submit)
                            .await?;
                    },
                }

                Ok(())
            },
            Some(TreasuryEvent::SolanaMintDropSigned(payload)) => {
                let status = TransactionStatus::from_i32(payload.status)
                    .ok_or(ProcessorError::TransactionStatusNotFound)?;

                if status == TransactionStatus::Failed {
                    self.processor()
                        .mint_drop_failed(key, SolanaTransactionFailureReason::Sign)
                        .await?;

                    return Ok(());
                }

                let signature_result = self.solana().submit_transaction(&payload);

                match signature_result {
                    Ok(signature) => {
                        self.mint_drop_submitted(key, signature).await?;
                    },
                    Err(_) => {
                        self.processor()
                            .mint_drop_failed(key, SolanaTransactionFailureReason::Submit)
                            .await?;
                    },
                }

                Ok(())
            },
            Some(TreasuryEvent::SolanaTransferAssetSigned(payload)) => {
                let status = TransactionStatus::from_i32(payload.status)
                    .ok_or(ProcessorError::TransactionStatusNotFound)?;

                if status == TransactionStatus::Failed {
                    self.processor()
                        .transfer_asset_failed(key, SolanaTransactionFailureReason::Sign)
                        .await?;

                    return Ok(());
                }

                let signature_result = self.solana().submit_transaction(&payload);

                match signature_result {
                    Ok(signature) => {
                        self.transfer_asset_submitted(key, signature).await?;
                    },
                    Err(_) => {
                        self.processor()
                            .transfer_asset_failed(key, SolanaTransactionFailureReason::Submit)
                            .await?;
                    },
                }

                Ok(())
            },
            Some(TreasuryEvent::SolanaRetryCreateDropSigned(payload)) => {
                let status = TransactionStatus::from_i32(payload.status)
                    .ok_or(ProcessorError::TransactionStatusNotFound)?;

                if status == TransactionStatus::Failed {
                    self.processor()
                        .retry_create_drop_failed(key, SolanaTransactionFailureReason::Sign)
                        .await?;

                    return Ok(());
                }

                let signature_result = self.solana().submit_transaction(&payload);

                match signature_result {
                    Ok(signature) => {
                        self.retry_create_drop_submitted(key, signature).await?;
                    },
                    Err(_) => {
                        self.processor()
                            .retry_create_drop_failed(key, SolanaTransactionFailureReason::Submit)
                            .await?;
                    },
                }

                Ok(())
            },
            Some(TreasuryEvent::SolanaRetryMintDropSigned(payload)) => {
                let status = TransactionStatus::from_i32(payload.status)
                    .ok_or(ProcessorError::TransactionStatusNotFound)?;

                if status == TransactionStatus::Failed {
                    self.processor()
                        .retry_mint_drop_failed(key, SolanaTransactionFailureReason::Sign)
                        .await?;

                    return Ok(());
                }
                let signature_result = self.solana().submit_transaction(&payload);

                match signature_result {
                    Ok(signature) => {
                        self.retry_mint_drop_submitted(key, signature).await?;
                    },
                    Err(_) => {
                        self.processor()
                            .retry_mint_drop_failed(key, SolanaTransactionFailureReason::Submit)
                            .await?;
                    },
                }

                Ok(())
            },
            _ => Ok(()),
        }
    }

    async fn create_drop_submitted(&self, key: SolanaNftEventKey, signature: String) -> Result<()> {
        let id = Uuid::parse_str(&key.id.clone())?;
        let collection = Collection::find_by_id(self.db(), id)
            .await?
            .ok_or(ProcessorError::RecordNotFound)?;

        self.producer()
            .send(
                Some(&SolanaNftEvents {
                    event: Some(CreateDropSubmitted(SolanaCompletedMintTransaction {
                        signature,
                        address: collection.mint,
                    })),
                }),
                Some(&key),
            )
            .await?;

        Ok(())
    }

    async fn update_drop_submitted(&self, key: SolanaNftEventKey, signature: String) -> Result<()> {
        self.producer()
            .send(
                Some(&SolanaNftEvents {
                    // TODO
                    event: Some(UpdateDropSubmitted(SolanaCompletedUpdateTransaction {
                        signature,
                    })),
                }),
                Some(&key),
            )
            .await?;

        Ok(())
    }

    async fn mint_drop_submitted(&self, key: SolanaNftEventKey, signature: String) -> Result<()> {
        let id = Uuid::parse_str(&key.id.clone())?;
        let collection_mint = CollectionMint::find_by_id(self.db(), id)
            .await?
            .ok_or(ProcessorError::RecordNotFound)?;

        self.producer()
            .send(
                Some(&SolanaNftEvents {
                    event: Some(MintDropSubmitted(SolanaCompletedMintTransaction {
                        signature,
                        address: collection_mint.mint,
                    })),
                }),
                Some(&key),
            )
            .await?;

        Ok(())
    }

    async fn transfer_asset_submitted(
        &self,
        key: SolanaNftEventKey,
        signature: String,
    ) -> Result<()> {
        self.producer()
            .send(
                Some(&SolanaNftEvents {
                    event: Some(TransferAssetSubmitted(SolanaCompletedTransferTransaction {
                        signature,
                    })),
                }),
                Some(&key),
            )
            .await?;

        Ok(())
    }

    async fn retry_create_drop_submitted(
        &self,
        key: SolanaNftEventKey,
        signature: String,
    ) -> Result<()> {
        let id = Uuid::parse_str(&key.id.clone())?;
        let collection = Collection::find_by_id(self.db(), id)
            .await?
            .ok_or(ProcessorError::RecordNotFound)?;

        self.producer()
            .send(
                Some(&SolanaNftEvents {
                    event: Some(RetryCreateDropSubmitted(SolanaCompletedMintTransaction {
                        signature,
                        address: collection.mint,
                    })),
                }),
                Some(&key),
            )
            .await?;

        Ok(())
    }

    async fn retry_mint_drop_submitted(
        &self,
        key: SolanaNftEventKey,
        signature: String,
    ) -> Result<()> {
        let id = Uuid::parse_str(&key.id.clone())?;
        let collection_mint = CollectionMint::find_by_id(self.db(), id)
            .await?
            .ok_or(ProcessorError::RecordNotFound)?;

        self.producer()
            .send(
                Some(&SolanaNftEvents {
                    event: Some(RetryMintDropSubmitted(SolanaCompletedMintTransaction {
                        signature,
                        address: collection_mint.mint,
                    })),
                }),
                Some(&key),
            )
            .await?;

        Ok(())
    }
}
