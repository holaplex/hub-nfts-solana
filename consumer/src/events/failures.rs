use std::pin::Pin;

use holaplex_hub_nfts_solana_core::proto::{
    solana_nft_events::Event::{
        CreateDropFailed, MintDropFailed, RetryCreateDropFailed, RetryMintDropFailed,
        TransferAssetFailed, UpdateDropFailed,
    },
    SolanaFailedTransaction, SolanaNftEventKey, SolanaNftEvents, SolanaTransactionFailureReason,
};
use hub_core::prelude::*;

use crate::events::Processor;

pub trait FailedTransactions {
    fn create_drop_failed(
        &self,
        key: SolanaNftEventKey,
        reason: SolanaTransactionFailureReason,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>>;
    fn retry_create_drop_failed(
        &self,
        key: SolanaNftEventKey,
        reason: SolanaTransactionFailureReason,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>>;
    fn mint_drop_failed(
        &self,
        key: SolanaNftEventKey,
        reason: SolanaTransactionFailureReason,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>>;
    fn retry_mint_drop_failed(
        &self,
        key: SolanaNftEventKey,
        reason: SolanaTransactionFailureReason,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>>;
    fn update_drop_failed(
        &self,
        key: SolanaNftEventKey,
        reason: SolanaTransactionFailureReason,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>>;
    fn transfer_asset_failed(
        &self,
        key: SolanaNftEventKey,
        reason: SolanaTransactionFailureReason,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>>;
}

impl FailedTransactions for Processor {
    fn create_drop_failed(
        &self,
        key: SolanaNftEventKey,
        reason: SolanaTransactionFailureReason,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async move {
            self.producer
                .send(
                    Some(&SolanaNftEvents {
                        event: Some(CreateDropFailed(SolanaFailedTransaction {
                            reason: reason as i32,
                        })),
                    }),
                    Some(&key),
                )
                .await
                .map_err(Into::into)
        })
    }

    fn retry_create_drop_failed(
        &self,
        key: SolanaNftEventKey,
        reason: SolanaTransactionFailureReason,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async move {
            self.producer
                .send(
                    Some(&SolanaNftEvents {
                        event: Some(RetryCreateDropFailed(SolanaFailedTransaction {
                            reason: reason as i32,
                        })),
                    }),
                    Some(&key),
                )
                .await
                .map_err(Into::into)
        })
    }

    fn mint_drop_failed(
        &self,
        key: SolanaNftEventKey,
        reason: SolanaTransactionFailureReason,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async move {
            self.producer
                .send(
                    Some(&SolanaNftEvents {
                        event: Some(MintDropFailed(SolanaFailedTransaction {
                            reason: reason as i32,
                        })),
                    }),
                    Some(&key),
                )
                .await
                .map_err(Into::into)
        })
    }

    fn retry_mint_drop_failed(
        &self,
        key: SolanaNftEventKey,
        reason: SolanaTransactionFailureReason,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async move {
            self.producer
                .send(
                    Some(&SolanaNftEvents {
                        event: Some(RetryMintDropFailed(SolanaFailedTransaction {
                            reason: reason as i32,
                        })),
                    }),
                    Some(&key),
                )
                .await
                .map_err(Into::into)
        })
    }

    fn update_drop_failed(
        &self,
        key: SolanaNftEventKey,
        reason: SolanaTransactionFailureReason,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async move {
            self.producer
                .send(
                    Some(&SolanaNftEvents {
                        event: Some(UpdateDropFailed(SolanaFailedTransaction {
                            reason: reason as i32,
                        })),
                    }),
                    Some(&key),
                )
                .await
                .map_err(Into::into)
        })
    }

    fn transfer_asset_failed(
        &self,
        key: SolanaNftEventKey,
        reason: SolanaTransactionFailureReason,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async move {
            self.producer
                .send(
                    Some(&SolanaNftEvents {
                        event: Some(TransferAssetFailed(SolanaFailedTransaction {
                            reason: reason as i32,
                        })),
                    }),
                    Some(&key),
                )
                .await
                .map_err(Into::into)
        })
    }
}
