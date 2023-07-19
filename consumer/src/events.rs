use holaplex_hub_nfts_solana_core::{
    db,
    proto::{
        nft_events::Event as NftEvent,
        solana_nft_events::Event as SolanaNftEvent,
        treasury_events::{Event as TreasuryEvent, SolanaTransactionResult, TransactionStatus},
        MetaplexMasterEditionTransaction, MintMetaplexEditionTransaction,
        SolanaCompletedMintTransaction, SolanaCompletedTransferTransaction,
        SolanaCompletedUpdateTransaction, SolanaFailedTransaction, SolanaNftEventKey,
        SolanaNftEvents, SolanaPendingTransaction, SolanaTransactionFailureReason,
        TransferMetaplexAssetTransaction,
    },
    sea_orm::{DbErr, Set},
    Collection, CollectionMint, Services,
};
use holaplex_hub_nfts_solana_entity::{collection_mints, collections};
use hub_core::{
    chrono::Utc,
    prelude::*,
    producer::{Producer, SendError},
    thiserror,
    util::DebugShim,
    uuid,
    uuid::Uuid,
};

use crate::{
    backend::{self, MasterEditionAddresses},
    solana::{Solana, UncompressedRef},
};

#[derive(Debug, thiserror::Error)]
pub enum ProcessorErrorKind {
    #[error("Associated record not found in database")]
    RecordNotFound,
    #[error("Transaction status not found in treasury event payload")]
    TransactionStatusNotFound,

    #[error("Error processing Solana operation")]
    Solana(#[source] Error),
    #[error("Error sending message")]
    SendError(#[from] SendError),
    #[error("Invalid UUID")]
    InvalidUuid(#[from] uuid::Error),
    #[error("Database error")]
    DbError(#[from] DbErr),
}

#[derive(Debug, thiserror::Error)]
#[error("Error handling {} of {}", src.name(), evt.name())]
pub struct ProcessorError {
    #[source]
    kind: ProcessorErrorKind,
    evt: EventKind,
    src: ErrorSource,
}

impl ProcessorError {
    fn new(kind: ProcessorErrorKind, evt: EventKind, src: ErrorSource) -> Self {
        Self { kind, evt, src }
    }
}

type ProcessResult<T> = std::result::Result<T, ProcessorErrorKind>;
type Result<T> = std::result::Result<T, ProcessorError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum ErrorSource {
    NftFailure,
    NftSignRequest,
    TreasuryStatus,
    TreasurySuccess,
    TreasuryFailure,
}

impl ErrorSource {
    fn name(self) -> &'static str {
        match self {
            Self::NftFailure => "NFT failure response",
            Self::NftSignRequest => "NFT transaction signature request",
            Self::TreasuryStatus => "treasury status check",
            Self::TreasurySuccess => "treasury success response",
            Self::TreasuryFailure => "treasury success failure",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EventKind {
    CreateDrop,
    MintDrop,
    UpdateDrop,
    TransferAsset,
    RetryCreateDrop,
    RetryMintDrop,
}

impl EventKind {
    fn name(self) -> &'static str {
        match self {
            Self::CreateDrop => "drop creation",
            Self::MintDrop => "drop mint",
            Self::UpdateDrop => "drop update",
            Self::TransferAsset => "drop asset transfer",
            Self::RetryCreateDrop => "drop creation retry",
            Self::RetryMintDrop => "drop mint retry",
        }
    }

    fn into_sign_request(self, tx: SolanaPendingTransaction) -> SolanaNftEvent {
        match self {
            EventKind::CreateDrop => SolanaNftEvent::CreateDropSigningRequested(tx),
            EventKind::MintDrop => SolanaNftEvent::MintDropSigningRequested(tx),
            EventKind::UpdateDrop => SolanaNftEvent::UpdateDropSigningRequested(tx),
            EventKind::TransferAsset => SolanaNftEvent::TransferAssetSigningRequested(tx),
            EventKind::RetryCreateDrop => SolanaNftEvent::RetryCreateDropSigningRequested(tx),
            EventKind::RetryMintDrop => SolanaNftEvent::RetryMintDropSigningRequested(tx),
        }
    }

    async fn into_success(
        self,
        db: &db::Connection,
        key: &SolanaNftEventKey,
        signature: String,
    ) -> ProcessResult<SolanaNftEvent> {
        let id = || Uuid::parse_str(&key.id);

        Ok(match self {
            Self::CreateDrop => {
                let id = id()?;
                let collection = Collection::find_by_id(db, id)
                    .await?
                    .ok_or(ProcessorErrorKind::RecordNotFound)?;

                SolanaNftEvent::CreateDropSubmitted(SolanaCompletedMintTransaction {
                    signature,
                    address: collection.mint,
                })
            },
            Self::MintDrop => {
                let id = id()?;
                let collection_mint = CollectionMint::find_by_id(db, id)
                    .await?
                    .ok_or(ProcessorErrorKind::RecordNotFound)?;

                SolanaNftEvent::MintDropSubmitted(SolanaCompletedMintTransaction {
                    signature,
                    address: collection_mint.mint,
                })
            },
            Self::UpdateDrop => {
                SolanaNftEvent::UpdateDropSubmitted(SolanaCompletedUpdateTransaction { signature })
            },
            Self::TransferAsset => {
                SolanaNftEvent::TransferAssetSubmitted(SolanaCompletedTransferTransaction {
                    signature,
                })
            },
            Self::RetryCreateDrop => {
                let id = id()?;
                let collection = Collection::find_by_id(db, id)
                    .await?
                    .ok_or(ProcessorErrorKind::RecordNotFound)?;

                SolanaNftEvent::RetryCreateDropSubmitted(SolanaCompletedMintTransaction {
                    signature,
                    address: collection.mint,
                })
            },
            Self::RetryMintDrop => {
                let id = id()?;
                let collection_mint = CollectionMint::find_by_id(db, id)
                    .await?
                    .ok_or(ProcessorErrorKind::RecordNotFound)?;

                SolanaNftEvent::RetryMintDropSubmitted(SolanaCompletedMintTransaction {
                    signature,
                    address: collection_mint.mint,
                })
            },
        })
    }

    fn into_failure(self, tx: SolanaFailedTransaction) -> SolanaNftEvent {
        match self {
            Self::CreateDrop => SolanaNftEvent::CreateDropFailed(tx),
            Self::MintDrop => SolanaNftEvent::MintDropFailed(tx),
            Self::UpdateDrop => SolanaNftEvent::UpdateDropFailed(tx),
            Self::TransferAsset => SolanaNftEvent::TransferAssetFailed(tx),
            Self::RetryCreateDrop => SolanaNftEvent::RetryCreateDropFailed(tx),
            Self::RetryMintDrop => SolanaNftEvent::RetryMintDropFailed(tx),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Processor {
    solana: DebugShim<Solana>,
    db: db::Connection,
    producer: Producer<SolanaNftEvents>,
}

impl Processor {
    #[inline]
    #[must_use]
    pub fn new(solana: Solana, db: db::Connection, producer: Producer<SolanaNftEvents>) -> Self {
        Self {
            solana: DebugShim(solana),
            db,
            producer,
        }
    }

    #[inline]
    fn solana(&self) -> &Solana {
        &self.solana.0
    }

    pub async fn process(&self, msg: Services) -> Result<()> {
        match msg {
            Services::Nfts(key, msg) => {
                let key = SolanaNftEventKey::from(key);

                match msg.event {
                    Some(NftEvent::SolanaCreateDrop(payload)) => {
                        self.process_nft(
                            EventKind::CreateDrop,
                            &key,
                            self.create_drop(&UncompressedRef(self.solana()), &key, payload),
                        )
                        .await
                    },
                    Some(NftEvent::SolanaMintDrop(payload)) => {
                        self.process_nft(
                            EventKind::MintDrop,
                            &key,
                            self.mint_drop(&UncompressedRef(self.solana()), &key, payload),
                        )
                        .await
                    },
                    Some(NftEvent::SolanaUpdateDrop(payload)) => {
                        self.process_nft(
                            EventKind::UpdateDrop,
                            &key,
                            self.update_drop(&UncompressedRef(self.solana()), &key, payload),
                        )
                        .await
                    },
                    Some(NftEvent::SolanaTransferAsset(payload)) => {
                        self.process_nft(
                            EventKind::TransferAsset,
                            &key,
                            self.transfer_asset(&UncompressedRef(self.solana()), &key, payload),
                        )
                        .await
                    },
                    Some(NftEvent::SolanaRetryDrop(payload)) => {
                        self.process_nft(
                            EventKind::RetryCreateDrop,
                            &key,
                            self.retry_create_drop(&UncompressedRef(self.solana()), &key, payload),
                        )
                        .await
                    },
                    Some(NftEvent::SolanaRetryMintDrop(payload)) => {
                        self.process_nft(
                            EventKind::RetryMintDrop,
                            &key,
                            self.retry_mint_drop(&UncompressedRef(self.solana()), &key, payload),
                        )
                        .await
                    },
                    _ => Ok(()),
                }
            },
            Services::Treasury(key, msg) => {
                let key = SolanaNftEventKey::from(key);

                match msg.event {
                    Some(TreasuryEvent::SolanaCreateDropSigned(res)) => {
                        self.process_treasury(EventKind::CreateDrop, key, res).await
                    },
                    Some(TreasuryEvent::SolanaMintDropSigned(res)) => {
                        self.process_treasury(EventKind::MintDrop, key, res).await
                    },
                    Some(TreasuryEvent::SolanaUpdateDropSigned(res)) => {
                        self.process_treasury(EventKind::UpdateDrop, key, res).await
                    },
                    Some(TreasuryEvent::SolanaTransferAssetSigned(res)) => {
                        self.process_treasury(EventKind::TransferAsset, key, res)
                            .await
                    },
                    Some(TreasuryEvent::SolanaRetryCreateDropSigned(res)) => {
                        self.process_treasury(EventKind::RetryCreateDrop, key, res)
                            .await
                    },
                    Some(TreasuryEvent::SolanaRetryMintDropSigned(res)) => {
                        self.process_treasury(EventKind::RetryMintDrop, key, res)
                            .await
                    },
                    _ => Ok(()),
                }
            },
        }
    }

    async fn process_nft(
        &self,
        kind: EventKind,
        key: &SolanaNftEventKey,
        fut: impl Future<Output = ProcessResult<SolanaPendingTransaction>>,
    ) -> Result<()> {
        match fut.await {
            Ok(tx) => self
                .producer
                .send(
                    Some(&SolanaNftEvents {
                        event: Some(kind.into_sign_request(tx)),
                    }),
                    Some(key),
                )
                .await
                .map_err(|e| ProcessorError::new(e.into(), kind, ErrorSource::NftSignRequest)),
            Err(e) => {
                warn!(
                    "{:?}",
                    Error::new(e).context(format!("Error processing {}", kind.name()))
                );
                self.event_failed(kind, key, SolanaTransactionFailureReason::Assemble)
                    .await
                    .map_err(|k| ProcessorError::new(k, kind, ErrorSource::NftFailure))
            },
        }
    }

    async fn process_treasury(
        &self,
        kind: EventKind,
        key: SolanaNftEventKey,
        res: SolanaTransactionResult,
    ) -> Result<()> {
        let status = TransactionStatus::from_i32(res.status).ok_or_else(|| {
            ProcessorError::new(
                ProcessorErrorKind::TransactionStatusNotFound,
                kind,
                ErrorSource::TreasuryStatus,
            )
        })?;

        if status == TransactionStatus::Failed {
            return self
                .event_failed(kind, &key, SolanaTransactionFailureReason::Sign)
                .await
                .map_err(|k| ProcessorError::new(k, kind, ErrorSource::TreasuryStatus));
        }

        match self.solana().submit_transaction(&res) {
            Ok(sig) => self
                .event_submitted(kind, key, sig)
                .await
                .map_err(|k| ProcessorError::new(k, kind, ErrorSource::TreasurySuccess)),
            Err(e) => {
                warn!(
                    "{:?}",
                    e.context(format!("Error submitting {}", kind.name()))
                );
                self.event_failed(kind, &key, SolanaTransactionFailureReason::Submit)
                    .await
                    .map_err(|k| ProcessorError::new(k, kind, ErrorSource::TreasuryFailure))
            },
        }
    }

    async fn event_submitted(
        &self,
        kind: EventKind,
        key: SolanaNftEventKey,
        sig: String,
    ) -> ProcessResult<()> {
        self.producer
            .send(
                Some(&SolanaNftEvents {
                    event: Some(kind.into_success(&self.db, &key, sig).await?),
                }),
                Some(&key),
            )
            .await
            .map_err(Into::into)
    }

    async fn event_failed(
        &self,
        kind: EventKind,
        key: &SolanaNftEventKey,
        reason: SolanaTransactionFailureReason,
    ) -> ProcessResult<()> {
        self.producer
            .send(
                Some(&SolanaNftEvents {
                    event: Some(kind.into_failure(SolanaFailedTransaction {
                        reason: reason as i32,
                    })),
                }),
                Some(key),
            )
            .await
            .map_err(Into::into)
    }

    async fn create_drop<B: backend::CollectionBackend>(
        &self,
        backend: &B,
        key: &SolanaNftEventKey,
        payload: MetaplexMasterEditionTransaction,
    ) -> ProcessResult<SolanaPendingTransaction> {
        let tx = backend
            .create(payload.clone())
            .map_err(ProcessorErrorKind::Solana)?;

        let MasterEditionAddresses {
            metadata,
            associated_token_account,
            mint,
            master_edition,
            update_authority,
            owner,
        } = tx.addresses;
        let id = key.id.parse()?;

        let collection = collections::Model {
            id,
            master_edition: master_edition.to_string(),
            owner: owner.to_string(),
            metadata: metadata.to_string(),
            associated_token_account: associated_token_account.to_string(),
            mint: mint.to_string(),
            update_authority: update_authority.to_string(),
            ..Default::default()
        };

        Collection::create(&self.db, collection).await?;

        Ok(tx.into())
    }

    async fn mint_drop<B: backend::MintBackend>(
        &self,
        backend: &B,
        key: &SolanaNftEventKey,
        payload: MintMetaplexEditionTransaction,
    ) -> ProcessResult<SolanaPendingTransaction> {
        let id = Uuid::parse_str(&key.id.clone())?;
        let collection_id = Uuid::parse_str(&payload.collection_id)?;
        let collection = Collection::find_by_id(&self.db, collection_id)
            .await?
            .ok_or(ProcessorErrorKind::RecordNotFound)?;

        // TODO: the collection mint record may fail to be created if this fails. Need to handle upserting the record in retry mint.
        let collection_ty = todo!("determine collection type");
        let tx = backend
            .mint(collection_ty, &collection, payload)
            .map_err(ProcessorErrorKind::Solana)?;

        let collection_mint = collection_mints::Model {
            id,
            collection_id: collection.id,
            mint: tx.addresses.mint.to_string(),
            owner: tx.addresses.recipient.to_string(),
            associated_token_account: Some(tx.addresses.associated_token_account.to_string()),
            created_at: Utc::now().naive_utc(),
        };

        CollectionMint::create(&self.db, collection_mint).await?;

        Ok(tx.into())
    }

    async fn update_drop<B: backend::MintBackend>(
        &self,
        backend: &B,
        key: &SolanaNftEventKey,
        payload: MetaplexMasterEditionTransaction,
    ) -> ProcessResult<SolanaPendingTransaction> {
        let collection_id = Uuid::parse_str(&key.id.clone())?;
        let collection = Collection::find_by_id(&self.db, collection_id)
            .await?
            .ok_or(ProcessorErrorKind::RecordNotFound)?;

        let collection_ty = todo!("determine collection type");
        let tx = backend
            .try_update(collection_ty, &collection, payload)
            .map_err(ProcessorErrorKind::Solana)?;
        let Some(tx) = tx else { todo!("handle un-updateable assets") };

        Ok(tx.into())
    }

    async fn transfer_asset<B: backend::MintBackend>(
        &self,
        backend: &B,
        key: &SolanaNftEventKey,
        payload: TransferMetaplexAssetTransaction,
    ) -> ProcessResult<SolanaPendingTransaction> {
        let collection_mint_id = Uuid::parse_str(&payload.collection_mint_id.clone())?;
        let collection_mint = CollectionMint::find_by_id(&self.db, collection_mint_id)
            .await?
            .ok_or(ProcessorErrorKind::RecordNotFound)?;

        let collection_ty = todo!("determine collection type");
        let tx = backend
            .transfer(collection_ty, &collection_mint, payload)
            .await
            .map_err(ProcessorErrorKind::Solana)?;

        Ok(tx.into())
    }

    async fn retry_create_drop<B: backend::CollectionBackend>(
        &self,
        backend: &B,
        key: &SolanaNftEventKey,
        payload: MetaplexMasterEditionTransaction,
    ) -> ProcessResult<SolanaPendingTransaction> {
        let tx = backend
            .create(payload.clone())
            .map_err(ProcessorErrorKind::Solana)?;

        let MasterEditionAddresses {
            metadata,
            associated_token_account,
            mint,
            master_edition,
            update_authority,
            owner,
        } = tx.addresses;

        let collection_id = Uuid::parse_str(&key.id.clone())?;
        let collection = Collection::find_by_id(&self.db, collection_id)
            .await?
            .ok_or(ProcessorErrorKind::RecordNotFound)?;

        let mut collection: collections::ActiveModel = collection.into();

        collection.master_edition = Set(metadata.to_string());
        collection.associated_token_account = Set(associated_token_account.to_string());
        collection.mint = Set(mint.to_string());
        collection.master_edition = Set(master_edition.to_string());
        collection.update_authority = Set(update_authority.to_string());
        collection.owner = Set(owner.to_string());

        Collection::update(&self.db, collection).await?;

        Ok(tx.into())
    }

    async fn retry_mint_drop<B: backend::MintBackend>(
        &self,
        backend: &B,
        key: &SolanaNftEventKey,
        payload: MintMetaplexEditionTransaction,
    ) -> ProcessResult<SolanaPendingTransaction> {
        let id = Uuid::parse_str(&key.id.clone())?;

        let (collection_mint, collection) =
            CollectionMint::find_by_id_with_collection(&self.db, id)
                .await?
                .ok_or(ProcessorErrorKind::RecordNotFound)?;

        let collection = collection.ok_or(ProcessorErrorKind::RecordNotFound)?;

        let collection_ty = todo!("determine collection type");
        let tx = backend
            .mint(collection_ty, &collection, payload)
            .map_err(ProcessorErrorKind::Solana)?;

        let mut collection_mint: collection_mints::ActiveModel = collection_mint.into();

        collection_mint.mint = Set(tx.addresses.mint.to_string());
        collection_mint.owner = Set(tx.addresses.recipient.to_string());
        collection_mint.associated_token_account =
            Set(Some(tx.addresses.associated_token_account.to_string()));

        CollectionMint::update(&self.db, collection_mint).await?;

        Ok(tx.into())
    }
}
