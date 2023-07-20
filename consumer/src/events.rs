use holaplex_hub_nfts_solana_core::{
    db,
    proto::{
        nft_events::Event as NftEvent,
        solana_nft_events::Event as SolanaNftEvent,
        treasury_events::{Event as TreasuryEvent, SolanaTransactionResult, TransactionStatus},
        MetaplexMasterEditionTransaction, MintMetaplexEditionTransaction,
        MintMetaplexMetadataTransaction, SolanaCompletedMintTransaction,
        SolanaCompletedTransferTransaction, SolanaCompletedUpdateTransaction,
        SolanaFailedTransaction, SolanaNftEventKey, SolanaNftEvents, SolanaPendingTransaction,
        SolanaTransactionFailureReason, TransferMetaplexAssetTransaction, SolanaCreator, Metadata, SolanaCollectionPayload, File, Attribute, SolanaMintPayload,
    },
    sea_orm::{DbErr, Set},
    Collection, CollectionMint, CompressionLeaf, Services,
};
use holaplex_hub_nfts_solana_core::proto::CollectionImport;
use holaplex_hub_nfts_solana_entity::{collection_mints, collections, compression_leafs};
use hub_core::{
    chrono::Utc,
    prelude::*,
    producer::{Producer, SendError},
    thiserror, tokio,
    util::DebugShim,
    uuid,
    uuid::Uuid,
};
use mpl_token_metadata::pda::{find_master_edition_account, find_metadata_account};
use spl_associated_token_account::get_associated_token_address;

use crate::{
    asset_api::{self, Asset, RpcClient},
    backend::{
        CollectionBackend, MasterEditionAddresses, MintBackend, MintEditionAddresses,
        MintMetaplexAddresses, TransferBackend,
    },
    solana::{CompressedRef, EditionRef, Solana, UncompressedRef},
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
    #[error("Asset api error")]
    AssetApi(#[from] jsonrpsee::core::Error),
    #[error("Array Index not found")]
    IndexNotFound,
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
    ImportCollectionFailure,
}

impl ErrorSource {
    fn name(self) -> &'static str {
        match self {
            Self::NftFailure => "NFT failure response",
            Self::NftSignRequest => "NFT transaction signature request",
            Self::TreasuryStatus => "treasury status check",
            Self::TreasurySuccess => "treasury success response",
            Self::TreasuryFailure => "treasury success failure",
            Self::ImportCollectionFailure => "collection import failure",
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
    CreateCollection,
    RetryCreateCollection,
    UpdateCollection,
    MintToCollection,
    RetryMintToCollection,
    ImportCollection,
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
            Self::CreateCollection => "collection creation",
            Self::RetryCreateCollection => "collection creation retry",
            Self::UpdateCollection => "collection update",
            Self::MintToCollection => "mint to collection",
            Self::RetryMintToCollection => "mint to collection retry",
            Self::ImportCollection => "import collection",
        }
    }

    fn into_sign_request(self, tx: SolanaPendingTransaction) -> Result<SolanaNftEvent> {
        match self {
            EventKind::CreateDrop => Ok(SolanaNftEvent::CreateDropSigningRequested(tx)),
            EventKind::MintDrop => Ok(SolanaNftEvent::MintDropSigningRequested(tx)),
            EventKind::UpdateDrop => Ok(SolanaNftEvent::UpdateDropSigningRequested(tx)),
            EventKind::TransferAsset => Ok(SolanaNftEvent::TransferAssetSigningRequested(tx)),
            EventKind::RetryCreateDrop => Ok(SolanaNftEvent::RetryCreateDropSigningRequested(tx)),
            EventKind::RetryMintDrop => Ok(SolanaNftEvent::RetryMintDropSigningRequested(tx)),
            EventKind::CreateCollection => Ok(SolanaNftEvent::CreateCollectionSigningRequested(tx)),
            EventKind::UpdateCollection => Ok(SolanaNftEvent::UpdateCollectionSigningRequested(tx)),
            EventKind::RetryCreateCollection => {
                Ok(SolanaNftEvent::RetryCreateCollectionSigningRequested(tx))
            },
            EventKind::MintToCollection => Ok(SolanaNftEvent::MintToCollectionSigningRequested(tx)),
            EventKind::RetryMintToCollection => {
                Ok(SolanaNftEvent::RetryMintToCollectionSigningRequested(tx))
            },
            EventKind::ImportCollection => Err(ProcessorError::new(
                ProcessorErrorKind::Solana(anyhow!("Invalid Operation")),
                EventKind::ImportCollection,
                ErrorSource::ImportCollectionFailure,
            )),
        }
    }

    async fn into_success(
        self,
        db: &db::Connection,
        key: &SolanaNftEventKey,
        signature: String,
    ) -> ProcessResult<SolanaNftEvent> {
        let id = || Uuid::parse_str(&key.id);

        match self {
            Self::CreateDrop => {
                let id = id()?;
                let collection = Collection::find_by_id(db, id)
                    .await?
                    .ok_or(ProcessorErrorKind::RecordNotFound)?;

                Ok(SolanaNftEvent::CreateDropSubmitted(
                    SolanaCompletedMintTransaction {
                        signature,
                        address: collection.mint,
                    },
                ))
            },
            Self::CreateCollection => {
                let id = id()?;

                let collection = Collection::find_by_id(db, id)
                    .await?
                    .ok_or(ProcessorErrorKind::RecordNotFound)?;

                Ok(SolanaNftEvent::CreateCollectionSubmitted(
                    SolanaCompletedMintTransaction {
                        signature,
                        address: collection.mint,
                    },
                ))
            },
            Self::RetryCreateCollection => {
                let id = id()?;

                let collection = Collection::find_by_id(db, id)
                    .await?
                    .ok_or(ProcessorErrorKind::RecordNotFound)?;

                Ok(SolanaNftEvent::RetryCreateCollectionSubmitted(
                    SolanaCompletedMintTransaction {
                        signature,
                        address: collection.mint,
                    },
                ))
            },
            Self::MintToCollection => {
                let id = id()?;
                let collection_mint = CollectionMint::find_by_id(db, id)
                    .await?
                    .ok_or(ProcessorErrorKind::RecordNotFound)?;

                Ok(SolanaNftEvent::MintToCollectionSubmitted(SolanaCompletedMintTransaction {
                    signature,
                    address: collection_mint.mint.to_string(),
                }))
            },
            Self::UpdateCollection => Ok(SolanaNftEvent::UpdateCollectionSubmitted(
                SolanaCompletedUpdateTransaction { signature },
            )),
            Self::MintDrop => {
                let id = id()?;
                let collection_mint = CollectionMint::find_by_id(db, id)
                    .await?
                    .ok_or(ProcessorErrorKind::RecordNotFound)?;

                Ok(SolanaNftEvent::MintDropSubmitted(
                    SolanaCompletedMintTransaction {
                        signature,
                        address: collection_mint.mint,
                    },
                ))
            },
            Self::UpdateDrop => Ok(SolanaNftEvent::UpdateDropSubmitted(
                SolanaCompletedUpdateTransaction { signature },
            )),
            Self::TransferAsset => Ok(SolanaNftEvent::TransferAssetSubmitted(
                SolanaCompletedTransferTransaction { signature },
            )),
            Self::RetryCreateDrop => {
                let id = id()?;
                let collection = Collection::find_by_id(db, id)
                    .await?
                    .ok_or(ProcessorErrorKind::RecordNotFound)?;

                Ok(SolanaNftEvent::RetryCreateDropSubmitted(
                    SolanaCompletedMintTransaction {
                        signature,
                        address: collection.mint,
                    },
                ))
            },
            Self::RetryMintDrop => {
                let id = id()?;
                let collection_mint = CollectionMint::find_by_id(db, id)
                    .await?
                    .ok_or(ProcessorErrorKind::RecordNotFound)?;

                Ok(SolanaNftEvent::RetryMintDropSubmitted(
                    SolanaCompletedMintTransaction {
                        signature,
                        address: collection_mint.mint,
                    },
                ))
            },
            Self::RetryMintToCollection => {
                let id = id()?;
                let collection_mint = CollectionMint::find_by_id(db, id)
                    .await?
                    .ok_or(ProcessorErrorKind::RecordNotFound)?;

                Ok(SolanaNftEvent::RetryMintToCollectionSubmitted(SolanaCompletedMintTransaction {
                    signature,
                    address: collection_mint.mint,
                }))
            },
        
            Self::ImportCollection => Err(ProcessorErrorKind::Solana(anyhow!("Invalid Operation"))),
        }
    }

    fn into_failure(self, tx: SolanaFailedTransaction) -> ProcessResult<SolanaNftEvent> {
        match self {
            Self::CreateDrop => Ok(SolanaNftEvent::CreateDropFailed(tx)),
            Self::MintDrop => Ok(SolanaNftEvent::MintDropFailed(tx)),
            Self::UpdateDrop => Ok(SolanaNftEvent::UpdateDropFailed(tx)),
            Self::TransferAsset => Ok(SolanaNftEvent::TransferAssetFailed(tx)),
            Self::RetryCreateDrop => Ok(SolanaNftEvent::RetryCreateDropFailed(tx)),
            Self::RetryMintDrop => Ok(SolanaNftEvent::RetryMintDropFailed(tx)),
            Self::CreateCollection => Ok(SolanaNftEvent::CreateCollectionFailed(tx)),
            Self::RetryCreateCollection => Ok(SolanaNftEvent::RetryCreateCollectionFailed(tx)),
            Self::UpdateCollection => Ok(SolanaNftEvent::UpdateCollectionFailed(tx)),
            Self::MintToCollection => Ok(SolanaNftEvent::MintToCollectionFailed(tx)),
            Self::RetryMintToCollection => Ok(SolanaNftEvent::RetryMintToCollectionFailed(tx)),
            Self::ImportCollection => Err(ProcessorErrorKind::Solana(anyhow!("Invalid Operation"))),
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
                            self.create_collection(&UncompressedRef(self.solana()), &key, payload),
                        )
                        .await
                    },
                    Some(NftEvent::SolanaCreateCollection(payload)) => {
                        self.process_nft(
                            EventKind::CreateCollection,
                            &key,
                            self.create_collection(&UncompressedRef(self.solana()), &key, payload),
                        )
                        .await
                    },
                    Some(NftEvent::SolanaMintDrop(payload)) => {
                        self.process_nft(
                            EventKind::MintDrop,
                            &key,
                            self.mint_drop(&EditionRef(self.solana()), &key, payload),
                        )
                        .await
                    },
                    Some(NftEvent::SolanaMintToCollection(payload)) => {
                        self.process_nft(
                            EventKind::MintToCollection,
                            &key,
                            self.mint_to_collection(&key, payload),
                        )
                        .await
                    },
                    Some(NftEvent::SolanaUpdateDrop(payload)) => {
                        self.process_nft(
                            EventKind::UpdateDrop,
                            &key,
                            self.update_collection(&UncompressedRef(self.solana()), &key, payload),
                        )
                        .await
                    },
                    Some(NftEvent::SolanaUpdateCollection(payload)) => {
                        self.process_nft(
                            EventKind::UpdateCollection,
                            &key,
                            self.update_collection(&UncompressedRef(self.solana()), &key, payload),
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
                            self.retry_create_collection(
                                &UncompressedRef(self.solana()),
                                &key,
                                payload,
                            ),
                        )
                        .await
                    },
                    Some(NftEvent::SolanaRetryCreateCollection(payload)) => {
                        self.process_nft(
                            EventKind::RetryCreateCollection,
                            &key,
                            self.retry_create_collection(
                                &UncompressedRef(self.solana()),
                                &key,
                                payload,
                            ),
                        )
                        .await
                    },
                    Some(NftEvent::SolanaRetryMintDrop(payload)) => {
                        self.process_nft(
                            EventKind::RetryMintDrop,
                            &key,
                            self.retry_mint_drop(&EditionRef(self.solana()), &key, payload),
                        )
                        .await
                    },
                    Some(NftEvent::SolanaRetryMintToCollection(payload)) => {
                        self.process_nft(
                            EventKind::RetryMintToCollection,
                            &key,
                            self.retry_mint_to_collection(
                                &UncompressedRef(self.solana()),
                                &key,
                                payload,
                            ),
                        )
                        .await },
                    Some(NftEvent::StartedImportingSolanaCollection(payload)) => {
                        self.import_collection(key, payload).await.map_err(|k| {
                            ProcessorError::new(
                                k,
                                EventKind::ImportCollection,
                                ErrorSource::ImportCollectionFailure,
                            )
                        })
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
                    Some(TreasuryEvent::SolanaMintToCollectionSigned(res)) => {
                        self.process_treasury(EventKind::MintToCollection, key, res)
                            .await
                    },
                    Some(TreasuryEvent::SolanaRetryMintToCollectionSigned(res)) => {
                        self.process_treasury(EventKind::RetryMintToCollection, key, res)
                            .await
                    },
                    Some(TreasuryEvent::SolanaCreateCollectionSigned(res)) => {
                        self.process_treasury(EventKind::CreateCollection, key, res)
                            .await
                    },
                    Some(TreasuryEvent::SolanaUpdateCollectionSigned(res)) => {
                        self.process_treasury(EventKind::UpdateCollection, key, res)
                            .await
                    },
                    Some(TreasuryEvent::SolanaRetryCreateCollectionSigned(res)) => {
                        self.process_treasury(EventKind::RetryCreateCollection, key, res)
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
                        event: Some(kind.into_sign_request(tx)?),
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
                .event_submitted(kind, &key, sig)
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
        key: &SolanaNftEventKey,
        sig: String,
    ) -> ProcessResult<()> {
        self.producer
            .send(
                Some(&SolanaNftEvents {
                    event: Some(kind.into_success(&self.db, key, sig).await?),
                }),
                Some(key),
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
                    })?),
                }),
                Some(key),
            )
            .await
            .map_err(Into::into)
    }

    async fn create_collection<B: CollectionBackend>(
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

    async fn mint_to_collection(
        &self,
        key: &SolanaNftEventKey,
        payload: MintMetaplexMetadataTransaction,
    ) -> ProcessResult<SolanaPendingTransaction> {
        let id = Uuid::parse_str(&key.id.clone())?;
        let collection_id = Uuid::parse_str(&payload.collection_id)?;
        let collection = Collection::find_by_id(&self.db, collection_id)
            .await?
            .ok_or(ProcessorErrorKind::RecordNotFound)?;

        if payload.compressed {
            let backend = &CompressedRef(self.solana());

            let tx = backend
                .mint(&collection, payload)
                .map_err(ProcessorErrorKind::Solana)?;

            let compression_leaf = compression_leafs::Model {
                id,
                collection_id: collection.id,
                merkle_tree: tx.addresses.merkle_tree.to_string(),
                tree_authority: tx.addresses.tree_authority.to_string(),
                tree_delegate: tx.addresses.tree_delegate.to_string(),
                leaf_owner: tx.addresses.leaf_owner.to_string(),
                created_at: Utc::now().naive_utc(),
                ..Default::default()
            };

            CompressionLeaf::create(&self.db, compression_leaf).await?;

            return Ok(tx.into());
        }

        let backend = &UncompressedRef(self.solana());

        let tx = backend
            .mint(&collection, payload)
            .map_err(ProcessorErrorKind::Solana)?;

        let collection_mint = collection_mints::Model {
            id,
            collection_id: collection.id,
            owner: tx.addresses.recipient.to_string(),
            mint: tx.addresses.mint.to_string(),
            created_at: Utc::now().naive_utc(),
            associated_token_account: tx.addresses.associated_token_account.to_string(),
            ..Default::default()
        };

        CollectionMint::create(&self.db, collection_mint).await?;

        Ok(tx.into())
    }

    async fn mint_drop<B: MintBackend<MintMetaplexEditionTransaction, MintEditionAddresses>>(
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

        let tx = backend
            .mint(&collection, payload)
            .map_err(ProcessorErrorKind::Solana)?;

        let collection_mint = collection_mints::Model {
            id,
            collection_id: collection.id,
            mint: tx.addresses.mint.to_string(),
            owner: tx.addresses.recipient.to_string(),
            associated_token_account: tx.addresses.associated_token_account.to_string(),
            created_at: Utc::now().naive_utc(),
        };

        CollectionMint::create(&self.db, collection_mint).await?;

        Ok(tx.into())
    }

    async fn update_collection<B: CollectionBackend>(
        &self,
        backend: &B,
        key: &SolanaNftEventKey,
        payload: MetaplexMasterEditionTransaction,
    ) -> ProcessResult<SolanaPendingTransaction> {
        let collection_id = Uuid::parse_str(&key.id.clone())?;
        let collection = Collection::find_by_id(&self.db, collection_id)
            .await?
            .ok_or(ProcessorErrorKind::RecordNotFound)?;

        let tx = backend
            .update(&collection, payload)
            .map_err(ProcessorErrorKind::Solana)?;

        Ok(tx.into())
    }

    async fn transfer_asset<B: TransferBackend>(
        &self,
        backend: &B,
        _key: &SolanaNftEventKey,
        payload: TransferMetaplexAssetTransaction,
    ) -> ProcessResult<SolanaPendingTransaction> {
        let collection_mint_id = Uuid::parse_str(&payload.collection_mint_id.clone())?;
        let collection_mint = CollectionMint::find_by_id(&self.db, collection_mint_id)
            .await?
            .ok_or(ProcessorErrorKind::RecordNotFound)?;

        let tx = backend
            .transfer(&collection_mint, payload)
            .await
            .map_err(ProcessorErrorKind::Solana)?;

        Ok(tx.into())
    }

    async fn retry_create_collection<B: CollectionBackend>(
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

        collection.metadata = Set(metadata.to_string());
        collection.associated_token_account = Set(associated_token_account.to_string());
        collection.mint = Set(mint.to_string());
        collection.master_edition = Set(master_edition.to_string());
        collection.update_authority = Set(update_authority.to_string());
        collection.owner = Set(owner.to_string());

        Collection::update(&self.db, collection).await?;

        Ok(tx.into())
    }

    async fn retry_mint_drop<
        B: MintBackend<MintMetaplexEditionTransaction, MintEditionAddresses>,
    >(
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

        let tx = backend
            .mint(&collection, payload)
            .map_err(ProcessorErrorKind::Solana)?;

        let MintEditionAddresses {
            mint,
            recipient,
            associated_token_account,
            ..
        } = tx.addresses;

        let mut collection_mint: collection_mints::ActiveModel = collection_mint.into();

        collection_mint.mint = Set(mint.to_string());
        collection_mint.owner = Set(recipient.to_string());
        collection_mint.associated_token_account = Set(associated_token_account.to_string());

        CollectionMint::update(&self.db, collection_mint).await?;

        Ok(tx.into())
    }

    async fn retry_mint_to_collection<
        B: MintBackend<MintMetaplexMetadataTransaction, MintMetaplexAddresses>,
    >(
        &self,
        backend: &B,
        key: &SolanaNftEventKey,
        payload: MintMetaplexMetadataTransaction,
    ) -> ProcessResult<SolanaPendingTransaction> {
        let id = Uuid::parse_str(&key.id.clone())?;

        let (collection_mint, collection) =
            CollectionMint::find_by_id_with_collection(&self.db, id)
                .await?
                .ok_or(ProcessorErrorKind::RecordNotFound)?;

        let collection = collection.ok_or(ProcessorErrorKind::RecordNotFound)?;

        let tx = backend
            .mint(&collection, payload)
            .map_err(ProcessorErrorKind::Solana)?;

        let MintMetaplexAddresses {
            mint,
            recipient,
            associated_token_account,
            ..
        } = tx.addresses;

        let mut collection_mint: collection_mints::ActiveModel = collection_mint.into();

        collection_mint.mint = Set(mint.to_string());
        collection_mint.owner = Set(recipient.to_string());
        collection_mint.associated_token_account = Set(associated_token_account.to_string());

        CollectionMint::update(&self.db, collection_mint).await?;

        Ok(tx.into())
    }

    async fn import_collection(
        &self,
        key: SolanaNftEventKey,
        CollectionImport {
             mint_address }: CollectionImport,
    ) -> ProcessResult<()> {
        let rpc = &self.solana.0.asset_rpc();
        let db = &self.db;
        let producer = &self.producer;
        let mut page = 1;
        const MAX_LIMIT: u64 = 1000;

        let collection = rpc
            .get_asset(&mint_address)
            .await
            .map_err(ProcessorErrorKind::AssetApi)?;
        let collection_model = index_collection(key.clone(), collection, db, producer).await?;
        loop {
            let result = rpc
                .search_assets(vec!["collection", &mint_address], page)
                .await
                .map_err(ProcessorErrorKind::AssetApi)?;

            for asset in result.items {
                index_collection_mint(
                    key.clone(),
                    collection_model.id,
                    asset,
                    db,
                    producer.clone(),
                )
                .await?;
            }

            if result.total < MAX_LIMIT {
                break;
            }
            page += 1;
        }

        Ok(())
    }
}

async fn index_collection(
    key: SolanaNftEventKey,
    collection: Asset,
    db: &db::Connection,
    producer: &Producer<SolanaNftEvents>,
) -> ProcessResult<collections::Model> {
    let owner = collection.ownership.owner.into();
    let mint = collection.id.into();
    let seller_fee_basis_points = collection.royalty.basis_points;
    let metadata = collection.content.metadata;
    let files = collection
        .content
        .files
        .map(|fs| fs.iter().map(Into::into).collect())
        .unwrap_or_default();

    let image = collection
        .content
        .links
        .and_then(|links| links.get("image").map(ToOwned::to_owned))
        .flatten()
        .unwrap_or_default();

    let attributes = metadata
        .attributes
        .clone()
        .map(|attributes| attributes.iter().map(Into::into).collect::<Vec<_>>())
        .unwrap_or_default();

    let creators = collection
        .creators
        .iter()
        .map(|c| SolanaCreator {
            address: c.address.to_string(),
            verified: c.verified,
            share: c.share,
        })
        .collect::<Vec<_>>();
    // Collection Model fields
    let update_authority = &collection
        .authorities
        .get(0)
        .ok_or(ProcessorErrorKind::IndexNotFound)?
        .address;

    let ata = get_associated_token_address(&owner, &mint);
    let (metadata_pubkey, _) = find_metadata_account(&mint);

    let (master_edition, _) = find_master_edition_account(&mint);
    let collection_model = Collection::create(db, collections::Model {
        master_edition: master_edition.to_string(),
        update_authority: update_authority.to_string(),
        associated_token_account: ata.to_string(),
        owner: owner.to_string(),
        mint: mint.to_string(),
        metadata: metadata_pubkey.to_string(),
        ..Default::default()
    })
    .await?;

    producer
        .send(
            Some(&SolanaNftEvents {
                event: Some(SolanaNftEvent::ImportedExternalCollection(
                    SolanaCollectionPayload {
                        supply: collection.supply,
                        mint_address: mint.to_string(),
                        seller_fee_basis_points,
                        creators,
                        metadata: Some(Metadata {
                            name: metadata.name,
                            description: metadata.description,
                            symbol: metadata.symbol,
                            attributes,
                            uri: collection.content.json_uri,
                            image,
                        }),
                        files,
                        update_authority: update_authority.to_string(),
                    },
                )),
            }),
            Some(&key),
        )
        .await
        .map_err(ProcessorErrorKind::SendError)?;

    Ok(collection_model)
}

async fn index_collection_mint(
    key: SolanaNftEventKey,
    collection: Uuid,
    asset: Asset,
    db: &db::Connection,
    producer: Producer<SolanaNftEvents>,
) -> ProcessResult<()> {
    let key = key.clone();
    let producer = producer.clone();
    let owner = asset.ownership.owner.into();
    let mint = asset.id.into();
    let ata = get_associated_token_address(&owner, &mint);
    let seller_fee_basis_points = asset.royalty.basis_points;
    let metadata = asset.content.metadata;
    let update_authority = asset
        .authorities
        .get(0)
        .ok_or(ProcessorErrorKind::IndexNotFound)?
        .address
        .clone();

    let files = asset
        .content
        .files
        .map(|fs| fs.iter().map(Into::into).collect::<Vec<File>>())
        .unwrap_or_default();

    let image = asset
        .content
        .links
        .and_then(|links| links.get("image").map(ToOwned::to_owned))
        .flatten()
        .unwrap_or_default();

    let attributes = metadata
        .attributes
        .map(|attributes| attributes.iter().map(Into::into).collect())
        .unwrap_or_default();

    let creators = asset
        .creators
        .iter()
        .map(|c| SolanaCreator {
            address: c.address.to_string(),
            verified: c.verified,
            share: c.share,
        })
        .collect::<Vec<_>>();

    CollectionMint::create(db, collection_mints::Model {
        collection_id: collection,
        mint: mint.to_string(),
        owner: owner.to_string(),
        associated_token_account: Some(ata.to_string()),
        ..Default::default()
    })
    .await?;

    tokio::spawn(async move {
        producer
            .send(
                Some(&SolanaNftEvents {
                    event: Some(SolanaNftEvent::ImportedExternalMint(SolanaMintPayload {
                        collection_id: collection.to_string(),
                        mint_address: mint.to_string(),
                        owner: owner.to_string(),
                        seller_fee_basis_points,
                        compressed: asset.compression.compressed,
                        creators,
                        metadata: Some(Metadata {
                            name: metadata.name,
                            description: metadata.description,
                            symbol: metadata.symbol,
                            attributes,
                            uri: asset.content.json_uri,
                            image,
                        }),
                        files,
                        update_authority: update_authority.to_string(),
                    })),
                }),
                Some(&key),
            )
            .await
            .map_err(ProcessorErrorKind::SendError)
    });

    Ok(())
}

impl From<&asset_api::File> for File {
    fn from(file: &asset_api::File) -> Self {
        Self {
            uri: file.uri.clone(),
            mime: file.mime.clone(),
        }
    }
}

impl From<&asset_api::Attribute> for Attribute {
    fn from(attr: &asset_api::Attribute) -> Self {
        Self {
            value: attr.value.to_string(),
            trait_type: attr.trait_type.to_string(),
        }
    }
}
