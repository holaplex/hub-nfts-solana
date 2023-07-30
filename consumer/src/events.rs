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
        SolanaTransactionFailureReason, TransferMetaplexAssetTransaction,
    },
    sea_orm::{DbErr, Set},
    Collection, CollectionMint, CompressionLeaf, Services,
};
use holaplex_hub_nfts_solana_entity::{collection_mints, collections, compression_leafs};
use hub_core::{
    chrono::Utc,
    prelude::*,
    producer::{Producer, SendError},
    thiserror,
    util::DebugShim,
    uuid,
    uuid::Uuid,
};
use solana_program::pubkey::{ParsePubkeyError, Pubkey};
use solana_sdk::signature::Signature;

use crate::{
    backend::{
        CollectionBackend, MasterEditionAddresses, MintBackend, MintEditionAddresses,
        MintMetaplexAddresses, TransferBackend,
    },
    solana::{CompressedRef, EditionRef, Solana, SolanaAssetIdError, UncompressedRef},
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
    #[error("Public key parse error")]
    ParsePubkey(#[from] ParsePubkeyError),
    #[error("Unable to parse signature from string")]
    ParseString(#[from] solana_sdk::signature::ParseSignatureError),
    #[error("Unable to extract compression nonce from signature")]
    AssetId(#[from] SolanaAssetIdError),
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
    CreateCollection,
    RetryCreateCollection,
    UpdateCollection,
    MintToCollection,
    RetryMintToCollection,
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
            EventKind::CreateCollection => SolanaNftEvent::CreateCollectionSigningRequested(tx),
            EventKind::UpdateCollection => SolanaNftEvent::UpdateCollectionSigningRequested(tx),
            EventKind::RetryCreateCollection => {
                SolanaNftEvent::RetryCreateCollectionSigningRequested(tx)
            },
            EventKind::MintToCollection => SolanaNftEvent::MintToCollectionSigningRequested(tx),
            EventKind::RetryMintToCollection => {
                SolanaNftEvent::RetryMintToCollectionSigningRequested(tx)
            },
        }
    }

    async fn into_success(
        self,
        db: &db::Connection,
        solana: &Solana,
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
            Self::CreateCollection => {
                let id = id()?;

                let collection = Collection::find_by_id(db, id)
                    .await?
                    .ok_or(ProcessorErrorKind::RecordNotFound)?;

                SolanaNftEvent::CreateCollectionSubmitted(SolanaCompletedMintTransaction {
                    signature,
                    address: collection.mint,
                })
            },
            Self::RetryCreateCollection => {
                let id = id()?;

                let collection = Collection::find_by_id(db, id)
                    .await?
                    .ok_or(ProcessorErrorKind::RecordNotFound)?;

                SolanaNftEvent::RetryCreateCollectionSubmitted(SolanaCompletedMintTransaction {
                    signature,
                    address: collection.mint,
                })
            },
            Self::UpdateCollection => {
                SolanaNftEvent::UpdateCollectionSubmitted(SolanaCompletedUpdateTransaction {
                    signature,
                })
            },
            Self::MintToCollection => {
                let id = id()?;
                let collection_mint = CollectionMint::find_by_id(db, id).await?;

                let compression_leafs = CompressionLeaf::find_by_id(db, id).await?;

                let address = if let Some(compression_leaf) = compression_leafs {
                    let signature = Signature::from_str(&signature)?;
                    let nonce = solana.extract_compression_nonce(&signature)?;

                    let asset_id = mpl_bubblegum::utils::get_asset_id(
                        &Pubkey::from_str(&compression_leaf.merkle_tree)?,
                        nonce.into(),
                    );

                    let asset_id = asset_id.to_string();

                    let mut compression_leaf: compression_leafs::ActiveModel =
                        compression_leaf.into();

                    compression_leaf.asset_id = Set(Some(asset_id.clone()));

                    CompressionLeaf::update(db, compression_leaf).await?;

                    asset_id
                } else {
                    collection_mint
                        .ok_or(ProcessorErrorKind::RecordNotFound)?
                        .mint
                };

                SolanaNftEvent::MintToCollectionSubmitted(SolanaCompletedMintTransaction {
                    signature,
                    address,
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
            Self::RetryMintToCollection => {
                let id = id()?;
                let collection_mint = CollectionMint::find_by_id(db, id)
                    .await?
                    .ok_or(ProcessorErrorKind::RecordNotFound)?;

                SolanaNftEvent::RetryMintToCollectionSubmitted(SolanaCompletedMintTransaction {
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
            Self::CreateCollection => SolanaNftEvent::CreateCollectionFailed(tx),
            Self::RetryCreateCollection => SolanaNftEvent::RetryCreateCollectionFailed(tx),
            Self::UpdateCollection => SolanaNftEvent::UpdateCollectionFailed(tx),
            Self::MintToCollection => SolanaNftEvent::MintToCollectionFailed(tx),
            Self::RetryMintToCollection => SolanaNftEvent::RetryMintToCollectionFailed(tx),
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
                    event: Some(kind.into_success(&self.db, self.solana(), key, sig).await?),
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
                    })),
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
            created_at: Utc::now().naive_utc(),
        };

        Collection::create(&self.db, collection.into()).await?;

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
}
