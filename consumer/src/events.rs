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
        SolanaTransactionFailureReason, SwitchCollectionPayload, TransferMetaplexAssetTransaction,
        UpdateSolanaMintPayload,
    },
    sea_orm::{ActiveModelTrait, DatabaseConnection, DbErr, EntityTrait, Set},
    Collection, CollectionMint, CompressionLeaf, Services,
};
use holaplex_hub_nfts_solana_entity::{
    collection_mints, collections, compression_leafs, update_revisions,
};
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
        MintMetaplexAddresses, TransferBackend, UpdateCollectionMintAddresses,
    },
    solana::{CompressedRef, EditionRef, Solana, SolanaAssetIdError, UncompressedRef},
};

#[derive(Debug, thiserror::Error, Triage)]
pub enum ProcessorErrorKind {
    #[error("Associated record not found in database")]
    #[transient]
    RecordNotFound,
    #[error("Transaction status not found in treasury event payload")]
    TransactionStatusNotFound,

    #[error("Error processing Solana operation")]
    #[transient]
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

#[derive(Debug, thiserror::Error, Triage)]
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
    CreateOpenDrop,
    MintOpenDrop,
    UpdateOpenDrop,
    RetryCreateOpenDrop,
    RetryMintOpenDrop,

    CreateEditionDrop,
    MintEditionDrop,
    UpdateEditionDrop,
    TransferAsset,
    RetryCreateEditionDrop,
    RetryMintEditionDrop,
    CreateCollection,
    RetryCreateCollection,
    UpdateCollection,
    MintToCollection,
    RetryMintToCollection,
    UpdateCollectionMint,
    RetryUpdateCollectionMint,
    SwitchMintCollection,
}

impl EventKind {
    fn name(self) -> &'static str {
        match self {
            Self::CreateEditionDrop => "edition drop creation",
            Self::MintEditionDrop => "edition drop mint",
            Self::UpdateEditionDrop => "edition drop update",
            Self::TransferAsset => "asset transfer",
            Self::RetryCreateEditionDrop => "editiondrop creation retry",
            Self::RetryMintEditionDrop => "edition drop mint retry",
            Self::CreateCollection => "collection creation",
            Self::RetryCreateCollection => "collection creation retry",
            Self::UpdateCollection => "collection update",
            Self::MintToCollection => "mint to collection",
            Self::RetryMintToCollection => "mint to collection retry",
            Self::UpdateCollectionMint => "collection mint update",
            Self::RetryUpdateCollectionMint => "collection mint update retry",
            Self::SwitchMintCollection => "switch mint collection",
            Self::CreateOpenDrop => "open drop creation",
            Self::MintOpenDrop => "open drop mint",
            Self::UpdateOpenDrop => "open drop update",
            Self::RetryCreateOpenDrop => "open drop creation retry",
            Self::RetryMintOpenDrop => "open drop mint retry",
        }
    }

    fn into_sign_request(self, tx: SolanaPendingTransaction) -> SolanaNftEvent {
        match self {
            EventKind::CreateEditionDrop => SolanaNftEvent::CreateEditionDropSigningRequested(tx),
            EventKind::MintEditionDrop => SolanaNftEvent::MintEditionDropSigningRequested(tx),
            EventKind::UpdateEditionDrop => SolanaNftEvent::UpdateEditionDropSigningRequested(tx),
            EventKind::TransferAsset => SolanaNftEvent::TransferAssetSigningRequested(tx),
            EventKind::RetryCreateEditionDrop => {
                SolanaNftEvent::RetryCreateEditionDropSigningRequested(tx)
            },
            EventKind::RetryMintEditionDrop => {
                SolanaNftEvent::RetryMintEditionDropSigningRequested(tx)
            },
            EventKind::CreateCollection => SolanaNftEvent::CreateCollectionSigningRequested(tx),
            EventKind::UpdateCollection => SolanaNftEvent::UpdateCollectionSigningRequested(tx),
            EventKind::RetryCreateCollection => {
                SolanaNftEvent::RetryCreateCollectionSigningRequested(tx)
            },
            EventKind::MintToCollection => SolanaNftEvent::MintToCollectionSigningRequested(tx),
            EventKind::RetryMintToCollection => {
                SolanaNftEvent::RetryMintToCollectionSigningRequested(tx)
            },
            EventKind::UpdateCollectionMint => {
                SolanaNftEvent::UpdateCollectionMintSigningRequested(tx)
            },
            EventKind::RetryUpdateCollectionMint => {
                SolanaNftEvent::RetryUpdateMintSigningRequested(tx)
            },
            EventKind::SwitchMintCollection => {
                SolanaNftEvent::SwitchMintCollectionSigningRequested(tx)
            },
            EventKind::CreateOpenDrop => SolanaNftEvent::CreateOpenDropSigningRequested(tx),
            EventKind::MintOpenDrop => SolanaNftEvent::MintOpenDropSigningRequested(tx),
            EventKind::UpdateOpenDrop => SolanaNftEvent::UpdateOpenDropSigningRequested(tx),
            EventKind::RetryCreateOpenDrop => {
                SolanaNftEvent::RetryCreateOpenDropSigningRequested(tx)
            },
            EventKind::RetryMintOpenDrop => SolanaNftEvent::RetryMintOpenDropSigningRequested(tx),
        }
    }

    async fn into_success(
        self,
        conn: &DatabaseConnection,
        solana: &Solana,
        key: &SolanaNftEventKey,
        signature: String,
    ) -> ProcessResult<SolanaNftEvent> {
        let id = || Uuid::parse_str(&key.id);

        Ok(match self {
            Self::CreateEditionDrop => {
                let id = id()?;
                let collection = Collection::find_by_id(conn, id)
                    .await?
                    .ok_or(ProcessorErrorKind::RecordNotFound)?;

                SolanaNftEvent::CreateEditionDropSubmitted(SolanaCompletedMintTransaction {
                    signature,
                    address: collection.mint,
                })
            },
            Self::CreateCollection => {
                let id = id()?;

                let collection = Collection::find_by_id(conn, id)
                    .await?
                    .ok_or(ProcessorErrorKind::RecordNotFound)?;

                SolanaNftEvent::CreateCollectionSubmitted(SolanaCompletedMintTransaction {
                    signature,
                    address: collection.mint,
                })
            },
            Self::RetryCreateCollection => {
                let id = id()?;

                let collection = Collection::find_by_id(conn, id)
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
                let collection_mint = CollectionMint::find_by_id(conn, id).await?;

                let compression_leafs = CompressionLeaf::find_by_id(conn, id).await?;

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

                    CompressionLeaf::update(conn, compression_leaf).await?;

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
            Self::MintEditionDrop => {
                let id = id()?;
                let collection_mint = CollectionMint::find_by_id(conn, id)
                    .await?
                    .ok_or(ProcessorErrorKind::RecordNotFound)?;

                SolanaNftEvent::MintEditionDropSubmitted(SolanaCompletedMintTransaction {
                    signature,
                    address: collection_mint.mint,
                })
            },
            Self::UpdateEditionDrop => {
                SolanaNftEvent::UpdateEditionDropSubmitted(SolanaCompletedUpdateTransaction {
                    signature,
                })
            },
            Self::TransferAsset => {
                SolanaNftEvent::TransferAssetSubmitted(SolanaCompletedTransferTransaction {
                    signature,
                })
            },
            Self::RetryCreateEditionDrop => {
                let id = id()?;
                let collection = Collection::find_by_id(conn, id)
                    .await?
                    .ok_or(ProcessorErrorKind::RecordNotFound)?;

                SolanaNftEvent::RetryCreateEditionDropSubmitted(SolanaCompletedMintTransaction {
                    signature,
                    address: collection.mint,
                })
            },
            Self::RetryMintEditionDrop => {
                let id = id()?;
                let collection_mint = CollectionMint::find_by_id(conn, id)
                    .await?
                    .ok_or(ProcessorErrorKind::RecordNotFound)?;

                SolanaNftEvent::RetryMintEditionDropSubmitted(SolanaCompletedMintTransaction {
                    signature,
                    address: collection_mint.mint,
                })
            },
            Self::RetryMintToCollection => {
                let id = id()?;
                let collection_mint = CollectionMint::find_by_id(conn, id)
                    .await?
                    .ok_or(ProcessorErrorKind::RecordNotFound)?;

                SolanaNftEvent::RetryMintToCollectionSubmitted(SolanaCompletedMintTransaction {
                    signature,
                    address: collection_mint.mint,
                })
            },
            Self::UpdateCollectionMint => {
                SolanaNftEvent::UpdateCollectionMintSubmitted(SolanaCompletedUpdateTransaction {
                    signature,
                })
            },
            Self::RetryUpdateCollectionMint => {
                SolanaNftEvent::RetryUpdateMintSubmitted(SolanaCompletedUpdateTransaction {
                    signature,
                })
            },
            Self::SwitchMintCollection => {
                SolanaNftEvent::SwitchMintCollectionSubmitted(SolanaCompletedUpdateTransaction {
                    signature,
                })
            },
            Self::CreateOpenDrop => {
                let id = id()?;
                let collection = Collection::find_by_id(conn, id)
                    .await?
                    .ok_or(ProcessorErrorKind::RecordNotFound)?;

                SolanaNftEvent::CreateOpenDropSubmitted(SolanaCompletedMintTransaction {
                    signature,
                    address: collection.mint,
                })
            },
            Self::MintOpenDrop => {
                let id = id()?;
                let collection_mint = CollectionMint::find_by_id(conn, id)
                    .await?
                    .ok_or(ProcessorErrorKind::RecordNotFound)?;

                SolanaNftEvent::MintOpenDropSubmitted(SolanaCompletedMintTransaction {
                    signature,
                    address: collection_mint.mint,
                })
            },
            Self::UpdateOpenDrop => {
                SolanaNftEvent::UpdateOpenDropSubmitted(SolanaCompletedUpdateTransaction {
                    signature,
                })
            },
            Self::RetryCreateOpenDrop => {
                let id = id()?;
                let collection = Collection::find_by_id(conn, id)
                    .await?
                    .ok_or(ProcessorErrorKind::RecordNotFound)?;

                SolanaNftEvent::RetryCreateOpenDropSubmitted(SolanaCompletedMintTransaction {
                    signature,
                    address: collection.mint,
                })
            },
            Self::RetryMintOpenDrop => {
                let id = id()?;
                let collection_mint = CollectionMint::find_by_id(conn, id)
                    .await?
                    .ok_or(ProcessorErrorKind::RecordNotFound)?;

                SolanaNftEvent::RetryMintOpenDropSubmitted(SolanaCompletedMintTransaction {
                    signature,
                    address: collection_mint.mint,
                })
            },
        })
    }

    fn into_failure(self, tx: SolanaFailedTransaction) -> SolanaNftEvent {
        match self {
            Self::CreateEditionDrop => SolanaNftEvent::CreateEditionDropFailed(tx),
            Self::MintEditionDrop => SolanaNftEvent::MintEditionDropFailed(tx),
            Self::UpdateEditionDrop => SolanaNftEvent::UpdateEditionDropFailed(tx),
            Self::TransferAsset => SolanaNftEvent::TransferAssetFailed(tx),
            Self::RetryCreateEditionDrop => SolanaNftEvent::RetryCreateEditionDropFailed(tx),
            Self::RetryMintEditionDrop => SolanaNftEvent::RetryMintEditionDropFailed(tx),
            Self::CreateCollection => SolanaNftEvent::CreateCollectionFailed(tx),
            Self::RetryCreateCollection => SolanaNftEvent::RetryCreateCollectionFailed(tx),
            Self::UpdateCollection => SolanaNftEvent::UpdateCollectionFailed(tx),
            Self::MintToCollection => SolanaNftEvent::MintToCollectionFailed(tx),
            Self::RetryMintToCollection => SolanaNftEvent::RetryMintToCollectionFailed(tx),
            Self::UpdateCollectionMint => SolanaNftEvent::UpdateCollectionMintFailed(tx),
            Self::RetryUpdateCollectionMint => SolanaNftEvent::RetryUpdateMintFailed(tx),
            Self::SwitchMintCollection => SolanaNftEvent::SwitchMintCollectionFailed(tx),
            Self::CreateOpenDrop => SolanaNftEvent::CreateOpenDropFailed(tx),
            Self::MintOpenDrop => SolanaNftEvent::MintOpenDropFailed(tx),
            Self::UpdateOpenDrop => SolanaNftEvent::UpdateOpenDropFailed(tx),
            Self::RetryCreateOpenDrop => SolanaNftEvent::RetryCreateOpenDropFailed(tx),
            Self::RetryMintOpenDrop => SolanaNftEvent::RetryMintOpenDropFailed(tx),
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
                    Some(NftEvent::SolanaCreateEditionDrop(payload)) => {
                        self.process_nft(
                            EventKind::CreateEditionDrop,
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
                    Some(NftEvent::SolanaMintEditionDrop(payload)) => {
                        self.process_nft(
                            EventKind::MintEditionDrop,
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
                    Some(NftEvent::SolanaUpdateEditionDrop(payload)) => {
                        self.process_nft(
                            EventKind::UpdateEditionDrop,
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
                            self.transfer_asset(&key, payload),
                        )
                        .await
                    },
                    Some(NftEvent::SolanaRetryEditionDrop(payload)) => {
                        self.process_nft(
                            EventKind::RetryCreateEditionDrop,
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
                    Some(NftEvent::SolanaRetryMintEditionDrop(payload)) => {
                        self.process_nft(
                            EventKind::RetryMintEditionDrop,
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
                    Some(NftEvent::SolanaUpdatedCollectionMint(payload)) => {
                        self.process_nft(
                            EventKind::UpdateCollectionMint,
                            &key,
                            self.update_collection_mint(
                                &UncompressedRef(self.solana()),
                                &key,
                                payload,
                            ),
                        )
                        .await
                    },
                    Some(NftEvent::SolanaRetryUpdatedCollectionMint(_)) => {
                        self.process_nft(
                            EventKind::RetryUpdateCollectionMint,
                            &key,
                            self.retry_update_collection_mint(
                                &UncompressedRef(self.solana()),
                                &key,
                            ),
                        )
                        .await
                    },
                    Some(NftEvent::SolanaSwitchMintCollectionRequested(payload)) => {
                        self.process_nft(
                            EventKind::SwitchMintCollection,
                            &key,
                            self.switch_mint_collection(&UncompressedRef(self.solana()), payload),
                        )
                        .await
                    },
                    Some(NftEvent::SolanaCreateOpenDrop(payload)) => {
                        self.process_nft(
                            EventKind::CreateOpenDrop,
                            &key,
                            self.create_collection(&UncompressedRef(self.solana()), &key, payload),
                        )
                        .await
                    },
                    Some(NftEvent::SolanaMintOpenDrop(payload)) => {
                        self.process_nft(
                            EventKind::MintOpenDrop,
                            &key,
                            self.mint_to_collection(&key, payload),
                        )
                        .await
                    },
                    Some(NftEvent::SolanaUpdateOpenDrop(payload)) => {
                        self.process_nft(
                            EventKind::UpdateOpenDrop,
                            &key,
                            self.update_collection(&UncompressedRef(self.solana()), &key, payload),
                        )
                        .await
                    },
                    Some(NftEvent::SolanaRetryOpenDrop(payload)) => {
                        self.process_nft(
                            EventKind::RetryCreateOpenDrop,
                            &key,
                            self.retry_create_collection(
                                &UncompressedRef(self.solana()),
                                &key,
                                payload,
                            ),
                        )
                        .await
                    },
                    Some(NftEvent::SolanaRetryMintOpenDrop(payload)) => {
                        self.process_nft(
                            EventKind::RetryMintOpenDrop,
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
                    Some(TreasuryEvent::SolanaCreateEditionDropSigned(res)) => {
                        self.process_treasury(EventKind::CreateEditionDrop, key, res)
                            .await
                    },
                    Some(TreasuryEvent::SolanaMintEditionDropSigned(res)) => {
                        self.process_treasury(EventKind::MintEditionDrop, key, res)
                            .await
                    },
                    Some(TreasuryEvent::SolanaUpdateEditionDropSigned(res)) => {
                        self.process_treasury(EventKind::UpdateEditionDrop, key, res)
                            .await
                    },
                    Some(TreasuryEvent::SolanaTransferAssetSigned(res)) => {
                        self.process_treasury(EventKind::TransferAsset, key, res)
                            .await
                    },
                    Some(TreasuryEvent::SolanaRetryCreateEditionDropSigned(res)) => {
                        self.process_treasury(EventKind::RetryCreateEditionDrop, key, res)
                            .await
                    },
                    Some(TreasuryEvent::SolanaRetryMintEditionDropSigned(res)) => {
                        self.process_treasury(EventKind::RetryMintEditionDrop, key, res)
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
                    Some(TreasuryEvent::SolanaUpdateCollectionMintSigned(res)) => {
                        self.process_treasury(EventKind::UpdateCollectionMint, key, res)
                            .await
                    },
                    Some(TreasuryEvent::SolanaRetryUpdateCollectionMintSigned(res)) => {
                        self.process_treasury(EventKind::RetryUpdateCollectionMint, key, res)
                            .await
                    },
                    Some(TreasuryEvent::SolanaSwitchMintCollectionSigned(res)) => {
                        self.process_treasury(EventKind::SwitchMintCollection, key, res)
                            .await
                    },
                    Some(TreasuryEvent::SolanaCreateOpenDropSigned(res)) => {
                        self.process_treasury(EventKind::CreateOpenDrop, key, res)
                            .await
                    },
                    Some(TreasuryEvent::SolanaMintOpenDropSigned(res)) => {
                        self.process_treasury(EventKind::MintOpenDrop, key, res)
                            .await
                    },
                    Some(TreasuryEvent::SolanaUpdateOpenDropSigned(res)) => {
                        self.process_treasury(EventKind::UpdateOpenDrop, key, res)
                            .await
                    },
                    Some(TreasuryEvent::SolanaRetryCreateOpenDropSigned(res)) => {
                        self.process_treasury(EventKind::RetryCreateOpenDrop, key, res)
                            .await
                    },
                    Some(TreasuryEvent::SolanaRetryMintOpenDropSigned(res)) => {
                        self.process_treasury(EventKind::RetryMintOpenDrop, key, res)
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
        let conn = self.db.get();
        self.producer
            .send(
                Some(&SolanaNftEvents {
                    event: Some(kind.into_success(conn, self.solana(), key, sig).await?),
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
        let conn = self.db.get();
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

        Collection::create(conn, collection.into()).await?;

        Ok(tx.into())
    }

    async fn mint_to_collection(
        &self,
        key: &SolanaNftEventKey,
        payload: MintMetaplexMetadataTransaction,
    ) -> ProcessResult<SolanaPendingTransaction> {
        let conn = self.db.get();
        let id = Uuid::parse_str(&key.id.clone())?;
        let collection_id = Uuid::parse_str(&payload.collection_id)?;
        let collection = Collection::find_by_id(conn, collection_id)
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

            CompressionLeaf::create(conn, compression_leaf).await?;

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

        CollectionMint::create(conn, collection_mint).await?;

        Ok(tx.into())
    }

    async fn mint_drop<B: MintBackend<MintMetaplexEditionTransaction, MintEditionAddresses>>(
        &self,
        backend: &B,
        key: &SolanaNftEventKey,
        payload: MintMetaplexEditionTransaction,
    ) -> ProcessResult<SolanaPendingTransaction> {
        let conn = self.db.get();
        let id = Uuid::parse_str(&key.id.clone())?;
        let collection_id = Uuid::parse_str(&payload.collection_id)?;
        let collection = Collection::find_by_id(conn, collection_id)
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

        CollectionMint::create(conn, collection_mint).await?;

        Ok(tx.into())
    }

    async fn update_collection<B: CollectionBackend>(
        &self,
        backend: &B,
        key: &SolanaNftEventKey,
        payload: MetaplexMasterEditionTransaction,
    ) -> ProcessResult<SolanaPendingTransaction> {
        let conn = self.db.get();
        let collection_id = Uuid::parse_str(&key.id.clone())?;
        let collection = Collection::find_by_id(conn, collection_id)
            .await?
            .ok_or(ProcessorErrorKind::RecordNotFound)?;

        let tx = backend
            .update(&collection, payload)
            .map_err(ProcessorErrorKind::Solana)?;

        Ok(tx.into())
    }

    async fn update_collection_mint<B: CollectionBackend>(
        &self,
        backend: &B,
        key: &SolanaNftEventKey,
        payload: UpdateSolanaMintPayload,
    ) -> ProcessResult<SolanaPendingTransaction> {
        let collection_id = Uuid::parse_str(&payload.collection_id)?;
        let collection = Collection::find_by_id(self.db.get(), collection_id)
            .await?
            .ok_or(ProcessorErrorKind::RecordNotFound)?;
        let mint = CollectionMint::find_by_id(self.db.get(), payload.mint_id.parse()?)
            .await?
            .ok_or(ProcessorErrorKind::RecordNotFound)?;

        let tx = backend
            .update_mint(&collection, &mint, payload)
            .map_err(ProcessorErrorKind::Solana)?;

        let UpdateCollectionMintAddresses {
            payer,
            metadata,
            update_authority,
        } = tx.addresses.clone();
        let msg_bytes = tx.serialized_message.clone();

        let revision = update_revisions::ActiveModel {
            id: Set(key.id.parse()?),
            mint_id: Set(mint.id),
            serialized_message: Set(msg_bytes),
            payer: Set(payer.to_string()),
            metadata: Set(metadata.to_string()),
            update_authority: Set(update_authority.to_string()),
        };

        revision.insert(self.db.get()).await?;

        Ok(tx.into())
    }

    async fn retry_update_collection_mint<B: CollectionBackend>(
        &self,
        backend: &B,
        key: &SolanaNftEventKey,
    ) -> ProcessResult<SolanaPendingTransaction> {
        let revision = update_revisions::Entity::find_by_id(Uuid::from_str(&key.id)?)
            .one(self.db.get())
            .await?
            .ok_or(ProcessorErrorKind::RecordNotFound)?;

        let tx = backend
            .retry_update_mint(&revision)
            .map_err(ProcessorErrorKind::Solana)?;

        Ok(tx.into())
    }

    async fn transfer_asset(
        &self,
        _key: &SolanaNftEventKey,
        payload: TransferMetaplexAssetTransaction,
    ) -> ProcessResult<SolanaPendingTransaction> {
        let conn = self.db.get();
        let collection_mint_id = Uuid::parse_str(&payload.collection_mint_id.clone())?;
        let collection_mint = CollectionMint::find_by_id(conn, collection_mint_id).await?;

        if let Some(collection_mint) = collection_mint {
            let backend = &UncompressedRef(self.solana());

            let tx = backend
                .transfer(&collection_mint, payload)
                .await
                .map_err(ProcessorErrorKind::Solana)?;

            return Ok(tx.into());
        }

        let compression_leaf = CompressionLeaf::find_by_id(conn, collection_mint_id)
            .await?
            .ok_or(ProcessorErrorKind::RecordNotFound)?;

        let backend = &CompressedRef(self.solana());

        let tx = backend
            .transfer(&compression_leaf, payload)
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
        let conn = self.db.get();
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
        let collection = Collection::find_by_id(conn, collection_id)
            .await?
            .ok_or(ProcessorErrorKind::RecordNotFound)?;

        let mut collection: collections::ActiveModel = collection.into();

        collection.metadata = Set(metadata.to_string());
        collection.associated_token_account = Set(associated_token_account.to_string());
        collection.mint = Set(mint.to_string());
        collection.master_edition = Set(master_edition.to_string());
        collection.update_authority = Set(update_authority.to_string());
        collection.owner = Set(owner.to_string());

        Collection::update(conn, collection).await?;

        Ok(tx.into())
    }

    async fn switch_mint_collection<B: CollectionBackend>(
        &self,
        backend: &B,
        payload: SwitchCollectionPayload,
    ) -> ProcessResult<SolanaPendingTransaction> {
        let conn = self.db.get();

        let (mint, collection) =
            CollectionMint::find_by_id_with_collection(conn, payload.mint_id.parse()?)
                .await?
                .ok_or(ProcessorErrorKind::RecordNotFound)?;

        let collection = collection.ok_or(ProcessorErrorKind::RecordNotFound)?;

        let new_collection = Collection::find_by_id(conn, payload.collection_id.parse()?)
            .await?
            .ok_or(ProcessorErrorKind::RecordNotFound)?;

        let tx = backend
            .switch(&mint, &collection, &new_collection)
            .map_err(ProcessorErrorKind::Solana)?;

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
        let conn = self.db.get();
        let id = Uuid::parse_str(&key.id.clone())?;

        let (collection_mint, collection) = CollectionMint::find_by_id_with_collection(conn, id)
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

        CollectionMint::update(conn, collection_mint).await?;

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
        let conn = self.db.get();
        let id = Uuid::parse_str(&key.id.clone())?;

        let (collection_mint, collection) = CollectionMint::find_by_id_with_collection(conn, id)
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

        CollectionMint::update(conn, collection_mint).await?;

        Ok(tx.into())
    }
}
