use holaplex_hub_nfts_solana_core::{db::Connection, sea_orm::Set, Collection, CollectionMint};
use holaplex_hub_nfts_solana_entity::{collection_mints, collections};
use hub_core::{chrono::Utc, prelude::*, producer::Producer, thiserror::Error, uuid::Uuid};

use crate::{
    proto::{
        nft_events::Event::{
            SolanaCreateDrop, SolanaMintDrop, SolanaRetryDrop, SolanaRetryMintDrop,
            SolanaTransferAsset, SolanaUpdateDrop,
        },
        solana_nft_events::Event::{
            CreateDropFailed, CreateDropSigningRequested, CreateDropSubmitted, MintDropFailed,
            MintDropSigningRequested, MintDropSubmitted, RetryCreateDropFailed,
            RetryCreateDropSigningRequested, RetryCreateDropSubmitted, RetryMintDropFailed,
            RetryMintDropSigningRequested, RetryMintDropSubmitted, TransferAssetFailed,
            TransferAssetSigningRequested, TransferAssetSubmitted, UpdateDropFailed,
            UpdateDropSigningRequested, UpdateDropSubmitted,
        },
        treasury_events::{Event as TreasuryEvent, TransactionStatus},
        MetaplexMasterEditionTransaction, MintMetaplexEditionTransaction, NftEventKey,
        SolanaCompletedMintTransaction, SolanaCompletedTransferTransaction,
        SolanaCompletedUpdateTransaction, SolanaFailedTransaction, SolanaNftEventKey,
        SolanaNftEvents, SolanaPendingTransaction, SolanaTransactionFailureReason,
        TransferMetaplexAssetTransaction, TreasuryEventKey,
    },
    solana::{MasterEditionAddresses, Solana, TransactionResponse},
    Services,
};

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
                let key = SolanaNftEventKey::from(key);

                match e.event {
                    Some(SolanaCreateDrop(payload)) => {
                        let create_drop_result = self.create_drop(key.clone(), payload).await;

                        if create_drop_result.is_err() {
                            self.create_drop_failed(key, SolanaTransactionFailureReason::Assemble)
                                .await?;
                        }

                        Ok(())
                    },
                    Some(SolanaMintDrop(payload)) => {
                        let mint_drop_result = self.mint_drop(key.clone(), payload).await;

                        if mint_drop_result.is_err() {
                            self.mint_drop_failed(key, SolanaTransactionFailureReason::Assemble)
                                .await?;
                        }

                        Ok(())
                    },
                    Some(SolanaUpdateDrop(payload)) => {
                        let update_drop_result = self.update_drop(key.clone(), payload).await;

                        if update_drop_result.is_err() {
                            self.update_drop_failed(key, SolanaTransactionFailureReason::Assemble)
                                .await?;
                        }

                        Ok(())
                    },
                    Some(SolanaTransferAsset(payload)) => {
                        let transfer_asset_result = self.transfer_asset(key.clone(), payload).await;

                        if transfer_asset_result.is_err() {
                            self.transfer_asset_failed(
                                key,
                                SolanaTransactionFailureReason::Assemble,
                            )
                            .await?;
                        }

                        Ok(())
                    },
                    Some(SolanaRetryDrop(payload)) => {
                        let retry_drop_result = self.retry_drop(key.clone(), payload).await;

                        if retry_drop_result.is_err() {
                            self.retry_create_drop_failed(
                                key,
                                SolanaTransactionFailureReason::Assemble,
                            )
                            .await?;
                        }

                        Ok(())
                    },
                    Some(SolanaRetryMintDrop(payload)) => {
                        let retry_mint_drop_result =
                            self.retry_mint_drop(key.clone(), payload).await;

                        if retry_mint_drop_result.is_err() {
                            self.retry_mint_drop_failed(
                                key,
                                SolanaTransactionFailureReason::Assemble,
                            )
                            .await?;
                        }

                        Ok(())
                    },
                    Some(_) | None => Ok(()),
                }
            },
            Services::Treasury(key, e) => {
                let key = SolanaNftEventKey::from(key);

                match e.event {
                    Some(TreasuryEvent::SolanaCreateDropSigned(payload)) => {
                        let status = TransactionStatus::from_i32(payload.status)
                            .ok_or(ProcessorError::TransactionStatusNotFound)?;

                        if status == TransactionStatus::Failed {
                            self.create_drop_failed(key, SolanaTransactionFailureReason::Sign)
                                .await?;

                            return Ok(());
                        }

                        let signature_result = self.solana.submit_transaction(&payload);

                        match signature_result {
                            Ok(signature) => {
                                self.create_drop_submitted(key, signature).await?;
                            },
                            Err(_) => {
                                self.create_drop_failed(
                                    key,
                                    SolanaTransactionFailureReason::Submit,
                                )
                                .await?;
                            },
                        }

                        Ok(())
                    },
                    Some(TreasuryEvent::SolanaUpdateDropSigned(payload)) => {
                        let status = TransactionStatus::from_i32(payload.status)
                            .ok_or(ProcessorError::TransactionStatusNotFound)?;

                        if status == TransactionStatus::Failed {
                            self.update_drop_failed(key, SolanaTransactionFailureReason::Sign)
                                .await?;

                            return Ok(());
                        }

                        let signature_result = self.solana.submit_transaction(&payload);

                        match signature_result {
                            Ok(signature) => {
                                self.update_drop_submitted(key, signature).await?;
                            },
                            Err(_) => {
                                self.update_drop_failed(
                                    key,
                                    SolanaTransactionFailureReason::Submit,
                                )
                                .await?;
                            },
                        }

                        Ok(())
                    },
                    Some(TreasuryEvent::SolanaMintDropSigned(payload)) => {
                        let status = TransactionStatus::from_i32(payload.status)
                            .ok_or(ProcessorError::TransactionStatusNotFound)?;

                        if status == TransactionStatus::Failed {
                            self.mint_drop_failed(key, SolanaTransactionFailureReason::Sign)
                                .await?;

                            return Ok(());
                        }

                        let signature_result = self.solana.submit_transaction(&payload);

                        match signature_result {
                            Ok(signature) => {
                                self.mint_drop_submitted(key, signature).await?;
                            },
                            Err(_) => {
                                self.mint_drop_failed(key, SolanaTransactionFailureReason::Submit)
                                    .await?;
                            },
                        }

                        Ok(())
                    },
                    Some(TreasuryEvent::SolanaTransferAssetSigned(payload)) => {
                        let status = TransactionStatus::from_i32(payload.status)
                            .ok_or(ProcessorError::TransactionStatusNotFound)?;

                        if status == TransactionStatus::Failed {
                            self.transfer_asset_failed(key, SolanaTransactionFailureReason::Sign)
                                .await?;

                            return Ok(());
                        }

                        let signature_result = self.solana.submit_transaction(&payload);

                        match signature_result {
                            Ok(signature) => {
                                self.transfer_asset_submitted(key, signature).await?;
                            },
                            Err(_) => {
                                self.transfer_asset_failed(
                                    key,
                                    SolanaTransactionFailureReason::Submit,
                                )
                                .await?;
                            },
                        }

                        Ok(())
                    },
                    Some(TreasuryEvent::SolanaRetryCreateDropSigned(payload)) => {
                        let status = TransactionStatus::from_i32(payload.status)
                            .ok_or(ProcessorError::TransactionStatusNotFound)?;

                        if status == TransactionStatus::Failed {
                            self.retry_create_drop_failed(
                                key,
                                SolanaTransactionFailureReason::Sign,
                            )
                            .await?;

                            return Ok(());
                        }

                        let signature_result = self.solana.submit_transaction(&payload);

                        match signature_result {
                            Ok(signature) => {
                                self.retry_create_drop_submitted(key, signature).await?;
                            },
                            Err(_) => {
                                self.retry_create_drop_failed(
                                    key,
                                    SolanaTransactionFailureReason::Submit,
                                )
                                .await?;
                            },
                        }

                        Ok(())
                    },
                    Some(TreasuryEvent::SolanaRetryMintDropSigned(payload)) => {
                        let status = TransactionStatus::from_i32(payload.status)
                            .ok_or(ProcessorError::TransactionStatusNotFound)?;

                        if status == TransactionStatus::Failed {
                            self.retry_mint_drop_failed(key, SolanaTransactionFailureReason::Sign)
                                .await?;

                            return Ok(());
                        }
                        let signature_result = self.solana.submit_transaction(&payload);

                        match signature_result {
                            Ok(signature) => {
                                self.retry_mint_drop_submitted(key, signature).await?;
                            },
                            Err(_) => {
                                self.retry_mint_drop_failed(
                                    key,
                                    SolanaTransactionFailureReason::Submit,
                                )
                                .await?;
                            },
                        }

                        Ok(())
                    },
                    _ => Ok(()),
                }
            },
        }
    }

    async fn create_drop(
        &self,
        key: SolanaNftEventKey,
        payload: MetaplexMasterEditionTransaction,
    ) -> Result<()> {
        let tx = self.solana.create(payload.clone())?;

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

        self.producer
            .send(
                Some(&SolanaNftEvents {
                    event: Some(CreateDropSigningRequested(tx.into())),
                }),
                Some(&key),
            )
            .await?;

        Ok(())
    }

    async fn create_drop_failed(
        &self,
        key: SolanaNftEventKey,
        reason: SolanaTransactionFailureReason,
    ) -> Result<()> {
        self.producer
            .send(
                Some(&SolanaNftEvents {
                    event: Some(CreateDropFailed(SolanaFailedTransaction {
                        reason: reason as i32,
                    })),
                }),
                Some(&key),
            )
            .await?;

        Ok(())
    }

    async fn retry_drop(
        &self,
        key: SolanaNftEventKey,
        payload: MetaplexMasterEditionTransaction,
    ) -> Result<()> {
        let tx = self.solana.create(payload.clone())?;

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
            .ok_or(ProcessorError::RecordNotFound)?;

        let mut collection: collections::ActiveModel = collection.into();

        collection.master_edition = Set(metadata.to_string());
        collection.associated_token_account = Set(associated_token_account.to_string());
        collection.mint = Set(mint.to_string());
        collection.master_edition = Set(master_edition.to_string());
        collection.update_authority = Set(update_authority.to_string());
        collection.owner = Set(owner.to_string());

        Collection::update(&self.db, collection).await?;

        self.producer
            .send(
                Some(&SolanaNftEvents {
                    event: Some(RetryCreateDropSigningRequested(tx.into())),
                }),
                Some(&key),
            )
            .await?;

        Ok(())
    }

    async fn retry_create_drop_failed(
        &self,
        key: SolanaNftEventKey,
        reason: SolanaTransactionFailureReason,
    ) -> Result<()> {
        self.producer
            .send(
                Some(&SolanaNftEvents {
                    event: Some(RetryCreateDropFailed(SolanaFailedTransaction {
                        reason: reason as i32,
                    })),
                }),
                Some(&key),
            )
            .await?;

        Ok(())
    }

    async fn mint_drop(
        &self,
        key: SolanaNftEventKey,
        payload: MintMetaplexEditionTransaction,
    ) -> Result<()> {
        let id = Uuid::parse_str(&key.id.clone())?;
        let collection_id = Uuid::parse_str(&payload.collection_id)?;
        let collection = Collection::find_by_id(&self.db, collection_id)
            .await?
            .ok_or(ProcessorError::RecordNotFound)?;

        // TODO: the collection mint record may fail to be created if this fails. Need to handle upserting the record in retry mint.
        let tx = self.solana.mint(&collection, payload)?;

        let collection_mint = collection_mints::Model {
            id,
            collection_id: collection.id,
            mint: tx.addresses.mint.to_string(),
            owner: tx.addresses.recipient.to_string(),
            associated_token_account: Some(tx.addresses.associated_token_account.to_string()),
            created_at: Utc::now().naive_utc(),
        };

        CollectionMint::create(&self.db, collection_mint).await?;

        self.producer
            .send(
                Some(&SolanaNftEvents {
                    event: Some(MintDropSigningRequested(tx.into())),
                }),
                Some(&key),
            )
            .await?;

        Ok(())
    }

    async fn mint_drop_failed(
        &self,
        key: SolanaNftEventKey,
        reason: SolanaTransactionFailureReason,
    ) -> Result<()> {
        self.producer
            .send(
                Some(&SolanaNftEvents {
                    event: Some(MintDropFailed(SolanaFailedTransaction {
                        reason: reason as i32,
                    })),
                }),
                Some(&key),
            )
            .await?;

        Ok(())
    }

    async fn retry_mint_drop(
        &self,
        key: SolanaNftEventKey,
        payload: MintMetaplexEditionTransaction,
    ) -> Result<()> {
        let id = Uuid::parse_str(&key.id.clone())?;

        let (collection_mint, collection) =
            CollectionMint::find_by_id_with_collection(&self.db, id)
                .await?
                .ok_or(ProcessorError::RecordNotFound)?;

        let collection = collection.ok_or(ProcessorError::RecordNotFound)?;

        let tx = self.solana.mint(&collection, payload)?;

        let mut collection_mint: collection_mints::ActiveModel = collection_mint.into();

        collection_mint.mint = Set(tx.addresses.mint.to_string());
        collection_mint.owner = Set(tx.addresses.recipient.to_string());
        collection_mint.associated_token_account =
            Set(Some(tx.addresses.associated_token_account.to_string()));

        CollectionMint::update(&self.db, collection_mint).await?;

        self.producer
            .send(
                Some(&SolanaNftEvents {
                    event: Some(RetryMintDropSigningRequested(tx.into())),
                }),
                Some(&key),
            )
            .await?;

        Ok(())
    }

    async fn retry_mint_drop_failed(
        &self,
        key: SolanaNftEventKey,
        reason: SolanaTransactionFailureReason,
    ) -> Result<()> {
        self.producer
            .send(
                Some(&SolanaNftEvents {
                    event: Some(RetryMintDropFailed(SolanaFailedTransaction {
                        reason: reason as i32,
                    })),
                }),
                Some(&key),
            )
            .await?;

        Ok(())
    }

    async fn update_drop(
        &self,
        key: SolanaNftEventKey,
        payload: MetaplexMasterEditionTransaction,
    ) -> Result<()> {
        let collection_id = Uuid::parse_str(&key.id.clone())?;
        let collection = Collection::find_by_id(&self.db, collection_id)
            .await?
            .ok_or(ProcessorError::RecordNotFound)?;

        let tx = self.solana.update(&collection, payload)?;

        self.producer
            .send(
                Some(&SolanaNftEvents {
                    event: Some(UpdateDropSigningRequested(tx.into())),
                }),
                Some(&key),
            )
            .await?;

        Ok(())
    }

    async fn update_drop_failed(
        &self,
        key: SolanaNftEventKey,
        reason: SolanaTransactionFailureReason,
    ) -> Result<()> {
        self.producer
            .send(
                Some(&SolanaNftEvents {
                    event: Some(UpdateDropFailed(SolanaFailedTransaction {
                        reason: reason as i32,
                    })),
                }),
                Some(&key),
            )
            .await?;

        Ok(())
    }

    async fn transfer_asset(
        &self,
        key: SolanaNftEventKey,
        payload: TransferMetaplexAssetTransaction,
    ) -> Result<()> {
        let collection_mint_id = Uuid::parse_str(&payload.collection_mint_id.clone())?;
        let collection_mint = CollectionMint::find_by_id(&self.db, collection_mint_id)
            .await?
            .ok_or(ProcessorError::RecordNotFound)?;

        let tx = self.solana.transfer(&collection_mint, payload)?;

        self.producer
            .send(
                Some(&SolanaNftEvents {
                    event: Some(TransferAssetSigningRequested(tx.into())),
                }),
                Some(&key),
            )
            .await?;

        Ok(())
    }

    async fn transfer_asset_failed(
        &self,
        key: SolanaNftEventKey,
        reason: SolanaTransactionFailureReason,
    ) -> Result<()> {
        self.producer
            .send(
                Some(&SolanaNftEvents {
                    event: Some(TransferAssetFailed(SolanaFailedTransaction {
                        reason: reason as i32,
                    })),
                }),
                Some(&key),
            )
            .await?;

        Ok(())
    }

    async fn create_drop_submitted(&self, key: SolanaNftEventKey, signature: String) -> Result<()> {
        let id = Uuid::parse_str(&key.id.clone())?;
        let collection = Collection::find_by_id(&self.db, id)
            .await?
            .ok_or(ProcessorError::RecordNotFound)?;

        self.producer
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
        self.producer
            .send(
                Some(&SolanaNftEvents {
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
        let collection_mint = CollectionMint::find_by_id(&self.db, id)
            .await?
            .ok_or(ProcessorError::RecordNotFound)?;

        self.producer
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
        self.producer
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
        let collection = Collection::find_by_id(&self.db, id)
            .await?
            .ok_or(ProcessorError::RecordNotFound)?;

        self.producer
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
        let collection_mint = CollectionMint::find_by_id(&self.db, id)
            .await?
            .ok_or(ProcessorError::RecordNotFound)?;

        self.producer
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

impl<A> From<TransactionResponse<A>> for SolanaPendingTransaction {
    fn from(
        TransactionResponse {
            serialized_message,
            signatures_or_signers_public_keys,
            ..
        }: TransactionResponse<A>,
    ) -> Self {
        Self {
            serialized_message,
            signatures_or_signers_public_keys,
        }
    }
}
