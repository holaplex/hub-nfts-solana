use holaplex_hub_nfts_solana_core::{db::Connection, sea_orm::Set, Collection, CollectionMint};
use holaplex_hub_nfts_solana_entity::{collection_mints, collections};
use hub_core::{prelude::*, producer::Producer, thiserror::Error, uuid::Uuid};

use crate::{
    proto::{
        solana_events::Event::{
            CreateDrop, MintDrop, RetryDrop, RetryMintDrop, TransferAsset, UpdateDrop,
        },
        solana_nft_events::Event::{
            CreateDropSigningRequested, CreateDropSubmitted, MintDropSigningRequested,
            MintDropSubmitted, RetryCreateDropSigningRequested, RetryCreateDropSubmitted,
            RetryMintDropSigningRequested, RetryMintDropSubmitted, TransferAssetSigningRequested,
            TransferAssetSubmitted, UpdateDropSigningRequested, UpdateDropSubmitted,
        },
        treasury_events::Event as TreasuryEvent,
        MetaplexMasterEditionTransaction, MintMetaplexEditionTransaction, NftEventKey,
        SolanaCompletedTransaction, SolanaNftEventKey, SolanaNftEvents, SolanaPendingTransaction,
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
                    Some(CreateDrop(payload)) => self.create_drop(key, payload).await,
                    Some(MintDrop(payload)) => self.mint_drop(key, payload).await,
                    Some(UpdateDrop(payload)) => self.update_drop(key, payload).await,
                    Some(TransferAsset(payload)) => self.transfer_asset(key, payload).await,
                    Some(RetryDrop(payload)) => self.retry_drop(key, payload).await,
                    Some(RetryMintDrop(payload)) => self.retry_mint_drop(key, payload).await,
                    None => Ok(()),
                }
            },
            Services::Treasury(key, e) => {
                let key = SolanaNftEventKey::from(key);

                match e.event {
                    Some(TreasuryEvent::SolanaCreateDropSigned(payload)) => {
                        let signature = self.solana.submit_transaction(&payload)?;

                        self.create_drop_submitted(key, signature).await?;

                        Ok(())
                    },
                    Some(TreasuryEvent::SolanaUpdateDropSigned(payload)) => {
                        let signature = self.solana.submit_transaction(&payload)?;

                        self.update_drop_submitted(key, signature).await?;

                        Ok(())
                    },
                    Some(TreasuryEvent::SolanaMintDropSigned(payload)) => {
                        let signature = self.solana.submit_transaction(&payload)?;

                        self.mint_drop_submitted(key, signature).await?;

                        Ok(())
                    },
                    Some(TreasuryEvent::SolanaTransferAssetSigned(payload)) => {
                        let signature = self.solana.submit_transaction(&payload)?;

                        self.transfer_asset_submitted(key, signature).await?;

                        Ok(())
                    },
                    Some(TreasuryEvent::SolanaRetryCreateDropSigned(payload)) => {
                        let signature = self.solana.submit_transaction(&payload)?;

                        self.retry_create_drop_submitted(key, signature).await?;

                        Ok(())
                    },
                    Some(TreasuryEvent::SolanaRetryMintDropSigned(payload)) => {
                        let signature = self.solana.submit_transaction(&payload)?;

                        self.retry_mint_drop_submitted(key, signature).await?;

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
        let collection_id = payload.collection_id.parse()?;

        let collection = collections::Model {
            id: collection_id,
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

        let collection_id = Uuid::parse_str(&payload.collection_id.clone())?;
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

    async fn mint_drop(
        &self,
        key: SolanaNftEventKey,
        payload: MintMetaplexEditionTransaction,
    ) -> Result<()> {
        let MintMetaplexEditionTransaction { collection_id, .. } = payload.clone();
        let id = Uuid::parse_str(&key.id.clone())?;
        let collection_id = Uuid::parse_str(&collection_id)?;

        let collection = Collection::find_by_id(&self.db, collection_id)
            .await?
            .ok_or(ProcessorError::RecordNotFound)?;

        let tx = self.solana.mint(collection, payload)?;

        let collection_mint = collection_mints::Model {
            id,
            collection_id,
            mint: tx.addresses.mint.to_string(),
            owner: tx.addresses.owner.to_string(),
            associated_token_account: tx.addresses.associated_token_account.to_string(),
            ..Default::default()
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

    async fn retry_mint_drop(
        &self,
        key: SolanaNftEventKey,
        payload: MintMetaplexEditionTransaction,
    ) -> Result<()> {
        let id = Uuid::parse_str(&key.id.clone())?;

        let collection_mint = CollectionMint::find_by_id(&self.db, id)
            .await?
            .ok_or(ProcessorError::RecordNotFound)?;

        // TODO: can find collection_mint and collection in single query
        let collection = Collection::find_by_id(&self.db, collection_mint.collection_id)
            .await?
            .ok_or(ProcessorError::RecordNotFound)?;

        let tx = self.solana.mint(collection, payload)?;

        let mut collection_mint: collection_mints::ActiveModel = collection_mint.into();

        collection_mint.mint = Set(tx.addresses.mint.to_string());
        collection_mint.owner = Set(tx.addresses.owner.to_string());
        collection_mint.associated_token_account =
            Set(tx.addresses.associated_token_account.to_string());

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

    async fn update_drop(
        &self,
        key: SolanaNftEventKey,
        payload: MetaplexMasterEditionTransaction,
    ) -> Result<()> {
        let MetaplexMasterEditionTransaction { collection_id, .. } = payload.clone();
        let collection_id = Uuid::parse_str(&collection_id)?;
        let collection = Collection::find_by_id(&self.db, collection_id)
            .await?
            .ok_or(ProcessorError::RecordNotFound)?;

        let tx = self.solana.update(collection, payload)?;

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

    async fn transfer_asset(
        &self,
        key: SolanaNftEventKey,
        payload: TransferMetaplexAssetTransaction,
    ) -> Result<()> {
        let collection_mint_id = Uuid::parse_str(&payload.collection_mint_id.clone())?;
        let collection_mint = CollectionMint::find_by_id(&self.db, collection_mint_id)
            .await?
            .ok_or(ProcessorError::RecordNotFound)?;

        let tx = self.solana.transfer(collection_mint, payload)?;

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

    async fn create_drop_submitted(&self, key: SolanaNftEventKey, signature: String) -> Result<()> {
        self.producer
            .send(
                Some(&SolanaNftEvents {
                    event: Some(CreateDropSubmitted(SolanaCompletedTransaction {
                        signature,
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
                    event: Some(UpdateDropSubmitted(SolanaCompletedTransaction {
                        signature,
                    })),
                }),
                Some(&key),
            )
            .await?;

        Ok(())
    }

    async fn mint_drop_submitted(&self, key: SolanaNftEventKey, signature: String) -> Result<()> {
        self.producer
            .send(
                Some(&SolanaNftEvents {
                    event: Some(MintDropSubmitted(SolanaCompletedTransaction { signature })),
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
                    event: Some(TransferAssetSubmitted(SolanaCompletedTransaction {
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
        self.producer
            .send(
                Some(&SolanaNftEvents {
                    event: Some(RetryCreateDropSubmitted(SolanaCompletedTransaction {
                        signature,
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
        self.producer
            .send(
                Some(&SolanaNftEvents {
                    event: Some(RetryMintDropSubmitted(SolanaCompletedTransaction {
                        signature,
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
