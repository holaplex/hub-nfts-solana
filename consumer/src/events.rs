use holaplex_hub_nfts_solana_core::{db::Connection, Collection, CollectionMint};
use holaplex_hub_nfts_solana_entity::{collection_mints, collections};
use hub_core::{anyhow::Error, prelude::*, producer::Producer, thiserror::Error, uuid::Uuid};

use crate::{
    proto::{
        solana_events::Event::{
            CreateDrop, MintDrop, RetryDrop, RetryMintDrop, TransferAsset, UpdateDrop,
        },
        solana_nft_events::Event::{
            CreateDropSubmitted, MintDropSubmitted, RetryCreateDropSubmitted,
            RetryMintDropSubmitted, SignCreateDrop, SignMintDrop, SignTransferAsset,
            SignUpdateDrop, TransferAssetSubmitted, UpdateDropSubmitted,
        },
        treasury_events::{
            signed_transaction::Transaction, signed_transaction_event::Event, Event::MessageSigned,
            SignedTransaction, SignedTransactionEvent,
        },
        MetaplexMasterEditionTransaction, MintMetaplexEditionTransaction, NftEventKey,
        SolanaNftEventKey, SolanaNftEvents, SolanaTransaction, SolanaTransactionResponse,
        TransferMetaplexAssetTransaction, TreasuryEventKey, TreasuryEvents,
    },
    solana::{MasterEditionAddresses, Solana},
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
            Services::Nfts(key, e) => match e.event {
                Some(CreateDrop(payload)) => self.create_drop(key, payload).await,
                Some(MintDrop(payload)) => self.mint_drop(key, payload).await,
                Some(UpdateDrop(payload)) => self.update_drop(key, payload).await,
                Some(TransferAsset(payload)) => self.transfer_asset(key, payload).await,
                Some(RetryDrop(_payload)) => todo!(),
                Some(RetryMintDrop(_payload)) => todo!(),
                None => Ok(()),
            },
            Services::Treasury(key, e) => match e.event {
                Some(MessageSigned(SignedTransactionEvent { event })) => {
                    let event = event.ok_or(ProcessorError::MessageNotFound)?;
                    let key: SolanaNftEventKey = TreasuryEventKey::try_into(key)?;

                    match event {
                        Event::CreateDrop(SignedTransaction { transaction }) => {
                            let transaction = transaction.ok_or(ProcessorError::MessageNotFound)?;

                            let payload = match transaction {
                                Transaction::Solana(payload) => payload,
                                _ => return Ok(()),
                            };

                            let signature = self.solana.submit_transaction(payload.clone())?;

                            self.create_drop_submitted(key, signature).await?;

                            Ok(())
                        },
                        Event::UpdateDrop(SignedTransaction { transaction }) => {
                            let transaction = transaction.ok_or(ProcessorError::MessageNotFound)?;

                            let payload = match transaction {
                                Transaction::Solana(payload) => payload,
                                _ => return Ok(()),
                            };

                            let signature = self.solana.submit_transaction(payload.clone())?;

                            self.update_drop_submitted(key, signature).await?;

                            Ok(())
                        },
                        Event::MintDrop(SignedTransaction { transaction }) => {
                            let transaction = transaction.ok_or(ProcessorError::MessageNotFound)?;

                            let payload = match transaction {
                                Transaction::Solana(payload) => payload,
                                _ => return Ok(()),
                            };

                            let signature = self.solana.submit_transaction(payload.clone())?;

                            self.mint_drop_submitted(key, signature).await?;

                            Ok(())
                        },
                        Event::TransferAsset(SignedTransaction { transaction }) => {
                            let transaction = transaction.ok_or(ProcessorError::MessageNotFound)?;

                            let payload = match transaction {
                                Transaction::Solana(payload) => payload,
                                _ => return Ok(()),
                            };

                            let signature = self.solana.submit_transaction(payload.clone())?;

                            self.transfer_asset_submitted(key, signature).await?;

                            Ok(())
                        },
                        Event::RetryCreateDrop(SignedTransaction { transaction }) => {
                            let transaction = transaction.ok_or(ProcessorError::MessageNotFound)?;

                            let payload = match transaction {
                                Transaction::Solana(payload) => payload,
                                _ => return Ok(()),
                            };

                            let signature = self.solana.submit_transaction(payload.clone())?;

                            self.retry_create_drop_submitted(key, signature).await?;

                            Ok(())
                        },
                        Event::RetryMintDrop(SignedTransaction { transaction }) => {
                            let transaction = transaction.ok_or(ProcessorError::MessageNotFound)?;

                            let payload = match transaction {
                                Transaction::Solana(payload) => payload,
                                _ => return Ok(()),
                            };

                            let signature = self.solana.submit_transaction(payload.clone())?;

                            self.retry_mint_drop_submitted(key, signature).await?;

                            Ok(())
                        },
                    }
                },
                _ => Ok(()),
            },
        }
    }

    async fn create_drop(
        &self,
        key: NftEventKey,
        payload: MetaplexMasterEditionTransaction,
    ) -> Result<()> {
        let tx = self.solana.create(payload.clone()).await?;

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
                    event: Some(SignCreateDrop(SolanaTransaction {
                        serialized_message: tx.serialized_message,
                        signed_message_signatures: tx.signed_message_signatures,
                    })),
                }),
                Some(&key.into()),
            )
            .await?;

        Ok(())
    }

    async fn mint_drop(
        &self,
        key: NftEventKey,
        payload: MintMetaplexEditionTransaction,
    ) -> Result<()> {
        let MintMetaplexEditionTransaction { collection_id, .. } = payload.clone();
        let id = Uuid::parse_str(&key.id.clone())?;
        let collection_id = Uuid::parse_str(&collection_id)?;

        let collection = Collection::find_by_id(&self.db, collection_id)
            .await?
            .ok_or(ProcessorError::RecordNotFound)?;

        let tx = self.solana.mint(collection, payload).await?;

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
                    event: Some(SignMintDrop(SolanaTransaction {
                        serialized_message: tx.serialized_message,
                        signed_message_signatures: tx.signed_message_signatures,
                    })),
                }),
                Some(&key.into()),
            )
            .await?;

        Ok(())
    }

    async fn update_drop(
        &self,
        key: NftEventKey,
        payload: MetaplexMasterEditionTransaction,
    ) -> Result<()> {
        let MetaplexMasterEditionTransaction { collection_id, .. } = payload.clone();
        let collection_id = Uuid::parse_str(&collection_id)?;
        let collection = Collection::find_by_id(&self.db, collection_id)
            .await?
            .ok_or(ProcessorError::RecordNotFound)?;

        let tx = self.solana.update(collection, payload).await?;

        self.producer
            .send(
                Some(&SolanaNftEvents {
                    event: Some(SignUpdateDrop(SolanaTransaction {
                        serialized_message: tx.serialized_message,
                        signed_message_signatures: tx.signed_message_signatures,
                    })),
                }),
                Some(&key.into()),
            )
            .await?;

        Ok(())
    }

    async fn transfer_asset(
        &self,
        key: NftEventKey,
        payload: TransferMetaplexAssetTransaction,
    ) -> Result<()> {
        let collection_mint_id = Uuid::parse_str(&payload.collection_mint_id.clone())?;
        let collection_mint = CollectionMint::find_by_id(&self.db, collection_mint_id)
            .await?
            .ok_or(ProcessorError::RecordNotFound)?;

        let tx = self.solana.transfer(collection_mint, payload).await?;

        self.producer
            .send(
                Some(&SolanaNftEvents {
                    event: Some(SignTransferAsset(SolanaTransaction {
                        serialized_message: tx.serialized_message,
                        signed_message_signatures: tx.signed_message_signatures,
                    })),
                }),
                Some(&key.into()),
            )
            .await?;

        Ok(())
    }

    async fn create_drop_submitted(&self, key: SolanaNftEventKey, signature: String) -> Result<()> {
        self.producer
            .send(
                Some(&SolanaNftEvents {
                    event: Some(CreateDropSubmitted(SolanaTransactionResponse {
                        signature: Some(signature),
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
                    event: Some(UpdateDropSubmitted(SolanaTransactionResponse {
                        signature: Some(signature),
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
                    event: Some(MintDropSubmitted(SolanaTransactionResponse {
                        signature: Some(signature),
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
                    event: Some(TransferAssetSubmitted(SolanaTransactionResponse {
                        signature: Some(signature),
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
                    event: Some(RetryCreateDropSubmitted(SolanaTransactionResponse {
                        signature: Some(signature),
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
                    event: Some(RetryMintDropSubmitted(SolanaTransactionResponse {
                        signature: Some(signature),
                    })),
                }),
                Some(&key),
            )
            .await?;

        Ok(())
    }
}

impl TryFrom<TreasuryEventKey> for SolanaNftEventKey {
    type Error = Error;

    fn try_from(key: TreasuryEventKey) -> Result<Self, Self::Error> {
        let TreasuryEventKey {
            user_id,
            id,
            project_id,
        } = key;
        let project_id = project_id.ok_or(anyhow!("no project id"))?;

        Ok(Self {
            user_id,
            project_id,
            id,
        })
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
            user_id,
            project_id,
            id,
        }
    }
}
