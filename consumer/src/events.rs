use crate::{
    proto::{
        solana_events::Event::{
            CreateDrop, MintDrop, RetryDrop, RetryMintDrop, TransferAsset, UpdateDrop,
        },
        solana_nft_events::Event::{
            SignCreateDrop, SignMintDrop, SignTransferAsset, SignUpdateDrop,
        },
        MetaplexMasterEditionTransaction, MintMetaplexEditionTransaction, NftEventKey,
        SolanaNftEventKey, SolanaNftEvents, SolanaTransaction, TransferMetaplexAssetTransaction,
    },
    solana::{MasterEditionAddresses, Solana},
    Services,
};
use holaplex_hub_nfts_solana_core::{db::Connection, Collection, CollectionMint};
use holaplex_hub_nfts_solana_entity::{collection_mints, collections};
use hub_core::{prelude::*, producer::Producer, thiserror::Error, uuid::Uuid};

#[derive(Error, Debug)]
pub enum ProcessorError {
    #[error("record not found")]
    RecordNotFound,
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
        }
    }

    async fn create_drop(
        &self,
        NftEventKey { user_id, .. }: NftEventKey,
        payload: MetaplexMasterEditionTransaction,
    ) -> Result<()> {
        let tx = self.solana.create(payload.clone()).await?;

        let MasterEditionAddresses {
            metadata,
            associated_token_account,
            mint,
            master_edition,
            update_authority,
            ..
        } = tx.addresses;
        let id = payload.collection_id.parse()?;

        let collection = collections::Model {
            id,
            master_edition: master_edition.to_string(),
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
                Some(&SolanaNftEventKey {
                    id: id.to_string(),
                    user_id,
                }),
            )
            .await?;

        Ok(())
    }

    async fn mint_drop(
        &self,
        key: NftEventKey,
        payload: MintMetaplexEditionTransaction,
    ) -> Result<()> {
        let NftEventKey { id, user_id } = key;
        let MintMetaplexEditionTransaction { collection_id, .. } = payload.clone();
        let id = Uuid::parse_str(&id)?;
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
                Some(&SolanaNftEventKey {
                    id: id.to_string(),
                    user_id,
                }),
            )
            .await?;

        Ok(())
    }

    async fn update_drop(
        &self,
        key: NftEventKey,
        payload: MetaplexMasterEditionTransaction,
    ) -> Result<()> {
        let NftEventKey { user_id, .. } = key;
        let MetaplexMasterEditionTransaction { collection_id, .. } = payload.clone();
        let collection_id = Uuid::parse_str(&collection_id)?;
        let collection = Collection::find_by_id(&self.db, collection_id)
            .await?
            .ok_or(ProcessorError::RecordNotFound)?;

        let tx = self.solana.update(collection, payload.clone()).await?;

        self.producer
            .send(
                Some(&SolanaNftEvents {
                    event: Some(SignUpdateDrop(SolanaTransaction {
                        serialized_message: tx.serialized_message,
                        signed_message_signatures: tx.signed_message_signatures,
                    })),
                }),
                Some(&SolanaNftEventKey {
                    id: collection_id.to_string(),
                    user_id,
                }),
            )
            .await?;

        Ok(())
    }

    async fn transfer_asset(
        &self,
        key: NftEventKey,
        payload: TransferMetaplexAssetTransaction,
    ) -> Result<()> {
        let NftEventKey { id, user_id } = key;
        let collection_mint_id = Uuid::parse_str(&id)?;
        let collection_mint = CollectionMint::find_by_id(&self.db, collection_mint_id)
            .await?
            .ok_or(ProcessorError::RecordNotFound)?;

        let tx = self
            .solana
            .transfer(collection_mint, payload.clone())
            .await?;

        self.producer
            .send(
                Some(&SolanaNftEvents {
                    event: Some(SignTransferAsset(SolanaTransaction {
                        serialized_message: tx.serialized_message,
                        signed_message_signatures: tx.signed_message_signatures,
                    })),
                }),
                Some(&SolanaNftEventKey { id, user_id }),
            )
            .await?;

        Ok(())
    }
}
