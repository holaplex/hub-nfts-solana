use holaplex_hub_nfts_solana_core::{
    db,
    proto::{
        nft_events::Event as NftEvent, solana_nft_events::Event as SolanaNftEvent, Attribute,
        CollectionImport, File, Metadata, SolanaCollectionPayload, SolanaCreator,
        SolanaMintPayload, SolanaNftEventKey, SolanaNftEvents,
    },
    sea_orm::{DbErr, EntityTrait, ModelTrait, Set},
    Collection, Services,
};
use holaplex_hub_nfts_solana_entity::{collection_mints, collections, prelude::CollectionMints};
use hub_core::{
    backon::{ExponentialBuilder, Retryable},
    chrono::Utc,
    futures_util::stream,
    prelude::*,
    producer::{Producer, SendError},
    reqwest, thiserror,
    util::DebugShim,
    uuid::{self, Uuid},
};
use mpl_token_metadata::pda::{find_master_edition_account, find_metadata_account};
use spl_associated_token_account::get_associated_token_address;

use crate::{
    asset_api::{self, Asset, RpcClient},
    solana::Solana,
};

const CONCURRENT_REQUESTS: usize = 64;

#[derive(Debug, thiserror::Error, Triage)]
pub enum ProcessorError {
    #[error("Missing update authority (index 0) on asset")]
    #[transient]
    MissingUpdateAuthority,

    #[error("Error fetching metadata JSON")]
    JsonFetch(#[source] reqwest::Error),
    #[error("JSONRPC error")]
    JsonRpc(#[from] jsonrpsee::core::Error),
    #[error("Invalid UUID")]
    InvalidUuid(#[from] uuid::Error),
    #[error("Invalid conversion from byte slice to public key")]
    #[permanent]
    InvalidPubkey(#[source] std::array::TryFromSliceError),
    #[error("Database error")]
    DbError(#[from] DbErr),
    #[error("Error sending message")]
    SendError(#[from] SendError),
}

type Result<T> = std::result::Result<T, ProcessorError>;

// TODO: could this just be a newtype over events::Processor?
#[derive(Debug, Clone)]
pub struct Processor {
    solana: DebugShim<Solana>,
    db: db::Connection,
    producer: Producer<SolanaNftEvents>,
}

impl Processor {
    pub fn new(solana: Solana, db: db::Connection, producer: Producer<SolanaNftEvents>) -> Self {
        Self {
            solana: DebugShim(solana),
            db,
            producer,
        }
    }

    pub async fn process(&self, msg: &Services) -> Result<Option<()>> {
        match msg {
            Services::Nfts(key, msg) => {
                let key = SolanaNftEventKey::from(key.clone());

                match msg.event {
                    Some(NftEvent::StartedImportingSolanaCollection(ref c)) => {
                        self.process_import(key, c.clone()).await.map(Some)
                    },
                    _ => Ok(None),
                }
            },
            Services::Treasury(..) => Ok(None),
        }
    }

    async fn process_import(
        &self,
        SolanaNftEventKey {
            id,
            project_id,
            user_id,
        }: SolanaNftEventKey,
        CollectionImport { mint_address }: CollectionImport,
    ) -> Result<()> {
        const MAX_LIMIT: u64 = 1000;

        let rpc = &self.solana.0.asset_rpc();
        let conn = self.db.get();

        let mut page = 1;

        let collection = rpc.get_asset(&mint_address).await?;

        let collection_model = Collection::find_by_id(conn, id.parse()?).await?;

        if let Some(collection_model) = collection_model {
            info!(
                "Deleting already indexed collection: {:?}",
                collection_model.id
            );
            collection_model.delete(conn).await?;
        }

        info!("Importing collection: {:?}", collection.id.to_string());

        let collection_model = self
            .index_collection(project_id.clone(), user_id.clone(), collection)
            .await?;

        loop {
            let result = rpc
                .search_assets(vec!["collection", &mint_address], page)
                .await?;

            let mut mints: Vec<collection_mints::ActiveModel> = Vec::new();
            let mut futures = Vec::new();

            for asset in result.items {
                let project_id = project_id.clone();
                let user_id = user_id.clone();

                // Check whether NFT is burned
                if asset.ownership.owner.0.is_empty() {
                    continue;
                }

                info!("Importing mint: {:?}", asset.id.to_string());

                futures.push(self.collection_mint_event(
                    project_id,
                    user_id,
                    collection_model.id,
                    asset,
                ));
            }

            let mut buffered = stream::iter(futures).buffer_unordered(CONCURRENT_REQUESTS);
            while let Some(model) = buffered.next().await {
                mints.push(model?);
            }

            CollectionMints::insert_many(mints).exec(conn).await?;

            if result.total < MAX_LIMIT {
                break;
            }
            page += 1;
        }

        Ok(())
    }

    async fn index_collection(
        &self,
        project_id: String,
        user_id: String,
        collection: Asset,
    ) -> Result<collections::Model> {
        let conn = self.db.get();
        let producer = &self.producer;
        let owner = collection
            .ownership
            .owner
            .try_into()
            .map_err(ProcessorError::InvalidPubkey)?;
        let mint = collection
            .id
            .try_into()
            .map_err(ProcessorError::InvalidPubkey)?;
        let seller_fee_basis_points = collection.royalty.basis_points;

        let json_uri = collection.content.json_uri.clone();
        let json_metadata = Self::get_metadata_json(json_uri.clone()).await?;

        let files: Vec<File> = collection
            .content
            .files
            .map(|fs| fs.iter().map(Into::into).collect())
            .unwrap_or_default();

        let image = json_metadata.image.unwrap_or_default();

        let attributes = json_metadata
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
            .ok_or(ProcessorError::MissingUpdateAuthority)?
            .address;

        let ata = get_associated_token_address(&owner, &mint);
        let (metadata_pubkey, _) = find_metadata_account(&mint);

        let (master_edition, _) = find_master_edition_account(&mint);
        let collection_model = Collection::create(
            conn,
            collections::ActiveModel {
                master_edition: Set(master_edition.to_string()),
                update_authority: Set(update_authority.to_string()),
                associated_token_account: Set(ata.to_string()),
                owner: Set(owner.to_string()),
                mint: Set(mint.to_string()),
                metadata: Set(metadata_pubkey.to_string()),
                ..Default::default()
            },
        )
        .await?;

        producer
            .send(
                Some(&SolanaNftEvents {
                    event: Some(SolanaNftEvent::ImportedExternalCollection(
                        SolanaCollectionPayload {
                            supply: collection.supply.map(|s| s.print_max_supply),
                            mint_address: mint.to_string(),
                            seller_fee_basis_points,
                            creators,
                            metadata: Some(Metadata {
                                name: json_metadata.name,
                                description: json_metadata.description,
                                symbol: json_metadata.symbol.unwrap_or_default(),
                                attributes,
                                uri: collection.content.json_uri,
                                image,
                            }),
                            files,
                            update_authority: update_authority.to_string(),
                        },
                    )),
                }),
                Some(&SolanaNftEventKey {
                    id: collection_model.id.to_string(),
                    project_id,
                    user_id,
                }),
            )
            .await?;

        Ok(collection_model)
    }

    async fn get_metadata_json(uri: String) -> Result<asset_api::Metadata> {
        let json_metadata = (|| async {
            reqwest::get(uri.clone())
                .await?
                .json::<asset_api::Metadata>()
                .await
        })
        .retry(
            &ExponentialBuilder::default()
                .with_jitter()
                .with_min_delay(Duration::from_millis(200))
                .with_max_times(10),
        )
        .await
        .map_err(ProcessorError::JsonFetch)?;

        Ok(json_metadata)
    }

    async fn collection_mint_event(
        &self,
        project_id: String,
        user_id: String,
        collection: Uuid,
        asset: Asset,
    ) -> Result<collection_mints::ActiveModel> {
        let producer = self.producer.clone();
        let owner = asset
            .ownership
            .owner
            .try_into()
            .map_err(ProcessorError::InvalidPubkey)?;
        let mint = asset.id.try_into().map_err(ProcessorError::InvalidPubkey)?;
        let ata = get_associated_token_address(&owner, &mint);
        let seller_fee_basis_points = asset.royalty.basis_points;

        let update_authority = asset
            .authorities
            .get(0)
            .ok_or(ProcessorError::MissingUpdateAuthority)?
            .address
            .clone();

        let json_metadata = Self::get_metadata_json(asset.content.json_uri.clone()).await?;

        let files: Vec<File> = asset
            .content
            .files
            .map(|fs| fs.iter().map(Into::into).collect())
            .unwrap_or_default();

        let image = json_metadata.image.unwrap_or_default();

        let attributes = json_metadata
            .attributes
            .clone()
            .map(|attributes| attributes.iter().map(Into::into).collect::<Vec<_>>())
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

        #[allow(clippy::cast_sign_loss)]
        let uuid = Uuid::from_u64_pair(Utc::now().timestamp_nanos() as u64, rand::random::<u64>());

        let mint_model = collection_mints::Model {
            id: uuid,
            collection_id: collection,
            mint: mint.to_string(),
            owner: owner.to_string(),
            associated_token_account: ata.to_string(),
            created_at: Utc::now().naive_utc(),
        };

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
                            name: json_metadata.name,
                            description: json_metadata.description,
                            symbol: json_metadata.symbol.unwrap_or_default(),
                            attributes,
                            uri: asset.content.json_uri,
                            image,
                        }),
                        files,
                        update_authority: update_authority.to_string(),
                    })),
                }),
                Some(&SolanaNftEventKey {
                    id: uuid.to_string(),
                    user_id,
                    project_id,
                }),
            )
            .await?;

        Ok(mint_model.into())
    }
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
