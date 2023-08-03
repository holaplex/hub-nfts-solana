use holaplex_hub_nfts_solana_entity::compression_leafs::{ActiveModel, Column, Entity, Model};
use sea_orm::prelude::*;

pub struct CompressionLeaf;

impl CompressionLeaf {
    pub async fn create(conn: &DatabaseConnection, model: Model) -> Result<Model, DbErr> {
        let active_model: ActiveModel = model.into();

        active_model.insert(conn).await
    }

    pub async fn find_by_id(conn: &DatabaseConnection, id: Uuid) -> Result<Option<Model>, DbErr> {
        Entity::find().filter(Column::Id.eq(id)).one(conn).await
    }

    pub async fn find_by_asset_id(
        conn: &DatabaseConnection,
        address: String,
    ) -> Result<Option<Model>, DbErr> {
        Entity::find()
            .filter(Column::AssetId.eq(address))
            .one(conn)
            .await
    }

    pub async fn update(conn: &DatabaseConnection, model: ActiveModel) -> Result<Model, DbErr> {
        model.update(conn).await
    }
}
