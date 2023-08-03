use holaplex_hub_nfts_solana_entity::collections::{ActiveModel, Column, Entity, Model};
use sea_orm::prelude::*;

pub struct Collection;

impl Collection {
    pub async fn create(conn: &DatabaseConnection, am: ActiveModel) -> Result<Model, DbErr> {
        am.insert(conn).await
    }

    pub async fn find_by_id(conn: &DatabaseConnection, id: Uuid) -> Result<Option<Model>, DbErr> {
        Entity::find().filter(Column::Id.eq(id)).one(conn).await
    }

    pub async fn find_by_mint(
        conn: &DatabaseConnection,
        mint: String,
    ) -> Result<Option<Model>, DbErr> {
        Entity::find().filter(Column::Mint.eq(mint)).one(conn).await
    }

    pub async fn update(conn: &DatabaseConnection, model: ActiveModel) -> Result<Model, DbErr> {
        model.update(conn).await
    }
}
