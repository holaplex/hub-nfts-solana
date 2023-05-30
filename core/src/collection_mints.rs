use crate::db::Connection;
use holaplex_hub_nfts_solana_entity::collection_mints::{ActiveModel, Column, Entity, Model};
use sea_orm::prelude::*;

pub struct CollectionMint;

impl CollectionMint {
    pub async fn create(db: &Connection, model: Model) -> Result<ActiveModel, DbErr> {
        let conn = db.get();

        let active_model: ActiveModel = model.into();

        active_model.save(conn).await
    }

    pub async fn find_by_id(db: &Connection, id: Uuid) -> Result<Option<Model>, DbErr> {
        let conn = db.get();

        Entity::find().filter(Column::Id.eq(id)).one(conn).await
    }
}
