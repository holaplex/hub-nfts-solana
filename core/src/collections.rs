use holaplex_hub_nfts_solana_entity::collections::{ActiveModel, Column, Entity, Model};
use sea_orm::prelude::*;

use crate::db::Connection;

pub struct Collection;

impl Collection {
    pub async fn create(db: &Connection, am: ActiveModel) -> Result<Model, DbErr> {
        let conn = db.get();

        am.insert(conn).await
    }

    pub async fn find_by_id(db: &Connection, id: Uuid) -> Result<Option<Model>, DbErr> {
        let conn = db.get();

        Entity::find().filter(Column::Id.eq(id)).one(conn).await
    }

    pub async fn find_by_mint(db: &Connection, mint: String) -> Result<Option<Model>, DbErr> {
        let conn = db.get();

        Entity::find().filter(Column::Mint.eq(mint)).one(conn).await
    }

    pub async fn update(db: &Connection, model: ActiveModel) -> Result<Model, DbErr> {
        let conn = db.get();

        model.update(conn).await
    }
}
