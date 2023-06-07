use holaplex_hub_nfts_solana_entity::collections::{ActiveModel, Column, Entity, Model};
use sea_orm::prelude::*;

use crate::db::Connection;

pub struct Collection;

impl Collection {
    pub async fn create(db: &Connection, model: Model) -> Result<Model, DbErr> {
        let conn = db.get();

        let active_model: ActiveModel = model.into();

        active_model.insert(conn).await
    }

    pub async fn find_by_id(db: &Connection, id: Uuid) -> Result<Option<Model>, DbErr> {
        let conn = db.get();

        Entity::find().filter(Column::Id.eq(id)).one(conn).await
    }
}
