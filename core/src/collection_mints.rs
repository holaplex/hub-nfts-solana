use holaplex_hub_nfts_solana_entity::{
    collection_mints::{ActiveModel, Column, Entity, Model, Relation},
    editions,
};
use sea_orm::{prelude::*, JoinType, QuerySelect, Set};

use crate::db::Connection;

pub struct CollectionMint;

impl CollectionMint {
    pub async fn create(db: &Connection, model: Model) -> Result<Model, DbErr> {
        let conn = db.get();

        let active_model: ActiveModel = model.into();

        active_model.insert(conn).await
    }

    pub async fn find_by_id(db: &Connection, id: Uuid) -> Result<Option<Model>, DbErr> {
        let conn = db.get();

        Entity::find().filter(Column::Id.eq(id)).one(conn).await
    }

    pub async fn update_owner_and_ata(
        db: &Connection,
        model: &Model,
        owner: String,
        ata: String,
    ) -> Result<Model, DbErr> {
        let conn = db.get();

        let mut active_model: ActiveModel = model.clone().into();
        active_model.owner = Set(owner);
        active_model.associated_token_account = Set(Some(ata));
        active_model.update(conn).await
    }

    pub async fn find_by_ata(db: &Connection, ata: String) -> Result<Option<Model>, DbErr> {
        let conn = db.get();

        Entity::find()
            .filter(Column::AssociatedTokenAccount.eq(ata))
            .one(conn)
            .await
    }

    pub async fn find_by_id_with_edition(
        db: &Connection,
        id: Uuid,
    ) -> Result<Option<(Model, Option<editions::Model>)>, DbErr> {
        let conn = db.get();

        Entity::find()
            .join(JoinType::InnerJoin, Relation::Editions.def())
            .find_also_related(editions::Entity)
            .filter(Column::Id.eq(id))
            .one(conn)
            .await
    }

    pub async fn update(db: &Connection, model: ActiveModel) -> Result<Model, DbErr> {
        let conn = db.get();

        model.update(conn).await
    }
}
