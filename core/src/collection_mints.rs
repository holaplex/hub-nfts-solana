use holaplex_hub_nfts_solana_entity::{
    collection_mints::{ActiveModel, Column, Entity, Model},
    collections,
};
use sea_orm::{prelude::*, Set};

pub struct CollectionMint;

impl CollectionMint {
    pub async fn create(conn: &DatabaseConnection, model: Model) -> Result<Model, DbErr> {
        let active_model: ActiveModel = model.into();

        active_model.insert(conn).await
    }

    pub async fn find_by_id(conn: &DatabaseConnection, id: Uuid) -> Result<Option<Model>, DbErr> {
        Entity::find().filter(Column::Id.eq(id)).one(conn).await
    }

    pub async fn update_owner_and_ata(
        conn: &DatabaseConnection,
        model: &Model,
        owner: String,
        ata: String,
    ) -> Result<Model, DbErr> {
        let mut active_model: ActiveModel = model.clone().into();
        active_model.owner = Set(owner);
        active_model.associated_token_account = Set(ata);
        active_model.update(conn).await
    }

    pub async fn find_by_ata(
        conn: &DatabaseConnection,
        ata: String,
    ) -> Result<Option<Model>, DbErr> {
        Entity::find()
            .filter(Column::AssociatedTokenAccount.eq(ata))
            .one(conn)
            .await
    }

    pub async fn find_by_id_with_collection(
        conn: &DatabaseConnection,
        id: Uuid,
    ) -> Result<Option<(Model, Option<collections::Model>)>, DbErr> {
        Entity::find()
            .find_also_related(collections::Entity)
            .filter(Column::Id.eq(id))
            .one(conn)
            .await
    }

    pub async fn update(conn: &DatabaseConnection, model: ActiveModel) -> Result<Model, DbErr> {
        model.update(conn).await
    }
}
