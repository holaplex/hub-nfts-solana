//! `SeaORM` Entity. Generated by sea-orm-codegen 0.10.5

use sea_orm::entity::prelude::*;

// TODO: ensure collection mint is updated by indexer when transfer instruction processed by the indexer
#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq, Default)]
#[sea_orm(table_name = "collection_mints")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: Uuid,
    pub collection_id: Uuid,
    #[sea_orm(column_type = "Text")]
    pub mint: String,
    #[sea_orm(column_type = "Text")]
    pub owner: String,
    #[sea_orm(column_type = "Text", nullable)]
    pub associated_token_account: Option<String>,
    pub created_at: DateTime,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::editions::Entity",
        from = "Column::CollectionId",
        to = "super::editions::Column::Id",
        on_update = "Cascade",
        on_delete = "Cascade"
    )]
    Editions,
    #[sea_orm(
        belongs_to = "super::certified_collections::Entity",
        from = "Column::CollectionId",
        to = "super::certified_collections::Column::Id",
        on_update = "Cascade",
        on_delete = "Cascade"
    )]
    CertifiedCollections,
}

impl Related<super::editions::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Editions.def()
    }
}

impl Related<super::certified_collections::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::CertifiedCollections.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
