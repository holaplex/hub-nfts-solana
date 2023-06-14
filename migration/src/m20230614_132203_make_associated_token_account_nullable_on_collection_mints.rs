use sea_orm_migration::prelude::*;

use crate::m20230530_131917_create_collection_mints_table::CollectionMints;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(CollectionMints::Table)
                    .modify_column(ColumnDef::new(CollectionMints::AssociatedTokenAccount).null())
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(CollectionMints::Table)
                    .modify_column(
                        ColumnDef::new(CollectionMints::AssociatedTokenAccount).not_null(),
                    )
                    .to_owned(),
            )
            .await
    }
}
