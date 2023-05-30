use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

use crate::m20230529_134752_create_collections_table::Collections;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(CollectionMints::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(CollectionMints::Id)
                            .uuid()
                            .not_null()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(CollectionMints::CollectionId)
                            .uuid()
                            .not_null(),
                    )
                    .col(ColumnDef::new(CollectionMints::Mint).text().not_null())
                    .col(ColumnDef::new(CollectionMints::Owner).text().not_null())
                    .col(
                        ColumnDef::new(CollectionMints::AssociatedTokenAccount)
                            .text()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(CollectionMints::CreatedAt)
                            .timestamp()
                            .not_null()
                            .extra("default now()".to_string()),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk-drops_collections_id")
                            .from(CollectionMints::Table, CollectionMints::CollectionId)
                            .to(Collections::Table, Collections::Id)
                            .on_delete(ForeignKeyAction::Cascade)
                            .on_update(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                IndexCreateStatement::new()
                    .name("collection-mints_associated_token_account_idx")
                    .table(CollectionMints::Table)
                    .col(CollectionMints::AssociatedTokenAccount)
                    .index_type(IndexType::Hash)
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                IndexCreateStatement::new()
                    .name("collection-mints_address_idx")
                    .table(CollectionMints::Table)
                    .col(CollectionMints::Mint)
                    .index_type(IndexType::Hash)
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                IndexCreateStatement::new()
                    .name("collection-mints_owner_idx")
                    .table(CollectionMints::Table)
                    .col(CollectionMints::Owner)
                    .index_type(IndexType::Hash)
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(CollectionMints::Table).to_owned())
            .await
    }
}

#[derive(Iden)]
pub enum CollectionMints {
    Table,
    Id,
    CollectionId,
    Mint,
    Owner,
    AssociatedTokenAccount,
    CreatedAt,
}
