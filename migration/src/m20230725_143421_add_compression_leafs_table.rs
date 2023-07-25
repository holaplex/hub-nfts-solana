use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(CompressionLeafs::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(CompressionLeafs::Id)
                            .uuid()
                            .not_null()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(CompressionLeafs::CollectionId)
                            .uuid()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(CompressionLeafs::MerkleTree)
                            .text()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(CompressionLeafs::TreeAuthority)
                            .text()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(CompressionLeafs::TreeDelegate)
                            .text()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(CompressionLeafs::LeafOwner)
                            .text()
                            .not_null(),
                    )
                    .col(ColumnDef::new(CompressionLeafs::AssetId).text())
                    .col(
                        ColumnDef::new(CompressionLeafs::CreatedAt)
                            .timestamp()
                            .not_null()
                            .extra("default now()".to_string()),
                    )
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(CompressionLeafs::Table).to_owned())
            .await
    }
}

/// Learn more at https://docs.rs/sea-query#iden
#[derive(Iden)]
enum CompressionLeafs {
    Table,
    Id,
    MerkleTree,
    TreeAuthority,
    TreeDelegate,
    LeafOwner,
    AssetId,
    CollectionId,
    CreatedAt,
}
