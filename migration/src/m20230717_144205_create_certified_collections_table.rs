use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(CertifiedCollections::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(CertifiedCollections::Id)
                            .uuid()
                            .not_null()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(CertifiedCollections::UpdateAuthority)
                            .string()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(CertifiedCollections::AssociatedTokenAccount)
                            .string()
                            .not_null(),
                    )
                    .col(ColumnDef::new(CertifiedCollections::Owner).string().not_null())
                    .col(ColumnDef::new(CertifiedCollections::Mint).string().not_null())
                    .col(ColumnDef::new(CertifiedCollections::Metadata).string().not_null())
                    .col(
                        ColumnDef::new(CertifiedCollections::CreatedAt)
                            .timestamp()
                            .not_null()
                            .extra("default now()".to_string()),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                IndexCreateStatement::new()
                    .name("collections-master_edition_idx")
                    .table(CertifiedCollections::Table)
                    .col(CertifiedCollections::MasterEdition)
                    .index_type(IndexType::Hash)
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(CertifiedCollections::Table).to_owned())
            .await
    }
}

#[derive(Iden)]
pub enum CertifiedCollections {
    Table,
    Id,
    AssociatedTokenAccount,
    UpdateAuthority,
    Owner,
    Mint,
    Metadata,
    CreatedAt,
}
