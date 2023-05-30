use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(Collections::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(Collections::Id)
                            .uuid()
                            .not_null()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(Collections::MasterEdition)
                            .string()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(Collections::UpdateAuthority)
                            .string()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(Collections::AssociatedTokenAccount)
                            .string()
                            .not_null(),
                    )
                    .col(ColumnDef::new(Collections::Owner).string().not_null())
                    .col(ColumnDef::new(Collections::Mint).string().not_null())
                    .col(ColumnDef::new(Collections::Metadata).string().not_null())
                    .col(
                        ColumnDef::new(Collections::CreatedAt)
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
                    .table(Collections::Table)
                    .col(Collections::MasterEdition)
                    .index_type(IndexType::Hash)
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(Collections::Table).to_owned())
            .await
    }
}

#[derive(Iden)]
pub enum Collections {
    Table,
    Id,
    MasterEdition,
    AssociatedTokenAccount,
    UpdateAuthority,
    Owner,
    Mint,
    Metadata,
    CreatedAt,
}
