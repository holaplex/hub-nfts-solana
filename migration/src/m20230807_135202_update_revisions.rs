use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(UpdateRevisions::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(UpdateRevisions::Id)
                            .uuid()
                            .not_null()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(UpdateRevisions::MintId).uuid().not_null())
                    .col(
                        ColumnDef::new(UpdateRevisions::SerializedMessage)
                            .binary()
                            .not_null(),
                    )
                    .col(ColumnDef::new(UpdateRevisions::Payer).string().not_null())
                    .col(
                        ColumnDef::new(UpdateRevisions::Metadata)
                            .string()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(UpdateRevisions::UpdateAuthority)
                            .string()
                            .not_null(),
                    )
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(UpdateRevisions::Table).to_owned())
            .await
    }
}

#[derive(Iden)]
enum UpdateRevisions {
    Table,
    Id,
    MintId,
    SerializedMessage,
    Payer,
    Metadata,
    UpdateAuthority,
}
