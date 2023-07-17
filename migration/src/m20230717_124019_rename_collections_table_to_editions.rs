use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .rename_table(
                Table::rename()
                    .table(Collections::Table, Editions::Table)
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .rename_table(
                Table::rename()
                    .table(Editions::Table, Collections::Table)
                    .to_owned(),
            )
            .await
    }
}

#[derive(Iden)]
enum Collections {
    Table,
}

#[derive(Iden)]
enum Editions {
    Table,
}
