use sea_orm_migration::{prelude::*, sea_orm::Statement};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let db = manager.get_connection();

        let stmt = Statement::from_string(
            manager.get_database_backend(),
            r#"ALTER TABLE collections ALTER COLUMN id SET DEFAULT gen_random_uuid();"#.to_string(),
        );

        db.execute(stmt).await?;

        let stmt = Statement::from_string(
            manager.get_database_backend(),
            r#"ALTER TABLE collection_mints ALTER COLUMN id SET DEFAULT gen_random_uuid();"#
                .to_string(),
        );

        db.execute(stmt).await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let db = manager.get_connection();

        let stmt = Statement::from_string(
            manager.get_database_backend(),
            r#"ALTER TABLE collections ALTER COLUMN id DROP DEFAULT;"#.to_string(),
        );

        db.execute(stmt).await?;

        let stmt = Statement::from_string(
            manager.get_database_backend(),
            r#"ALTER TABLE collection_mints ALTER COLUMN id DROP DEFAULT;"#.to_string(),
        );

        db.execute(stmt).await?;

        Ok(())
    }
}
