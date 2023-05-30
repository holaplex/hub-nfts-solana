pub use sea_orm_migration::prelude::*;

mod m20230529_134752_create_collections_table;
mod m20230530_131917_create_collection_mints_table;

pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m20230529_134752_create_collections_table::Migration),
            Box::new(m20230530_131917_create_collection_mints_table::Migration),
        ]
    }
}
