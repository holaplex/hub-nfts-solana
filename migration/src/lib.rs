pub use sea_orm_migration::prelude::*;

mod m20230529_134752_create_collections_table;
mod m20230530_131917_create_collection_mints_table;
mod m20230614_132203_make_associated_token_account_nullable_on_collection_mints;
mod m20230616_091724_backfill_associated_token_account_on_collection_mints;
mod m20230721_135829_set_default_collection_and_mint_id;
mod m20230725_143421_add_compression_leafs_table;

pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m20230529_134752_create_collections_table::Migration),
            Box::new(m20230530_131917_create_collection_mints_table::Migration),
            Box::new(m20230614_132203_make_associated_token_account_nullable_on_collection_mints::Migration),
            Box::new(m20230616_091724_backfill_associated_token_account_on_collection_mints::Migration),
            Box::new(m20230725_143421_add_compression_leafs_table::Migration),
            Box::new(m20230721_135829_set_default_collection_and_mint_id::Migration),
        ]
    }
}
