use std::str::FromStr;

use holaplex_hub_nfts_solana_entity::collection_mints::{ActiveModel, Entity};
use sea_orm_migration::{
    prelude::*,
    sea_orm::{ActiveModelTrait, EntityTrait, Set},
    DbErr,
};
use solana_sdk::pubkey::Pubkey;
use spl_associated_token_account::get_associated_token_address;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let db = manager.get_connection();

        manager
            .alter_table(
                Table::alter()
                    .table(CollectionMints::Table)
                    .add_column_if_not_exists(
                        ColumnDef::new(CollectionMints::AssociatedTokenAccount)
                            .text()
                            .null(),
                    )
                    .to_owned(),
            )
            .await?;

        let mints = Entity::find().all(manager.get_connection()).await?;

        for mint in mints {
            let wallet_address = Pubkey::from_str(&mint.owner)
                .map_err(|err| DbErr::Custom(format!("Failed to parse Pubkey: {}", err)))?;
            let mint_address = Pubkey::from_str(&mint.mint)
                .map_err(|err| DbErr::Custom(format!("Failed to parse Pubkey: {}", err)))?;
            let associated_token_account =
                get_associated_token_address(&wallet_address, &mint_address);

            let mut mint: ActiveModel = mint.into();
            mint.associated_token_account = Set(associated_token_account.to_string());

            mint.update(db).await?;
        }

        manager
            .alter_table(
                Table::alter()
                    .table(CollectionMints::Table)
                    .modify_column(
                        ColumnDef::new(CollectionMints::AssociatedTokenAccount)
                            .text()
                            .not_null(),
                    )
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(CollectionMints::Table)
                    .drop_column(CollectionMints::AssociatedTokenAccount)
                    .to_owned(),
            )
            .await
    }
}

/// Learn more at https://docs.rs/sea-query#iden
#[derive(Iden)]
enum CollectionMints {
    Table,
    AssociatedTokenAccount,
}
