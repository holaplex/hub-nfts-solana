use holaplex_hub_nfts_solana_entity::{collection_mints, editions};

use holaplex_hub_nfts_solana_core::proto::{
    MetaplexMasterEditionTransaction, MintMetaplexEditionTransaction, SolanaPendingTransaction,
    TransferMetaplexAssetTransaction,
};
use hub_core::prelude::*;
use solana_program::pubkey::Pubkey;

#[derive(Clone)]
pub struct MasterEditionAddresses {
    pub metadata: Pubkey,
    pub associated_token_account: Pubkey,
    pub owner: Pubkey,
    pub master_edition: Pubkey,
    pub mint: Pubkey,
    pub update_authority: Pubkey,
}

#[derive(Clone)]
pub struct MintEditionAddresses {
    pub edition: Pubkey,
    pub mint: Pubkey,
    pub metadata: Pubkey,
    pub owner: Pubkey,
    pub associated_token_account: Pubkey,
    pub recipient: Pubkey,
}

#[derive(Clone)]
pub struct UpdateMasterEditionAddresses {
    pub metadata: Pubkey,
    pub update_authority: Pubkey,
}

#[derive(Clone)]
pub struct TransferAssetAddresses {
    pub owner: Pubkey,
    pub recipient: Pubkey,
    pub recipient_associated_token_account: Pubkey,
    pub owner_associated_token_account: Pubkey,
}

pub struct CertifiedCollectionAddresses {
    pub owner: Pubkey,
    pub mint: Pubkey,
    pub associated_token_account: Pubkey,
    pub update_authority: Pubkey,
    pub metadata: Pubkey,
}

/// Represents a response from a transaction on the blockchain. This struct
/// provides the serialized message and the signatures of the signed message.
pub struct TransactionResponse<A> {
    /// The serialized version of the message from the transaction.
    pub serialized_message: Vec<u8>,

    /// The signatures of the signed message or the public keys of wallets that should sign the transaction. Order matters.
    pub signatures_or_signers_public_keys: Vec<String>,

    /// Addresses that are related to the transaction.
    pub addresses: A,
}

impl<A> From<TransactionResponse<A>> for SolanaPendingTransaction {
    fn from(
        TransactionResponse {
            serialized_message,
            signatures_or_signers_public_keys,
            ..
        }: TransactionResponse<A>,
    ) -> Self {
        Self {
            serialized_message,
            signatures_or_signers_public_keys,
        }
    }
}

// TODO: include this in collections::Model
pub enum CollectionType {
    Legacy,
    Verified,
}

// Legacy, Verified
#[async_trait]
pub trait CollectionBackend<T, R> {
    fn create(
        &self,
        txn: MetaplexMasterEditionTransaction,
    ) -> Result<TransactionResponse<MasterEditionAddresses>>;
}

// Uncompressed, Compressed
#[async_trait]
pub trait MintBackend {
    fn mint(
        &self,
        collection_ty: CollectionType,
        edition: &editions::Model,
        txn: MintMetaplexEditionTransaction,
    ) -> Result<TransactionResponse<MintEditionAddresses>>;

    // TODO: probably better to replace all errors here with an Error enum
    fn try_update(
        &self,
        collection_ty: CollectionType,
        edition: &editions::Model,
        txn: MetaplexMasterEditionTransaction,
    ) -> Result<Option<TransactionResponse<UpdateMasterEditionAddresses>>>;

    // Right now only this one needs to be async to support hitting the asset
    // API for transfer data
    async fn transfer(
        &self,
        collection_ty: CollectionType,
        collection_mint: &collection_mints::Model,
        txn: TransferMetaplexAssetTransaction,
    ) -> Result<TransactionResponse<TransferAssetAddresses>>;
}
