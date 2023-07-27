use holaplex_hub_nfts_solana_core::proto::{
    MetaplexMasterEditionTransaction, SolanaPendingTransaction, TransferMetaplexAssetTransaction,
};
use holaplex_hub_nfts_solana_entity::{collection_mints, collections};
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
pub struct MintMetaplexAddresses {
    pub mint: Pubkey,
    pub metadata: Pubkey,
    pub owner: Pubkey,
    pub associated_token_account: Pubkey,
    pub recipient: Pubkey,
    pub update_authority: Pubkey,
}

#[derive(Clone)]
pub struct MintCompressedMintV1Addresses {
    pub merkle_tree: Pubkey,
    pub tree_authority: Pubkey,
    pub tree_delegate: Pubkey,
    pub leaf_owner: Pubkey,
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

pub trait CollectionBackend {
    fn create(
        &self,
        txn: MetaplexMasterEditionTransaction,
    ) -> Result<TransactionResponse<MasterEditionAddresses>>;

    fn update(
        &self,
        collection: &collections::Model,
        txn: MetaplexMasterEditionTransaction,
    ) -> Result<TransactionResponse<UpdateMasterEditionAddresses>>;
}

pub trait MintBackend<T, R> {
    fn mint(&self, collection: &collections::Model, txn: T) -> Result<TransactionResponse<R>>;
}

#[async_trait]
pub trait TransferBackend {
    async fn transfer(
        &self,
        collection_mint: &collection_mints::Model,
        txn: TransferMetaplexAssetTransaction,
    ) -> Result<TransactionResponse<TransferAssetAddresses>>;
}
