use core::fmt;
use std::collections::HashMap;

use solana_program::pubkey::Pubkey;
mod b58 {
    use serde::{de::Visitor, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(bytes: &[u8], ser: S) -> Result<S::Ok, S::Error> {
        ser.serialize_str(&bs58::encode(bytes).into_string())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(de: D) -> Result<Vec<u8>, D::Error> {
        struct Vis;

        impl<'a> Visitor<'a> for Vis {
            type Value = Vec<u8>;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a base58-encoded string")
            }

            fn visit_str<E: serde::de::Error>(self, s: &str) -> Result<Self::Value, E> {
                bs58::decode(s).into_vec().map_err(E::custom)
            }
        }

        de.deserialize_str(Vis)
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Base58(#[serde(with = "b58")] pub Vec<u8>);

impl From<Vec<u8>> for Base58 {
    fn from(v: Vec<u8>) -> Self {
        Self(v)
    }
}

impl From<Base58> for Vec<u8> {
    fn from(Base58(v): Base58) -> Self {
        v
    }
}

impl TryFrom<Base58> for Pubkey {
    type Error = std::array::TryFromSliceError;

    fn try_from(Base58(v): Base58) -> Result<Self, Self::Error> {
        Pubkey::try_from(v.as_slice())
    }
}

impl fmt::Display for Base58 {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(&bs58::encode(&self.0).into_string())
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Asset {
    pub interface: String,
    pub id: Base58,
    pub content: Content,
    pub authorities: Vec<AssetAuthority>,
    pub compression: AssetCompression,
    pub grouping: Vec<AssetGrouping>,
    pub royalty: AssetRoyalty,
    pub creators: Vec<AssetCreator>,
    pub ownership: AssetOwnership,
    pub supply: Option<AssetSupply>,
    pub mutable: bool,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AssetSupply {
    pub print_max_supply: u32,
    pub print_current_supply: u32,
    pub edition_nonce: Option<u64>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AssetAuthority {
    pub address: Base58,
    pub scopes: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AssetCompression {
    pub eligible: bool,
    pub compressed: bool,
    pub data_hash: Option<Base58>,
    pub creator_hash: Option<Base58>,
    pub asset_hash: Option<Base58>,
    pub tree: Option<Base58>,
    pub seq: u32,
    pub leaf_id: u32,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AssetGrouping {
    pub group_key: String,
    pub group_value: Base58,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AssetRoyalty {
    pub royalty_model: String,
    pub target: Option<serde_json::Number>, // TODO: what type is this
    pub percent: serde_json::Number, // TODO: this is fractional, use BCD to avoid rounding error
    pub basis_points: u32,
    pub primary_sale_happened: bool,
    pub locked: bool,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AssetCreator {
    pub address: Base58,
    pub share: u32,
    pub verified: bool,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AssetOwnership {
    pub frozen: bool,
    pub delegated: bool,
    pub delegate: Option<Base58>,
    pub ownership_model: String,
    pub owner: Base58,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AssetProof {
    pub root: Base58,
    pub proof: Vec<Base58>,
    pub node_index: u32,
    pub leaf: Base58,
    pub tree_id: Base58,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SearchAssetsResult {
    pub total: u64,
    pub limit: u64,
    pub page: u64,
    pub items: Vec<Asset>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Content {
    #[serde(rename = "$schema")]
    pub schema: String,
    pub json_uri: String,
    pub files: Option<Vec<File>>,
    pub metadata: Metadata,
    pub links: Option<HashMap<String, Option<String>>>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct File {
    pub uri: String,
    pub mime: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde:: Deserialize)]
pub struct Metadata {
    pub attributes: Option<Vec<Attribute>>,
    pub description: Option<String>,
    pub name: String,
    pub symbol: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Attribute {
    pub value: serde_json::Value,
    pub trait_type: serde_json::Value,
}

#[jsonrpsee::proc_macros::rpc(client)]
pub trait Rpc {
    #[method(name = "getAsset", param_kind = map)]
    fn get_asset(&self, id: &str) -> Result<Asset, Error>;

    #[method(name = "getAssetProof", param_kind = map)]
    fn get_asset_proof(&self, id: &str) -> Result<AssetProof, Error>;

    // Supposedly Triton offers these but their docs were crashing my browser
    // so I don't know what the signatures are.

    // #[method(name = "getAssetsByAuthority")]
    // fn get_assets_by_authority(&self);

    // #[method(name = "getAssetsByOwner")]
    // fn get_assets_by_owner(&self);

    // #[method(name = "getAssetsByGroup")]
    // fn get_assets_by_group(&self);

    // #[method(name = "getAssetsByCreator")]
    // fn get_assets_by_creator(&self);

    #[method(name = "searchAssets", param_kind = map)]
    fn search_assets(&self, grouping: Vec<&str>, page: u64) -> Result<SearchAssetsResult, Error>;
}
