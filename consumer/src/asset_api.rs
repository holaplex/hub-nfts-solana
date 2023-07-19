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

#[derive(serde::Serialize, serde::Deserialize)]
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

#[derive(serde::Serialize, serde::Deserialize)]
pub struct Asset {
    pub interface: String,
    pub id: Base58,
    pub content: serde_json::Value,
    pub authorities: Vec<AssetAuthority>,
    pub compression: AssetCompression,
    pub grouping: Vec<AssetGrouping>,
    pub royalty: AssetRoyalty,
    pub creators: Vec<AssetCreator>,
    pub ownership: AssetOwnership,
    pub supply: Option<u32>,
    pub mutable: bool,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct AssetAuthority {
    pub address: Base58,
    pub scopes: Vec<String>,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct AssetCompression {
    pub eligible: bool,
    pub compressed: bool,
    pub data_hash: Base58,
    pub creator_hash: Base58,
    pub asset_hash: Base58,
    pub tree: Base58,
    pub seq: u32,
    pub leaf_id: u32,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct AssetGrouping {
    pub group_key: String,
    pub group_value: Base58,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct AssetRoyalty {
    pub royalty_model: String,
    pub target: Option<serde_json::Number>, // TODO: what type is this
    pub percent: serde_json::Number, // TODO: this is fractional, use BCD to avoid rounding error
    pub basis_points: u32,
    pub primary_sale_happened: bool,
    pub locked: bool,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct AssetCreator {
    pub address: Base58,
    pub share: u32,
    pub verified: bool,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct AssetOwnership {
    pub frozen: bool,
    pub delegated: bool,
    pub delegate: Base58,
    pub ownership_model: String,
    pub owner: Base58,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct AssetProof {
    pub root: Base58,
    pub proof: Vec<Base58>,
    pub node_index: u32,
    pub leaf: Base58,
    pub tree_id: Base58,
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

    // #[method(name = "searchAssets")]
    // fn search_assets(&self);
}
