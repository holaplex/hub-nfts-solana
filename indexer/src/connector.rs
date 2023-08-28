use std::{collections::HashMap, vec};

use futures::{channel::mpsc::SendError, Sink, Stream};
use hub_core::prelude::*;
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::{prelude::*, tonic::Status};

#[derive(Clone)]
pub struct GeyserGrpcConnector {
    endpoint: String,
    x_token: Option<String>,
}

impl GeyserGrpcConnector {
    pub fn new(endpoint: String, x_token: Option<String>) -> Self {
        Self { endpoint, x_token }
    }

    pub async fn subscribe(
        &self,
    ) -> Result<(
        impl Sink<SubscribeRequest, Error = SendError>,
        impl Stream<Item = Result<SubscribeUpdate, Status>>,
    )> {
        let mut client =
            GeyserGrpcClient::connect(self.endpoint.clone(), self.x_token.clone(), None)?;
        let (subscribe_tx, stream) = client.subscribe().await?;

        Ok((subscribe_tx, stream))
    }

    pub fn build_request() -> SubscribeRequest {
        let mut slots = HashMap::new();
        slots.insert("client".to_owned(), SubscribeRequestFilterSlots {});

        let mut transactions = HashMap::new();
        transactions.insert("client".to_string(), SubscribeRequestFilterTransactions {
            vote: Some(false),
            failed: Some(false),
            signature: None,
            account_include: vec![spl_token::ID.to_string(), mpl_bubblegum::ID.to_string()],
            account_exclude: Vec::new(),
            account_required: Vec::new(),
        });

        SubscribeRequest {
            accounts: HashMap::new(),
            slots,
            transactions,
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
            commitment: Some(CommitmentLevel::Finalized as i32),
            accounts_data_slice: vec![],
        }
    }
}
