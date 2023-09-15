use hub_core::{
    anyhow::Result,
    metrics::{Encoder, TextEncoder},
};
use poem::{handler, http::StatusCode, web::Data};

use crate::Metrics;

#[handler]
pub fn health() -> StatusCode {
    StatusCode::OK
}

#[handler]
pub fn metrics_handler(Data(metrics): Data<&Metrics>) -> Result<String> {
    let mut buffer = vec![];
    let encoder = TextEncoder::new();
    encoder.encode(&metrics.registry.gather(), &mut buffer)?;
    Ok(String::from_utf8_lossy(&buffer).into_owned())
}
