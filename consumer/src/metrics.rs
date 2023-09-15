#[allow(clippy::wildcard_imports)]
use hub_core::{
    anyhow::{anyhow, Result},
    metrics::*,
};

#[derive(Debug, Clone)]
pub struct Metrics {
    pub registry: Registry,
    pub provider: MeterProvider,
    pub rpc_tx_submission_duration_ms_bucket: Histogram<i64>,
    pub rpc_tx_assembly_duration_ms_bucket: Histogram<i64>,
}

impl Metrics {
    /// Res
    /// # Errors
    pub fn new() -> Result<Self> {
        let registry = Registry::new();
        let exporter = hub_core::metrics::exporter()
            .with_registry(registry.clone())
            .with_namespace("hub_nfts_solana")
            .build()
            .map_err(|e| anyhow!("Failed to build exporter: {}", e))?;

        let provider = MeterProvider::builder()
            .with_reader(exporter)
            .with_resource(Resource::new(vec![KeyValue::new(
                "service.name",
                "hub-nfts-solana",
            )]))
            .build();

        let meter = provider.meter("hub-nfts-solana");

        let rpc_tx_submission_duration_ms_bucket = meter
            .i64_histogram("rpc_tx_submission.time")
            .with_unit(Unit::new("ms"))
            .with_description("RPC Tx duration time in milliseconds.")
            .init();

        let rpc_tx_assembly_duration_ms_bucket = meter
            .i64_histogram("rpc_tx_assembly.time")
            .with_unit(Unit::new("ms"))
            .with_description("Transaction assembly duration time in milliseconds.")
            .init();

        Ok(Self {
            registry,
            provider,
            rpc_tx_submission_duration_ms_bucket,
            rpc_tx_assembly_duration_ms_bucket,
        })
    }
}
