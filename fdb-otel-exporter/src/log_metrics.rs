use crate::{
    fdb_gauge::{
        ElapsedRateFDBGauge, FDBGauge, HistogramPercentileFDBGauge, RateCounterFDBGauge,
        SimpleFDBGauge, TotalCounterFDBGauge,
    },
    gauge_config::{read_gauge_config_file, GaugeConfig, GaugeType},
};
use anyhow::{Context, Result};
use opentelemetry::{metrics::Meter, KeyValue};
use serde_json::Value;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Clone)]
pub struct LogMetrics {
    gauges: Vec<Arc<dyn FDBGauge>>,
}

impl LogMetrics {
    pub fn new(meter: &Meter) -> Result<Self> {
        let config_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("gauge_config.toml");
        let configs = read_gauge_config_file(&config_path)?;

        let mut gauges: Vec<Arc<dyn FDBGauge>> = configs
            .into_iter()
            .map(|config| -> Arc<dyn FDBGauge> {
                match config {
                    GaugeConfig {
                        trace_type,
                        field_name,
                        gauge_name,
                        description,
                        gauge_type: GaugeType::Simple,
                    } => Arc::new(SimpleFDBGauge::new(
                        trace_type,
                        field_name,
                        gauge_name,
                        description,
                        meter,
                    )),
                    GaugeConfig {
                        trace_type,
                        field_name,
                        gauge_name,
                        description,
                        gauge_type: GaugeType::CounterTotal,
                    } => Arc::new(TotalCounterFDBGauge::new(
                        trace_type,
                        field_name,
                        gauge_name,
                        description,
                        meter,
                    )),
                    GaugeConfig {
                        trace_type,
                        field_name,
                        gauge_name,
                        description,
                        gauge_type: GaugeType::CounterRate,
                    } => Arc::new(RateCounterFDBGauge::new(
                        trace_type,
                        field_name,
                        gauge_name,
                        description,
                        meter,
                    )),
                    GaugeConfig {
                        trace_type,
                        field_name,
                        gauge_name,
                        description,
                        gauge_type: GaugeType::ElapsedRate,
                    } => Arc::new(ElapsedRateFDBGauge::new(
                        trace_type,
                        field_name,
                        gauge_name,
                        description,
                        meter,
                    )),
                }
            })
            .collect();

        gauges.push(Arc::new(HistogramPercentileFDBGauge::new(
            "tLog",
            "commit",
            0.5,
            "tl_median_commit_latency",
            "Median tlog commit latency",
            meter,
        )));

        Ok(Self { gauges })
    }

    pub fn record(&self, trace_event: &TraceEvent) -> Result<()> {
        let machine = trace_event
            .get("Machine")
            .and_then(|value| value.as_str())
            .map(str::to_owned)
            .with_context(|| "Missing or invalid Machine field")?;

        let storage_labels = [KeyValue::new("machine", machine.clone())];
        for gauge in self.gauges.iter() {
            gauge.record(trace_event, &storage_labels)?;
        }
        Ok(())
    }
}

pub type TraceEvent = HashMap<String, Value>;
