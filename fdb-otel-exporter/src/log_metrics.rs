use crate::{
    fdb_gauge::{FDBGauge, RateCounterFDBGauge, SimpleFDBGauge, TotalCounterFDBGauge},
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

        let gauges: Vec<Arc<dyn FDBGauge>> = configs
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
                }
            })
            .collect();

        Ok(Self { gauges })
    }

    pub fn record(&self, trace_event: &LogRecord) -> Result<()> {
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

pub type LogRecord = HashMap<String, Value>;

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::metrics::noop::NoopMeterProvider;
    use opentelemetry::metrics::MeterProvider as _;
    use serde_json::json;
    use std::collections::HashMap;

    fn build_metrics() -> LogMetrics {
        let meter = NoopMeterProvider::new().meter("test");
        LogMetrics::new(&meter).expect("failed to initialize log metrics for tests")
    }

    fn build_record(
        record_type: &str,
        durable_version: Option<&str>,
        version: Option<&str>,
    ) -> LogRecord {
        let mut record: LogRecord = HashMap::new();
        record.insert("Machine".to_string(), json!("machine-1"));
        record.insert("Type".to_string(), json!(record_type));

        if let Some(value) = durable_version {
            record.insert("DurableVersion".to_string(), json!(value));
        }

        if let Some(value) = version {
            record.insert("Version".to_string(), json!(value));
        }

        record
    }

    #[test]
    fn record_errors_when_storage_metrics_missing_version() {
        let metrics = build_metrics();
        let record = build_record("StorageMetrics", None, None);

        let error = metrics.record(&record).unwrap_err();

        assert!(error.to_string().contains("Missing Version field"));
    }

    #[test]
    fn record_errors_when_storage_metrics_has_invalid_version() {
        let metrics = build_metrics();
        let record = build_record("StorageMetrics", Some("abc"), Some("def"));

        let error = metrics.record(&record).unwrap_err();

        assert!(error.to_string().contains("invalid digit found in string"));
    }

    #[test]
    fn record_allows_non_storage_metrics_without_version() {
        let metrics = build_metrics();
        let record = build_record("Trace", None, None);

        metrics
            .record(&record)
            .expect("non StorageMetrics record should not require Version");
    }
}
