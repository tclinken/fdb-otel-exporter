use crate::{
    fdb_gauge::{
        ElapsedRateFDBGauge, FDBGauge, HistogramPercentileFDBGauge, RateCounterFDBGauge,
        SimpleFDBGauge, TotalCounterFDBGauge,
    },
    gauge_config::{
        read_gauge_config_file, GaugeDefinition, HistogramPercentileGaugeDefinition,
        StandardGaugeDefinition,
    },
};
use anyhow::{Context, Result};
use opentelemetry::{metrics::Meter, KeyValue};
use serde_json::Value;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

// Holds the configured gauges derived from the on-disk gauge configuration.
#[derive(Clone)]
pub struct LogMetrics {
    gauges: Vec<Arc<dyn FDBGauge>>,
}

impl LogMetrics {
    // Load gauge definitions from `gauge_config.toml` and instantiate their implementations.
    pub fn new(meter: &Meter) -> Result<Self> {
        let config_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("gauge_config.toml");
        let configs = read_gauge_config_file(&config_path)?;

        let gauges: Vec<Arc<dyn FDBGauge>> = configs
            .into_iter()
            .map(|config| -> Arc<dyn FDBGauge> {
                match config {
                    GaugeDefinition::Simple(StandardGaugeDefinition {
                        trace_type,
                        field_name,
                        gauge_name,
                        description,
                    }) => Arc::new(SimpleFDBGauge::new(
                        trace_type,
                        field_name,
                        gauge_name,
                        description,
                        meter,
                    )),
                    GaugeDefinition::CounterTotal(StandardGaugeDefinition {
                        trace_type,
                        field_name,
                        gauge_name,
                        description,
                    }) => Arc::new(TotalCounterFDBGauge::new(
                        trace_type,
                        field_name,
                        gauge_name,
                        description,
                        meter,
                    )),
                    GaugeDefinition::CounterRate(StandardGaugeDefinition {
                        trace_type,
                        field_name,
                        gauge_name,
                        description,
                    }) => Arc::new(RateCounterFDBGauge::new(
                        trace_type,
                        field_name,
                        gauge_name,
                        description,
                        meter,
                    )),
                    GaugeDefinition::ElapsedRate(StandardGaugeDefinition {
                        trace_type,
                        field_name,
                        gauge_name,
                        description,
                    }) => Arc::new(ElapsedRateFDBGauge::new(
                        trace_type,
                        field_name,
                        gauge_name,
                        description,
                        meter,
                    )),
                    GaugeDefinition::HistogramPercentile(HistogramPercentileGaugeDefinition {
                        group,
                        op,
                        percentile,
                        gauge_name,
                        description,
                    }) => Arc::new(HistogramPercentileFDBGauge::new(
                        group,
                        op,
                        percentile,
                        gauge_name,
                        description,
                        meter,
                    )),
                }
            })
            .collect();

        Ok(Self { gauges })
    }

    // Record a single FoundationDB trace event across every configured gauge.
    pub fn record(&self, trace_event: &TraceEvent) -> Result<()> {
        let machine = trace_event
            .get("Machine")
            .and_then(|value| value.as_str())
            .map(str::to_owned)
            .with_context(|| "Missing or invalid Machine field")?;

        let roles = trace_event
            .get("Roles")
            .and_then(|value| value.as_str())
            .map(str::to_owned);

        let mut storage_labels = vec![KeyValue::new("machine", machine)];
        if let Some(roles) = roles {
            storage_labels.push(KeyValue::new("Roles", roles));
        }
        for gauge in self.gauges.iter() {
            gauge.record(trace_event, &storage_labels)?;
        }
        Ok(())
    }
}

pub type TraceEvent = HashMap<String, Value>;

#[cfg(test)]
impl LogMetrics {
    fn from_gauges(gauges: Vec<Arc<dyn FDBGauge>>) -> Self {
        Self { gauges }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::metrics::MeterProvider;
    use opentelemetry::KeyValue;
    use opentelemetry_sdk::metrics::{ManualReader, SdkMeterProvider};
    use std::sync::{Arc, Mutex};

    #[derive(Clone)]
    struct TestGauge {
        calls: Arc<Mutex<Vec<Vec<KeyValue>>>>,
    }

    impl TestGauge {
        fn new(calls: Arc<Mutex<Vec<Vec<KeyValue>>>>) -> Self {
            Self { calls }
        }
    }

    impl FDBGauge for TestGauge {
        fn record(&self, _trace_event: &HashMap<String, Value>, labels: &[KeyValue]) -> Result<()> {
            self.calls.lock().unwrap().push(labels.to_vec());
            Ok(())
        }
    }

    fn test_meter() -> Meter {
        let reader = ManualReader::builder().build();
        let provider = SdkMeterProvider::builder().with_reader(reader).build();
        provider.meter("test")
    }

    #[test]
    fn new_loads_gauge_config() {
        let meter = test_meter();
        LogMetrics::new(&meter).expect("should load gauges from config");
    }

    #[test]
    fn record_requires_machine_field() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let gauges: Vec<Arc<dyn FDBGauge>> = vec![Arc::new(TestGauge::new(Arc::clone(&calls)))];
        let log_metrics = LogMetrics::from_gauges(gauges);

        let mut event = HashMap::new();
        event.insert("Type".to_string(), Value::String("StorageMetrics".into()));

        let err = log_metrics.record(&event).expect_err("machine required");
        assert!(
            err.to_string().contains("Machine"),
            "unexpected error message: {err}"
        );
        assert!(
            calls.lock().unwrap().is_empty(),
            "gauge should not be called"
        );
    }

    #[test]
    fn record_invokes_gauges_with_machine_label() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let gauges: Vec<Arc<dyn FDBGauge>> = vec![Arc::new(TestGauge::new(Arc::clone(&calls)))];
        let log_metrics = LogMetrics::from_gauges(gauges);

        let mut event = HashMap::new();
        event.insert("Machine".to_string(), Value::String("10.0.0.1".into()));
        event.insert("Type".to_string(), Value::String("StorageMetrics".into()));
        event.insert("BytesInput".to_string(), Value::String("0 0 0".into()));

        log_metrics.record(&event).expect("record should succeed");

        let recorded = calls.lock().unwrap();
        assert_eq!(recorded.len(), 1);
        let labels = &recorded[0];
        assert!(
            labels
                .iter()
                .any(|kv| kv.key.as_str() == "machine" && kv.value.to_string() == "10.0.0.1"),
            "expected machine label, got {labels:?}"
        );
    }
}
