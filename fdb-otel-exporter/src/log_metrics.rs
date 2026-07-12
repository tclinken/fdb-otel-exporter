use crate::{
    fdb_counter::{SevCounter, SlowTaskCounter},
    fdb_gauge::{
        ElapsedRateFDBGauge, HistogramPercentileFDBGauge, RateCounterFDBGauge, SimpleFDBGauge,
        TotalCounterFDBGauge,
    },
    fdb_metric::FDBMetric,
    gauge_config::{
        read_gauge_config_file, GaugeDefinition, HistogramPercentileGaugeDefinition,
        StandardGaugeDefinition,
    },
};
use anyhow::{Context, Result};
use opentelemetry::{metrics::Meter, KeyValue};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

type HistogramOutputConfig = (f64, String, String);
type HistogramConfigGroups = BTreeMap<(String, String), Vec<HistogramOutputConfig>>;
type MetricHandlers = Vec<Arc<dyn FDBMetric>>;
type HistogramHandlersByOp = HashMap<String, MetricHandlers>;
type HistogramHandlersByGroup = HashMap<String, HistogramHandlersByOp>;

#[derive(Debug)]
struct NamedMetricFailure {
    name: String,
    source: anyhow::Error,
}

/// Aggregate error returned after every applicable metric handler has had a chance to run.
#[derive(Debug)]
pub struct MetricRecordError {
    failures: Vec<NamedMetricFailure>,
}

impl fmt::Display for MetricRecordError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "failed to record {} metric handler(s)",
            self.failures.len()
        )?;
        for failure in &self.failures {
            write!(formatter, "; {}: {:#}", failure.name, failure.source)?;
        }
        Ok(())
    }
}

impl std::error::Error for MetricRecordError {}

// Holds global metrics and configured metrics indexed by FoundationDB trace event type.
#[derive(Clone)]
pub struct LogMetrics {
    global_metrics: MetricHandlers,
    metrics_by_type: HashMap<String, MetricHandlers>,
    histogram_metrics_by_key: HistogramHandlersByGroup,
}

impl LogMetrics {
    // Load gauge definitions from `gauge_config.toml` and instantiate their implementations.
    pub fn new(meter: &Meter) -> Result<Self> {
        let config_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("gauge_config.toml");
        let configs = read_gauge_config_file(&config_path)?;

        let mut metrics: Vec<Arc<dyn FDBMetric>> = Vec::new();
        let mut histogram_groups = HistogramConfigGroups::new();

        for config in configs {
            let metric: Arc<dyn FDBMetric> = match config {
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
                }) => {
                    histogram_groups.entry((group, op)).or_default().push((
                        percentile,
                        gauge_name,
                        description,
                    ));
                    continue;
                }
            };
            metrics.push(metric);
        }

        metrics.extend(
            histogram_groups
                .into_iter()
                .map(|((group, op), percentiles)| {
                    Arc::new(HistogramPercentileFDBGauge::new_grouped(
                        group,
                        op,
                        percentiles,
                        meter,
                    )) as Arc<dyn FDBMetric>
                }),
        );

        metrics.extend(
            [10, 20, 30, 40]
                .into_iter()
                .map(|severity| Arc::new(SevCounter::new(severity, meter)) as Arc<dyn FDBMetric>),
        );

        metrics.extend([10, 100, 1000].into_iter().map(|threshold_ms| {
            Arc::new(SlowTaskCounter::new(threshold_ms, meter)) as Arc<dyn FDBMetric>
        }));

        Ok(Self::route_metrics(metrics))
    }

    fn route_metrics(metrics: Vec<Arc<dyn FDBMetric>>) -> Self {
        let mut global_metrics = Vec::new();
        let mut metrics_by_type: HashMap<String, MetricHandlers> = HashMap::new();
        let mut histogram_metrics_by_key = HistogramHandlersByGroup::new();

        for metric in metrics {
            if let Some((group, op)) = metric.histogram_key() {
                histogram_metrics_by_key
                    .entry(group.to_owned())
                    .or_default()
                    .entry(op.to_owned())
                    .or_default()
                    .push(metric);
            } else if let Some(event_type) = metric.event_type() {
                metrics_by_type
                    .entry(event_type.to_owned())
                    .or_default()
                    .push(metric);
            } else {
                global_metrics.push(metric);
            }
        }

        Self {
            global_metrics,
            metrics_by_type,
            histogram_metrics_by_key,
        }
    }

    // Record a single FoundationDB trace event across its type-specific and global metrics.
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

        let trace_type = trace_event
            .get("Type")
            .and_then(|value| value.as_str())
            .with_context(|| "Missing or invalid Type field")?;

        let mut failures = Vec::new();
        let histogram_metrics = if trace_type == "Histogram" {
            let group = trace_event
                .get("Group")
                .and_then(|value| value.as_str())
                .with_context(|| "Missing or invalid Group field")?;
            let op = trace_event
                .get("Op")
                .and_then(|value| value.as_str())
                .with_context(|| "Missing or invalid Op field")?;
            self.histogram_metrics_by_key
                .get(group)
                .and_then(|metrics_by_op| metrics_by_op.get(op))
        } else {
            None
        };
        let type_metrics = self
            .metrics_by_type
            .get(trace_type)
            .into_iter()
            .flatten()
            .chain(histogram_metrics.into_iter().flatten());

        for metric in type_metrics.chain(self.global_metrics.iter()) {
            if let Err(source) = metric.record(trace_event, &storage_labels) {
                failures.push(NamedMetricFailure {
                    name: metric.name().to_owned(),
                    source,
                });
            }
        }

        if !failures.is_empty() {
            return Err(MetricRecordError { failures }.into());
        }

        Ok(())
    }
}

pub type TraceEvent = HashMap<String, Value>;

#[cfg(test)]
impl LogMetrics {
    pub(crate) fn from_metrics(metrics: Vec<Arc<dyn FDBMetric>>) -> Self {
        Self::route_metrics(metrics)
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

    impl FDBMetric for TestGauge {
        fn record(&self, _trace_event: &HashMap<String, Value>, labels: &[KeyValue]) -> Result<()> {
            self.calls.lock().unwrap().push(labels.to_vec());
            Ok(())
        }
    }

    #[derive(Clone)]
    struct RoutedTestMetric {
        name: String,
        event_type: Option<String>,
        histogram_key: Option<(String, String)>,
        calls: Arc<Mutex<Vec<String>>>,
        should_fail: bool,
    }

    impl RoutedTestMetric {
        fn new(
            name: &str,
            event_type: Option<&str>,
            calls: Arc<Mutex<Vec<String>>>,
            should_fail: bool,
        ) -> Self {
            Self {
                name: name.to_owned(),
                event_type: event_type.map(str::to_owned),
                histogram_key: None,
                calls,
                should_fail,
            }
        }

        fn histogram(name: &str, group: &str, op: &str, calls: Arc<Mutex<Vec<String>>>) -> Self {
            Self {
                name: name.to_owned(),
                event_type: Some("Histogram".to_owned()),
                histogram_key: Some((group.to_owned(), op.to_owned())),
                calls,
                should_fail: false,
            }
        }
    }

    impl FDBMetric for RoutedTestMetric {
        fn name(&self) -> &str {
            &self.name
        }

        fn event_type(&self) -> Option<&str> {
            self.event_type.as_deref()
        }

        fn histogram_key(&self) -> Option<(&str, &str)> {
            self.histogram_key
                .as_ref()
                .map(|(group, op)| (group.as_str(), op.as_str()))
        }

        fn record(
            &self,
            _trace_event: &HashMap<String, Value>,
            _labels: &[KeyValue],
        ) -> Result<()> {
            self.calls.lock().unwrap().push(self.name.clone());
            if self.should_fail {
                anyhow::bail!("intentional failure");
            }
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
        let metrics: Vec<Arc<dyn FDBMetric>> = vec![Arc::new(TestGauge::new(Arc::clone(&calls)))];
        let log_metrics = LogMetrics::from_metrics(metrics);

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
        let metrics: Vec<Arc<dyn FDBMetric>> = vec![Arc::new(TestGauge::new(Arc::clone(&calls)))];
        let log_metrics = LogMetrics::from_metrics(metrics);

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

    #[test]
    fn record_continues_after_a_metric_failure_and_returns_named_aggregate() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let metrics: Vec<Arc<dyn FDBMetric>> = vec![
            Arc::new(RoutedTestMetric::new(
                "first_metric",
                Some("StorageMetrics"),
                Arc::clone(&calls),
                true,
            )),
            Arc::new(RoutedTestMetric::new(
                "second_metric",
                Some("StorageMetrics"),
                Arc::clone(&calls),
                false,
            )),
        ];
        let log_metrics = LogMetrics::from_metrics(metrics);
        let event = HashMap::from([
            ("Machine".to_owned(), Value::String("10.0.0.1".into())),
            ("Type".to_owned(), Value::String("StorageMetrics".into())),
        ]);

        let error = log_metrics
            .record(&event)
            .expect_err("the aggregate should report the failed metric");

        assert!(error.downcast_ref::<MetricRecordError>().is_some());
        assert!(error.to_string().contains("first_metric"));
        assert!(error.to_string().contains("intentional failure"));
        assert_eq!(
            *calls.lock().unwrap(),
            vec!["first_metric".to_owned(), "second_metric".to_owned()],
            "a failed metric must not prevent later independent metrics from running"
        );
    }

    #[test]
    fn record_invokes_only_matching_type_and_global_metrics() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let metrics: Vec<Arc<dyn FDBMetric>> = vec![
            Arc::new(RoutedTestMetric::new(
                "storage_metric",
                Some("StorageMetrics"),
                Arc::clone(&calls),
                false,
            )),
            Arc::new(RoutedTestMetric::new(
                "proxy_metric",
                Some("ProxyMetrics"),
                Arc::clone(&calls),
                false,
            )),
            Arc::new(RoutedTestMetric::new(
                "global_metric",
                None,
                Arc::clone(&calls),
                false,
            )),
        ];
        let log_metrics = LogMetrics::from_metrics(metrics);
        let event = HashMap::from([
            ("Machine".to_owned(), Value::String("10.0.0.1".into())),
            ("Type".to_owned(), Value::String("StorageMetrics".into())),
        ]);

        log_metrics.record(&event).expect("record should succeed");

        assert_eq!(
            *calls.lock().unwrap(),
            vec!["storage_metric".to_owned(), "global_metric".to_owned()],
            "metrics for unrelated trace event types must not be invoked"
        );
    }

    #[test]
    fn record_invokes_only_matching_histogram_group_and_op() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let metrics: Vec<Arc<dyn FDBMetric>> = vec![
            Arc::new(RoutedTestMetric::histogram(
                "read_histogram",
                "StorageServer",
                "Read",
                Arc::clone(&calls),
            )),
            Arc::new(RoutedTestMetric::histogram(
                "write_histogram",
                "StorageServer",
                "Write",
                Arc::clone(&calls),
            )),
            Arc::new(RoutedTestMetric::histogram(
                "proxy_histogram",
                "CommitProxy",
                "Read",
                Arc::clone(&calls),
            )),
        ];
        let log_metrics = LogMetrics::from_metrics(metrics);
        let event = HashMap::from([
            ("Machine".to_owned(), Value::String("10.0.0.1".into())),
            ("Type".to_owned(), Value::String("Histogram".into())),
            ("Group".to_owned(), Value::String("StorageServer".into())),
            ("Op".to_owned(), Value::String("Read".into())),
        ]);

        log_metrics.record(&event).expect("record should succeed");

        assert_eq!(
            *calls.lock().unwrap(),
            vec!["read_histogram".to_owned()],
            "histogram metrics for unrelated (Group, Op) keys must not be invoked"
        );
    }
}
