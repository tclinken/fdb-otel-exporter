use anyhow::{Context, Result};
use opentelemetry::metrics::{Gauge, Meter};
use opentelemetry::KeyValue;
use serde_json::Value;
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    f64,
    sync::{Arc, Mutex},
};

#[derive(Debug, Clone, Copy)]
enum HistogramUnit {
    Milliseconds,
    Bytes,
    Count,
}

impl HistogramUnit {
    fn divisor(&self) -> f64 {
        match self {
            Self::Milliseconds => 1_000_000.0,
            Self::Bytes => 1.0,
            Self::Count => 1.0,
        }
    }

    fn convert_bucket_upper(&self, bucket_value: f64) -> u64 {
        match self {
            Self::Milliseconds => (bucket_value * 1000.0) as u64,
            Self::Bytes | Self::Count => bucket_value as u64,
        }
    }
}

// Snapshot of a histogram bucket expressed in the trace's base units (microseconds, bytes, or counts),
// along with per-bucket counts and cumulative totals.
#[derive(Debug, Clone, Copy)]
struct HistogramBucket {
    lower_bound: u64,
    upper_bound: u64,
    count: u64,
    cumulative_count: u64,
}

// Interpolate a percentile value from histogram buckets assuming an exponential distribution.
// The buckets are derived from FoundationDB `LessThan` lines, converted to their base units
// (microseconds for latency histograms, bytes for size histograms, or counts for raw counters),
// and paired with running cumulative counts so this helper can locate the bucket that spans the
// percentile and solve for the interpolated value.
fn interpolate_exponential_percentile(
    buckets: &[HistogramBucket],
    total_count: u64,
    percentile: f64,
    unit_divisor: f64,
) -> Option<f64> {
    if buckets.is_empty() || total_count == 0 || unit_divisor <= 0.0 || !unit_divisor.is_finite() {
        return None;
    }

    let percentile = percentile.clamp(0.0, 1.0);
    let total_count_f64 = total_count as f64;

    if percentile >= 1.0 {
        return buckets
            .last()
            .map(|bucket| bucket.upper_bound as f64 / unit_divisor);
    }

    let target_rank = percentile * total_count_f64;
    let mut bucket_index = buckets.len().saturating_sub(1);

    for (index, bucket) in buckets.iter().enumerate() {
        if bucket.count == 0 {
            continue;
        }
        if (bucket.cumulative_count as f64) >= target_rank {
            bucket_index = index;
            break;
        }
    }

    let bucket = buckets[bucket_index];

    let bucket_lower_value = bucket.lower_bound as f64 / unit_divisor;
    let bucket_upper_value = bucket.upper_bound as f64 / unit_divisor;

    if bucket_upper_value <= 0.0 {
        return Some(bucket_upper_value);
    }

    let bucket_cdf = (bucket.cumulative_count as f64) / total_count_f64;
    let lower_cumulative_count = bucket.cumulative_count.saturating_sub(bucket.count);
    let prev_cdf = (lower_cumulative_count as f64) / total_count_f64;
    let bucket_mass = (bucket.count as f64) / total_count_f64;

    if bucket_mass <= 0.0 {
        return Some(bucket_upper_value);
    }

    if percentile <= prev_cdf {
        return Some(bucket_lower_value);
    }

    let lambda = if bucket_cdf >= 1.0 {
        if bucket_lower_value > 0.0 && prev_cdf < 1.0 {
            -((1.0 - prev_cdf).ln()) / bucket_lower_value
        } else {
            f64::INFINITY
        }
    } else {
        -((1.0 - bucket_cdf).ln()) / bucket_upper_value
    };

    if !lambda.is_finite() || lambda <= 0.0 {
        return Some(bucket_upper_value);
    }

    let relative_percentile =
        ((percentile - prev_cdf) / bucket_mass).clamp(0.0, 1.0 - f64::EPSILON);

    let exp_neg_lambda_lower = (-lambda * bucket_lower_value).exp();
    let exp_neg_lambda_upper = (-lambda * bucket_upper_value).exp();

    let denom = exp_neg_lambda_lower - exp_neg_lambda_upper;
    if denom <= 0.0 {
        return Some(bucket_upper_value);
    }

    let target = exp_neg_lambda_lower - relative_percentile * denom;

    if target <= 0.0 {
        return Some(bucket_upper_value);
    }

    let value = -target.ln() / lambda;

    if !value.is_finite() {
        return Some(bucket_upper_value);
    }

    Some(value.clamp(bucket_lower_value, bucket_upper_value))
}

pub trait FDBGauge: Send + Sync {
    fn record(&self, trace_event: &HashMap<String, Value>, labels: &[KeyValue]) -> Result<()>;
}

#[derive(Clone)]
struct FDBGaugeImpl {
    trace_type: String,
    field_name: String,
    gauge: Gauge<f64>,
}

fn get_trace_field<'a>(
    trace_event: &'a HashMap<String, Value>,
    field_name: &str,
) -> Result<&'a str> {
    trace_event
        .get(field_name)
        .and_then(|value| value.as_str())
        .with_context(|| format!("Missing {field_name} field"))
}

impl FDBGaugeImpl {
    fn new(
        trace_type: impl Into<String>,
        field_name: impl Into<String>,
        gauge_name: impl Into<String>,
        description: impl Into<String>,
        meter: &Meter,
    ) -> Self {
        let gauge_name = gauge_name.into();
        let description = description.into();
        Self {
            trace_type: trace_type.into(),
            field_name: field_name.into(),
            gauge: meter
                .f64_gauge(gauge_name)
                .with_description(description)
                .init(),
        }
    }
}

#[derive(Clone)]
pub struct SimpleFDBGauge {
    gauge_impl: FDBGaugeImpl,
    samples: Arc<Mutex<HashMap<LabelKey, VecDeque<TimedSample>>>>,
}

impl SimpleFDBGauge {
    pub fn new(
        trace_type: impl Into<String>,
        field_name: impl Into<String>,
        gauge_name: impl Into<String>,
        description: impl Into<String>,
        meter: &Meter,
    ) -> Self {
        Self {
            gauge_impl: FDBGaugeImpl::new(trace_type, field_name, gauge_name, description, meter),
            samples: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl FDBGauge for SimpleFDBGauge {
    fn record(&self, trace_event: &HashMap<String, Value>, labels: &[KeyValue]) -> Result<()> {
        let trace_type = get_trace_field(trace_event, "Type")?;

        if trace_type == self.gauge_impl.trace_type {
            let value = trace_event
                .get(self.gauge_impl.field_name.as_str())
                .and_then(|v| v.as_str())
                .with_context(|| format!("Missing {} field", self.gauge_impl.field_name))?;
            let sample = value.parse::<f64>()?;
            let time = get_trace_field(trace_event, "Time")?.parse::<f64>()?;

            let key = LabelKey::from_labels(labels);
            let averaged = {
                let mut samples = self
                    .samples
                    .lock()
                    .expect("simple gauge sample cache poisoned");
                let window = samples.entry(key).or_insert_with(VecDeque::new);
                window.push_back(TimedSample {
                    time,
                    value: sample,
                });
                while let Some(front) = window.front() {
                    if time - front.time > ROLLING_WINDOW_SECONDS {
                        window.pop_front();
                    } else {
                        break;
                    }
                }
                let count = window.len() as f64;
                if count == 0.0 {
                    sample
                } else {
                    window.iter().map(|s| s.value).sum::<f64>() / count
                }
            };

            self.gauge_impl.gauge.record(averaged, labels);
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct TotalCounterFDBGauge {
    gauge_impl: FDBGaugeImpl,
}

impl TotalCounterFDBGauge {
    pub fn new(
        trace_type: impl Into<String>,
        field_name: impl Into<String>,
        gauge_name: impl Into<String>,
        description: impl Into<String>,
        meter: &Meter,
    ) -> Self {
        Self {
            gauge_impl: FDBGaugeImpl::new(trace_type, field_name, gauge_name, description, meter),
        }
    }
}

impl FDBGauge for TotalCounterFDBGauge {
    fn record(&self, trace_event: &HashMap<String, Value>, labels: &[KeyValue]) -> Result<()> {
        let trace_type = get_trace_field(trace_event, "Type")?;

        if trace_type == self.gauge_impl.trace_type {
            let value = get_trace_field(trace_event, self.gauge_impl.field_name.as_str())?;
            self.gauge_impl.gauge.record(
                value
                    .split(' ')
                    .nth(2)
                    .with_context(|| format!("Malformed {} counter", self.gauge_impl.field_name))?
                    .parse::<f64>()?,
                labels,
            );
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct LabelKey(Vec<(String, String)>);

impl LabelKey {
    fn from_labels(labels: &[KeyValue]) -> Self {
        let mut entries: Vec<(String, String)> = labels
            .iter()
            .map(|kv| (kv.key.as_str().to_string(), kv.value.to_string()))
            .collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
        Self(entries)
    }
}

const ROLLING_WINDOW_SECONDS: f64 = 15.0;

#[derive(Clone, Copy)]
struct TimedSample {
    time: f64,
    value: f64,
}

#[derive(Clone)]
// Maintains a 15 second rolling mean of raw samples keyed by label set so Prometheus scrapes see a
// stable value even when scrape periods exceed log emission frequency.
pub struct RateCounterFDBGauge {
    gauge_impl: FDBGaugeImpl,
    samples: Arc<Mutex<HashMap<LabelKey, VecDeque<TimedSample>>>>,
}

impl RateCounterFDBGauge {
    pub fn new(
        trace_type: impl Into<String>,
        field_name: impl Into<String>,
        gauge_name: impl Into<String>,
        description: impl Into<String>,
        meter: &Meter,
    ) -> Self {
        Self {
            gauge_impl: FDBGaugeImpl::new(trace_type, field_name, gauge_name, description, meter),
            samples: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl FDBGauge for RateCounterFDBGauge {
    fn record(&self, trace_event: &HashMap<String, Value>, labels: &[KeyValue]) -> Result<()> {
        let trace_type = get_trace_field(trace_event, "Type")?;

        if trace_type == self.gauge_impl.trace_type {
            let value = get_trace_field(trace_event, self.gauge_impl.field_name.as_str())?;
            let sample = value
                .split(' ')
                .next()
                .with_context(|| format!("Malformed {} counter", self.gauge_impl.field_name))?
                .parse::<f64>()?;
            let time = get_trace_field(trace_event, "Time")?.parse::<f64>()?;

            let key = LabelKey::from_labels(labels);
            let averaged = {
                let mut samples = self
                    .samples
                    .lock()
                    .expect("rate counter sample cache poisoned");
                let window = samples.entry(key).or_insert_with(VecDeque::new);
                window.push_back(TimedSample {
                    time,
                    value: sample,
                });
                while let Some(front) = window.front() {
                    if time - front.time > ROLLING_WINDOW_SECONDS {
                        window.pop_front();
                    } else {
                        break;
                    }
                }
                let count = window.len() as f64;
                if count == 0.0 {
                    sample
                } else {
                    window.iter().map(|s| s.value).sum::<f64>() / count
                }
            };

            self.gauge_impl.gauge.record(averaged, labels);
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct ElapsedRateFDBGauge {
    gauge_impl: FDBGaugeImpl,
    samples: Arc<Mutex<HashMap<LabelKey, VecDeque<TimedSample>>>>,
}

impl ElapsedRateFDBGauge {
    pub fn new(
        trace_type: impl Into<String>,
        field_name: impl Into<String>,
        gauge_name: impl Into<String>,
        description: impl Into<String>,
        meter: &Meter,
    ) -> Self {
        Self {
            gauge_impl: FDBGaugeImpl::new(trace_type, field_name, gauge_name, description, meter),
            samples: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl FDBGauge for ElapsedRateFDBGauge {
    fn record(&self, trace_event: &HashMap<String, Value>, labels: &[KeyValue]) -> Result<()> {
        let trace_type = get_trace_field(trace_event, "Type")?;

        if trace_type == self.gauge_impl.trace_type {
            let value = get_trace_field(trace_event, self.gauge_impl.field_name.as_str())?
                .parse::<f64>()?;
            let elapsed = get_trace_field(trace_event, "Elapsed")?.parse::<f64>()?;
            let time = get_trace_field(trace_event, "Time")?.parse::<f64>()?;
            let sample = value / elapsed;

            let key = LabelKey::from_labels(labels);
            let averaged = {
                let mut samples = self
                    .samples
                    .lock()
                    .expect("elapsed rate sample cache poisoned");
                let window = samples.entry(key).or_insert_with(VecDeque::new);
                window.push_back(TimedSample {
                    time,
                    value: sample,
                });
                while let Some(front) = window.front() {
                    if time - front.time > ROLLING_WINDOW_SECONDS {
                        window.pop_front();
                    } else {
                        break;
                    }
                }
                let count = window.len() as f64;
                if count == 0.0 {
                    sample
                } else {
                    window.iter().map(|s| s.value).sum::<f64>() / count
                }
            };

            self.gauge_impl.gauge.record(averaged, labels);
        }
        Ok(())
    }
}

// Because histograms are precomputed, interpolate percentiles and emit as gauge
pub struct HistogramPercentileFDBGauge {
    percentile: f64,
    group: String,
    op: String,
    gauge: Gauge<f64>,
}

impl HistogramPercentileFDBGauge {
    // Record pre-aggregated histogram percentiles as gauges. FoundationDB log files contain
    // histogram buckets (with upper-bound thresholds) for each `(Group, Op)` combination. This
    // gauge collects buckets from the matching log event and interpolates the requested percentile
    // under an exponential assumption.
    pub fn new(
        group: impl Into<String>,
        op: impl Into<String>,
        percentile: f64,
        gauge_name: impl Into<String>,
        description: impl Into<String>,
        meter: &Meter,
    ) -> Self {
        Self {
            percentile,
            group: group.into(),
            op: op.into(),
            gauge: meter
                .f64_gauge(gauge_name.into())
                .with_description(description.into())
                .init(),
        }
    }
}

impl FDBGauge for HistogramPercentileFDBGauge {
    fn record(&self, trace_event: &HashMap<String, Value>, labels: &[KeyValue]) -> Result<()> {
        if get_trace_field(trace_event, "Type")? != "Histogram" {
            return Ok(());
        }
        if get_trace_field(trace_event, "Group")? != self.group {
            return Ok(());
        }
        if get_trace_field(trace_event, "Op")? != self.op {
            return Ok(());
        }

        let unit_str = get_trace_field(trace_event, "Unit")?;
        let unit = match unit_str {
            "milliseconds" => HistogramUnit::Milliseconds,
            "bytes" => HistogramUnit::Bytes,
            "count" => HistogramUnit::Count,
            _ => return Ok(()),
        };
        let unit_divisor = unit.divisor();

        let total_count = get_trace_field(trace_event, "TotalCount")?.parse::<u64>()?;
        if total_count == 0 {
            return Ok(());
        }

        let mut hist: BTreeMap<u64, u64> = BTreeMap::new();

        for (k, v) in trace_event {
            if k.starts_with("LessThan") {
                let bucket_value = k.strip_prefix("LessThan").unwrap().parse::<f64>()?;
                let bucket_upper = unit.convert_bucket_upper(bucket_value);
                let count = v
                    .as_str()
                    .with_context(|| "Trace event values should be strings")?
                    .parse::<u64>()?;
                hist.insert(bucket_upper, count);
            }
        }

        if hist.is_empty() {
            return Ok(());
        }

        let mut buckets: Vec<HistogramBucket> = Vec::new();
        let mut cumulative = 0u64;
        let hist_entries: Vec<(u64, u64)> = hist
            .iter()
            .map(|(upper_bound, count)| (*upper_bound, *count))
            .collect();

        let Some((mut expected_upper, _)) = hist_entries.first().copied() else {
            return Ok(());
        };

        for (upper_bound, count) in hist_entries {
            while expected_upper < upper_bound {
                let lower_bound = expected_upper / 2;
                buckets.push(HistogramBucket {
                    lower_bound,
                    upper_bound: expected_upper,
                    count: 0,
                    cumulative_count: cumulative,
                });
                expected_upper = expected_upper.saturating_mul(2);
                if expected_upper == 0 {
                    break;
                }
            }

            cumulative += count;

            buckets.push(HistogramBucket {
                lower_bound: upper_bound / 2,
                upper_bound,
                count,
                cumulative_count: cumulative,
            });

            expected_upper = match upper_bound.checked_mul(2) {
                Some(value) => value,
                None => upper_bound,
            };
        }

        if let Some(interpolated_value) =
            interpolate_exponential_percentile(&buckets, total_count, self.percentile, unit_divisor)
        {
            self.gauge.record(interpolated_value, labels);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::metrics::{Meter, MeterProvider};
    use opentelemetry::KeyValue;
    use opentelemetry_prometheus::exporter as prometheus_exporter;
    use opentelemetry_sdk::metrics::{ManualReader, SdkMeterProvider};
    use prometheus::Registry;

    fn bucket(upper_bound: u64, count: u64, cumulative: u64) -> HistogramBucket {
        HistogramBucket {
            lower_bound: upper_bound / 2,
            upper_bound,
            count,
            cumulative_count: cumulative,
        }
    }

    fn test_meter() -> Meter {
        let reader = ManualReader::builder().build();
        let provider = SdkMeterProvider::builder().with_reader(reader).build();
        provider.meter("test")
    }

    fn prometheus_meter() -> (SdkMeterProvider, Meter, Registry) {
        let registry = Registry::new();
        let reader = prometheus_exporter()
            .with_registry(registry.clone())
            .build()
            .expect("prometheus exporter");
        let provider = SdkMeterProvider::builder().with_reader(reader).build();
        let meter = provider.meter("test");
        (provider, meter, registry)
    }

    fn base_event_with_type(trace_type: &str) -> HashMap<String, Value> {
        let mut event = HashMap::new();
        event.insert("Type".to_string(), Value::String(trace_type.to_string()));
        event
    }

    fn base_histogram_event() -> HashMap<String, Value> {
        let mut event = base_event_with_type("Histogram");
        event.insert("Group".into(), Value::String("StorageServer".into()));
        event.insert("Op".into(), Value::String("Read".into()));
        event
    }

    fn test_histogram_gauge(meter: &Meter) -> HistogramPercentileFDBGauge {
        HistogramPercentileFDBGauge::new(
            "StorageServer",
            "Read",
            0.5,
            "ss_read_latency_p50_test",
            "Read latency",
            meter,
        )
    }

    #[test]
    fn simple_gauge_records_matching_events() {
        let meter = test_meter();
        let gauge = SimpleFDBGauge::new(
            "StorageMetrics",
            "Version",
            "ss_version_test",
            "Test version gauge",
            &meter,
        );

        let mut event = base_event_with_type("StorageMetrics");
        event.insert("Version".into(), Value::String("123".into()));
        event.insert("Time".into(), Value::String("1.0".into()));

        gauge.record(&event, &[]).expect("record should succeed");
    }

    #[test]
    fn simple_gauge_applies_rolling_window() {
        let (provider, meter, registry) = prometheus_meter();
        let _provider = provider;
        let gauge = SimpleFDBGauge::new(
            "StorageMetrics",
            "Version",
            "ss_version_test",
            "Test version gauge",
            &meter,
        );

        let mut event = base_event_with_type("StorageMetrics");
        let labels = vec![KeyValue::new("machine", "test")];

        event.insert("Version".into(), Value::String("10".into()));
        event.insert("Time".into(), Value::String("100.0".into()));
        gauge
            .record(&event, &labels)
            .expect("initial record should succeed");

        event.insert("Version".into(), Value::String("20".into()));
        event.insert("Time".into(), Value::String("105.0".into()));
        gauge
            .record(&event, &labels)
            .expect("second record should succeed");

        event.insert("Version".into(), Value::String("30".into()));
        event.insert("Time".into(), Value::String("110.0".into()));
        gauge
            .record(&event, &labels)
            .expect("third record should succeed");

        let avg_three = gauge_value(&registry, "ss_version_test", "machine", "test");
        assert!(
            (avg_three - 20.0).abs() < f64::EPSILON,
            "expected average of first three samples to be 20.0, got {avg_three}"
        );

        event.insert("Version".into(), Value::String("40".into()));
        event.insert("Time".into(), Value::String("120.0".into()));
        gauge
            .record(&event, &labels)
            .expect("fourth record should succeed");

        let avg_four = gauge_value(&registry, "ss_version_test", "machine", "test");
        assert!(
            (avg_four - 30.0).abs() < f64::EPSILON,
            "expected average of the most recent samples to be 30.0, got {avg_four}"
        );
    }

    #[test]
    fn simple_gauge_errors_when_field_missing() {
        let meter = test_meter();
        let gauge = SimpleFDBGauge::new(
            "StorageMetrics",
            "Version",
            "ss_version_test",
            "Test version gauge",
            &meter,
        );

        let event = base_event_with_type("StorageMetrics");
        let err = gauge
            .record(&event, &[])
            .expect_err("missing field should error");
        assert!(
            err.to_string().contains("Version"),
            "unexpected error message: {err}"
        );
    }

    #[test]
    fn total_counter_gauge_parses_third_component() {
        let meter = test_meter();
        let gauge = TotalCounterFDBGauge::new(
            "StorageMetrics",
            "BytesDurable",
            "ss_bytes_durable_test",
            "Total bytes durable",
            &meter,
        );

        let mut event = base_event_with_type("StorageMetrics");
        event.insert("BytesDurable".into(), Value::String("1 2 3".into()));

        gauge.record(&event, &[]).expect("record should succeed");
    }

    #[test]
    fn rate_counter_gauge_parses_first_component() {
        let meter = test_meter();
        let gauge = RateCounterFDBGauge::new(
            "ProxyMetrics",
            "TxnCommitIn",
            "cp_txn_commit_in_test",
            "Txn commit rate",
            &meter,
        );

        let mut event = base_event_with_type("ProxyMetrics");
        event.insert("TxnCommitIn".into(), Value::String("42 100 200".into()));
        event.insert("Time".into(), Value::String("1.0".into()));

        gauge.record(&event, &[]).expect("record should succeed");
    }

    #[test]
    fn rate_counter_gauge_averages_last_three_samples() {
        let (provider, meter, registry) = prometheus_meter();
        let _provider = provider;
        let gauge = RateCounterFDBGauge::new(
            "ProxyMetrics",
            "TxnCommitIn",
            "cp_txn_commit_in_test",
            "Txn commit rate",
            &meter,
        );

        let mut event = base_event_with_type("ProxyMetrics");
        let labels = vec![KeyValue::new("machine", "test")];

        event.insert("TxnCommitIn".into(), Value::String("10 0 0".into()));
        event.insert("Time".into(), Value::String("100.0".into()));
        gauge
            .record(&event, &labels)
            .expect("initial record should succeed");

        event.insert("TxnCommitIn".into(), Value::String("20 0 0".into()));
        event.insert("Time".into(), Value::String("105.0".into()));
        gauge
            .record(&event, &labels)
            .expect("second record should succeed");

        event.insert("TxnCommitIn".into(), Value::String("30 0 0".into()));
        event.insert("Time".into(), Value::String("110.0".into()));
        gauge
            .record(&event, &labels)
            .expect("third record should succeed");

        let value_after_three = gauge_value(&registry, "cp_txn_commit_in_test", "machine", "test");
        assert!(
            (value_after_three - 20.0).abs() < f64::EPSILON,
            "expected average of first three samples to be 20.0, got {value_after_three}"
        );

        event.insert("TxnCommitIn".into(), Value::String("40 0 0".into()));
        event.insert("Time".into(), Value::String("120.0".into()));
        gauge
            .record(&event, &labels)
            .expect("fourth record should succeed");

        let value_after_four = gauge_value(&registry, "cp_txn_commit_in_test", "machine", "test");
        assert!(
            (value_after_four - 30.0).abs() < f64::EPSILON,
            "expected average of most recent three samples to be 30.0, got {value_after_four}"
        );
    }

    fn gauge_value(registry: &Registry, name: &str, label_name: &str, label_value: &str) -> f64 {
        let families = registry.gather();
        let family = families
            .iter()
            .find(|mf| mf.get_name() == name)
            .unwrap_or_else(|| panic!("metric family {name} not found"));
        let metric = family
            .get_metric()
            .iter()
            .find(|metric| {
                metric
                    .get_label()
                    .iter()
                    .any(|label| label.get_name() == label_name && label.get_value() == label_value)
            })
            .unwrap_or_else(|| panic!("metric with label {label_name}={label_value} not found"));
        metric.get_gauge().get_value()
    }

    #[test]
    fn elapsed_rate_gauge_divides_by_elapsed() {
        let meter = test_meter();
        let gauge = ElapsedRateFDBGauge::new(
            "ProcessMetrics",
            "CPUSeconds",
            "process_cpu_util_test",
            "CPU utilization",
            &meter,
        );

        let mut event = base_event_with_type("ProcessMetrics");
        event.insert("CPUSeconds".into(), Value::String("10.0".into()));
        event.insert("Elapsed".into(), Value::String("2.0".into()));
        event.insert("Time".into(), Value::String("1.0".into()));

        gauge.record(&event, &[]).expect("record should succeed");
    }

    #[test]
    fn elapsed_rate_gauge_applies_rolling_window() {
        let (provider, meter, registry) = prometheus_meter();
        let _provider = provider;
        let gauge = ElapsedRateFDBGauge::new(
            "ProcessMetrics",
            "CPUSeconds",
            "process_cpu_util_test",
            "CPU utilization",
            &meter,
        );

        let mut event = base_event_with_type("ProcessMetrics");
        let labels = vec![KeyValue::new("machine", "test")];

        event.insert("CPUSeconds".into(), Value::String("10.0".into()));
        event.insert("Elapsed".into(), Value::String("2.0".into()));
        event.insert("Time".into(), Value::String("100.0".into()));
        gauge
            .record(&event, &labels)
            .expect("initial record should succeed");

        event.insert("CPUSeconds".into(), Value::String("20.0".into()));
        event.insert("Elapsed".into(), Value::String("2.0".into()));
        event.insert("Time".into(), Value::String("105.0".into()));
        gauge
            .record(&event, &labels)
            .expect("second record should succeed");

        event.insert("CPUSeconds".into(), Value::String("30.0".into()));
        event.insert("Elapsed".into(), Value::String("2.0".into()));
        event.insert("Time".into(), Value::String("110.0".into()));
        gauge
            .record(&event, &labels)
            .expect("third record should succeed");

        let avg_three = gauge_value(&registry, "process_cpu_util_test", "machine", "test");
        assert!(
            (avg_three - 10.0).abs() < f64::EPSILON,
            "expected average of first three samples to be 10.0, got {avg_three}"
        );

        event.insert("CPUSeconds".into(), Value::String("40.0".into()));
        event.insert("Elapsed".into(), Value::String("2.0".into()));
        event.insert("Time".into(), Value::String("120.0".into()));
        gauge
            .record(&event, &labels)
            .expect("fourth record should succeed");

        let avg_four = gauge_value(&registry, "process_cpu_util_test", "machine", "test");
        assert!(
            (avg_four - 15.0).abs() < f64::EPSILON,
            "expected average of most recent samples to be 15.0, got {avg_four}"
        );
    }

    #[test]
    fn histogram_percentile_records_matching_histogram() {
        let meter = test_meter();
        let gauge = test_histogram_gauge(&meter);

        let mut event = base_histogram_event();
        event.insert("Unit".into(), Value::String("milliseconds".into()));
        event.insert("TotalCount".into(), Value::String("10".into()));
        event.insert("LessThan1.0".into(), Value::String("4".into()));
        event.insert("LessThan2.0".into(), Value::String("6".into()));

        gauge.record(&event, &[]).expect("record should succeed");
    }

    #[test]
    fn histogram_percentile_skips_non_histogram_events() {
        let meter = test_meter();
        let gauge = test_histogram_gauge(&meter);

        let event = base_event_with_type("LatencyMetrics");

        gauge
            .record(&event, &[])
            .expect("non-histogram events should be ignored");
    }

    #[test]
    fn histogram_percentile_skips_when_group_differs() {
        let meter = test_meter();
        let gauge = test_histogram_gauge(&meter);

        let mut event = base_histogram_event();
        event.insert("Group".into(), Value::String("OtherGroup".into()));
        event.insert("Unit".into(), Value::String("milliseconds".into()));
        event.insert("TotalCount".into(), Value::String("5".into()));

        gauge
            .record(&event, &[])
            .expect("events for other groups should be ignored");
    }

    #[test]
    fn histogram_percentile_skips_when_op_differs() {
        let meter = test_meter();
        let gauge = test_histogram_gauge(&meter);

        let mut event = base_histogram_event();
        event.insert("Op".into(), Value::String("Write".into()));
        event.insert("Unit".into(), Value::String("milliseconds".into()));
        event.insert("TotalCount".into(), Value::String("5".into()));

        gauge
            .record(&event, &[])
            .expect("events for other ops should be ignored");
    }

    #[test]
    fn histogram_percentile_skips_unknown_units() {
        let meter = test_meter();
        let gauge = test_histogram_gauge(&meter);

        let mut event = base_histogram_event();
        event.insert("Unit".into(), Value::String("seconds".into()));

        gauge
            .record(&event, &[])
            .expect("unknown histogram units should be ignored");
    }

    #[test]
    fn histogram_percentile_skips_zero_total_count() {
        let meter = test_meter();
        let gauge = test_histogram_gauge(&meter);

        let mut event = base_histogram_event();
        event.insert("Unit".into(), Value::String("milliseconds".into()));
        event.insert("TotalCount".into(), Value::String("0".into()));

        gauge
            .record(&event, &[])
            .expect("zero total count histograms should be ignored");
    }

    #[test]
    fn histogram_percentile_skips_without_buckets() {
        let meter = test_meter();
        let gauge = test_histogram_gauge(&meter);

        let mut event = base_histogram_event();
        event.insert("Unit".into(), Value::String("milliseconds".into()));
        event.insert("TotalCount".into(), Value::String("10".into()));

        gauge
            .record(&event, &[])
            .expect("histograms without buckets should be ignored");
    }

    #[test]
    fn histogram_percentile_handles_missing_intermediate_buckets() {
        let meter = test_meter();
        let gauge = test_histogram_gauge(&meter);

        let mut event = base_histogram_event();
        event.insert("Unit".into(), Value::String("milliseconds".into()));
        event.insert("TotalCount".into(), Value::String("8".into()));
        event.insert("LessThan1.0".into(), Value::String("3".into()));
        event.insert("LessThan8.0".into(), Value::String("5".into()));

        gauge
            .record(&event, &[])
            .expect("histograms with gaps should be interpolated");
    }

    #[test]
    fn interpolates_percentile_within_bucket() {
        let buckets = vec![bucket(1_000, 50, 50), bucket(2_000, 50, 100)];
        let unit_divisor = 1_000_000.0;
        let value = interpolate_exponential_percentile(&buckets, 100, 0.25, unit_divisor)
            .expect("percentile value");
        let expected = {
            let bucket_upper_seconds = 1_000f64 / unit_divisor;
            let lambda = -((1.0 - 0.5f64).ln()) / bucket_upper_seconds;
            let exp_lower = (-lambda * (bucket_upper_seconds / 2.0)).exp();
            let exp_upper = (-lambda * bucket_upper_seconds).exp();
            let target = exp_lower - 0.5 * (exp_lower - exp_upper);
            -target.ln() / lambda
        };
        assert!(
            (value - expected).abs() < 1e-12,
            "value {value} != {expected}"
        );
    }

    #[test]
    fn interpolates_percentile_in_middle_bucket() {
        let buckets = vec![
            bucket(1_000, 50, 50),
            bucket(2_000, 30, 80),
            bucket(4_000, 20, 100),
        ];
        let total_count = 100u64;
        let percentile = 0.6;
        let unit_divisor = 1_000_000.0;

        let value =
            interpolate_exponential_percentile(&buckets, total_count, percentile, unit_divisor)
                .expect("percentile value");

        let middle_bucket = buckets[1];
        let bucket_upper_seconds = middle_bucket.upper_bound as f64 / unit_divisor;
        let bucket_lower_seconds = middle_bucket.lower_bound as f64 / unit_divisor;
        let bucket_cdf = middle_bucket.cumulative_count as f64 / total_count as f64;
        let prev_cdf = buckets[0].cumulative_count as f64 / total_count as f64;
        let bucket_mass = middle_bucket.count as f64 / total_count as f64;

        let lambda = -((1.0 - bucket_cdf).ln()) / bucket_upper_seconds;
        let exp_lower = (-lambda * bucket_lower_seconds).exp();
        let exp_upper = (-lambda * bucket_upper_seconds).exp();
        let relative = ((percentile - prev_cdf) / bucket_mass).clamp(0.0, 1.0 - f64::EPSILON);
        let target = exp_lower - relative * (exp_lower - exp_upper);
        let expected = -target.ln() / lambda;

        assert!(
            (value - expected).abs() < 1e-12,
            "value {value} != {expected}"
        );
    }

    #[test]
    fn interpolates_percentile_in_last_bucket() {
        let buckets = vec![
            bucket(1_000, 50, 50),
            bucket(2_000, 30, 80),
            bucket(4_000, 20, 100),
        ];
        let total_count = 100u64;
        let percentile = 0.95;
        let unit_divisor = 1_000_000.0;

        let value =
            interpolate_exponential_percentile(&buckets, total_count, percentile, unit_divisor)
                .expect("percentile value");

        let last_bucket = buckets[2];
        let bucket_upper_seconds = last_bucket.upper_bound as f64 / unit_divisor;
        let bucket_lower_seconds = last_bucket.lower_bound as f64 / unit_divisor;
        let bucket_cdf = last_bucket.cumulative_count as f64 / total_count as f64;
        let prev_cdf = buckets[1].cumulative_count as f64 / total_count as f64;
        let bucket_mass = last_bucket.count as f64 / total_count as f64;

        let lambda = if bucket_cdf >= 1.0 && bucket_lower_seconds > 0.0 && prev_cdf < 1.0 {
            -((1.0 - prev_cdf).ln()) / bucket_lower_seconds
        } else {
            -((1.0 - bucket_cdf).ln()) / bucket_upper_seconds
        };
        let exp_lower = (-lambda * bucket_lower_seconds).exp();
        let exp_upper = (-lambda * bucket_upper_seconds).exp();
        let relative = ((percentile - prev_cdf) / bucket_mass).clamp(0.0, 1.0 - f64::EPSILON);
        let target = exp_lower - relative * (exp_lower - exp_upper);
        let expected = -target.ln() / lambda;

        assert!(
            (value - expected).abs() < 1e-12,
            "value {value} != {expected}"
        );
    }

    #[test]
    fn clamps_to_bucket_lower_for_zero_percentile() {
        let buckets = vec![bucket(1_000, 50, 50), bucket(2_000, 50, 100)];
        let value = interpolate_exponential_percentile(&buckets, 100, 0.0, 1_000_000.0)
            .expect("percentile value");
        assert!((value - 0.0005).abs() < 1e-12);
    }

    #[test]
    fn returns_bucket_upper_for_full_percentile() {
        let buckets = vec![bucket(1_000, 50, 50), bucket(2_000, 50, 100)];
        let value = interpolate_exponential_percentile(&buckets, 100, 1.0, 1_000_000.0)
            .expect("percentile value");
        assert!((value - 0.002).abs() < 1e-12);
    }

    #[test]
    fn none_for_empty_input() {
        assert!(interpolate_exponential_percentile(&[], 0, 0.5, 1.0).is_none());
    }

    #[test]
    fn interpolates_histogram_without_scaling_for_unit_one() {
        for &upper in &[128u64, 32u64] {
            let buckets = vec![bucket(upper, 50, 50), bucket(upper * 2, 50, 100)];
            let value = interpolate_exponential_percentile(&buckets, 100, 0.25, 1.0)
                .expect("percentile value");

            let bucket_upper = upper as f64;
            let lambda = -((1.0 - 0.5f64).ln()) / bucket_upper;
            let exp_lower = (-lambda * (bucket_upper / 2.0)).exp();
            let exp_upper = (-lambda * bucket_upper).exp();
            let target = exp_lower - 0.5 * (exp_lower - exp_upper);
            let expected = -target.ln() / lambda;

            assert!(
                (value - expected).abs() < 1e-12,
                "upper {upper} value {value} != {expected}"
            );
        }
    }
}
