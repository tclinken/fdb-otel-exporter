use crate::fdb_metric::FDBMetric;
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

    fn bucket_lower_bound(&self, previous_upper_bound: Option<u64>, upper_bound: u64) -> u64 {
        match self {
            Self::Milliseconds | Self::Bytes => power_of_two_bucket_lower_bound(upper_bound),
            Self::Count => previous_upper_bound.unwrap_or(0),
        }
    }

    fn interpolate_percentile(
        &self,
        buckets: &[HistogramBucket],
        total_count: u64,
        percentile: f64,
    ) -> Option<f64> {
        match self {
            Self::Milliseconds | Self::Bytes => {
                interpolate_geometric_percentile(buckets, total_count, percentile, self.divisor())
            }
            Self::Count => {
                interpolate_linear_percentile(buckets, total_count, percentile, self.divisor())
            }
        }
    }
}

fn power_of_two_bucket_lower_bound(upper_bound: u64) -> u64 {
    if upper_bound == 2 {
        0
    } else {
        upper_bound / 2
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

type BucketInterpolator = fn(f64, f64, f64) -> f64;

fn interpolate_geometric(lower_bound: f64, upper_bound: f64, fraction: f64) -> f64 {
    // Log-space interpolation is undefined at zero, so use linear interpolation
    // for the first FDB bucket, which contains values in [0, 2).
    if lower_bound <= 0.0 {
        return interpolate_linear(lower_bound, upper_bound, fraction);
    }

    lower_bound * (fraction * (upper_bound / lower_bound).ln()).exp()
}

fn interpolate_linear(lower_bound: f64, upper_bound: f64, fraction: f64) -> f64 {
    lower_bound + fraction * (upper_bound - lower_bound)
}

// Interpolate a percentile value from histogram buckets using the supplied
// within-bucket interpolation function.
// The buckets are derived from FoundationDB `LessThan` lines, converted to their base units
// (microseconds for latency histograms, bytes for size histograms, or counts for raw counters),
// and paired with running cumulative counts so this helper can locate the bucket that spans the
// percentile and interpolate within it.
fn interpolate_percentile(
    buckets: &[HistogramBucket],
    total_count: u64,
    percentile: f64,
    unit_divisor: f64,
    interpolate_bucket: BucketInterpolator,
) -> Option<f64> {
    if buckets.is_empty()
        || total_count == 0
        || !percentile.is_finite()
        || unit_divisor <= 0.0
        || !unit_divisor.is_finite()
    {
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

    if !bucket_lower_value.is_finite()
        || !bucket_upper_value.is_finite()
        || bucket_lower_value < 0.0
        || bucket_upper_value < bucket_lower_value
    {
        return None;
    }

    let lower_cumulative_count = bucket.cumulative_count.saturating_sub(bucket.count);

    if bucket.count == 0 {
        return Some(bucket_upper_value);
    }

    let relative_rank =
        ((target_rank - lower_cumulative_count as f64) / bucket.count as f64).clamp(0.0, 1.0);
    let value = interpolate_bucket(bucket_lower_value, bucket_upper_value, relative_rank);

    if !value.is_finite() {
        return Some(bucket_upper_value);
    }

    Some(value.clamp(bucket_lower_value, bucket_upper_value))
}

// FDB's latency and byte histograms use power-of-two bucket boundaries. Treat
// observations as uniformly distributed in log space within those buckets.
fn interpolate_geometric_percentile(
    buckets: &[HistogramBucket],
    total_count: u64,
    percentile: f64,
    unit_divisor: f64,
) -> Option<f64> {
    interpolate_percentile(
        buckets,
        total_count,
        percentile,
        unit_divisor,
        interpolate_geometric,
    )
}

// FDB also has linearly spaced count histograms, which must not use geometric
// interpolation.
fn interpolate_linear_percentile(
    buckets: &[HistogramBucket],
    total_count: u64,
    percentile: f64,
    unit_divisor: f64,
) -> Option<f64> {
    interpolate_percentile(
        buckets,
        total_count,
        percentile,
        unit_divisor,
        interpolate_linear,
    )
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
    rolling_window: RollingWindow,
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
            rolling_window: RollingWindow::new(ROLLING_WINDOW_SECONDS),
        }
    }
}

impl FDBMetric for SimpleFDBGauge {
    fn record(&self, trace_event: &HashMap<String, Value>, labels: &[KeyValue]) -> Result<()> {
        let trace_type = get_trace_field(trace_event, "Type")?;

        if trace_type == self.gauge_impl.trace_type {
            let value = trace_event
                .get(self.gauge_impl.field_name.as_str())
                .and_then(|v| v.as_str())
                .with_context(|| format!("Missing {} field", self.gauge_impl.field_name))?;
            let sample = value.parse::<f64>()?;
            let time = get_trace_field(trace_event, "Time")?.parse::<f64>()?;

            let averaged = self.rolling_window.observe(labels, time, sample);

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

impl FDBMetric for TotalCounterFDBGauge {
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
struct RollingWindow {
    window_seconds: f64,
    samples: Arc<Mutex<HashMap<LabelKey, VecDeque<TimedSample>>>>,
}

impl RollingWindow {
    fn new(window_seconds: f64) -> Self {
        Self {
            window_seconds,
            samples: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn observe(&self, labels: &[KeyValue], time: f64, value: f64) -> f64 {
        let key = LabelKey::from_labels(labels);
        let mut samples = self
            .samples
            .lock()
            .expect("rolling window sample cache poisoned");
        let window = samples.entry(key).or_default();
        window.push_back(TimedSample { time, value });
        while let Some(front) = window.front() {
            if time - front.time > self.window_seconds {
                window.pop_front();
            } else {
                break;
            }
        }
        let count = window.len() as f64;
        if count == 0.0 {
            value
        } else {
            window.iter().map(|s| s.value).sum::<f64>() / count
        }
    }
}

#[derive(Clone)]
// Maintains a 15 second rolling mean of raw samples keyed by label set so Prometheus scrapes see a
// stable value even when scrape periods exceed log emission frequency.
pub struct RateCounterFDBGauge {
    gauge_impl: FDBGaugeImpl,
    rolling_window: RollingWindow,
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
            rolling_window: RollingWindow::new(ROLLING_WINDOW_SECONDS),
        }
    }
}

impl FDBMetric for RateCounterFDBGauge {
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

            let averaged = self.rolling_window.observe(labels, time, sample);

            self.gauge_impl.gauge.record(averaged, labels);
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct ElapsedRateFDBGauge {
    gauge_impl: FDBGaugeImpl,
    rolling_window: RollingWindow,
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
            rolling_window: RollingWindow::new(ROLLING_WINDOW_SECONDS),
        }
    }
}

impl FDBMetric for ElapsedRateFDBGauge {
    fn record(&self, trace_event: &HashMap<String, Value>, labels: &[KeyValue]) -> Result<()> {
        let trace_type = get_trace_field(trace_event, "Type")?;

        if trace_type == self.gauge_impl.trace_type {
            let value = get_trace_field(trace_event, self.gauge_impl.field_name.as_str())?
                .parse::<f64>()?;
            let elapsed = get_trace_field(trace_event, "Elapsed")?.parse::<f64>()?;
            let time = get_trace_field(trace_event, "Time")?.parse::<f64>()?;
            let sample = value / elapsed;

            let averaged = self.rolling_window.observe(labels, time, sample);

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
    // according to the bucket layout: geometric for power-of-two latency and byte buckets, and
    // linear for unit-width count buckets.
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

impl FDBMetric for HistogramPercentileFDBGauge {
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
        let mut previous_upper_bound = None;
        for (upper_bound, count) in hist {
            cumulative += count;

            buckets.push(HistogramBucket {
                lower_bound: unit.bucket_lower_bound(previous_upper_bound, upper_bound),
                upper_bound,
                count,
                cumulative_count: cumulative,
            });
            previous_upper_bound = Some(upper_bound);
        }

        if let Some(interpolated_value) =
            unit.interpolate_percentile(&buckets, total_count, self.percentile)
        {
            self.gauge.record(interpolated_value, labels);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::metrics::{find_metric, prometheus_meter};
    use opentelemetry::metrics::{Meter, MeterProvider};
    use opentelemetry::KeyValue;
    use opentelemetry_sdk::metrics::{ManualReader, SdkMeterProvider};
    use prometheus::Registry;

    fn bucket(lower_bound: u64, upper_bound: u64, count: u64, cumulative: u64) -> HistogramBucket {
        HistogramBucket {
            lower_bound,
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
        let metric = find_metric(registry, name, label_name, label_value)
            .unwrap_or_else(|| panic!("metric {name} with {label_name}={label_value} not found"));
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
    fn histogram_percentile_uses_zero_lower_bound_for_first_fdb_bucket() {
        let (provider, meter, registry) = prometheus_meter();
        let _provider = provider;
        let gauge = test_histogram_gauge(&meter);
        let labels = vec![KeyValue::new("machine", "test")];

        let mut event = base_histogram_event();
        event.insert("Unit".into(), Value::String("milliseconds".into()));
        event.insert("TotalCount".into(), Value::String("4".into()));
        event.insert("LessThan0.002".into(), Value::String("4".into()));

        gauge
            .record(&event, &labels)
            .expect("first histogram bucket should be interpolated");

        let value = gauge_value(&registry, "ss_read_latency_p50_test", "machine", "test");
        assert!(
            (value - 0.000_001).abs() < 1e-12,
            "expected midpoint of [0, 2) microseconds, got {value}"
        );
    }

    #[test]
    fn histogram_percentile_interpolates_count_buckets_linearly() {
        let (provider, meter, registry) = prometheus_meter();
        let _provider = provider;
        let gauge = test_histogram_gauge(&meter);
        let labels = vec![KeyValue::new("machine", "test")];

        let mut event = base_histogram_event();
        event.insert("Unit".into(), Value::String("count".into()));
        event.insert("TotalCount".into(), Value::String("6".into()));
        event.insert("LessThan1".into(), Value::String("2".into()));
        event.insert("LessThan2".into(), Value::String("4".into()));

        gauge
            .record(&event, &labels)
            .expect("count histogram should be interpolated");

        let value = gauge_value(&registry, "ss_read_latency_p50_test", "machine", "test");
        assert!(
            (value - 1.25).abs() < 1e-12,
            "expected linear interpolation within [1, 2), got {value}"
        );
    }

    #[test]
    fn histogram_percentile_interpolates_count_buckets_from_trace_boundaries() {
        let (provider, meter, registry) = prometheus_meter();
        let _provider = provider;
        let gauge = HistogramPercentileFDBGauge::new(
            "modifyItemCount",
            "L1",
            0.5,
            "modify_item_count_p50_test",
            "Modify item count p50",
            &meter,
        );
        let labels = vec![KeyValue::new("machine", "10.0.221.58:4500")];
        let event: HashMap<String, Value> = serde_json::from_str(
            r#"{
                "Type": "Histogram",
                "Group": "modifyItemCount",
                "Op": "L1",
                "Unit": "count",
                "LessThan20": "18",
                "LessThan40": "4",
                "LessThan60": "109",
                "TotalCount": "183"
            }"#,
        )
        .expect("example histogram event should parse");

        gauge
            .record(&event, &labels)
            .expect("count histogram should be interpolated");

        let value = gauge_value(
            &registry,
            "modify_item_count_p50_test",
            "machine",
            "10.0.221.58:4500",
        );
        let expected = 40.0 + ((0.5 * 183.0 - 22.0) / 109.0) * 20.0;
        assert!(
            (value - expected).abs() < 1e-12,
            "expected p50 interpolation within the [40, 60) bucket, got {value}"
        );
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
    fn geometric_interpolation_uses_bucket_midpoint() {
        let buckets = vec![bucket(500, 1_000, 50, 50), bucket(1_000, 2_000, 50, 100)];
        let unit_divisor = 1_000_000.0;
        let value = interpolate_geometric_percentile(&buckets, 100, 0.25, unit_divisor)
            .expect("percentile value");
        let expected = (500.0_f64 * 1_000.0).sqrt() / unit_divisor;
        assert!(
            (value - expected).abs() < 1e-12,
            "value {value} != {expected}"
        );
    }

    #[test]
    fn geometric_interpolation_uses_rank_within_middle_bucket() {
        let buckets = vec![
            bucket(500, 1_000, 50, 50),
            bucket(1_000, 2_000, 30, 80),
            bucket(2_000, 4_000, 20, 100),
        ];
        let unit_divisor = 1_000_000.0;

        let value = interpolate_geometric_percentile(&buckets, 100, 0.6, unit_divisor)
            .expect("percentile value");
        let expected = 1_000.0 * 2.0_f64.powf(1.0 / 3.0) / unit_divisor;

        assert!(
            (value - expected).abs() < 1e-12,
            "value {value} != {expected}"
        );
    }

    #[test]
    fn geometric_interpolation_uses_rank_within_final_bucket() {
        let buckets = vec![
            bucket(500, 1_000, 50, 50),
            bucket(1_000, 2_000, 30, 80),
            bucket(2_000, 4_000, 20, 100),
        ];
        let unit_divisor = 1_000_000.0;

        let value = interpolate_geometric_percentile(&buckets, 100, 0.95, unit_divisor)
            .expect("percentile value");
        let expected = 2_000.0 * 2.0_f64.powf(0.75) / unit_divisor;

        assert!(
            (value - expected).abs() < 1e-12,
            "value {value} != {expected}"
        );
    }

    #[test]
    fn geometric_interpolation_returns_bucket_lower_for_zero_percentile() {
        let buckets = vec![bucket(500, 1_000, 50, 50), bucket(1_000, 2_000, 50, 100)];
        let value = interpolate_geometric_percentile(&buckets, 100, 0.0, 1_000_000.0)
            .expect("percentile value");
        assert!((value - 0.0005).abs() < 1e-12);
    }

    #[test]
    fn geometric_interpolation_returns_bucket_upper_for_full_percentile() {
        let buckets = vec![bucket(500, 1_000, 50, 50), bucket(1_000, 2_000, 50, 100)];
        let value = interpolate_geometric_percentile(&buckets, 100, 1.0, 1_000_000.0)
            .expect("percentile value");
        assert!((value - 0.002).abs() < 1e-12);
    }

    #[test]
    fn none_for_empty_input() {
        assert!(interpolate_geometric_percentile(&[], 0, 0.5, 1.0).is_none());
    }

    #[test]
    fn geometric_interpolation_works_without_unit_scaling() {
        for &upper in &[128u64, 32u64] {
            let buckets = vec![
                bucket(upper / 2, upper, 50, 50),
                bucket(upper, upper * 2, 50, 100),
            ];
            let value = interpolate_geometric_percentile(&buckets, 100, 0.25, 1.0)
                .expect("percentile value");
            let expected = ((upper / 2) as f64 * upper as f64).sqrt();

            assert!(
                (value - expected).abs() < 1e-12,
                "upper {upper} value {value} != {expected}"
            );
        }
    }

    #[test]
    fn geometric_interpolation_is_linear_at_zero() {
        let buckets = vec![bucket(0, 2, 100, 100)];
        let value =
            interpolate_geometric_percentile(&buckets, 100, 0.25, 1.0).expect("percentile value");
        assert!((value - 0.5).abs() < 1e-12);
    }

    #[test]
    fn linear_interpolation_supports_unit_width_count_buckets() {
        let buckets = vec![bucket(0, 1, 2, 2), bucket(1, 2, 4, 6)];
        let value = interpolate_linear_percentile(&buckets, 6, 0.5, 1.0).expect("percentile value");
        assert!((value - 1.25).abs() < 1e-12);
    }
}
