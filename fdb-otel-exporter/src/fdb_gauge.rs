use crate::fdb_metric::FDBMetric;
use anyhow::{ensure, Context, Result};
use opentelemetry::metrics::{Meter, ObservableGauge};
use opentelemetry::KeyValue;
use serde_json::Value;
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    f64,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

const GAUGE_VALUE_TTL: Duration = Duration::from_secs(5 * 60);

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

    fn bucket_lower_bound(&self, upper_bound: u64, previous_upper_bound: Option<u64>) -> u64 {
        match self {
            Self::Milliseconds | Self::Bytes => power_of_two_bucket_lower_bound(upper_bound),
            // FoundationDB's countLinear histogram width is configured at the call site and is
            // not included in the trace event. It also omits empty buckets. The previous emitted
            // boundary is therefore the only defensible lower bound available to the exporter;
            // assuming a unit-width bucket substantially overstates percentiles for wider ranges.
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
    gauge: PersistentGauge,
}

#[derive(Clone)]
struct CachedGaugeValue {
    value: f64,
    labels: Vec<KeyValue>,
    updated_at: Instant,
}

#[derive(Clone)]
struct PersistentGauge {
    name: String,
    values: Arc<Mutex<HashMap<LabelKey, CachedGaugeValue>>>,
    // Retain the observable instrument for as long as the metric is registered.
    _instrument: ObservableGauge<f64>,
}

impl PersistentGauge {
    fn new(gauge_name: impl Into<String>, description: impl Into<String>, meter: &Meter) -> Self {
        let name = gauge_name.into();
        let values = Arc::new(Mutex::new(HashMap::<LabelKey, CachedGaugeValue>::new()));
        let callback_values = Arc::clone(&values);
        let instrument = meter
            .f64_observable_gauge(name.clone())
            .with_description(description.into())
            .with_callback(move |observer| {
                let now = Instant::now();
                let Ok(mut values) = callback_values.lock() else {
                    return;
                };

                values.retain(|_, sample| {
                    now.saturating_duration_since(sample.updated_at) <= GAUGE_VALUE_TTL
                });
                for sample in values.values() {
                    observer.observe(sample.value, &sample.labels);
                }
            })
            .init();

        Self {
            name,
            values,
            _instrument: instrument,
        }
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn record(&self, value: f64, labels: &[KeyValue]) -> Result<()> {
        ensure!(value.is_finite(), "gauge value must be finite");
        let now = Instant::now();
        let mut values = self
            .values
            .lock()
            .map_err(|_| anyhow::anyhow!("gauge value cache poisoned"))?;
        values.retain(|_, sample| {
            now.saturating_duration_since(sample.updated_at) <= GAUGE_VALUE_TTL
        });
        values.insert(
            LabelKey::from_labels(labels),
            CachedGaugeValue {
                value,
                labels: labels.to_vec(),
                updated_at: now,
            },
        );
        Ok(())
    }
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

fn parse_finite_value(value: &str, field_name: &str) -> Result<f64> {
    let value = value
        .parse::<f64>()
        .with_context(|| format!("Invalid {field_name} field"))?;
    ensure!(value.is_finite(), "{field_name} field must be finite");
    Ok(value)
}

fn parse_finite_trace_field(trace_event: &HashMap<String, Value>, field_name: &str) -> Result<f64> {
    parse_finite_value(get_trace_field(trace_event, field_name)?, field_name)
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
            gauge: PersistentGauge::new(gauge_name, description, meter),
        }
    }
}

#[derive(Clone)]
pub struct SimpleFDBGauge {
    gauge_impl: FDBGaugeImpl,
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
        }
    }
}

impl FDBMetric for SimpleFDBGauge {
    fn name(&self) -> &str {
        self.gauge_impl.gauge.name()
    }

    fn event_type(&self) -> Option<&str> {
        Some(&self.gauge_impl.trace_type)
    }

    fn record(&self, trace_event: &HashMap<String, Value>, labels: &[KeyValue]) -> Result<()> {
        let trace_type = get_trace_field(trace_event, "Type")?;

        if trace_type == self.gauge_impl.trace_type {
            let value = trace_event
                .get(self.gauge_impl.field_name.as_str())
                .and_then(|v| v.as_str())
                .with_context(|| format!("Missing {} field", self.gauge_impl.field_name))?;
            let sample = parse_finite_value(value, self.gauge_impl.field_name.as_str())?;
            self.gauge_impl.gauge.record(sample, labels)?;
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
    fn name(&self) -> &str {
        self.gauge_impl.gauge.name()
    }

    fn event_type(&self) -> Option<&str> {
        Some(&self.gauge_impl.trace_type)
    }

    fn record(&self, trace_event: &HashMap<String, Value>, labels: &[KeyValue]) -> Result<()> {
        let trace_type = get_trace_field(trace_event, "Type")?;

        if trace_type == self.gauge_impl.trace_type {
            let value = get_trace_field(trace_event, self.gauge_impl.field_name.as_str())?;
            let value = value
                .split_whitespace()
                .nth(2)
                .with_context(|| format!("Malformed {} counter", self.gauge_impl.field_name))?;
            let value = parse_finite_value(value, self.gauge_impl.field_name.as_str())?;
            self.gauge_impl.gauge.record(value, labels)?;
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
    samples: Arc<Mutex<HashMap<LabelKey, RollingSeries>>>,
}

struct RollingSeries {
    samples: VecDeque<TimedSample>,
    updated_at: Instant,
}

impl RollingWindow {
    fn new(window_seconds: f64) -> Self {
        Self {
            window_seconds,
            samples: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn observe(&self, labels: &[KeyValue], time: f64, value: f64) -> Result<f64> {
        let key = LabelKey::from_labels(labels);
        let now = Instant::now();
        let mut samples = self
            .samples
            .lock()
            .map_err(|_| anyhow::anyhow!("rolling window sample cache poisoned"))?;
        samples.retain(|_, series| {
            now.saturating_duration_since(series.updated_at) <= GAUGE_VALUE_TTL
        });

        let series = samples.entry(key).or_insert_with(|| RollingSeries {
            samples: VecDeque::new(),
            updated_at: now,
        });
        series.updated_at = now;
        series.samples.push_back(TimedSample { time, value });
        series
            .samples
            .make_contiguous()
            .sort_by(|left, right| left.time.total_cmp(&right.time));

        let newest_time = series.samples.back().map_or(time, |sample| sample.time);
        while let Some(front) = series.samples.front() {
            if newest_time - front.time > self.window_seconds {
                series.samples.pop_front();
            } else {
                break;
            }
        }
        let count = series.samples.len() as f64;
        if count == 0.0 {
            Ok(value)
        } else {
            Ok(series
                .samples
                .iter()
                .map(|sample| sample.value)
                .sum::<f64>()
                / count)
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
    fn name(&self) -> &str {
        self.gauge_impl.gauge.name()
    }

    fn event_type(&self) -> Option<&str> {
        Some(&self.gauge_impl.trace_type)
    }

    fn record(&self, trace_event: &HashMap<String, Value>, labels: &[KeyValue]) -> Result<()> {
        let trace_type = get_trace_field(trace_event, "Type")?;

        if trace_type == self.gauge_impl.trace_type {
            let value = get_trace_field(trace_event, self.gauge_impl.field_name.as_str())?;
            let sample = value
                .split_whitespace()
                .next()
                .with_context(|| format!("Malformed {} counter", self.gauge_impl.field_name))?;
            let sample = parse_finite_value(sample, self.gauge_impl.field_name.as_str())?;
            let time = parse_finite_trace_field(trace_event, "Time")?;

            let averaged = self.rolling_window.observe(labels, time, sample)?;

            self.gauge_impl.gauge.record(averaged, labels)?;
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
    fn name(&self) -> &str {
        self.gauge_impl.gauge.name()
    }

    fn event_type(&self) -> Option<&str> {
        Some(&self.gauge_impl.trace_type)
    }

    fn record(&self, trace_event: &HashMap<String, Value>, labels: &[KeyValue]) -> Result<()> {
        let trace_type = get_trace_field(trace_event, "Type")?;

        if trace_type == self.gauge_impl.trace_type {
            let value = parse_finite_trace_field(trace_event, self.gauge_impl.field_name.as_str())?;
            let elapsed = parse_finite_trace_field(trace_event, "Elapsed")?;
            ensure!(elapsed > 0.0, "Elapsed field must be greater than zero");
            let time = parse_finite_trace_field(trace_event, "Time")?;
            let sample = value / elapsed;

            let averaged = self.rolling_window.observe(labels, time, sample)?;

            self.gauge_impl.gauge.record(averaged, labels)?;
        }
        Ok(())
    }
}

// Because histograms are precomputed, interpolate percentiles and emit as gauge
pub struct HistogramPercentileFDBGauge {
    group: String,
    op: String,
    name: String,
    percentiles: Vec<HistogramPercentileOutput>,
}

struct HistogramPercentileOutput {
    percentile: f64,
    gauge: PersistentGauge,
}

impl HistogramPercentileFDBGauge {
    // Record pre-aggregated histogram percentiles as gauges. FoundationDB log files contain
    // histogram buckets (with upper-bound thresholds) for each `(Group, Op)` combination. This
    // gauge collects buckets from the matching log event and interpolates the requested percentile
    // according to the bucket layout: geometric for power-of-two latency and byte buckets, and
    // linear between the available boundaries for count buckets.
    #[cfg(test)]
    pub fn new(
        group: impl Into<String>,
        op: impl Into<String>,
        percentile: f64,
        gauge_name: impl Into<String>,
        description: impl Into<String>,
        meter: &Meter,
    ) -> Self {
        Self::new_grouped(
            group,
            op,
            vec![(percentile, gauge_name.into(), description.into())],
            meter,
        )
    }

    /// Build one histogram handler for every configured percentile sharing the same `(Group, Op)`.
    /// The trace buckets are parsed once per event and reused for all output gauges.
    pub fn new_grouped(
        group: impl Into<String>,
        op: impl Into<String>,
        percentiles: Vec<(f64, String, String)>,
        meter: &Meter,
    ) -> Self {
        let group = group.into();
        let op = op.into();
        Self {
            name: format!("Histogram[{group}/{op}]"),
            group,
            op,
            percentiles: percentiles
                .into_iter()
                .map(
                    |(percentile, gauge_name, description)| HistogramPercentileOutput {
                        percentile,
                        gauge: PersistentGauge::new(gauge_name, description, meter),
                    },
                )
                .collect(),
        }
    }
}

impl FDBMetric for HistogramPercentileFDBGauge {
    fn name(&self) -> &str {
        &self.name
    }

    fn event_type(&self) -> Option<&str> {
        Some("Histogram")
    }

    fn histogram_key(&self) -> Option<(&str, &str)> {
        Some((&self.group, &self.op))
    }

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
                let bucket_value = parse_finite_value(
                    k.strip_prefix("LessThan").unwrap(),
                    "histogram bucket boundary",
                )?;
                ensure!(
                    bucket_value >= 0.0,
                    "histogram bucket boundary must not be negative"
                );
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
            cumulative = cumulative
                .checked_add(count)
                .context("histogram bucket count overflow")?;

            buckets.push(HistogramBucket {
                lower_bound: unit.bucket_lower_bound(upper_bound, previous_upper_bound),
                upper_bound,
                count,
                cumulative_count: cumulative,
            });
            previous_upper_bound = Some(upper_bound);
        }

        let mut failures = Vec::new();
        for output in &self.percentiles {
            if let Some(interpolated_value) =
                unit.interpolate_percentile(&buckets, total_count, output.percentile)
            {
                if let Err(error) = output.gauge.record(interpolated_value, labels) {
                    failures.push(format!("{}: {error:#}", output.gauge.name()));
                }
            }
        }

        if !failures.is_empty() {
            anyhow::bail!(
                "failed to update {} histogram percentile gauge(s): {}",
                failures.len(),
                failures.join("; ")
            );
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

        gauge.record(&event, &[]).expect("record should succeed");
    }

    #[test]
    fn simple_gauge_records_latest_value_and_persists_across_gathers() {
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
        gauge
            .record(&event, &labels)
            .expect("initial record should succeed");

        event.insert("Version".into(), Value::String("20".into()));
        gauge
            .record(&event, &labels)
            .expect("second record should succeed");

        event.insert("Version".into(), Value::String("30".into()));
        gauge
            .record(&event, &labels)
            .expect("third record should succeed");

        let first_gather = gauge_value(&registry, "ss_version_test", "machine", "test");
        assert!(
            (first_gather - 30.0).abs() < f64::EPSILON,
            "expected the latest point-in-time value, got {first_gather}"
        );

        let second_gather = gauge_value(&registry, "ss_version_test", "machine", "test");
        assert!(
            (second_gather - 30.0).abs() < f64::EPSILON,
            "expected the gauge to remain present on a consecutive gather, got {second_gather}"
        );
    }

    #[test]
    fn simple_gauge_rejects_non_finite_values() {
        let meter = test_meter();
        let gauge = SimpleFDBGauge::new(
            "StorageMetrics",
            "Version",
            "ss_version_test",
            "Test version gauge",
            &meter,
        );

        for value in ["NaN", "inf", "-inf"] {
            let mut event = base_event_with_type("StorageMetrics");
            event.insert("Version".into(), Value::String(value.into()));
            let error = gauge
                .record(&event, &[])
                .expect_err("non-finite values must be rejected");
            assert!(
                error.to_string().contains("finite"),
                "unexpected error: {error}"
            );
        }
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
    fn total_counter_gauge_rejects_non_finite_value() {
        let meter = test_meter();
        let gauge = TotalCounterFDBGauge::new(
            "StorageMetrics",
            "BytesDurable",
            "ss_bytes_durable_test",
            "Total bytes durable",
            &meter,
        );

        let mut event = base_event_with_type("StorageMetrics");
        event.insert("BytesDurable".into(), Value::String("1 2 NaN".into()));

        let error = gauge
            .record(&event, &[])
            .expect_err("non-finite counter values must be rejected");
        assert!(
            error.to_string().contains("finite"),
            "unexpected error: {error}"
        );
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
    fn rate_counter_gauge_rejects_non_finite_sample_and_time() {
        let meter = test_meter();
        let gauge = RateCounterFDBGauge::new(
            "ProxyMetrics",
            "TxnCommitIn",
            "cp_txn_commit_in_test",
            "Txn commit rate",
            &meter,
        );

        let mut event = base_event_with_type("ProxyMetrics");
        event.insert("TxnCommitIn".into(), Value::String("NaN 0 0".into()));
        event.insert("Time".into(), Value::String("1.0".into()));
        let error = gauge
            .record(&event, &[])
            .expect_err("non-finite samples must be rejected");
        assert!(
            error.to_string().contains("finite"),
            "unexpected error: {error}"
        );

        event.insert("TxnCommitIn".into(), Value::String("42 0 0".into()));
        event.insert("Time".into(), Value::String("inf".into()));
        let error = gauge
            .record(&event, &[])
            .expect_err("non-finite timestamps must be rejected");
        assert!(
            error.to_string().contains("finite"),
            "unexpected error: {error}"
        );
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

    #[test]
    fn rolling_window_discards_expired_out_of_order_samples() {
        let window = RollingWindow::new(ROLLING_WINDOW_SECONDS);
        let labels = vec![KeyValue::new("machine", "test")];

        window
            .observe(&labels, 100.0, 10.0)
            .expect("initial sample should be accepted");
        let latest = window
            .observe(&labels, 120.0, 30.0)
            .expect("newer sample should be accepted");
        assert!((latest - 30.0).abs() < f64::EPSILON);

        let after_late_sample = window
            .observe(&labels, 90.0, 5.0)
            .expect("late sample should be handled");
        assert!(
            (after_late_sample - 30.0).abs() < f64::EPSILON,
            "expired late samples must not re-enter the active window"
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
    fn elapsed_rate_gauge_rejects_non_positive_and_non_finite_elapsed() {
        let meter = test_meter();
        let gauge = ElapsedRateFDBGauge::new(
            "ProcessMetrics",
            "CPUSeconds",
            "process_cpu_util_test",
            "CPU utilization",
            &meter,
        );

        for elapsed in ["0", "-1", "NaN", "inf"] {
            let mut event = base_event_with_type("ProcessMetrics");
            event.insert("CPUSeconds".into(), Value::String("10.0".into()));
            event.insert("Elapsed".into(), Value::String(elapsed.into()));
            event.insert("Time".into(), Value::String("1.0".into()));

            gauge
                .record(&event, &[])
                .expect_err("invalid elapsed values must be rejected");
        }
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
    fn grouped_histogram_handler_emits_multiple_percentiles() {
        let (provider, meter, registry) = prometheus_meter();
        let _provider = provider;
        let gauge = HistogramPercentileFDBGauge::new_grouped(
            "StorageServer",
            "Read",
            vec![
                (
                    0.5,
                    "ss_read_latency_grouped_p50_test".to_owned(),
                    "Read latency p50".to_owned(),
                ),
                (
                    0.9,
                    "ss_read_latency_grouped_p90_test".to_owned(),
                    "Read latency p90".to_owned(),
                ),
            ],
            &meter,
        );
        let labels = vec![KeyValue::new("machine", "test")];
        let mut event = base_histogram_event();
        event.insert("Unit".into(), Value::String("milliseconds".into()));
        event.insert("TotalCount".into(), Value::String("10".into()));
        event.insert("LessThan1.0".into(), Value::String("5".into()));
        event.insert("LessThan2.0".into(), Value::String("5".into()));

        gauge
            .record(&event, &labels)
            .expect("all grouped percentiles should be recorded");

        let p50 = gauge_value(
            &registry,
            "ss_read_latency_grouped_p50_test",
            "machine",
            "test",
        );
        let p90 = gauge_value(
            &registry,
            "ss_read_latency_grouped_p90_test",
            "machine",
            "test",
        );
        assert!((p50 - 0.001).abs() < 1e-12, "unexpected p50: {p50}");
        assert!(
            (p90 - 0.001_741_101_126_592_248_2).abs() < 1e-12,
            "unexpected p90: {p90}"
        );
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
    fn histogram_percentile_uses_reported_count_boundaries_instead_of_unit_width() {
        let (provider, meter, registry) = prometheus_meter();
        let _provider = provider;
        let gauge = test_histogram_gauge(&meter);
        let labels = vec![KeyValue::new("machine", "test")];

        let mut event = base_histogram_event();
        event.insert("Unit".into(), Value::String("count".into()));
        event.insert("TotalCount".into(), Value::String("6".into()));
        event.insert("LessThan10".into(), Value::String("2".into()));
        event.insert("LessThan20".into(), Value::String("4".into()));

        gauge
            .record(&event, &labels)
            .expect("count histogram should be interpolated");

        let value = gauge_value(&registry, "ss_read_latency_p50_test", "machine", "test");
        assert!(
            (value - 12.5).abs() < 1e-12,
            "expected linear interpolation within [10, 20), got {value}"
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
