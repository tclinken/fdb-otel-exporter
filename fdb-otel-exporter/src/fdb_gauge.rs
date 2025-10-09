use anyhow::{Context, Result};
use opentelemetry::metrics::{Gauge, Meter};
use opentelemetry::KeyValue;
use serde_json::Value;
use std::{
    collections::{BTreeMap, HashMap},
    f64,
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

impl FDBGauge for SimpleFDBGauge {
    fn record(&self, trace_event: &HashMap<String, Value>, labels: &[KeyValue]) -> Result<()> {
        let trace_type = get_trace_field(trace_event, "Type")?;

        if trace_type == self.gauge_impl.trace_type {
            let value = trace_event
                .get(self.gauge_impl.field_name.as_str())
                .and_then(|v| v.as_str())
                .with_context(|| format!("Missing {} field", self.gauge_impl.field_name))?;
            self.gauge_impl.gauge.record(value.parse::<f64>()?, labels);
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

pub struct RateCounterFDBGauge {
    gauge_impl: FDBGaugeImpl,
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
        }
    }
}

impl FDBGauge for RateCounterFDBGauge {
    fn record(&self, trace_event: &HashMap<String, Value>, labels: &[KeyValue]) -> Result<()> {
        let trace_type = get_trace_field(trace_event, "Type")?;

        if trace_type == self.gauge_impl.trace_type {
            let value = get_trace_field(trace_event, self.gauge_impl.field_name.as_str())?;
            self.gauge_impl.gauge.record(
                value
                    .split(' ')
                    .next()
                    .with_context(|| format!("Malformed {} counter", self.gauge_impl.field_name))?
                    .parse::<f64>()?,
                labels,
            );
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct ElapsedRateFDBGauge {
    gauge_impl: FDBGaugeImpl,
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
            self.gauge_impl.gauge.record(value / elapsed, labels);
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
    use opentelemetry_sdk::metrics::{ManualReader, SdkMeterProvider};

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

    fn base_event_with_type(trace_type: &str) -> HashMap<String, Value> {
        let mut event = HashMap::new();
        event.insert("Type".to_string(), Value::String(trace_type.to_string()));
        event
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

        gauge.record(&event, &[]).expect("record should succeed");
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

        gauge.record(&event, &[]).expect("record should succeed");
    }

    #[test]
    fn histogram_percentile_records_matching_histogram() {
        let meter = test_meter();
        let gauge = HistogramPercentileFDBGauge::new(
            "StorageServer",
            "Read",
            0.5,
            "ss_read_latency_p50_test",
            "Read latency",
            &meter,
        );

        let mut event = base_event_with_type("Histogram");
        event.insert("Group".into(), Value::String("StorageServer".into()));
        event.insert("Op".into(), Value::String("Read".into()));
        event.insert("Unit".into(), Value::String("milliseconds".into()));
        event.insert("TotalCount".into(), Value::String("10".into()));
        event.insert("LessThan1.0".into(), Value::String("4".into()));
        event.insert("LessThan2.0".into(), Value::String("6".into()));

        gauge.record(&event, &[]).expect("record should succeed");
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
