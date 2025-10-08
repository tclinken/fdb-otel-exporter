use anyhow::{Context, Result};
use opentelemetry::metrics::{Gauge, Meter};
use opentelemetry::KeyValue;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};

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
        .with_context(|| format!("Missing {} field", field_name))
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
            let value = get_trace_field(trace_event, &self.gauge_impl.field_name.as_str())?
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

        // FIXME: Only milliseconds supported for now:
        if get_trace_field(trace_event, "Unit")? != "milliseconds" {
            return Ok(());
        }

        let total_count = get_trace_field(trace_event, "TotalCount")?.parse::<u64>()?;
        let mut hist: BTreeMap<u64, u64> = BTreeMap::new();

        for (k, v) in trace_event {
            if k.starts_with("LessThan") {
                let bucket_ms = k.strip_prefix("LessThan").unwrap().parse::<f64>()?;
                let bucket_micros = (bucket_ms * 1000.0) as u64;
                let count = v
                    .as_str()
                    .with_context(|| "Trace event values should be strings")?
                    .parse::<u64>()?;
                hist.insert(bucket_micros, count);
            }
        }

        let cutoff = self.percentile * (total_count as f64);
        let mut seen = 0;

        // TODO: Perform interpolation here:
        for (micros, count) in hist {
            seen += count;
            if (seen as f64) >= cutoff {
                self.gauge.record((micros as f64) / 1000000.0, labels);
                break;
            }
        }

        Ok(())
    }
}
