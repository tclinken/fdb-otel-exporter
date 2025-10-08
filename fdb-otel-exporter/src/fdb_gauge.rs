use anyhow::{Context, Result};
use opentelemetry::metrics::{Gauge, Meter};
use opentelemetry::KeyValue;
use serde_json::Value;
use std::collections::HashMap;

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
