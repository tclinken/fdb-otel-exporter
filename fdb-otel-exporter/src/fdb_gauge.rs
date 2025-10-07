use anyhow::{Context, Result};
use opentelemetry::metrics::{Gauge, Meter};
use opentelemetry::KeyValue;
use serde_json::Value;
use std::collections::HashMap;

pub trait FDBGauge: Send + Sync {
    fn record(&self, trace_event: &HashMap<String, Value>, labels: &[KeyValue]) -> Result<()>;
}

#[derive(Clone)]
pub struct SimpleFDBGauge {
    trace_type: String,
    field_name: String,
    gauge: Gauge<f64>,
}

impl SimpleFDBGauge {
    pub fn new(
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

impl FDBGauge for SimpleFDBGauge {
    fn record(&self, trace_event: &HashMap<String, Value>, labels: &[KeyValue]) -> Result<()> {
        let trace_type = trace_event
            .get("Type")
            .and_then(|value| value.as_str())
            .with_context(|| "Missing trace type")?;

        if trace_type == self.trace_type {
            let value = trace_event
                .get(self.field_name.as_str())
                .and_then(|v| v.as_str())
                .with_context(|| format!("Missing {} field", self.field_name))?;
            self.gauge.record(value.parse::<f64>()?, labels);
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct TotalCounterFDBGauge {
    trace_type: String,
    field_name: String,
    gauge: Gauge<u64>,
}

impl TotalCounterFDBGauge {
    pub fn new(
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
                .u64_gauge(gauge_name)
                .with_description(description)
                .init(),
        }
    }
}

impl FDBGauge for TotalCounterFDBGauge {
    fn record(&self, trace_event: &HashMap<String, Value>, labels: &[KeyValue]) -> Result<()> {
        let trace_type = trace_event
            .get("Type")
            .and_then(|value| value.as_str())
            .with_context(|| "Missing trace type")?;

        if trace_type == self.trace_type {
            let value = trace_event
                .get(self.field_name.as_str())
                .and_then(|v| v.as_str())
                .with_context(|| format!("Missing {} field", self.field_name))?;
            self.gauge.record(
                value
                    .split(' ')
                    .nth(2)
                    .with_context(|| format!("Malformed {} counter", self.field_name))?
                    .parse::<u64>()?,
                labels,
            );
        }
        Ok(())
    }
}

pub struct RateCounterFDBGauge {
    trace_type: String,
    field_name: String,
    gauge: Gauge<f64>,
}

impl RateCounterFDBGauge {
    pub fn new(
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

impl FDBGauge for RateCounterFDBGauge {
    fn record(&self, trace_event: &HashMap<String, Value>, labels: &[KeyValue]) -> Result<()> {
        let trace_type = trace_event
            .get("Type")
            .and_then(|value| value.as_str())
            .with_context(|| "Missing trace type")?;

        if trace_type == self.trace_type {
            let value = trace_event
                .get(self.field_name.as_str())
                .and_then(|v| v.as_str())
                .with_context(|| format!("Missing {} field", self.field_name))?;
            self.gauge.record(
                value
                    .split(' ')
                    .next()
                    .with_context(|| format!("Malformed {} counter", self.field_name))?
                    .parse::<f64>()?,
                labels,
            );
        }
        Ok(())
    }
}
