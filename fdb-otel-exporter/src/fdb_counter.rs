use anyhow::{Context, Result};
use opentelemetry::metrics::{Counter, Meter};
use opentelemetry::KeyValue;
use serde_json::Value;
use std::collections::HashMap;

#[derive(Clone)]
pub struct SevCounter {
    severity: u64,
    counter: Counter<u64>,
}

impl SevCounter {
    pub fn new(severity: u64, meter: &Meter) -> Self {
        Self {
            severity,
            counter: meter
                .u64_counter(format!("process_sev{severity}_counter"))
                .with_description(format!("Counter of severity {severity} trace events"))
                .init(),
        }
    }

    pub fn record(&self, trace_event: &HashMap<String, Value>, labels: &[KeyValue]) -> Result<()> {
        if trace_event
            .get("Severity")
            .with_context(|| "Missing Severity field")?
            .as_str()
            .with_context(|| "Invalid Severity field")?
            .parse::<u64>()
            .with_context(|| "Invalid Severity field")?
            == self.severity
        {
            self.counter.add(1, labels);
        }
        Ok(())
    }
}
