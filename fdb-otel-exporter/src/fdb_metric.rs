use anyhow::Result;
use opentelemetry::KeyValue;
use serde_json::Value;
use std::collections::HashMap;

/// Common interface for FoundationDB metrics that can process trace events.
pub trait FDBMetric: Send + Sync {
    fn record(&self, trace_event: &HashMap<String, Value>, labels: &[KeyValue]) -> Result<()>;
}
