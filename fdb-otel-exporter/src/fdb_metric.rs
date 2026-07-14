use anyhow::Result;
use opentelemetry::KeyValue;
use serde_json::Value;
use std::collections::HashMap;

/// Common interface for FoundationDB metrics that can process trace events.
pub trait FDBMetric: Send + Sync {
    /// Human-readable identifier used when reporting an individual metric failure.
    fn name(&self) -> &str {
        std::any::type_name::<Self>()
    }

    /// Trace event type handled by this metric.
    ///
    /// Returning `None` registers the metric as a global handler that receives every event.
    fn event_type(&self) -> Option<&str> {
        None
    }

    /// Histogram `(Group, Op)` handled by this metric, when it has a more specific route.
    fn histogram_key(&self) -> Option<(&str, &str)> {
        None
    }

    fn record(&self, trace_event: &HashMap<String, Value>, labels: &[KeyValue]) -> Result<()>;
}
