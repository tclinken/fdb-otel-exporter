use crate::fdb_metric::FDBMetric;
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
}

impl FDBMetric for SevCounter {
    fn record(&self, trace_event: &HashMap<String, Value>, labels: &[KeyValue]) -> Result<()> {
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

pub struct SlowTaskCounter {
    threshold_ms: u64,
    counter: Counter<u64>,
}

impl SlowTaskCounter {
    pub fn new(threshold_ms: u64, meter: &Meter) -> Self {
        Self {
            threshold_ms,
            counter: meter
                .u64_counter(format!("process_slow_task_{threshold_ms}_ms"))
                .with_description(format!(
                    "Counter of slow tasks longer than {threshold_ms} ms"
                ))
                .init(),
        }
    }
}

impl FDBMetric for SlowTaskCounter {
    fn record(&self, trace_event: &HashMap<String, Value>, labels: &[KeyValue]) -> Result<()> {
        let trace_type = trace_event
            .get("Type")
            .with_context(|| "Missing Type field")?
            .as_str()
            .with_context(|| "Invalid Type field")?;

        if trace_type == "SlowTask" {
            let duration_sec = trace_event
                .get("Duration")
                .with_context(|| "Missing Duration field")?
                .as_str()
                .with_context(|| "Invalid Duration field")?
                .parse::<f64>()
                .with_context(|| "Invalid Duration field")?;

            if duration_sec > ((self.threshold_ms as f64) / 1000.0) {
                self.counter.add(1, labels);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::metrics::{find_metric, prometheus_meter};
    use opentelemetry::KeyValue;
    use prometheus::Registry;

    fn counter_value(registry: &Registry, name: &str, label_name: &str, label_value: &str) -> f64 {
        let metric = match find_metric(registry, name, label_name, label_value) {
            Some(metric) => metric,
            None => return 0.0,
        };
        metric.get_counter().get_value()
    }

    #[test]
    fn record_increments_matching_severity() {
        let (provider, meter, registry) = prometheus_meter();
        let counter = SevCounter::new(10, &meter);

        let mut event = HashMap::new();
        event.insert("Severity".into(), Value::String("10".into()));
        let labels = vec![KeyValue::new("machine", "test")];

        counter
            .record(&event, &labels)
            .expect("record should succeed");

        provider.force_flush().expect("force_flush should succeed");

        let value = counter_value(&registry, "process_sev10_counter", "machine", "test");
        assert!(
            (value - 1.0).abs() < f64::EPSILON,
            "expected counter value 1.0, got {value}"
        );
    }

    #[test]
    fn record_skips_mismatched_severity() {
        let (provider, meter, registry) = prometheus_meter();
        let counter = SevCounter::new(10, &meter);

        let mut event = HashMap::new();
        event.insert("Severity".into(), Value::String("20".into()));
        let labels = vec![KeyValue::new("machine", "test")];

        counter
            .record(&event, &labels)
            .expect("record should succeed");

        provider.force_flush().expect("force_flush should succeed");

        let value = counter_value(&registry, "process_sev10_counter", "machine", "test");
        assert!(
            value.abs() < f64::EPSILON,
            "expected counter value 0.0, got {value}"
        );
    }

    #[test]
    fn record_errors_without_severity() {
        let (_provider, meter, _registry) = prometheus_meter();
        let counter = SevCounter::new(10, &meter);

        let event = HashMap::new();
        let labels = vec![KeyValue::new("machine", "test")];

        let error = counter
            .record(&event, &labels)
            .expect_err("missing severity should error");

        assert!(
            error.to_string().contains("Missing Severity field"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn slow_task_counter_increments_above_threshold() {
        let (provider, meter, registry) = prometheus_meter();
        let counter = SlowTaskCounter::new(100, &meter);

        let mut event = HashMap::new();
        event.insert("Type".into(), Value::String("SlowTask".into()));
        event.insert("Duration".into(), Value::String("0.150".into()));
        let labels = vec![KeyValue::new("machine", "test")];

        counter
            .record(&event, &labels)
            .expect("record should succeed");

        provider.force_flush().expect("force_flush should succeed");

        let value = counter_value(&registry, "process_slow_task_100_ms", "machine", "test");
        assert!(
            (value - 1.0).abs() < f64::EPSILON,
            "expected counter value 1.0, got {value}"
        );
    }

    #[test]
    fn slow_task_counter_skips_when_duration_below_threshold() {
        let (provider, meter, registry) = prometheus_meter();
        let counter = SlowTaskCounter::new(100, &meter);

        let mut event = HashMap::new();
        event.insert("Type".into(), Value::String("SlowTask".into()));
        event.insert("Duration".into(), Value::String("0.050".into()));
        let labels = vec![KeyValue::new("machine", "test")];

        counter
            .record(&event, &labels)
            .expect("record should succeed");

        provider.force_flush().expect("force_flush should succeed");

        let value = counter_value(&registry, "process_slow_task_100_ms", "machine", "test");
        assert!(
            value.abs() < f64::EPSILON,
            "expected counter value 0.0, got {value}"
        );
    }

    #[test]
    fn slow_task_counter_skips_non_slow_task_events() {
        let (provider, meter, registry) = prometheus_meter();
        let counter = SlowTaskCounter::new(100, &meter);

        let mut event = HashMap::new();
        event.insert("Type".into(), Value::String("Other".into()));
        event.insert("Duration".into(), Value::String("1.0".into()));
        let labels = vec![KeyValue::new("machine", "test")];

        counter
            .record(&event, &labels)
            .expect("record should succeed");

        provider.force_flush().expect("force_flush should succeed");

        let value = counter_value(&registry, "process_slow_task_100_ms", "machine", "test");
        assert!(
            value.abs() < f64::EPSILON,
            "expected counter value 0.0, got {value}"
        );
    }

    #[test]
    fn slow_task_counter_errors_without_type() {
        let (_provider, meter, _registry) = prometheus_meter();
        let counter = SlowTaskCounter::new(100, &meter);

        let mut event = HashMap::new();
        event.insert("Duration".into(), Value::String("0.150".into()));
        let labels = vec![KeyValue::new("machine", "test")];

        let error = counter
            .record(&event, &labels)
            .expect_err("missing type should error");

        assert!(
            error.to_string().contains("Missing Type field"),
            "unexpected error: {error}"
        );
    }
}
