use serde_json::{json, Value};
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub struct FakeTraceEvent {
    trace_type: String,
    time: f64,
    fields: HashMap<String, String>,
}

impl FakeTraceEvent {
    pub fn new<S: Into<String>>(trace_type: S) -> Self {
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| Duration::from_secs(0))
            .as_secs_f64();
        Self {
            trace_type: trace_type.into(),
            time,
            fields: HashMap::new(),
        }
    }

    pub fn detail<K, V>(mut self, k: K, v: V) -> Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        self.fields.insert(k.into(), v.into());
        self
    }

    pub fn log(self) -> Value {
        let mut dict: HashMap<String, String> = HashMap::from([
            ("Time".to_string(), format!("{:6}", self.time)),
            ("Type".to_string(), self.trace_type.clone()),
            ("Machine".to_string(), "127.0.0.1:4000".to_string()),
        ]);
        dict.extend(self.fields);
        json!(dict)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detail_fields_are_included_in_log_output() {
        let value = FakeTraceEvent::new("StorageMetrics")
            .detail("Version", "123")
            .log();

        assert_eq!(value.get("Version").and_then(Value::as_str), Some("123"));
    }

    #[test]
    fn log_populates_core_fields() {
        let value = FakeTraceEvent::new("Trace")
            .detail("Severity", "info")
            .log();

        assert_eq!(value.get("Type").and_then(Value::as_str), Some("Trace"));
        assert_eq!(
            value.get("Machine").and_then(Value::as_str),
            Some("127.0.0.1:4000")
        );
        assert!(value.get("Time").and_then(Value::as_str).is_some());
    }
}
