use opentelemetry::metrics::{Counter, Meter};

#[derive(Clone)]
pub struct ExporterMetrics {
    processed_events: Counter<u64>,
    parse_errors: Counter<u64>,
    record_errors: Counter<u64>,
}

impl ExporterMetrics {
    pub fn new(meter: &Meter) -> Self {
        let processed_events = meter
            .u64_counter("fdb_exporter_events")
            .with_description("Number of FoundationDB log events successfully processed")
            .init();

        let parse_errors = meter
            .u64_counter("fdb_exporter_parse_errors")
            .with_description("Number of FoundationDB log lines that failed JSON parsing")
            .init();

        let record_errors = meter
            .u64_counter("fdb_exporter_record_errors")
            .with_description("Number of FoundationDB log events that failed metric recording")
            .init();

        Self {
            processed_events,
            parse_errors,
            record_errors,
        }
    }

    pub fn record_processed(&self) {
        self.processed_events.add(1, &[]);
    }

    pub fn record_parse_error(&self) {
        self.parse_errors.add(1, &[]);
    }

    pub fn record_record_error(&self) {
        self.record_errors.add(1, &[]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::metrics::prometheus_meter;
    use opentelemetry::metrics::MeterProvider;
    use opentelemetry_sdk::metrics::{ManualReader, SdkMeterProvider};

    fn test_meter() -> Meter {
        let reader = ManualReader::builder().build();
        let provider = SdkMeterProvider::builder().with_reader(reader).build();
        provider.meter("test")
    }

    #[test]
    fn metrics_accept_updates() {
        let meter = test_meter();
        let metrics = ExporterMetrics::new(&meter);

        metrics.record_processed();
        metrics.record_parse_error();
        metrics.record_record_error();
    }

    #[test]
    fn prometheus_counter_names_have_one_total_suffix() {
        let (provider, meter, registry) = prometheus_meter();
        let _provider = provider;
        let metrics = ExporterMetrics::new(&meter);

        metrics.record_processed();
        metrics.record_parse_error();
        metrics.record_record_error();

        let names: Vec<String> = registry
            .gather()
            .into_iter()
            .map(|family| family.get_name().to_string())
            .collect();

        for name in [
            "fdb_exporter_events_total",
            "fdb_exporter_parse_errors_total",
            "fdb_exporter_record_errors_total",
        ] {
            assert!(
                names.iter().any(|candidate| candidate == name),
                "missing {name}"
            );
        }
        assert!(
            names.iter().all(|name| !name.ends_with("_total_total")),
            "counter names must not contain duplicate suffixes: {names:?}"
        );
    }
}
