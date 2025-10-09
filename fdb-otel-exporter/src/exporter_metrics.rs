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
            .u64_counter("fdb_exporter_events_total")
            .with_description("Number of FoundationDB log events successfully processed")
            .init();

        let parse_errors = meter
            .u64_counter("fdb_exporter_parse_errors_total")
            .with_description("Number of FoundationDB log lines that failed JSON parsing")
            .init();

        let record_errors = meter
            .u64_counter("fdb_exporter_record_errors_total")
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
}
