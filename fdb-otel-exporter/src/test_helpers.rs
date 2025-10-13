pub mod metrics {
    use opentelemetry::metrics::{Meter, MeterProvider};
    use opentelemetry_prometheus::exporter as prometheus_exporter;
    use opentelemetry_sdk::metrics::SdkMeterProvider;
    use prometheus::{proto::Metric, Registry};

    pub fn prometheus_meter() -> (SdkMeterProvider, Meter, Registry) {
        let registry = Registry::new();
        let reader = prometheus_exporter()
            .with_registry(registry.clone())
            .build()
            .expect("prometheus exporter");
        let provider = SdkMeterProvider::builder().with_reader(reader).build();
        let meter = provider.meter("test");
        (provider, meter, registry)
    }

    pub fn find_metric(
        registry: &Registry,
        name: &str,
        label_name: &str,
        label_value: &str,
    ) -> Option<Metric> {
        let families = registry.gather();
        let mut family = families.iter().find(|mf| mf.get_name() == name);
        if family.is_none() {
            let fallback = format!("{name}_total");
            family = families.iter().find(|mf| mf.get_name() == fallback);
        }

        family?.get_metric().iter().find_map(|metric| {
            if metric
                .get_label()
                .iter()
                .any(|label| label.get_name() == label_name && label.get_value() == label_value)
            {
                Some(metric.clone())
            } else {
                None
            }
        })
    }
}
