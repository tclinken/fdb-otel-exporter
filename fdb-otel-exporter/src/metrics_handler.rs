use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use prometheus::{proto::MetricFamily, Encoder, Registry, TextEncoder};
use std::sync::Arc;

// Shared state passed to the `/metrics` endpoint so requests can scrape the Prometheus registry.
#[derive(Clone)]
pub struct AppState {
    registry: Arc<Registry>,
}

impl AppState {
    // Store the Prometheus registry in an `Arc` for cheap cloning across requests.
    pub fn new(registry: Arc<Registry>) -> Self {
        Self {
            registry: registry.clone(),
        }
    }
}

// Render all collected metrics using the text exposition format expected by Prometheus.
pub async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    let metric_families = state.registry.gather();

    match encode_metrics(&metric_families) {
        Ok(payload) => (StatusCode::OK, payload).into_response(),
        Err(MetricsEncodeError::Encode(error)) => {
            tracing::error!(?error, "failed to encode metrics");
            (StatusCode::INTERNAL_SERVER_ERROR, "metrics encoding failed").into_response()
        }
        Err(MetricsEncodeError::Utf8(error)) => {
            tracing::error!(?error, "metrics not valid UTF-8");
            (StatusCode::INTERNAL_SERVER_ERROR, "metrics buffer invalid").into_response()
        }
    }
}

#[derive(Debug)]
enum MetricsEncodeError {
    Encode(prometheus::Error),
    Utf8(std::string::FromUtf8Error),
}

impl From<prometheus::Error> for MetricsEncodeError {
    fn from(value: prometheus::Error) -> Self {
        Self::Encode(value)
    }
}

impl From<std::string::FromUtf8Error> for MetricsEncodeError {
    fn from(value: std::string::FromUtf8Error) -> Self {
        Self::Utf8(value)
    }
}

fn encode_metrics(metric_families: &[MetricFamily]) -> Result<String, MetricsEncodeError> {
    encode_metrics_with(&TextEncoder::new(), metric_families)
}

fn encode_metrics_with<E: Encoder>(
    encoder: &E,
    metric_families: &[MetricFamily],
) -> Result<String, MetricsEncodeError> {
    let mut buffer = Vec::new();
    encoder.encode(metric_families, &mut buffer)?;
    Ok(String::from_utf8(buffer)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use axum::http::StatusCode;
    use prometheus::{Gauge, Registry};
    use std::io::Write;

    struct FailingEncoder;

    impl Encoder for FailingEncoder {
        fn encode<W: Write>(
            &self,
            _metric_families: &[MetricFamily],
            _writer: &mut W,
        ) -> prometheus::Result<()> {
            Err(prometheus::Error::Msg("encode failure".into()))
        }

        fn format_type(&self) -> &str {
            "test"
        }
    }

    struct InvalidUtf8Encoder;

    impl Encoder for InvalidUtf8Encoder {
        fn encode<W: Write>(
            &self,
            _metric_families: &[MetricFamily],
            writer: &mut W,
        ) -> prometheus::Result<()> {
            writer.write_all(&[0xff, 0xfe, 0xfd])?;
            Ok(())
        }

        fn format_type(&self) -> &str {
            "test"
        }
    }

    #[tokio::test]
    async fn metrics_handler_renders_registry() {
        let registry = Registry::new();
        let gauge = Gauge::new("test_metric_total", "Test metric").expect("gauge");
        gauge.set(42.0);
        registry
            .register(Box::new(gauge.clone()))
            .expect("register gauge");

        let app_state = AppState::new(Arc::new(registry));
        let response = metrics_handler(State(app_state)).await.into_response();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("body bytes");
        let payload = String::from_utf8(body.to_vec()).expect("utf8 payload");
        assert!(
            payload.contains("test_metric_total"),
            "payload missing metric: {payload}"
        );
    }

    #[test]
    fn encode_metrics_reports_encoder_errors() {
        let families = Vec::<MetricFamily>::new();
        let err = encode_metrics_with(&FailingEncoder, &families)
            .expect_err("encoding should fail with custom encoder");
        match err {
            MetricsEncodeError::Encode(_) => {}
            other => panic!("unexpected error variant: {other:?}"),
        }
    }

    #[test]
    fn encode_metrics_reports_utf8_errors() {
        let families = Vec::<MetricFamily>::new();
        let err = encode_metrics_with(&InvalidUtf8Encoder, &families)
            .expect_err("encoding should fail with invalid utf8");
        match err {
            MetricsEncodeError::Utf8(_) => {}
            other => panic!("unexpected error variant: {other:?}"),
        }
    }
}
