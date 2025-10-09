use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use prometheus::{Encoder, Registry, TextEncoder};
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
    let encoder = TextEncoder::new();
    let mut buffer = Vec::new();

    if let Err(error) = encoder.encode(&metric_families, &mut buffer) {
        tracing::error!(?error, "failed to encode metrics");
        return (StatusCode::INTERNAL_SERVER_ERROR, "metrics encoding failed").into_response();
    }

    match String::from_utf8(buffer) {
        Ok(payload) => (StatusCode::OK, payload).into_response(),
        Err(error) => {
            tracing::error!(?error, "metrics not valid UTF-8");
            (StatusCode::INTERNAL_SERVER_ERROR, "metrics buffer invalid").into_response()
        }
    }
}
