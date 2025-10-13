mod config;
mod exporter_metrics;
mod fdb_counter;
mod fdb_gauge;
mod fdb_metric;
mod gauge_config;
mod log_metrics;
mod metrics_handler;
#[cfg(test)]
mod test_helpers;
mod watch_logs;

use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{anyhow, Context, Result};
use axum::{http::StatusCode, routing::get, Router};
use config::AppConfig;
use opentelemetry::KeyValue;
use opentelemetry_sdk::{metrics::SdkMeterProvider, Resource};
use prometheus::Registry;
use tokio::{net::TcpListener, signal};
use tracing_subscriber::{fmt, EnvFilter};

use metrics_handler::{metrics_handler, AppState};
use watch_logs::watch_logs;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging, metrics infrastructure, and start watching FDB trace logs.
    let config = AppConfig::from_env().context("failed to load exporter configuration")?;
    init_tracing(&config)?;

    let (registry, meter_provider) = init_metrics()?;
    let meter_provider = Arc::new(meter_provider);

    tracing::info!(log_dir = %config.log_dir.display(), "watching JSON logs directory");
    if let Err(err) = watch_logs(
        &config.log_dir,
        Arc::clone(&meter_provider),
        config.log_poll_interval,
    )
    .await
    {
        tracing::error!(?err, "watch_logs failed");
        return Err(err);
    }

    let app_state = AppState::new(registry.clone());

    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/health", get(|| async { StatusCode::OK }))
        .with_state(app_state);

    let listener = TcpListener::bind(config.listen_addr).await?;
    tracing::info!("listening on {}", listener.local_addr()?);

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    Ok(())
}

async fn shutdown_signal() {
    // Wait for Ctrl+C or SIGTERM so axum can drain outstanding requests cleanly.
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    tracing::info!("shutdown signal received");
}

fn init_tracing(config: &AppConfig) -> Result<()> {
    // Configure tracing to mirror logs into a rolling file whose location can be overridden via env.
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let log_path = config.trace_log_file.clone();

    let (directory, file_name) = match log_path.file_name().and_then(|name| name.to_str()) {
        Some(name) if !name.is_empty() => {
            let parent = log_path.parent().filter(|p| !p.as_os_str().is_empty());
            let directory = parent
                .map(Path::to_path_buf)
                .unwrap_or_else(|| PathBuf::from("."));
            (directory, name.to_string())
        }
        _ => (log_path.clone(), String::from("tracing.log")),
    };

    fs::create_dir_all(&directory).with_context(|| {
        format!(
            "failed to create tracing log directory {}",
            directory.display()
        )
    })?;

    let file_appender = tracing_appender::rolling::never(&directory, &file_name);
    let (file_writer, guard) = tracing_appender::non_blocking(file_appender);
    let _ = Box::leak(Box::new(guard));

    fmt()
        .with_env_filter(filter)
        .with_writer(file_writer)
        .with_ansi(false)
        .try_init()
        .map_err(|error| anyhow!("failed to initialize tracing subscriber: {error}"))?;

    Ok(())
}

fn init_metrics() -> Result<(Arc<Registry>, SdkMeterProvider)> {
    // Build a Prometheus-backed meter provider so OpenTelemetry metrics feed the `/metrics` endpoint.
    let registry = Registry::new();

    let exporter = opentelemetry_prometheus::exporter()
        .with_registry(registry.clone())
        .build()?;

    let resource = Resource::new(vec![KeyValue::new("service.name", "fdb-otel-exporter")]);

    let provider = SdkMeterProvider::builder()
        .with_resource(resource)
        .with_reader(exporter)
        .build();

    Ok((Arc::new(registry), provider))
}
