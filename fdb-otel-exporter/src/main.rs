mod fake_trace_event;
mod fdb_gauge;
mod gauge_config;
mod log_metrics;
mod metrics_handler;
mod sample_log_writer;
mod watch_logs;
use metrics_handler::{metrics_handler, AppState};
use sample_log_writer::generate_samples;
use watch_logs::watch_logs;

use std::{
    env, fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{anyhow, Context, Result};
use axum::{http::StatusCode, routing::get, Router};
use opentelemetry::KeyValue;
use opentelemetry_sdk::{metrics::SdkMeterProvider, Resource};
use prometheus::Registry;
use tokio::{net::TcpListener, signal};
use tracing_subscriber::{fmt, EnvFilter};

const LOG_DIR_ENV: &str = "LOG_DIR";
const DEFAULT_LOG_DIR: &str = "logs";
const GENERATE_SAMPLE_LOGS_ENV: &str = "GENERATE_SAMPLE_LOGS";
const TRACE_LOG_FILE_ENV: &str = "TRACE_LOG_FILE";
const DEFAULT_TRACE_LOG_FILE: &str = "logs/tracing.log";

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing()?;

    let (registry, meter_provider) = init_metrics()?;
    let meter_provider = Arc::new(meter_provider);

    let log_dir = env::var(LOG_DIR_ENV).unwrap_or_else(|_| DEFAULT_LOG_DIR.to_string());
    tracing::info!(log_dir, "watching JSON logs directory");
    let log_dir_path = PathBuf::from(&log_dir);
    watch_logs(&log_dir_path, Arc::clone(&meter_provider)).await?;

    let should_generate_samples = env::var(GENERATE_SAMPLE_LOGS_ENV)
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE"))
        .unwrap_or(false);

    if should_generate_samples {
        generate_samples(log_dir_path.clone());
    }

    let app_state = AppState::new(registry.clone());

    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/health", get(|| async { StatusCode::OK }))
        .with_state(app_state);

    let listener = TcpListener::bind("0.0.0.0:9200").await?;
    tracing::info!("listening on {}", listener.local_addr()?);

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    Ok(())
}

async fn shutdown_signal() {
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

fn init_tracing() -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let log_file =
        env::var(TRACE_LOG_FILE_ENV).unwrap_or_else(|_| DEFAULT_TRACE_LOG_FILE.to_string());
    let log_path = PathBuf::from(&log_file);

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
