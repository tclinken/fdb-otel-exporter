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
    future::IntoFuture,
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
use tracing_appender::{
    non_blocking::WorkerGuard,
    rolling::{RollingFileAppender, Rotation},
};
use tracing_subscriber::{fmt, fmt::writer::MakeWriterExt, EnvFilter};

use metrics_handler::{metrics_handler, readiness_handler, AppState};
use watch_logs::watch_logs;

const TRACE_LOG_RETENTION_FILES: usize = 7;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging, metrics infrastructure, and start watching FDB trace logs.
    let config = AppConfig::from_env().context("failed to load exporter configuration")?;
    let _tracing_guard = init_tracing(&config)?;

    let (registry, meter_provider) = init_metrics()?;
    let meter_provider = Arc::new(meter_provider);

    let service_result = run_service(&config, registry, Arc::clone(&meter_provider)).await;
    let shutdown_result = meter_provider
        .shutdown()
        .context("failed to shut down metrics provider");

    match (service_result, shutdown_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Err(service_error), Err(shutdown_error)) => {
            tracing::error!(?shutdown_error, "metrics provider shutdown failed");
            Err(service_error)
        }
    }
}

async fn run_service(
    config: &AppConfig,
    registry: Arc<Registry>,
    meter_provider: Arc<SdkMeterProvider>,
) -> Result<()> {
    tracing::info!(log_dir = %config.log_dir.display(), "watching JSON logs directory");
    let mut log_watcher = watch_logs(
        &config.log_dir,
        Arc::clone(&meter_provider),
        config.log_poll_interval,
        config.gauge_config_path.as_deref(),
    )
    .await
    .context("failed to start log directory watcher")?;

    let app_state = AppState::new(registry.clone(), log_watcher.readiness());

    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/health", get(|| async { StatusCode::OK }))
        .route("/ready", get(readiness_handler))
        .with_state(app_state);

    let listener = TcpListener::bind(config.listen_addr).await?;
    tracing::info!("listening on {}", listener.local_addr()?);

    let (server_shutdown, await_server_shutdown) = tokio::sync::oneshot::channel::<()>();
    let server = axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = await_server_shutdown.await;
        })
        .into_future();
    tokio::pin!(server);

    tokio::select! {
        _ = shutdown_signal() => {
            let _ = server_shutdown.send(());
            log_watcher.shutdown().await;
            server.await?;
            Ok(())
        }
        watcher_result = log_watcher.wait() => {
            let _ = server_shutdown.send(());
            if let Err(error) = server.await {
                tracing::error!(?error, "metrics server failed while ingestion was stopping");
            }

            match watcher_result {
                Ok(()) => Err(anyhow!("log directory watcher terminated unexpectedly")),
                Err(error) => Err(error.context("log directory watcher terminated")),
            }
        }
        server_result = &mut server => {
            log_watcher.shutdown().await;
            server_result?;
            Ok(())
        }
    }
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

fn init_tracing(config: &AppConfig) -> Result<WorkerGuard> {
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

    let file_appender = build_file_appender(&directory, &file_name)?;
    let (file_writer, guard) = tracing_appender::non_blocking(file_appender);
    let log_writer = std::io::stdout.and(file_writer);

    fmt()
        .with_env_filter(filter)
        .with_writer(log_writer)
        .with_ansi(false)
        .try_init()
        .map_err(|error| anyhow!("failed to initialize tracing subscriber: {error}"))?;

    Ok(guard)
}

fn build_file_appender(directory: &Path, file_name: &str) -> Result<RollingFileAppender> {
    RollingFileAppender::builder()
        .rotation(Rotation::DAILY)
        .filename_prefix(file_name)
        .max_log_files(TRACE_LOG_RETENTION_FILES)
        .build(directory)
        .with_context(|| {
            format!(
                "failed to initialize tracing log appender in {}",
                directory.display()
            )
        })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_appender_initialization_is_fallible() {
        let temp_dir = tempfile::tempdir().expect("temp directory");
        let non_directory = temp_dir.path().join("not-a-directory");
        std::fs::write(&non_directory, b"file").expect("create blocking file");

        let error = build_file_appender(&non_directory, "tracing.log")
            .expect_err("a file cannot be used as the log directory");

        assert!(
            error
                .to_string()
                .contains("failed to initialize tracing log appender"),
            "unexpected error: {error}"
        );
    }
}
