use crate::log_metrics::{LogMetrics, TraceEvent};
use anyhow::{Context, Result};
use opentelemetry::metrics::MeterProvider;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use std::collections::HashSet;
use std::io::SeekFrom;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncSeekExt, BufReader};
use tokio::time;

pub async fn watch_logs(
    log_dir_path: &PathBuf,
    meter_provider: Arc<SdkMeterProvider>,
) -> Result<()> {
    let meter = meter_provider.meter("fdb-otel-exporter");
    let log_metrics =
        LogMetrics::new(&meter).with_context(|| "failed to load gauge configuration")?;

    fs::create_dir_all(&log_dir_path)
        .await
        .with_context(|| format!("failed to create log directory {}", log_dir_path.display()))?;

    let watcher_dir = log_dir_path.clone();
    let dir_metrics = log_metrics.clone();
    tokio::spawn(async move {
        if let Err(error) = run_log_directory(watcher_dir, dir_metrics).await {
            tracing::error!(?error, "log directory watcher terminated");
        }
    });
    Ok(())
}

async fn run_log_directory(dir: PathBuf, metrics: LogMetrics) -> Result<()> {
    let mut tailed: HashSet<PathBuf> = HashSet::new();

    loop {
        match fs::read_dir(&dir).await {
            Ok(mut entries) => {
                while let Some(entry) = entries.next_entry().await? {
                    let path = entry.path();
                    if !path.is_file() {
                        continue;
                    }

                    let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
                        continue;
                    };

                    if !file_name.starts_with("trace.") || !file_name.ends_with(".json") {
                        continue;
                    }

                    if tailed.insert(path.clone()) {
                        tracing::info!(file = %path.display(), "starting log tailer");
                        let task_metrics = metrics.clone();
                        tokio::spawn(async move {
                            if let Err(error) = run_log_tailer(path.clone(), task_metrics).await {
                                tracing::error!(?error, file = %path.display(), "log tailer exited");
                            }
                        });
                    }
                }
            }
            Err(error) => {
                tracing::warn!(?error, dir = %dir.display(), "failed to read log directory");
            }
        }

        time::sleep(Duration::from_secs(2)).await;
    }
}

async fn run_log_tailer(path: PathBuf, metrics: LogMetrics) -> Result<()> {
    loop {
        match OpenOptions::new().read(true).open(&path).await {
            Ok(mut file) => {
                if let Err(error) = file
                    .seek(SeekFrom::End(0))
                    .await
                    .with_context(|| format!("failed to seek log file {}", path.display()))
                {
                    tracing::warn!(?error, "unable to initialize log tail, retrying");
                    time::sleep(Duration::from_secs(1)).await;
                    continue;
                }

                let mut reader = BufReader::new(file);
                let mut line = String::new();

                loop {
                    line.clear();
                    match reader.read_line(&mut line).await {
                        Ok(0) => {
                            time::sleep(Duration::from_millis(250)).await;
                        }
                        Ok(_) => {
                            let trimmed = line.trim();
                            if trimmed.is_empty() {
                                continue;
                            }

                            match serde_json::from_str::<TraceEvent>(trimmed) {
                                Ok(record) => {
                                    if let Err(error) = metrics.record(&record) {
                                        tracing::warn!(?error, raw_line = %trimmed, "failed to record log line");
                                    }
                                }
                                Err(error) => {
                                    tracing::warn!(?error, raw_line = %trimmed, "failed to parse log line");
                                }
                            }
                        }
                        Err(error) => {
                            tracing::warn!(?error, "log tailer read error, reopening file");
                            time::sleep(Duration::from_secs(1)).await;
                            break;
                        }
                    }
                }
            }
            Err(error) => {
                tracing::warn!(?error, log_path = %path.display(), "log file unavailable, retrying");
                time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}
