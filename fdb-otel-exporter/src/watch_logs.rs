use crate::{
    exporter_metrics::ExporterMetrics,
    log_metrics::{LogMetrics, TraceEvent},
};
use anyhow::{Context, Result};
use opentelemetry::metrics::MeterProvider;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use std::collections::HashSet;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncSeekExt, BufReader};
use tokio::time;

// Discover JSON trace logs under `log_dir_path` and push their events through the configured gauges.
pub async fn watch_logs(
    log_dir_path: &Path,
    meter_provider: Arc<SdkMeterProvider>,
    poll_interval: Duration,
) -> Result<()> {
    let meter = meter_provider.meter("fdb-otel-exporter");
    let exporter_metrics = ExporterMetrics::new(&meter);
    let log_metrics =
        LogMetrics::new(&meter).with_context(|| "failed to load gauge configuration")?;

    fs::create_dir_all(log_dir_path)
        .await
        .with_context(|| format!("failed to create log directory {}", log_dir_path.display()))?;

    let watcher_dir = log_dir_path.to_path_buf();
    let dir_metrics = log_metrics.clone();
    let directory_metrics = exporter_metrics.clone();
    tokio::spawn(async move {
        if let Err(error) =
            run_log_directory(watcher_dir, dir_metrics, directory_metrics, poll_interval).await
        {
            tracing::error!(?error, "log directory watcher terminated");
        }
    });
    Ok(())
}

// Poll the log directory, spawning a tail task for each new `trace.*.json` file encountered.
async fn run_log_directory(
    dir: PathBuf,
    metrics: LogMetrics,
    exporter_metrics: ExporterMetrics,
    poll_interval: Duration,
) -> Result<()> {
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

                    if !should_tail_file(file_name) {
                        continue;
                    }

                    if tailed.insert(path.clone()) {
                        tracing::info!(file = %path.display(), "starting log tailer");
                        let task_metrics = metrics.clone();
                        let task_exporter_metrics = exporter_metrics.clone();
                        tokio::spawn(async move {
                            if let Err(error) =
                                run_log_tailer(path.clone(), task_metrics, task_exporter_metrics)
                                    .await
                            {
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

        time::sleep(poll_interval).await;
    }
}

// Tail a single trace file and forward each JSON line to the metrics recorder.
async fn run_log_tailer(
    path: PathBuf,
    metrics: LogMetrics,
    exporter_metrics: ExporterMetrics,
) -> Result<()> {
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
                                Ok(record) => match metrics.record(&record) {
                                    Ok(()) => exporter_metrics.record_processed(),
                                    Err(error) => {
                                        exporter_metrics.record_record_error();
                                        tracing::warn!(
                                            ?error,
                                            raw_line = %trimmed,
                                            "failed to record log line"
                                        );
                                    }
                                },
                                Err(error) => {
                                    exporter_metrics.record_parse_error();
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

fn should_tail_file(file_name: &str) -> bool {
    file_name.starts_with("trace.") && file_name.ends_with(".json")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fdb_gauge::FDBGauge;
    use anyhow::Result;
    use opentelemetry_sdk::metrics::{ManualReader, SdkMeterProvider};
    use serde_json::json;
    use std::sync::Mutex;
    use tempfile::tempdir;
    use tokio::fs::OpenOptions;
    use tokio::io::AsyncWriteExt;
    use tokio::time::{timeout, Duration as TokioDuration};

    fn test_meter_provider() -> Arc<SdkMeterProvider> {
        let reader = ManualReader::builder().build();
        Arc::new(SdkMeterProvider::builder().with_reader(reader).build())
    }

    #[derive(Clone)]
    struct RecordingGauge {
        events: Arc<Mutex<Vec<TraceEvent>>>,
    }

    impl RecordingGauge {
        fn new(events: Arc<Mutex<Vec<TraceEvent>>>) -> Self {
            Self { events }
        }
    }

    impl FDBGauge for RecordingGauge {
        fn record(
            &self,
            trace_event: &TraceEvent,
            _labels: &[opentelemetry::KeyValue],
        ) -> Result<()> {
            self.events.lock().unwrap().push(trace_event.clone());
            Ok(())
        }
    }

    #[test]
    fn should_tail_file_filters_trace_logs() {
        assert!(should_tail_file("trace.1.json"));
        assert!(should_tail_file("trace.some_process.json"));
        assert!(!should_tail_file("trace.1.xml"));
        assert!(!should_tail_file("random.log"));
        assert!(!should_tail_file("tracejson"));
    }

    #[tokio::test]
    async fn watch_logs_creates_missing_directory() {
        let temp_dir = tempdir().expect("temp dir");
        let log_dir = temp_dir.path().join("logs");
        let provider = test_meter_provider();
        assert!(
            tokio::fs::metadata(&log_dir).await.is_err(),
            "log dir should not exist before watch_logs"
        );

        watch_logs(&log_dir, provider, TokioDuration::from_millis(50))
            .await
            .expect("watch_logs should succeed");

        // Allow spawned tasks to start.
        let _ = timeout(TokioDuration::from_millis(50), tokio::task::yield_now()).await;

        assert!(
            tokio::fs::metadata(&log_dir).await.unwrap().is_dir(),
            "watch_logs should create log directory"
        );
    }

    #[tokio::test]
    async fn run_log_directory_records_trace_events() -> Result<()> {
        let temp_dir = tempdir()?;
        let trace_path = temp_dir.path().join("trace.42.json");
        let ignored_path = temp_dir.path().join("ignored.log");

        tokio::fs::File::create(&trace_path).await?;
        tokio::fs::File::create(&ignored_path).await?;

        let events = Arc::new(Mutex::new(Vec::new()));
        let gauges: Vec<Arc<dyn FDBGauge>> = vec![Arc::new(RecordingGauge::new(events.clone()))];
        let log_metrics = LogMetrics::from_gauges(gauges);

        let provider = test_meter_provider();
        let meter = provider.meter("run_log_directory_records_trace_events");
        let exporter_metrics = ExporterMetrics::new(&meter);

        let poll_interval = TokioDuration::from_millis(20);
        let dir = temp_dir.path().to_path_buf();

        let handle = tokio::spawn(run_log_directory(
            dir,
            log_metrics,
            exporter_metrics,
            poll_interval,
        ));

        tokio::time::sleep(TokioDuration::from_millis(60)).await;

        {
            let mut file = OpenOptions::new()
                .append(true)
                .open(&trace_path)
                .await
                .context("failed to open trace file for append")?;

            let event = json!({
                "Machine": "machine-01",
                "Roles": "storage",
                "Type": "TestTrace"
            });
            file.write_all(serde_json::to_string(&event)?.as_bytes())
                .await
                .context("failed to write trace event")?;
            file.write_all(b"\n").await?;
            file.flush().await?;
        }

        for _ in 0..50 {
            if !events.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(TokioDuration::from_millis(10)).await;
        }

        handle.abort();
        let _ = handle.await;

        let recorded = events.lock().unwrap();
        assert_eq!(
            recorded.len(),
            1,
            "expected exactly one trace event to be recorded"
        );

        Ok(())
    }
}
