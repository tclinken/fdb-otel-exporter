use crate::{
    exporter_metrics::ExporterMetrics,
    log_metrics::{LogMetrics, TraceEvent},
    metrics_handler::IngestionReadiness,
};
use anyhow::{Context, Result};
use async_trait::async_trait;
use opentelemetry::metrics::MeterProvider;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use std::collections::{HashMap, HashSet};
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::fs;
use tokio::io::{AsyncBufReadExt, AsyncSeekExt, BufReader};
use tokio::task::JoinHandle;
use tokio::time;

#[cfg(test)]
const FILE_RETRY_DELAY: Duration = Duration::from_millis(10);
#[cfg(not(test))]
const FILE_RETRY_DELAY: Duration = Duration::from_secs(1);

#[cfg(test)]
const EOF_POLL_DELAY: Duration = Duration::from_millis(10);
#[cfg(not(test))]
const EOF_POLL_DELAY: Duration = Duration::from_millis(250);

const LOG_LINE_PREVIEW_CHARS: usize = 256;

// Owns the directory watcher task and the readiness state it updates.
pub struct LogWatcher {
    task: JoinHandle<Result<()>>,
    readiness: IngestionReadiness,
}

impl LogWatcher {
    pub fn readiness(&self) -> IngestionReadiness {
        self.readiness.clone()
    }

    // Wait for an unexpected watcher exit so the service can fail rather than
    // continuing to serve stale metrics indefinitely.
    pub async fn wait(&mut self) -> Result<()> {
        let result = (&mut self.task).await;
        self.readiness.set_watcher_running(false);
        result.context("log directory watcher task failed")?
    }

    // Stop the parent watcher. Dropping its TailTasks aborts every child tailer.
    pub async fn shutdown(mut self) {
        self.readiness.set_watcher_running(false);
        self.task.abort();

        match (&mut self.task).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::warn!(?error, "log directory watcher failed during shutdown");
            }
            Err(error) if error.is_cancelled() => {}
            Err(error) => {
                tracing::warn!(?error, "log directory watcher task failed during shutdown");
            }
        }
    }
}

impl Drop for LogWatcher {
    fn drop(&mut self) {
        self.readiness.set_watcher_running(false);
        self.task.abort();
    }
}

struct WatcherStatusGuard(IngestionReadiness);

impl Drop for WatcherStatusGuard {
    fn drop(&mut self) {
        self.0.set_watcher_running(false);
    }
}

// Discover JSON trace logs under `log_dir_path` and push their events through the configured gauges.
pub async fn watch_logs(
    log_dir_path: &Path,
    meter_provider: Arc<SdkMeterProvider>,
    poll_interval: Duration,
    gauge_config_path: Option<&Path>,
) -> Result<LogWatcher> {
    watch_logs_with_fs(
        log_dir_path,
        meter_provider,
        poll_interval,
        RealTraceFileSystem,
        gauge_config_path,
    )
    .await
}

async fn watch_logs_with_fs<F>(
    log_dir_path: &Path,
    meter_provider: Arc<SdkMeterProvider>,
    poll_interval: Duration,
    fs: F,
    gauge_config_path: Option<&Path>,
) -> Result<LogWatcher>
where
    F: TraceFileSystem,
{
    let meter = meter_provider.meter("fdb-otel-exporter");
    let exporter_metrics = ExporterMetrics::new(&meter);
    let log_metrics = match gauge_config_path {
        Some(path) => LogMetrics::from_config_path(&meter, path),
        None => LogMetrics::new(&meter),
    }
    .with_context(|| "failed to load gauge configuration")?;

    fs.create_dir_all(log_dir_path)
        .await
        .with_context(|| format!("failed to create log directory {}", log_dir_path.display()))?;

    let watcher_dir = log_dir_path.to_path_buf();
    let dir_metrics = log_metrics.clone();
    let directory_metrics = exporter_metrics.clone();
    let dir_fs = fs.clone();
    let readiness = IngestionReadiness::default();
    let task_readiness = readiness.clone();
    let task = tokio::spawn(async move {
        task_readiness.set_watcher_running(true);
        let _status_guard = WatcherStatusGuard(task_readiness.clone());
        run_log_directory(
            watcher_dir,
            dir_metrics,
            directory_metrics,
            poll_interval,
            task_readiness,
            dir_fs,
        )
        .await
    });
    Ok(LogWatcher { task, readiness })
}

// Poll the log directory, spawning a tail task for each new `trace.*.json` file encountered.
async fn run_log_directory(
    dir: PathBuf,
    metrics: LogMetrics,
    exporter_metrics: ExporterMetrics,
    poll_interval: Duration,
    readiness: IngestionReadiness,
    fs: impl TraceFileSystem,
) -> Result<()> {
    let mut tailers = TailTasks::default();

    loop {
        tailers.reap_finished().await;

        match fs.read_dir(&dir).await {
            Ok(entries) => {
                readiness.set_directory_scan_successful(true);
                let trace_paths: HashSet<PathBuf> = entries
                    .into_iter()
                    .filter(|path| {
                        path.file_name()
                            .and_then(|name| name.to_str())
                            .is_some_and(should_tail_file)
                    })
                    .collect();

                tailers.stop_missing(&trace_paths).await;

                for path in trace_paths {
                    if tailers.contains(&path) {
                        continue;
                    }

                    tracing::info!(file = %path.display(), "starting log tailer");
                    let task_metrics = metrics.clone();
                    let task_exporter_metrics = exporter_metrics.clone();
                    let task_path = path.clone();
                    let task_fs = fs.clone();
                    let handle = tokio::spawn(async move {
                        if let Err(error) = run_log_tailer(
                            task_path.clone(),
                            task_metrics,
                            task_exporter_metrics,
                            task_fs,
                        )
                        .await
                        {
                            tracing::error!(
                                ?error,
                                file = %task_path.display(),
                                "log tailer exited"
                            );
                        }
                    });
                    tailers.insert(path, handle);
                }
            }
            Err(error) => {
                readiness.set_directory_scan_successful(false);
                tracing::warn!(?error, dir = %dir.display(), "failed to read log directory");
            }
        }

        time::sleep(poll_interval).await;
    }
}

#[derive(Default)]
struct TailTasks {
    handles: HashMap<PathBuf, JoinHandle<()>>,
}

impl TailTasks {
    fn contains(&self, path: &Path) -> bool {
        self.handles.contains_key(path)
    }

    fn insert(&mut self, path: PathBuf, handle: JoinHandle<()>) {
        self.handles.insert(path, handle);
    }

    async fn reap_finished(&mut self) {
        let finished: Vec<PathBuf> = self
            .handles
            .iter()
            .filter(|(_, handle)| handle.is_finished())
            .map(|(path, _)| path.clone())
            .collect();

        for path in finished {
            if let Some(handle) = self.handles.remove(&path) {
                log_join_error(&path, handle.await);
            }
        }
    }

    async fn stop_missing(&mut self, active_paths: &HashSet<PathBuf>) {
        let missing: Vec<PathBuf> = self
            .handles
            .keys()
            .filter(|path| !active_paths.contains(*path))
            .cloned()
            .collect();

        for path in missing {
            if let Some(handle) = self.handles.remove(&path) {
                tracing::info!(file = %path.display(), "stopping log tailer for removed file");
                handle.abort();
                log_join_error(&path, handle.await);
            }
        }
    }
}

impl Drop for TailTasks {
    fn drop(&mut self) {
        for handle in self.handles.values() {
            handle.abort();
        }
    }
}

fn log_join_error(path: &Path, result: std::result::Result<(), tokio::task::JoinError>) {
    if let Err(error) = result {
        if !error.is_cancelled() {
            tracing::error!(?error, file = %path.display(), "log tailer task failed");
        }
    }
}

// Tail a single trace file and forward each JSON line to the metrics recorder.
async fn run_log_tailer(
    path: PathBuf,
    metrics: LogMetrics,
    exporter_metrics: ExporterMetrics,
    fs: impl TraceFileSystem,
) -> Result<()> {
    let mut identity = None;
    let mut committed_offset = 0;

    loop {
        match fs.open_reader(&path).await {
            Ok(mut reader) => {
                let opened_state = reader.state();
                if identity.as_ref() != Some(&opened_state.identity)
                    || opened_state.len < committed_offset
                {
                    identity = Some(opened_state.identity.clone());
                    committed_offset = 0;
                }

                if let Err(error) = reader.seek_to(committed_offset).await.with_context(|| {
                    format!(
                        "failed to seek log file {} to byte {}",
                        path.display(),
                        committed_offset
                    )
                }) {
                    tracing::warn!(?error, "unable to initialize log tail, retrying");
                    time::sleep(FILE_RETRY_DELAY).await;
                    continue;
                }

                let opened_identity = opened_state.identity;
                let mut read_offset = committed_offset;
                let mut pending_line = String::new();

                loop {
                    let mut chunk = String::new();
                    match reader.read_line(&mut chunk).await {
                        Ok(0) => match fs.file_state(&path).await {
                            Ok(current_state)
                                if current_state.identity != opened_identity
                                    || current_state.len < read_offset =>
                            {
                                tracing::info!(
                                    file = %path.display(),
                                    "log file was replaced or truncated; restarting at beginning"
                                );
                                identity = Some(current_state.identity);
                                committed_offset = 0;
                                break;
                            }
                            Ok(_) => time::sleep(EOF_POLL_DELAY).await,
                            Err(error) => {
                                tracing::warn!(
                                    ?error,
                                    file = %path.display(),
                                    "unable to inspect log file, reopening"
                                );
                                time::sleep(FILE_RETRY_DELAY).await;
                                break;
                            }
                        },
                        Ok(bytes_read) => {
                            read_offset += bytes_read as u64;
                            pending_line.push_str(&chunk);

                            if pending_line.ends_with('\n') {
                                let trimmed = pending_line.trim();
                                if !trimmed.is_empty() {
                                    handle_log_line(trimmed, &metrics, &exporter_metrics);
                                }
                                committed_offset = read_offset;
                                pending_line.clear();
                            }
                        }
                        Err(error) => {
                            tracing::warn!(?error, "log tailer read error, reopening file");
                            time::sleep(FILE_RETRY_DELAY).await;
                            break;
                        }
                    }
                }
            }
            Err(error) => {
                tracing::warn!(?error, log_path = %path.display(), "log file unavailable, retrying");
                time::sleep(FILE_RETRY_DELAY).await;
            }
        }
    }
}

fn should_tail_file(file_name: &str) -> bool {
    file_name.starts_with("trace.") && file_name.ends_with(".json")
}

fn handle_log_line(trimmed: &str, metrics: &LogMetrics, exporter_metrics: &ExporterMetrics) {
    match serde_json::from_str::<TraceEvent>(trimmed) {
        Ok(record) => match metrics.record(&record) {
            Ok(()) => exporter_metrics.record_processed(),
            Err(error) => {
                exporter_metrics.record_record_error();
                let line_preview = log_line_preview(trimmed);
                tracing::warn!(
                    ?error,
                    line_preview = %line_preview,
                    "failed to record log line"
                );
            }
        },
        Err(error) => {
            exporter_metrics.record_parse_error();
            let line_preview = log_line_preview(trimmed);
            tracing::warn!(?error, line_preview = %line_preview, "failed to parse log line");
        }
    }
}

fn log_line_preview(line: &str) -> String {
    let mut chars = line.chars();
    let mut preview: String = chars.by_ref().take(LOG_LINE_PREVIEW_CHARS).collect();
    if chars.next().is_some() {
        preview.push('…');
    }
    preview
}

#[async_trait]
trait TraceFileReader {
    fn state(&self) -> TraceFileState;
    async fn seek_to(&mut self, offset: u64) -> Result<()>;
    async fn read_line(&mut self, buf: &mut String) -> Result<usize>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TraceFileState {
    identity: TraceFileIdentity,
    len: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum TraceFileIdentity {
    #[cfg(unix)]
    Unix { device: u64, inode: u64 },
    #[cfg(not(unix))]
    Portable {
        created: Option<std::time::SystemTime>,
    },
    #[cfg(test)]
    Test(u64),
}

#[async_trait]
trait TraceFileSystem: Clone + Send + Sync + 'static {
    type Reader: TraceFileReader + Send;

    async fn create_dir_all(&self, dir: &Path) -> Result<()>;
    async fn read_dir(&self, dir: &Path) -> Result<Vec<PathBuf>>;
    async fn file_state(&self, path: &Path) -> Result<TraceFileState>;
    async fn open_reader(&self, path: &Path) -> Result<Self::Reader>;
}

#[derive(Clone, Default)]
struct RealTraceFileSystem;

struct RealTraceFileReader {
    reader: BufReader<tokio::fs::File>,
    state: TraceFileState,
}

#[async_trait]
impl TraceFileReader for RealTraceFileReader {
    fn state(&self) -> TraceFileState {
        self.state.clone()
    }

    async fn seek_to(&mut self, offset: u64) -> Result<()> {
        self.reader.seek(SeekFrom::Start(offset)).await?;
        Ok(())
    }

    async fn read_line(&mut self, buf: &mut String) -> Result<usize> {
        let bytes = self.reader.read_line(buf).await?;
        Ok(bytes)
    }
}

#[async_trait]
impl TraceFileSystem for RealTraceFileSystem {
    type Reader = RealTraceFileReader;

    async fn create_dir_all(&self, dir: &Path) -> Result<()> {
        fs::create_dir_all(dir)
            .await
            .with_context(|| format!("failed to create log directory {}", dir.display()))
    }

    async fn read_dir(&self, dir: &Path) -> Result<Vec<PathBuf>> {
        let mut entries = fs::read_dir(dir).await?;
        let mut paths = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            let metadata = entry.metadata().await?;
            if metadata.is_file() {
                paths.push(entry.path());
            }
        }
        Ok(paths)
    }

    async fn file_state(&self, path: &Path) -> Result<TraceFileState> {
        let metadata = fs::metadata(path).await?;
        Ok(real_file_state(&metadata))
    }

    async fn open_reader(&self, path: &Path) -> Result<Self::Reader> {
        let file = fs::OpenOptions::new().read(true).open(path).await?;
        let state = real_file_state(&file.metadata().await?);
        Ok(RealTraceFileReader {
            reader: BufReader::new(file),
            state,
        })
    }
}

fn real_file_state(metadata: &std::fs::Metadata) -> TraceFileState {
    #[cfg(unix)]
    let identity = {
        use std::os::unix::fs::MetadataExt;
        TraceFileIdentity::Unix {
            device: metadata.dev(),
            inode: metadata.ino(),
        }
    };

    #[cfg(not(unix))]
    let identity = TraceFileIdentity::Portable {
        created: metadata.created().ok(),
    };

    TraceFileState {
        identity,
        len: metadata.len(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fdb_metric::FDBMetric;
    use anyhow::{anyhow, Result};
    use opentelemetry_sdk::metrics::{ManualReader, SdkMeterProvider};
    use serde_json::json;
    use std::collections::{HashMap, VecDeque};
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use std::sync::Mutex;
    use tokio::time::{timeout, Duration as TokioDuration};
    use vfs::{MemoryFS, VfsFileType, VfsPath};

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

    async fn wait_for(condition: impl Fn() -> bool) {
        timeout(TokioDuration::from_secs(1), async {
            loop {
                if condition() {
                    return;
                }
                tokio::time::sleep(TokioDuration::from_millis(5)).await;
            }
        })
        .await
        .expect("condition was not met before timeout");
    }

    fn trace_payload(machine: &str) -> Result<String> {
        Ok(serde_json::to_string(&json!({
            "Machine": machine,
            "Roles": "storage",
            "Type": "TestTrace"
        }))?)
    }

    impl FDBMetric for RecordingGauge {
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
        let fs = MemoryTraceFileSystem::new();
        let log_dir = PathBuf::from("/logs");
        let provider = test_meter_provider();
        assert!(
            !fs.exists(&log_dir),
            "log dir should not exist before watch_logs"
        );

        let watcher = watch_logs_with_fs(
            &log_dir,
            provider,
            TokioDuration::from_millis(50),
            fs.clone(),
            None,
        )
        .await
        .expect("watch_logs should succeed");

        // Allow spawned tasks to start.
        let _ = timeout(TokioDuration::from_millis(50), tokio::task::yield_now()).await;

        assert!(
            fs.exists(&log_dir),
            "watch_logs should create log directory"
        );

        watcher.shutdown().await;
    }

    #[tokio::test]
    async fn watch_logs_surfaces_directory_creation_errors() {
        let fs = MemoryTraceFileSystem::new();
        fs.fail_next_create_dir(anyhow!("boom"));
        let log_dir = PathBuf::from("/logs");
        let provider = test_meter_provider();

        let error =
            match watch_logs_with_fs(&log_dir, provider, TokioDuration::from_millis(50), fs, None)
                .await
            {
                Ok(watcher) => {
                    watcher.shutdown().await;
                    panic!("create_dir errors should bubble up");
                }
                Err(error) => error,
            };

        assert!(
            error.to_string().contains("failed to create log directory"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn watch_logs_reports_readiness_and_stops_child_tailers() -> Result<()> {
        let fs = MemoryTraceFileSystem::new();
        let log_dir = PathBuf::from("/logs");
        fs.create_dir_all(&log_dir).await?;
        fs.create_trace_file(&log_dir.join("trace.supervised.json"))?;

        let watcher = watch_logs_with_fs(
            &log_dir,
            test_meter_provider(),
            TokioDuration::from_millis(10),
            fs.clone(),
            None,
        )
        .await?;
        let readiness = watcher.readiness();

        wait_for(|| readiness.is_ready() && fs.active_reader_count() == 1).await;

        watcher.shutdown().await;
        wait_for(|| fs.active_reader_count() == 0).await;
        assert!(!readiness.is_ready());
        Ok(())
    }

    #[tokio::test]
    async fn run_log_directory_records_trace_events() -> Result<()> {
        let fs = MemoryTraceFileSystem::new();
        let log_dir = PathBuf::from("/logs");

        fs.create_dir_all(&log_dir).await?;

        let trace_path = log_dir.join("trace.42.json");
        let ignored_path = log_dir.join("ignored.log");

        fs.create_trace_file(&trace_path)?;
        fs.create_regular_file(&ignored_path)?;

        let events = Arc::new(Mutex::new(Vec::new()));
        let metrics: Vec<Arc<dyn FDBMetric>> = vec![Arc::new(RecordingGauge::new(events.clone()))];
        let log_metrics = LogMetrics::from_metrics(metrics);

        let provider = test_meter_provider();
        let meter = provider.meter("run_log_directory_records_trace_events");
        let exporter_metrics = ExporterMetrics::new(&meter);

        let poll_interval = TokioDuration::from_millis(20);

        let handle = tokio::spawn(run_log_directory(
            log_dir.clone(),
            log_metrics,
            exporter_metrics,
            poll_interval,
            IngestionReadiness::default(),
            fs.clone(),
        ));

        tokio::time::sleep(TokioDuration::from_millis(60)).await;

        let event = json!({
            "Machine": "machine-01",
            "Roles": "storage",
            "Type": "TestTrace"
        });
        fs.append_line(&trace_path, &serde_json::to_string(&event)?)?;
        fs.append_line(&trace_path, "\n")?;

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

    #[tokio::test]
    async fn run_log_directory_continues_after_read_dir_error() -> Result<()> {
        let fs = MemoryTraceFileSystem::new();
        let log_dir = PathBuf::from("/logs");
        fs.create_dir_all(&log_dir).await?;
        fs.fail_next_read_dir(anyhow!("read dir failure"));

        let provider = test_meter_provider();
        let meter = provider.meter("run_log_directory_continues_after_read_dir_error");
        let exporter_metrics = ExporterMetrics::new(&meter);
        let log_metrics = LogMetrics::from_metrics(Vec::<Arc<dyn FDBMetric>>::new());
        let readiness = IngestionReadiness::default();
        readiness.set_watcher_running(true);

        let handle = tokio::spawn(run_log_directory(
            log_dir.clone(),
            log_metrics,
            exporter_metrics,
            TokioDuration::from_millis(20),
            readiness.clone(),
            fs.clone(),
        ));

        tokio::time::sleep(TokioDuration::from_millis(80)).await;

        handle.abort();
        let _ = handle.await;

        assert!(
            fs.failures.lock().unwrap().read_dir.is_empty(),
            "read_dir failure queue should be drained"
        );
        assert!(
            readiness.is_ready(),
            "readiness should recover after a successful directory scan"
        );

        Ok(())
    }

    #[tokio::test]
    async fn run_log_tailer_reads_existing_content_from_beginning() -> Result<()> {
        let fs = MemoryTraceFileSystem::new();
        let trace_path = PathBuf::from("/logs/trace.existing.json");
        fs.create_trace_file(&trace_path)?;
        fs.append_line(&trace_path, &trace_payload("machine-existing")?)?;
        fs.append_line(&trace_path, "\n")?;

        let events = Arc::new(Mutex::new(Vec::new()));
        let metrics: Vec<Arc<dyn FDBMetric>> = vec![Arc::new(RecordingGauge::new(events.clone()))];
        let provider = test_meter_provider();
        let exporter_metrics = ExporterMetrics::new(&provider.meter("existing_content"));
        let handle = tokio::spawn(run_log_tailer(
            trace_path,
            LogMetrics::from_metrics(metrics),
            exporter_metrics,
            fs,
        ));

        wait_for(|| events.lock().unwrap().len() == 1).await;

        handle.abort();
        let _ = handle.await;
        assert_eq!(events.lock().unwrap().len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn run_log_tailer_waits_for_newline_before_parsing_partial_write() -> Result<()> {
        let fs = MemoryTraceFileSystem::new();
        let trace_path = PathBuf::from("/logs/trace.partial.json");
        fs.create_trace_file(&trace_path)?;

        let events = Arc::new(Mutex::new(Vec::new()));
        let metrics: Vec<Arc<dyn FDBMetric>> = vec![Arc::new(RecordingGauge::new(events.clone()))];
        let provider = test_meter_provider();
        let exporter_metrics = ExporterMetrics::new(&provider.meter("partial_write"));
        let handle = tokio::spawn(run_log_tailer(
            trace_path.clone(),
            LogMetrics::from_metrics(metrics),
            exporter_metrics,
            fs.clone(),
        ));

        wait_for(|| fs.active_reader_count() == 1).await;
        fs.append_line(&trace_path, &trace_payload("machine-partial")?)?;
        tokio::time::sleep(TokioDuration::from_millis(40)).await;
        assert!(
            events.lock().unwrap().is_empty(),
            "an incomplete JSON line must not be parsed at temporary EOF"
        );

        fs.append_line(&trace_path, "\n")?;
        wait_for(|| events.lock().unwrap().len() == 1).await;

        handle.abort();
        let _ = handle.await;
        Ok(())
    }

    #[tokio::test]
    async fn run_log_tailer_preserves_events_written_during_open_retry() -> Result<()> {
        let fs = MemoryTraceFileSystem::new();
        let trace_path = PathBuf::from("/logs/trace.retry-gap.json");
        fs.create_trace_file(&trace_path)?;
        fs.fail_next_open_reader(anyhow!("temporary open failure"));

        let events = Arc::new(Mutex::new(Vec::new()));
        let metrics: Vec<Arc<dyn FDBMetric>> = vec![Arc::new(RecordingGauge::new(events.clone()))];
        let provider = test_meter_provider();
        let exporter_metrics = ExporterMetrics::new(&provider.meter("retry_gap"));
        let handle = tokio::spawn(run_log_tailer(
            trace_path.clone(),
            LogMetrics::from_metrics(metrics),
            exporter_metrics,
            fs.clone(),
        ));

        wait_for(|| fs.open_attempt_count() == 1).await;
        fs.append_line(&trace_path, &trace_payload("machine-retry-gap")?)?;
        fs.append_line(&trace_path, "\n")?;

        wait_for(|| events.lock().unwrap().len() == 1).await;
        assert!(fs.open_attempt_count() >= 2);

        handle.abort();
        let _ = handle.await;
        Ok(())
    }

    #[tokio::test]
    async fn run_log_tailer_restarts_after_truncation() -> Result<()> {
        let fs = MemoryTraceFileSystem::new();
        let trace_path = PathBuf::from("/logs/trace.truncated.json");
        fs.create_trace_file(&trace_path)?;
        fs.append_line(&trace_path, &trace_payload("machine-before-truncate")?)?;
        fs.append_line(&trace_path, "\n")?;

        let events = Arc::new(Mutex::new(Vec::new()));
        let metrics: Vec<Arc<dyn FDBMetric>> = vec![Arc::new(RecordingGauge::new(events.clone()))];
        let provider = test_meter_provider();
        let exporter_metrics = ExporterMetrics::new(&provider.meter("truncate"));
        let handle = tokio::spawn(run_log_tailer(
            trace_path.clone(),
            LogMetrics::from_metrics(metrics),
            exporter_metrics,
            fs.clone(),
        ));

        wait_for(|| events.lock().unwrap().len() == 1).await;
        let open_attempts = fs.open_attempt_count();
        fs.truncate_trace_file(&trace_path)?;
        wait_for(|| fs.open_attempt_count() > open_attempts).await;
        fs.append_line(&trace_path, &trace_payload("machine-after-truncate")?)?;
        fs.append_line(&trace_path, "\n")?;
        wait_for(|| events.lock().unwrap().len() == 2).await;

        handle.abort();
        let _ = handle.await;
        Ok(())
    }

    #[tokio::test]
    async fn run_log_tailer_restarts_after_same_path_replacement() -> Result<()> {
        let fs = MemoryTraceFileSystem::new();
        let trace_path = PathBuf::from("/logs/trace.rotated.json");
        fs.create_trace_file(&trace_path)?;
        fs.append_line(&trace_path, &trace_payload("machine-before-rotate")?)?;
        fs.append_line(&trace_path, "\n")?;

        let events = Arc::new(Mutex::new(Vec::new()));
        let metrics: Vec<Arc<dyn FDBMetric>> = vec![Arc::new(RecordingGauge::new(events.clone()))];
        let provider = test_meter_provider();
        let exporter_metrics = ExporterMetrics::new(&provider.meter("replacement"));
        let handle = tokio::spawn(run_log_tailer(
            trace_path.clone(),
            LogMetrics::from_metrics(metrics),
            exporter_metrics,
            fs.clone(),
        ));

        wait_for(|| events.lock().unwrap().len() == 1).await;
        fs.replace_trace_file(&trace_path)?;
        fs.append_line(&trace_path, &trace_payload("machine-after-rotate")?)?;
        fs.append_line(&trace_path, "\n")?;
        wait_for(|| events.lock().unwrap().len() == 2).await;

        handle.abort();
        let _ = handle.await;
        Ok(())
    }

    #[tokio::test]
    async fn run_log_directory_cleans_up_removed_and_parented_tailers() -> Result<()> {
        let fs = MemoryTraceFileSystem::new();
        let log_dir = PathBuf::from("/logs");
        let trace_path = log_dir.join("trace.cleanup.json");
        fs.create_dir_all(&log_dir).await?;
        fs.create_trace_file(&trace_path)?;

        let provider = test_meter_provider();
        let exporter_metrics = ExporterMetrics::new(&provider.meter("tailer_cleanup"));
        let handle = tokio::spawn(run_log_directory(
            log_dir,
            LogMetrics::from_metrics(Vec::<Arc<dyn FDBMetric>>::new()),
            exporter_metrics,
            TokioDuration::from_millis(10),
            IngestionReadiness::default(),
            fs.clone(),
        ));

        wait_for(|| fs.active_reader_count() == 1).await;
        fs.remove_trace_file(&trace_path)?;
        wait_for(|| fs.active_reader_count() == 0).await;

        fs.create_trace_file(&trace_path)?;
        wait_for(|| fs.active_reader_count() == 1).await;
        handle.abort();
        let _ = handle.await;
        wait_for(|| fs.active_reader_count() == 0).await;
        Ok(())
    }

    #[test]
    fn handle_log_line_records_trace_events() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let metrics: Vec<Arc<dyn FDBMetric>> = vec![Arc::new(RecordingGauge::new(events.clone()))];
        let log_metrics = LogMetrics::from_metrics(metrics);

        let provider = test_meter_provider();
        let meter = provider.meter("handle_log_line_records_trace_events");
        let exporter_metrics = ExporterMetrics::new(&meter);

        let event = json!({
            "Machine": "machine-01",
            "Roles": "storage",
            "Type": "TestTrace"
        });
        let payload = serde_json::to_string(&event).expect("serialize event");
        handle_log_line(&payload, &log_metrics, &exporter_metrics);

        let recorded = events.lock().unwrap();
        assert_eq!(
            recorded.len(),
            1,
            "expected exactly one trace event to be recorded"
        );
    }

    #[test]
    fn log_line_preview_is_bounded_on_character_boundaries() {
        let line = "é".repeat(LOG_LINE_PREVIEW_CHARS + 1);
        let preview = log_line_preview(&line);

        assert_eq!(preview.chars().count(), LOG_LINE_PREVIEW_CHARS + 1);
        assert!(preview.ends_with('…'));
        assert!(!preview.contains(&line));
        assert_eq!(log_line_preview("short line"), "short line");
    }

    #[tokio::test]
    async fn run_log_tailer_retries_open_errors() -> Result<()> {
        let fs = MemoryTraceFileSystem::new();
        let log_dir = PathBuf::from("/logs");
        fs.create_dir_all(&log_dir).await?;
        let trace_path = log_dir.join("trace.7.json");
        fs.create_trace_file(&trace_path)?;
        fs.fail_next_open_reader(anyhow!("open failure"));

        let events = Arc::new(Mutex::new(Vec::new()));
        let metrics: Vec<Arc<dyn FDBMetric>> = vec![Arc::new(RecordingGauge::new(events.clone()))];
        let log_metrics = LogMetrics::from_metrics(metrics);

        let provider = test_meter_provider();
        let meter = provider.meter("run_log_tailer_retries_open_errors");
        let exporter_metrics = ExporterMetrics::new(&meter);

        let path_clone = trace_path.clone();
        let fs_clone = fs.clone();
        let handle = tokio::spawn(run_log_tailer(
            path_clone,
            log_metrics,
            exporter_metrics,
            fs_clone,
        ));

        tokio::time::sleep(TokioDuration::from_millis(1100)).await;

        let event = json!({
            "Machine": "machine-open",
            "Roles": "storage",
            "Type": "TestTrace"
        });
        fs.append_line(&trace_path, &serde_json::to_string(&event)?)?;
        fs.append_line(&trace_path, "\n")?;

        for _ in 0..80 {
            if !events.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(TokioDuration::from_millis(20)).await;
        }

        handle.abort();
        let _ = handle.await;

        assert_eq!(events.lock().unwrap().len(), 1);
        assert!(
            fs.failures.lock().unwrap().open_reader.is_empty(),
            "open_reader failure queue should be drained"
        );

        Ok(())
    }

    #[tokio::test]
    async fn run_log_tailer_retries_seek_errors() -> Result<()> {
        let fs = MemoryTraceFileSystem::new();
        let log_dir = PathBuf::from("/logs");
        fs.create_dir_all(&log_dir).await?;
        let trace_path = log_dir.join("trace.8.json");
        fs.create_trace_file(&trace_path)?;
        fs.fail_next_seek(anyhow!("seek failure"));

        let events = Arc::new(Mutex::new(Vec::new()));
        let metrics: Vec<Arc<dyn FDBMetric>> = vec![Arc::new(RecordingGauge::new(events.clone()))];
        let log_metrics = LogMetrics::from_metrics(metrics);

        let provider = test_meter_provider();
        let meter = provider.meter("run_log_tailer_retries_seek_errors");
        let exporter_metrics = ExporterMetrics::new(&meter);

        let path_clone = trace_path.clone();
        let fs_clone = fs.clone();
        let handle = tokio::spawn(run_log_tailer(
            path_clone,
            log_metrics,
            exporter_metrics,
            fs_clone,
        ));

        tokio::time::sleep(TokioDuration::from_millis(1100)).await;

        let event = json!({
            "Machine": "machine-seek",
            "Roles": "storage",
            "Type": "TestTrace"
        });
        fs.append_line(&trace_path, &serde_json::to_string(&event)?)?;
        fs.append_line(&trace_path, "\n")?;

        for _ in 0..80 {
            if !events.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(TokioDuration::from_millis(20)).await;
        }

        handle.abort();
        let _ = handle.await;

        assert_eq!(events.lock().unwrap().len(), 1);
        assert!(
            fs.failures.lock().unwrap().seek.is_empty(),
            "seek failure queue should be drained"
        );

        Ok(())
    }

    #[tokio::test]
    async fn run_log_tailer_retries_read_errors() -> Result<()> {
        let fs = MemoryTraceFileSystem::new();
        let log_dir = PathBuf::from("/logs");
        fs.create_dir_all(&log_dir).await?;
        let trace_path = log_dir.join("trace.9.json");
        fs.create_trace_file(&trace_path)?;
        fs.fail_next_read(anyhow!("read failure"));

        let events = Arc::new(Mutex::new(Vec::new()));
        let metrics: Vec<Arc<dyn FDBMetric>> = vec![Arc::new(RecordingGauge::new(events.clone()))];
        let log_metrics = LogMetrics::from_metrics(metrics);

        let provider = test_meter_provider();
        let meter = provider.meter("run_log_tailer_retries_read_errors");
        let exporter_metrics = ExporterMetrics::new(&meter);

        let path_clone = trace_path.clone();
        let fs_clone = fs.clone();
        let handle = tokio::spawn(run_log_tailer(
            path_clone,
            log_metrics,
            exporter_metrics,
            fs_clone,
        ));

        tokio::time::sleep(TokioDuration::from_millis(1100)).await;

        let event = json!({
            "Machine": "machine-read",
            "Roles": "storage",
            "Type": "TestTrace"
        });
        fs.append_line(&trace_path, &serde_json::to_string(&event)?)?;
        fs.append_line(&trace_path, "\n")?;

        for _ in 0..80 {
            if !events.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(TokioDuration::from_millis(20)).await;
        }

        handle.abort();
        let _ = handle.await;

        assert_eq!(events.lock().unwrap().len(), 1);
        assert!(
            fs.failures.lock().unwrap().read.is_empty(),
            "read failure queue should be drained"
        );

        Ok(())
    }

    #[derive(Clone)]
    struct MemoryTraceFileSystem {
        root: VfsPath,
        files: Arc<Mutex<HashMap<String, Arc<MemoryTraceFile>>>>,
        failures: Arc<Mutex<MemoryFsFailures>>,
        next_file_id: Arc<AtomicU64>,
        active_readers: Arc<AtomicUsize>,
        open_attempts: Arc<AtomicUsize>,
    }

    impl MemoryTraceFileSystem {
        fn new() -> Self {
            Self {
                root: VfsPath::new(MemoryFS::new()),
                files: Arc::new(Mutex::new(HashMap::new())),
                failures: Arc::new(Mutex::new(MemoryFsFailures::default())),
                next_file_id: Arc::new(AtomicU64::new(1)),
                active_readers: Arc::new(AtomicUsize::new(0)),
                open_attempts: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn to_vfs_path(&self, path: &Path) -> Result<VfsPath> {
            let normalized = normalize_path(path)?;
            if normalized.is_empty() {
                Ok(self.root.clone())
            } else {
                self.root.join(&normalized).map_err(|error| anyhow!(error))
            }
        }

        fn exists(&self, path: &Path) -> bool {
            match self.to_vfs_path(path) {
                Ok(vpath) => vpath.exists().unwrap_or(false),
                Err(_) => false,
            }
        }

        fn create_trace_file(&self, path: &Path) -> Result<()> {
            let vpath = self.to_vfs_path(path)?;
            vpath
                .parent()
                .create_dir_all()
                .map_err(|error| anyhow!(error))?;
            drop(vpath.create_file().map_err(|error| anyhow!(error))?);
            let file = Arc::new(MemoryTraceFile {
                id: self.next_file_id.fetch_add(1, Ordering::Relaxed),
                data: Mutex::new(Vec::new()),
            });
            self.files
                .lock()
                .unwrap()
                .insert(normalize_path(path)?, file);
            Ok(())
        }

        fn create_regular_file(&self, path: &Path) -> Result<()> {
            let vpath = self.to_vfs_path(path)?;
            vpath
                .parent()
                .create_dir_all()
                .map_err(|error| anyhow!(error))?;
            drop(vpath.create_file().map_err(|error| anyhow!(error))?);
            Ok(())
        }

        fn append_line(&self, path: &Path, contents: &str) -> Result<()> {
            let key = normalize_path(path)?;
            let file = self
                .files
                .lock()
                .unwrap()
                .get(&key)
                .cloned()
                .with_context(|| format!("virtual file {} not found", path.display()))?;
            let mut data = file.data.lock().unwrap();
            data.extend_from_slice(contents.as_bytes());
            Ok(())
        }

        fn truncate_trace_file(&self, path: &Path) -> Result<()> {
            let key = normalize_path(path)?;
            let file = self
                .files
                .lock()
                .unwrap()
                .get(&key)
                .cloned()
                .with_context(|| format!("virtual file {} not found", path.display()))?;
            file.data.lock().unwrap().clear();
            Ok(())
        }

        fn replace_trace_file(&self, path: &Path) -> Result<()> {
            let key = normalize_path(path)?;
            let mut files = self.files.lock().unwrap();
            if !files.contains_key(&key) {
                return Err(anyhow!("virtual file {} not found", path.display()));
            }
            files.insert(
                key,
                Arc::new(MemoryTraceFile {
                    id: self.next_file_id.fetch_add(1, Ordering::Relaxed),
                    data: Mutex::new(Vec::new()),
                }),
            );
            Ok(())
        }

        fn remove_trace_file(&self, path: &Path) -> Result<()> {
            self.files.lock().unwrap().remove(&normalize_path(path)?);
            self.to_vfs_path(path)?
                .remove_file()
                .map_err(|error| anyhow!(error))
        }

        fn active_reader_count(&self) -> usize {
            self.active_readers.load(Ordering::SeqCst)
        }

        fn open_attempt_count(&self) -> usize {
            self.open_attempts.load(Ordering::SeqCst)
        }

        fn fail_next_create_dir(&self, error: impl Into<anyhow::Error>) {
            self.failures
                .lock()
                .unwrap()
                .create_dir
                .push_back(error.into());
        }

        fn fail_next_read_dir(&self, error: impl Into<anyhow::Error>) {
            self.failures
                .lock()
                .unwrap()
                .read_dir
                .push_back(error.into());
        }

        fn fail_next_open_reader(&self, error: impl Into<anyhow::Error>) {
            self.failures
                .lock()
                .unwrap()
                .open_reader
                .push_back(error.into());
        }

        fn fail_next_seek(&self, error: impl Into<anyhow::Error>) {
            self.failures.lock().unwrap().seek.push_back(error.into());
        }

        fn fail_next_read(&self, error: impl Into<anyhow::Error>) {
            self.failures.lock().unwrap().read.push_back(error.into());
        }
    }

    #[async_trait]
    impl TraceFileSystem for MemoryTraceFileSystem {
        type Reader = MemoryTraceFileReader;

        async fn create_dir_all(&self, dir: &Path) -> Result<()> {
            if let Some(error) = self.failures.lock().unwrap().create_dir.pop_front() {
                return Err(error);
            }
            let vpath = self.to_vfs_path(dir)?;
            vpath.create_dir_all().map_err(|error| anyhow!(error))?;
            Ok(())
        }

        async fn read_dir(&self, dir: &Path) -> Result<Vec<PathBuf>> {
            if let Some(error) = self.failures.lock().unwrap().read_dir.pop_front() {
                return Err(error);
            }
            let dir_path = self.to_vfs_path(dir)?;
            let entries = dir_path.read_dir().map_err(|error| anyhow!(error))?;
            let mut paths = Vec::new();
            for entry in entries {
                let metadata = entry.metadata().map_err(|error| anyhow!(error))?;
                if metadata.file_type == VfsFileType::File {
                    paths.push(dir.join(entry.filename()));
                }
            }
            Ok(paths)
        }

        async fn file_state(&self, path: &Path) -> Result<TraceFileState> {
            let key = normalize_path(path)?;
            let file = self
                .files
                .lock()
                .unwrap()
                .get(&key)
                .cloned()
                .with_context(|| format!("virtual file {} not found", path.display()))?;
            let len = file.data.lock().unwrap().len() as u64;
            Ok(TraceFileState {
                identity: TraceFileIdentity::Test(file.id),
                len,
            })
        }

        async fn open_reader(&self, path: &Path) -> Result<Self::Reader> {
            self.open_attempts.fetch_add(1, Ordering::SeqCst);
            if let Some(error) = self.failures.lock().unwrap().open_reader.pop_front() {
                return Err(error);
            }
            let key = normalize_path(path)?;
            let file = self
                .files
                .lock()
                .unwrap()
                .get(&key)
                .cloned()
                .with_context(|| format!("virtual file {} not found", path.display()))?;
            let id = file.id;
            let len = file.data.lock().unwrap().len() as u64;
            self.active_readers.fetch_add(1, Ordering::SeqCst);
            Ok(MemoryTraceFileReader {
                file,
                offset: 0,
                failures: Arc::clone(&self.failures),
                state: TraceFileState {
                    identity: TraceFileIdentity::Test(id),
                    len,
                },
                active_readers: Arc::clone(&self.active_readers),
            })
        }
    }

    struct MemoryTraceFile {
        id: u64,
        data: Mutex<Vec<u8>>,
    }

    struct MemoryTraceFileReader {
        file: Arc<MemoryTraceFile>,
        offset: usize,
        failures: Arc<Mutex<MemoryFsFailures>>,
        state: TraceFileState,
        active_readers: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl TraceFileReader for MemoryTraceFileReader {
        fn state(&self) -> TraceFileState {
            self.state.clone()
        }

        async fn seek_to(&mut self, offset: u64) -> Result<()> {
            if let Some(error) = self.failures.lock().unwrap().seek.pop_front() {
                return Err(error);
            }
            self.offset = usize::try_from(offset)?;
            Ok(())
        }

        async fn read_line(&mut self, buf: &mut String) -> Result<usize> {
            if let Some(error) = self.failures.lock().unwrap().read.pop_front() {
                return Err(error);
            }
            let bytes = {
                let data = self.file.data.lock().unwrap();
                if self.offset >= data.len() {
                    return Ok(0);
                }
                let slice = &data[self.offset..];
                let newline_pos = slice.iter().position(|b| *b == b'\n');
                let end = match newline_pos {
                    Some(idx) => self.offset + idx + 1,
                    None => data.len(),
                };
                let bytes = data[self.offset..end].to_vec();
                self.offset = end;
                bytes
            };

            let line = String::from_utf8(bytes)?;
            buf.push_str(&line);
            Ok(line.len())
        }
    }

    impl Drop for MemoryTraceFileReader {
        fn drop(&mut self) {
            self.active_readers.fetch_sub(1, Ordering::SeqCst);
        }
    }

    fn path_to_string(path: &Path) -> Result<String> {
        Ok(path
            .to_str()
            .context(format!("path {} not valid UTF-8", path.display()))?
            .to_string())
    }

    fn normalize_path(path: &Path) -> Result<String> {
        let mut path = path_to_string(path)?;
        path = path.replace('\\', "/");
        while path.contains("//") {
            path = path.replace("//", "/");
        }
        let normalized = path.trim_start_matches('/').to_string();
        Ok(normalized)
    }

    #[derive(Default)]
    struct MemoryFsFailures {
        create_dir: VecDeque<anyhow::Error>,
        read_dir: VecDeque<anyhow::Error>,
        open_reader: VecDeque<anyhow::Error>,
        seek: VecDeque<anyhow::Error>,
        read: VecDeque<anyhow::Error>,
    }
}
