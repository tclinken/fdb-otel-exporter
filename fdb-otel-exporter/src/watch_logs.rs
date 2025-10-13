use crate::{
    exporter_metrics::ExporterMetrics,
    log_metrics::{LogMetrics, TraceEvent},
};
use anyhow::{Context, Result};
use async_trait::async_trait;
use opentelemetry::metrics::MeterProvider;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use std::collections::HashSet;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::fs;
use tokio::io::{AsyncBufReadExt, AsyncSeekExt, BufReader};
use tokio::time;

// Discover JSON trace logs under `log_dir_path` and push their events through the configured gauges.
pub async fn watch_logs(
    log_dir_path: &Path,
    meter_provider: Arc<SdkMeterProvider>,
    poll_interval: Duration,
) -> Result<()> {
    watch_logs_with_fs(
        log_dir_path,
        meter_provider,
        poll_interval,
        RealTraceFileSystem,
    )
    .await
}

async fn watch_logs_with_fs<F>(
    log_dir_path: &Path,
    meter_provider: Arc<SdkMeterProvider>,
    poll_interval: Duration,
    fs: F,
) -> Result<()>
where
    F: TraceFileSystem,
{
    let meter = meter_provider.meter("fdb-otel-exporter");
    let exporter_metrics = ExporterMetrics::new(&meter);
    let log_metrics =
        LogMetrics::new(&meter).with_context(|| "failed to load gauge configuration")?;

    fs.create_dir_all(log_dir_path)
        .await
        .with_context(|| format!("failed to create log directory {}", log_dir_path.display()))?;

    let watcher_dir = log_dir_path.to_path_buf();
    let dir_metrics = log_metrics.clone();
    let directory_metrics = exporter_metrics.clone();
    let dir_fs = fs.clone();
    tokio::spawn(async move {
        if let Err(error) = run_log_directory(
            watcher_dir,
            dir_metrics,
            directory_metrics,
            poll_interval,
            dir_fs,
        )
        .await
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
    fs: impl TraceFileSystem,
) -> Result<()> {
    let mut tailed: HashSet<PathBuf> = HashSet::new();

    loop {
        match fs.read_dir(&dir).await {
            Ok(entries) => {
                for path in entries {
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
                        let task_path = path.clone();
                        let task_fs = fs.clone();
                        tokio::spawn(async move {
                            if let Err(error) = run_log_tailer(
                                task_path.clone(),
                                task_metrics,
                                task_exporter_metrics,
                                task_fs,
                            )
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
    fs: impl TraceFileSystem,
) -> Result<()> {
    loop {
        match fs.open_reader(&path).await {
            Ok(mut reader) => {
                if let Err(error) = reader
                    .seek_to_end()
                    .await
                    .with_context(|| format!("failed to seek log file {}", path.display()))
                {
                    tracing::warn!(?error, "unable to initialize log tail, retrying");
                    time::sleep(Duration::from_secs(1)).await;
                    continue;
                }

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
                            handle_log_line(trimmed, &metrics, &exporter_metrics);
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

fn handle_log_line(trimmed: &str, metrics: &LogMetrics, exporter_metrics: &ExporterMetrics) {
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

#[async_trait]
trait TraceFileReader {
    async fn seek_to_end(&mut self) -> Result<()>;
    async fn read_line(&mut self, buf: &mut String) -> Result<usize>;
}

#[async_trait]
trait TraceFileSystem: Clone + Send + Sync + 'static {
    type Reader: TraceFileReader + Send;

    async fn create_dir_all(&self, dir: &Path) -> Result<()>;
    async fn read_dir(&self, dir: &Path) -> Result<Vec<PathBuf>>;
    async fn open_reader(&self, path: &Path) -> Result<Self::Reader>;
}

#[derive(Clone, Default)]
struct RealTraceFileSystem;

struct RealTraceFileReader {
    reader: BufReader<tokio::fs::File>,
}

#[async_trait]
impl TraceFileReader for RealTraceFileReader {
    async fn seek_to_end(&mut self) -> Result<()> {
        self.reader.get_mut().seek(SeekFrom::End(0)).await?;
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

    async fn open_reader(&self, path: &Path) -> Result<Self::Reader> {
        let file = fs::OpenOptions::new().read(true).open(path).await?;
        Ok(RealTraceFileReader {
            reader: BufReader::new(file),
        })
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

        watch_logs_with_fs(
            &log_dir,
            provider,
            TokioDuration::from_millis(50),
            fs.clone(),
        )
        .await
        .expect("watch_logs should succeed");

        // Allow spawned tasks to start.
        let _ = timeout(TokioDuration::from_millis(50), tokio::task::yield_now()).await;

        assert!(
            fs.exists(&log_dir),
            "watch_logs should create log directory"
        );
    }

    #[tokio::test]
    async fn watch_logs_surfaces_directory_creation_errors() {
        let fs = MemoryTraceFileSystem::new();
        fs.fail_next_create_dir(anyhow!("boom"));
        let log_dir = PathBuf::from("/logs");
        let provider = test_meter_provider();

        let error = watch_logs_with_fs(&log_dir, provider, TokioDuration::from_millis(50), fs)
            .await
            .expect_err("create_dir errors should bubble up");

        assert!(
            error.to_string().contains("failed to create log directory"),
            "unexpected error: {error}"
        );
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

        let handle = tokio::spawn(run_log_directory(
            log_dir.clone(),
            log_metrics,
            exporter_metrics,
            TokioDuration::from_millis(20),
            fs.clone(),
        ));

        tokio::time::sleep(TokioDuration::from_millis(80)).await;

        handle.abort();
        let _ = handle.await;

        assert!(
            fs.failures.lock().unwrap().read_dir.is_empty(),
            "read_dir failure queue should be drained"
        );

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
    }

    impl MemoryTraceFileSystem {
        fn new() -> Self {
            Self {
                root: VfsPath::new(MemoryFS::new()),
                files: Arc::new(Mutex::new(HashMap::new())),
                failures: Arc::new(Mutex::new(MemoryFsFailures::default())),
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
            let file = Arc::new(MemoryTraceFile::default());
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

        async fn open_reader(&self, path: &Path) -> Result<Self::Reader> {
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
            Ok(MemoryTraceFileReader {
                file,
                offset: 0,
                failures: Arc::clone(&self.failures),
            })
        }
    }

    #[derive(Default)]
    struct MemoryTraceFile {
        data: Mutex<Vec<u8>>,
    }

    struct MemoryTraceFileReader {
        file: Arc<MemoryTraceFile>,
        offset: usize,
        failures: Arc<Mutex<MemoryFsFailures>>,
    }

    #[async_trait]
    impl TraceFileReader for MemoryTraceFileReader {
        async fn seek_to_end(&mut self) -> Result<()> {
            if let Some(error) = self.failures.lock().unwrap().seek.pop_front() {
                return Err(error);
            }
            let data = self.file.data.lock().unwrap();
            self.offset = data.len();
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
