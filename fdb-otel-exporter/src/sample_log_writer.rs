use crate::fake_trace_event::FakeTraceEvent;
use anyhow::Result;
use std::path::PathBuf;
use std::time::Duration;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::time;

pub fn generate_samples(writer_dir: PathBuf) {
    tokio::spawn(async move {
        if let Err(error) = run_sample_log_writer(writer_dir).await {
            tracing::error!(?error, "sample log writer terminated");
        }
    });
}

async fn run_sample_log_writer(dir: PathBuf) -> Result<()> {
    let file_path = dir.join("trace.0.json");
    let mut sequence = 0usize;

    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&file_path)
        .await?;
    let mut writer = BufWriter::new(file);

    loop {
        sequence = sequence.wrapping_add(1);

        let payload = FakeTraceEvent::new("StorageMetrics".to_string())
            .detail("Version".to_string(), "100".to_string())
            .log();

        writer.write_all(payload.to_string().as_bytes()).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;

        time::sleep(Duration::from_secs(5)).await;
    }
}
