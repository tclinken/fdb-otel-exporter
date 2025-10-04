# Repository Guidelines

## Project Structure & Module Organization
- `fdb-otel-exporter/` contains the Rust telemetry service (`src/main.rs`) and Cargo manifests. Logs generated or tailed locally live in `fdb-otel-exporter/logs/`.
- `docker-compose.yml`, `prometheus.yml`, and `grafana-provisioning/` define the local observability stack.
- `dashboards/` holds Grafana dashboards; adjust JSON definitions under `grafana-provisioning/dashboards/` when shipping UI updates.

## Build, Test, and Development Commands
- `cargo fmt` (run in `fdb-otel-exporter/`) formats Rust sources using `rustfmt`.
- `cargo check` validates compilation without producing binaries; run before commits for quick feedback.
- `cargo test` executes unit/integration tests (add new tests under `fdb-otel-exporter/tests/` when needed).
- `docker compose up --build -d` rebuilds the service image, applies environment changes (e.g., `LOG_DIR`, `GENERATE_SAMPLE_LOGS=1`), and starts Grafana/Prometheus/Rust services.

## Coding Style & Naming Conventions
- Rust code targets edition 2021 with 4-space indentation; prefer descriptive `snake_case` for functions and variables, and `CamelCase` for types.
- Keep modules small and focused; place new async tasks or metrics helpers in their own files under `fdb-otel-exporter/src/` when complexity grows.
- Always run `cargo fmt` before committing; CI assumes formatted output.

## Testing Guidelines
- Use Rust’s built-in test framework; colocate simple tests with the code via `#[cfg(test)]` modules, and reserve `tests/` for end-to-end flows.
- Name tests after the behavior under test (e.g., `parses_storage_metrics_version`).
- When adding metrics handlers, provide fixtures under `fdb-otel-exporter/logs/` and assert parsed values with `cargo test`.

## Commit & Pull Request Guidelines
- Follow the repository’s precedent of short, imperative commit subjects (e.g., “Add fdb-otel-exporter crate”).
- Squash work-in-progress commits before opening a PR; each PR should describe intent, linked issues, and any dashboard or metric impact.
- Include CLI output for critical changes (e.g., `cargo check`, `docker compose up --build`) and attach screenshots when altering Grafana dashboards.

## Observability & Configuration Tips
- Default metrics target `http://localhost:9200/metrics`. Prometheus discovers the service via `prometheus.yml`; update scrape intervals there when tuning load.
- Use `LOG_DIR` to point at trace archives and `GENERATE_SAMPLE_LOGS=1` to seed `trace.json` with sample `StorageMetrics` events during local development.
