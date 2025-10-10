# Repository Guidelines

## Project Structure & Module Organization
- Primary service code lives in `fdb-otel-exporter/src/main.rs`; place supporting modules under `fdb-otel-exporter/src/`.
- Logs generated locally land in `fdb-otel-exporter/logs/`; reuse this directory for fixtures referenced by tests.
- Docker stack definitions sit in `docker-compose.yml`, `prometheus.yml`, and `grafana-provisioning/`; Grafana dashboards live in `dashboards/` and ship via `grafana-provisioning/dashboards/`.

## Build, Test, and Development Commands
- `cargo fmt` (run inside `fdb-otel-exporter/`) formats Rust sources; execute before every commit.
- `cargo check` validates compilation without producing binaries.
- `cargo clippy --tests` enforces lint clean builds for code and tests; run after each set of changes.
- `cargo test` runs the unit and integration suites, including files under `fdb-otel-exporter/tests/`.
- `docker compose up --build -d` rebuilds the service image, applies env changes (e.g., `LOG_DIR`), and starts the observability stack.

## Coding Style & Naming Conventions
- Target Rust 2021 with 4-space indentation; use `snake_case` for functions/variables and `CamelCase` for types.
- Keep modules focused; break out new async tasks or metrics helpers into dedicated files under `fdb-otel-exporter/src/`.
- Always run `cargo fmt` before pushing; CI rejects unformatted code.

## Testing Guidelines
- Use Rust’s built-in framework; colocate simple unit tests with `#[cfg(test)]` modules and reserve `fdb-otel-exporter/tests/` for end-to-end flows.
- Name tests after behavior, e.g., `parses_storage_metrics_version`.
- Store sample events in `fdb-otel-exporter/logs/trace.json`; update fixtures when introducing new metrics parsers.

## Commit & Pull Request Guidelines
- Write short, imperative commit subjects (e.g., “Add fdb-otel-exporter crate”).
- Squash WIP commits before opening a PR; describe intent, link issues, and call out dashboard or metric impact.
- Attach relevant CLI output (e.g., `cargo check`) and screenshots when modifying Grafana dashboards.

## Observability & Configuration Tips
- Default metrics endpoint is `http://localhost:9200/metrics`; adjust Prometheus scrape intervals via `prometheus.yml`.
- Set `LOG_DIR` to point at archived traces for local testing.
