# syntax=docker/dockerfile:1.7

# Build stage
FROM rust:1.88.0-bookworm AS builder
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends busybox-static \
    && rm -rf /var/lib/apt/lists/*

# Fetch dependencies before copying frequently changing application sources.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && touch src/main.rs
RUN --mount=type=cache,id=fdb-otel-exporter-cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    cargo fetch --locked

COPY gauge_config.toml ./
COPY src ./src
RUN mkdir -p logs

RUN --mount=type=cache,id=fdb-otel-exporter-cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=fdb-otel-exporter-target,target=/app/target,sharing=locked \
    cargo build --release --locked \
    && cp target/release/fdb-otel-exporter /tmp/fdb-otel-exporter

# Runtime stage
FROM gcr.io/distroless/cc-debian12

WORKDIR /app
ENV GAUGE_CONFIG_PATH=/app/gauge_config.toml \
    TRACE_LOG_FILE=/logs/tracing.log

COPY --from=builder /bin/busybox /bin/busybox
COPY --from=builder --chmod=0555 /tmp/fdb-otel-exporter /usr/local/bin/fdb-otel-exporter
COPY --from=builder --chown=nonroot:nonroot --chmod=0444 /app/gauge_config.toml /app/gauge_config.toml
COPY --from=builder --chown=nonroot:nonroot --chmod=0755 /app/logs /logs

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 CMD ["/bin/busybox", "wget", "-qO-", "http://127.0.0.1:9200/ready"]

USER nonroot:nonroot

EXPOSE 9200

CMD ["/usr/local/bin/fdb-otel-exporter"]
