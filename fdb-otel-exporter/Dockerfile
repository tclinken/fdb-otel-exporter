# Build stage
FROM rust:1.88 AS builder
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends busybox-static \
    && rm -rf /var/lib/apt/lists/*

# Pre-copy manifests to leverage Docker layer caching
COPY Cargo.toml Cargo.lock ./
COPY gauge_config.toml ./
COPY src ./src

RUN mkdir -p logs

RUN cargo build --release

# Runtime stage
FROM gcr.io/distroless/cc-debian12

COPY --from=builder /bin/busybox /bin/busybox
COPY --from=builder --chmod=0555 /app/target/release/fdb-otel-exporter /usr/local/bin/fdb-otel-exporter
COPY --from=builder --chown=nonroot:nonroot --chmod=0555 /app/gauge_config.toml /app/gauge_config.toml
COPY --from=builder --chown=nonroot:nonroot /app/logs /logs

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 CMD ["/bin/busybox", "wget", "-qO-", "http://127.0.0.1:9200/health"]

USER nonroot:nonroot

EXPOSE 9200

CMD ["/usr/local/bin/fdb-otel-exporter"]
