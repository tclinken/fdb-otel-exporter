# Build stage
FROM rust:1.88 AS builder
WORKDIR /app

# Pre-copy manifests to leverage Docker layer caching
COPY Cargo.toml Cargo.lock ./
COPY gauge_config.toml ./
COPY src ./src

RUN cargo build --release

# Runtime stage
FROM gcr.io/distroless/cc-debian12
COPY --from=builder /app/target/release/fdb-otel-exporter /usr/local/bin/fdb-otel-exporter
COPY --from=builder /app/gauge_config.toml /app/gauge_config.toml

EXPOSE 9200
CMD ["/usr/local/bin/fdb-otel-exporter"]
