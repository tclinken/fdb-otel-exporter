# Quality Checks

## Code Coverage

This crate uses [`cargo-llvm-cov`](https://github.com/taiki-e/cargo-llvm-cov) to collect coverage from unit and integration tests.

## Setup

1. Install LLVM tooling (once per toolchain):
   ```bash
   rustup component add llvm-tools-preview
   ```
2. Install the `cargo-llvm-cov` binary (once per machine):
   ```bash
   cargo install cargo-llvm-cov
   ```

## Running Coverage Locally

From the repository root:

- Generate an HTML report at `target/coverage/index.html`:
  ```bash
  cargo coverage
  ```
- Print a table summarising coverage without producing artifacts (useful for CI guards):
  ```bash
  cargo coverage-summary
  ```
- Enforce the project's 89% line-coverage floor:
  ```bash
  cargo coverage-check
  ```

`cargo coverage-check` exits non-zero when line coverage is below 89%, so it is
the alias to use as a required CI quality gate.

`cargo-llvm-cov` writes intermediate profiles under `target/`; they are ignored by default and safe to delete with:

```bash
cargo llvm-cov clean --workspace
```

## Dependency Audit

Install [`cargo-audit`](https://github.com/rustsec/rustsec/tree/main/cargo-audit)
once, then run the repository's scoped audit:

```bash
cargo install cargo-audit --locked
cargo security-audit
```

The alias ignores only `RUSTSEC-2024-0437`. The affected `protobuf` 2.x package
is required by `opentelemetry-prometheus` 0.16, and its vulnerable function
parses untrusted protobuf input. This exporter ingests FoundationDB JSON and only
constructs Prometheus metric models for text encoding, so that function is not
reachable from its inputs. All other audit findings still fail the command.

Removing the vulnerable protobuf 2.x dependency requires upgrading the
OpenTelemetry family to 0.29.1 or newer together with `prometheus` 0.14. That
multi-release API migration should be handled separately, with metric-output
compatibility tests; remove the scoped exception as part of that migration.
