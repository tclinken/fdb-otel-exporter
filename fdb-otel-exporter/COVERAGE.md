# Code Coverage

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

`cargo-llvm-cov` writes intermediate profiles under `target/`; they are ignored by default and safe to delete with:

```bash
cargo llvm-cov clean --workspace
```
