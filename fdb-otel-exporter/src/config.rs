use anyhow::{anyhow, Context, Result};
use std::{
    env::{self, VarError},
    net::SocketAddr,
    path::PathBuf,
    time::Duration,
};

pub const LOG_DIR_ENV: &str = "LOG_DIR";
pub const TRACE_LOG_FILE_ENV: &str = "TRACE_LOG_FILE";
pub const LISTEN_ADDR_ENV: &str = "LISTEN_ADDR";
pub const LOG_POLL_INTERVAL_ENV: &str = "LOG_POLL_INTERVAL_SECS";
const DEFAULT_LOG_DIR: &str = "logs";
const DEFAULT_TRACE_LOG_FILE: &str = "logs/tracing.log";
const DEFAULT_LISTEN_ADDR: &str = "0.0.0.0:9200";
const DEFAULT_POLL_INTERVAL_SECS: f64 = 2.0;

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub listen_addr: SocketAddr,
    pub log_dir: PathBuf,
    pub trace_log_file: PathBuf,
    pub log_poll_interval: Duration,
}

impl AppConfig {
    pub fn from_env() -> Result<Self> {
        let listen_addr_str =
            env::var(LISTEN_ADDR_ENV).unwrap_or_else(|_| DEFAULT_LISTEN_ADDR.to_string());
        let listen_addr = listen_addr_str.parse::<SocketAddr>().with_context(|| {
            format!(
                "environment variable {LISTEN_ADDR_ENV} expected to be a socket address, got {listen_addr_str}"
            )
        })?;

        let log_dir =
            PathBuf::from(env::var(LOG_DIR_ENV).unwrap_or_else(|_| DEFAULT_LOG_DIR.to_string()));

        let trace_log_file = PathBuf::from(
            env::var(TRACE_LOG_FILE_ENV).unwrap_or_else(|_| DEFAULT_TRACE_LOG_FILE.to_string()),
        );

        let log_poll_interval = Duration::from_secs_f64(parse_f64_env(
            LOG_POLL_INTERVAL_ENV,
            DEFAULT_POLL_INTERVAL_SECS,
        )?);

        Ok(Self {
            listen_addr,
            log_dir,
            trace_log_file,
            log_poll_interval,
        })
    }
}

fn parse_f64_env(key: &str, default: f64) -> Result<f64> {
    match env::var(key) {
        Ok(value) => value.parse::<f64>().with_context(|| {
            format!(
                "environment variable {key} expected to be a floating point number, got {value}"
            )
        }),
        Err(VarError::NotPresent) => Ok(default),
        Err(VarError::NotUnicode(_)) => {
            Err(anyhow!("environment variable {key} must be valid UTF-8"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};

    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    fn env_guard() -> std::sync::MutexGuard<'static, ()> {
        ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("env mutex poisoned")
    }

    fn with_env(vars: &[(&str, Option<&str>)], f: impl FnOnce()) {
        let _guard = env_guard();

        let previous: Vec<(String, Option<String>)> = vars
            .iter()
            .map(|(key, _)| (key.to_string(), env::var(key).ok()))
            .collect();

        for (key, value) in vars {
            match value {
                Some(val) => env::set_var(key, val),
                None => env::remove_var(key),
            }
        }

        f();

        for (key, value) in previous {
            match value {
                Some(val) => env::set_var(key, val),
                None => env::remove_var(key),
            }
        }
    }

    #[test]
    fn app_config_respects_env_overrides() {
        with_env(
            &[
                (LISTEN_ADDR_ENV, Some("127.0.0.1:1234")),
                (LOG_DIR_ENV, Some("/tmp/fdb")),
                (TRACE_LOG_FILE_ENV, Some("/tmp/tracing.log")),
                (LOG_POLL_INTERVAL_ENV, Some("5")),
            ],
            || {
                let config = AppConfig::from_env().expect("config should load with overrides");
                assert_eq!(config.listen_addr, "127.0.0.1:1234".parse().unwrap());
                assert_eq!(config.log_dir, PathBuf::from("/tmp/fdb"));
                assert_eq!(config.trace_log_file, PathBuf::from("/tmp/tracing.log"));
                assert_eq!(config.log_poll_interval, Duration::from_secs_f64(5.0));
            },
        );
    }

    #[test]
    fn app_config_uses_defaults_when_env_missing() {
        with_env(
            &[
                (LISTEN_ADDR_ENV, None),
                (LOG_DIR_ENV, None),
                (TRACE_LOG_FILE_ENV, None),
                (LOG_POLL_INTERVAL_ENV, None),
            ],
            || {
                let config = AppConfig::from_env().expect("config should load with defaults");
                assert_eq!(config.listen_addr, DEFAULT_LISTEN_ADDR.parse().unwrap());
                assert_eq!(config.log_dir, PathBuf::from(DEFAULT_LOG_DIR));
                assert_eq!(config.trace_log_file, PathBuf::from(DEFAULT_TRACE_LOG_FILE));
                assert_eq!(
                    config.log_poll_interval,
                    Duration::from_secs_f64(DEFAULT_POLL_INTERVAL_SECS)
                );
            },
        );
    }

    #[test]
    fn parse_f64_env_rejects_non_numeric_values() {
        with_env(&[(LOG_POLL_INTERVAL_ENV, Some("not-a-number"))], || {
            let error = parse_f64_env(LOG_POLL_INTERVAL_ENV, 1.0)
                .expect_err("invalid float strings should fail to parse");
            assert!(
                error
                    .to_string()
                    .contains("expected to be a floating point number"),
                "unexpected error message: {error}"
            );
        });
    }

    #[cfg(unix)]
    #[test]
    fn parse_f64_env_rejects_non_utf8_values() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;

        let _guard = env_guard();

        let previous = env::var_os(LOG_POLL_INTERVAL_ENV);
        let invalid = OsString::from_vec(vec![0xff, 0xfe, 0xfd]);
        env::set_var(LOG_POLL_INTERVAL_ENV, &invalid);

        let error = parse_f64_env(LOG_POLL_INTERVAL_ENV, 1.0)
            .expect_err("non-UTF8 env values should fail");
        assert!(
            error
                .to_string()
                .contains("must be valid UTF-8"),
            "unexpected error message: {error}"
        );

        match previous {
            Some(value) => env::set_var(LOG_POLL_INTERVAL_ENV, value),
            None => env::remove_var(LOG_POLL_INTERVAL_ENV),
        }
    }
}
