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
pub const GAUGE_CONFIG_PATH_ENV: &str = "GAUGE_CONFIG_PATH";
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
    pub gauge_config_path: Option<PathBuf>,
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

        let log_poll_interval =
            parse_duration_env(LOG_POLL_INTERVAL_ENV, DEFAULT_POLL_INTERVAL_SECS)?;

        let gauge_config_path = env::var_os(GAUGE_CONFIG_PATH_ENV)
            .map(PathBuf::from)
            .filter(|path| !path.as_os_str().is_empty());

        Ok(Self {
            listen_addr,
            log_dir,
            trace_log_file,
            log_poll_interval,
            gauge_config_path,
        })
    }
}

fn parse_duration_env(key: &str, default_secs: f64) -> Result<Duration> {
    let seconds = parse_f64_env(key, default_secs)?;

    if !seconds.is_finite() {
        return Err(anyhow!(
            "environment variable {key} must be a finite, positive number of seconds, got {seconds}"
        ));
    }

    if seconds <= 0.0 {
        return Err(anyhow!(
            "environment variable {key} must be greater than zero seconds, got {seconds}"
        ));
    }

    let duration = Duration::try_from_secs_f64(seconds).with_context(|| {
        format!(
            "environment variable {key} must be representable as a duration, got {seconds} seconds"
        )
    })?;

    if duration.is_zero() {
        return Err(anyhow!(
            "environment variable {key} must be at least one nanosecond, got {seconds} seconds"
        ));
    }

    Ok(duration)
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
    use std::{
        ffi::OsString,
        sync::{Mutex, OnceLock},
    };

    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    fn env_guard() -> std::sync::MutexGuard<'static, ()> {
        ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    struct EnvRestore(Vec<(String, Option<OsString>)>);

    impl EnvRestore {
        fn capture(keys: impl IntoIterator<Item = impl Into<String>>) -> Self {
            Self(
                keys.into_iter()
                    .map(|key| {
                        let key = key.into();
                        let value = env::var_os(&key);
                        (key, value)
                    })
                    .collect(),
            )
        }
    }

    impl Drop for EnvRestore {
        fn drop(&mut self) {
            for (key, value) in self.0.drain(..) {
                match value {
                    Some(value) => env::set_var(key, value),
                    None => env::remove_var(key),
                }
            }
        }
    }

    fn with_env(vars: &[(&str, Option<&str>)], f: impl FnOnce()) {
        let _guard = env_guard();
        let _restore = EnvRestore::capture(vars.iter().map(|(key, _)| *key));

        for (key, value) in vars {
            match value {
                Some(val) => env::set_var(key, val),
                None => env::remove_var(key),
            }
        }

        f();
    }

    #[test]
    fn app_config_respects_env_overrides() {
        with_env(
            &[
                (LISTEN_ADDR_ENV, Some("127.0.0.1:1234")),
                (LOG_DIR_ENV, Some("/tmp/fdb")),
                (TRACE_LOG_FILE_ENV, Some("/tmp/tracing.log")),
                (LOG_POLL_INTERVAL_ENV, Some("5")),
                (GAUGE_CONFIG_PATH_ENV, Some("/tmp/gauges.toml")),
            ],
            || {
                let config = AppConfig::from_env().expect("config should load with overrides");
                assert_eq!(config.listen_addr, "127.0.0.1:1234".parse().unwrap());
                assert_eq!(config.log_dir, PathBuf::from("/tmp/fdb"));
                assert_eq!(config.trace_log_file, PathBuf::from("/tmp/tracing.log"));
                assert_eq!(config.log_poll_interval, Duration::from_secs_f64(5.0));
                assert_eq!(
                    config.gauge_config_path,
                    Some(PathBuf::from("/tmp/gauges.toml"))
                );
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
                (GAUGE_CONFIG_PATH_ENV, None),
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
                assert_eq!(config.gauge_config_path, None);
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

    #[test]
    fn parse_duration_env_rejects_non_positive_values() {
        for value in ["0", "-0", "-1"] {
            with_env(&[(LOG_POLL_INTERVAL_ENV, Some(value))], || {
                let error = parse_duration_env(LOG_POLL_INTERVAL_ENV, 1.0)
                    .expect_err("non-positive poll intervals should be rejected");
                assert!(
                    error.to_string().contains("must be greater than zero"),
                    "unexpected error for {value}: {error}"
                );
            });
        }
    }

    #[test]
    fn parse_duration_env_rejects_non_finite_values() {
        for value in ["NaN", "inf", "-inf"] {
            with_env(&[(LOG_POLL_INTERVAL_ENV, Some(value))], || {
                let error = parse_duration_env(LOG_POLL_INTERVAL_ENV, 1.0)
                    .expect_err("non-finite poll intervals should be rejected");
                assert!(
                    error.to_string().contains("must be a finite"),
                    "unexpected error for {value}: {error}"
                );
            });
        }
    }

    #[test]
    fn parse_duration_env_rejects_overflow() {
        with_env(&[(LOG_POLL_INTERVAL_ENV, Some("1e300"))], || {
            let error = parse_duration_env(LOG_POLL_INTERVAL_ENV, 1.0)
                .expect_err("durations larger than Duration::MAX should be rejected");
            assert!(
                error.to_string().contains("must be representable"),
                "unexpected error: {error}"
            );
        });
    }

    #[test]
    fn parse_duration_env_rejects_values_that_round_to_zero() {
        with_env(&[(LOG_POLL_INTERVAL_ENV, Some("1e-20"))], || {
            let error = parse_duration_env(LOG_POLL_INTERVAL_ENV, 1.0)
                .expect_err("sub-nanosecond intervals that round to zero should be rejected");
            assert!(
                error.to_string().contains("at least one nanosecond"),
                "unexpected error: {error}"
            );
        });
    }

    #[test]
    fn parse_duration_env_accepts_one_nanosecond() {
        with_env(&[(LOG_POLL_INTERVAL_ENV, Some("0.000000001"))], || {
            let duration = parse_duration_env(LOG_POLL_INTERVAL_ENV, 1.0)
                .expect("one nanosecond is a valid poll interval");
            assert_eq!(duration, Duration::from_nanos(1));
        });
    }

    #[cfg(unix)]
    #[test]
    fn parse_f64_env_rejects_non_utf8_values() {
        use std::os::unix::ffi::OsStringExt;

        let _guard = env_guard();
        let _restore = EnvRestore::capture([LOG_POLL_INTERVAL_ENV]);
        let invalid = OsString::from_vec(vec![0xff, 0xfe, 0xfd]);
        env::set_var(LOG_POLL_INTERVAL_ENV, &invalid);

        let error =
            parse_f64_env(LOG_POLL_INTERVAL_ENV, 1.0).expect_err("non-UTF8 env values should fail");
        assert!(
            error.to_string().contains("must be valid UTF-8"),
            "unexpected error message: {error}"
        );
    }
}
