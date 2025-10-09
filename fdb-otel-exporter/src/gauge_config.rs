use anyhow::{bail, Context, Result};
use serde::de::{self, Deserializer};
use serde::Deserialize;
use std::{fs, path::Path};
use toml::Value;

// Helper enum used to map TOML sections to concrete gauge constructors.
#[derive(Debug, Clone, Deserialize, Default)]
enum GaugeType {
    #[default]
    Simple,
    CounterTotal,
    CounterRate,
    ElapsedRate,
}

impl GaugeType {
    fn from_section_name(section: &str) -> Option<Self> {
        match section {
            "simple_gauge" => Some(Self::Simple),
            "counter_total_gauge" => Some(Self::CounterTotal),
            "counter_rate_gauge" => Some(Self::CounterRate),
            "elapsed_rate_gauge" => Some(Self::ElapsedRate),
            _ => None,
        }
    }
}

// Deserialize a list of percentile values and validate they fall within `[0, 1]`.
fn deserialize_percentiles<'de, D>(deserializer: D) -> Result<Vec<f64>, D::Error>
where
    D: Deserializer<'de>,
{
    let percentiles = Vec::<f64>::deserialize(deserializer)?;

    if percentiles.is_empty() {
        return Err(de::Error::custom("percentiles list cannot be empty"));
    }

    percentiles
        .into_iter()
        .map(validate_percentile::<D::Error>)
        .collect()
}

// Ensure a single percentile entry is finite and within the inclusive range `[0, 1]`.
fn validate_percentile<E: de::Error>(value: f64) -> Result<f64, E> {
    if !value.is_finite() {
        return Err(de::Error::custom("percentile must be finite"));
    }

    if !(0.0..=1.0).contains(&value) {
        return Err(de::Error::custom(format!(
            "percentile {value} must be between 0.0 and 1.0"
        )));
    }

    Ok(value)
}

// Produce a gauge name suffix such as `p95_5` from a percentile value.
fn percentile_suffix(percentile: f64) -> String {
    let display = percentile_display(percentile).replace('.', "_");
    format!("p{display}")
}

// Format a percentile as a percentage string while trimming trailing zeros.
fn percentile_display(percentile: f64) -> String {
    let mut value = format!("{:.6}", percentile * 100.0);

    while value.contains('.') && value.ends_with('0') {
        value.pop();
    }

    if value.ends_with('.') {
        value.pop();
    }

    value
}

#[derive(Debug, Clone)]
pub struct StandardGaugeDefinition {
    pub trace_type: String,
    pub gauge_name: String,
    pub field_name: String,
    pub description: String,
}

#[derive(Debug, Clone)]
pub struct HistogramPercentileGaugeDefinition {
    pub group: String,
    pub op: String,
    pub percentile: f64,
    pub gauge_name: String,
    pub description: String,
}

#[derive(Debug, Clone)]
pub enum GaugeDefinition {
    Simple(StandardGaugeDefinition),
    CounterTotal(StandardGaugeDefinition),
    CounterRate(StandardGaugeDefinition),
    ElapsedRate(StandardGaugeDefinition),
    HistogramPercentile(HistogramPercentileGaugeDefinition),
}

#[derive(Debug, Clone, Deserialize)]
struct GaugeConfigEntry {
    trace_type: String,
    gauge_name: String,
    field_name: String,
    description: String,
}

#[derive(Debug, Clone, Deserialize)]
struct HistogramGaugeConfigEntry {
    group: String,
    op: String,
    #[serde(deserialize_with = "deserialize_percentiles")]
    percentiles: Vec<f64>,
    gauge_name: String,
    description: String,
}

// Read `gauge_config.toml` from disk and return the normalized gauge definitions.
pub fn read_gauge_config_file(toml_config: &Path) -> Result<Vec<GaugeDefinition>> {
    let contents = fs::read_to_string(toml_config)
        .with_context(|| format!("failed to read gauge config file {}", toml_config.display()))?;

    if contents.trim().is_empty() {
        return Ok(Vec::new());
    }

    let parsed_value: Value = toml::from_str(&contents).with_context(|| {
        format!(
            "failed to parse gauge config file {}",
            toml_config.display()
        )
    })?;

    parse_typed_gauge_configs(&parsed_value, toml_config)
}

// Expand the parsed TOML value into strongly-typed gauge definitions.
fn parse_typed_gauge_configs(value: &Value, toml_config: &Path) -> Result<Vec<GaugeDefinition>> {
    let table = value.as_table().with_context(|| {
        format!(
            "expected gauge config file {} to be a TOML table",
            toml_config.display()
        )
    })?;

    let mut gauges = Vec::new();
    let mut recognized_any = false;

    for (section, entries) in table {
        match section.as_str() {
            "histogram_percentile_gauge" => {
                recognized_any = true;

                let array = entries.as_array().with_context(|| {
                    format!(
                        "expected {} section to be an array in {}",
                        section,
                        toml_config.display()
                    )
                })?;

                for (index, entry_value) in array.iter().enumerate() {
                    let entry: HistogramGaugeConfigEntry =
                        entry_value.clone().try_into().with_context(|| {
                            format!(
                                "failed to parse {} entry {} in {}",
                                section,
                                index,
                                toml_config.display()
                            )
                        })?;

                    let HistogramGaugeConfigEntry {
                        group,
                        op,
                        percentiles,
                        gauge_name,
                        description,
                    } = entry;

                    let total = percentiles.len();
                    let base_gauge_name = gauge_name.clone();
                    let base_description = description.clone();

                    for percentile in percentiles.into_iter() {
                        let gauge_name = if total == 1 {
                            base_gauge_name.clone()
                        } else {
                            format!("{}_{}", base_gauge_name, percentile_suffix(percentile))
                        };

                        let description = if total == 1 {
                            base_description.clone()
                        } else {
                            format!("{} (p{})", base_description, percentile_display(percentile))
                        };

                        gauges.push(GaugeDefinition::HistogramPercentile(
                            HistogramPercentileGaugeDefinition {
                                group: group.clone(),
                                op: op.clone(),
                                percentile,
                                gauge_name,
                                description,
                            },
                        ));
                    }
                }
            }
            _ => {
                let Some(gauge_type) = GaugeType::from_section_name(section) else {
                    continue;
                };

                recognized_any = true;

                let array = entries.as_array().with_context(|| {
                    format!(
                        "expected {} section to be an array in {}",
                        section,
                        toml_config.display()
                    )
                })?;

                for (index, entry) in array.iter().enumerate() {
                    let entry: GaugeConfigEntry = entry.clone().try_into().with_context(|| {
                        format!(
                            "failed to parse {} entry {} in {}",
                            section,
                            index,
                            toml_config.display()
                        )
                    })?;

                    let standard = StandardGaugeDefinition {
                        trace_type: entry.trace_type,
                        gauge_name: entry.gauge_name,
                        field_name: entry.field_name,
                        description: entry.description,
                    };

                    gauges.push(match gauge_type {
                        GaugeType::Simple => GaugeDefinition::Simple(standard),
                        GaugeType::CounterTotal => GaugeDefinition::CounterTotal(standard),
                        GaugeType::CounterRate => GaugeDefinition::CounterRate(standard),
                        GaugeType::ElapsedRate => GaugeDefinition::ElapsedRate(standard),
                    });
                }
            }
        }
    }

    if recognized_any {
        Ok(gauges)
    } else {
        bail!(
            "gauge config file {} did not contain any recognized sections",
            toml_config.display()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    fn write_config(contents: &str) -> NamedTempFile {
        let file = NamedTempFile::new().expect("create temp config");
        std::fs::write(file.path(), contents.trim_start()).expect("write config");
        file
    }

    #[test]
    fn parses_standard_gauges() {
        let file = write_config(
            r#"
            [[simple_gauge]]
            trace_type = "StorageMetrics"
            gauge_name = "ss_version"
            field_name = "Version"
            description = "Storage server version"

            [[counter_total_gauge]]
            trace_type = "StorageMetrics"
            gauge_name = "ss_bytes_durable"
            field_name = "BytesDurable"
            description = "Durable bytes"

            [[elapsed_rate_gauge]]
            trace_type = "ProxyMetrics"
            gauge_name = "cp_cpu_util"
            field_name = "CPUSeconds"
            description = "Commit proxy CPU utilization"
            "#,
        );

        let gauges =
            read_gauge_config_file(file.path()).expect("standard gauges should parse successfully");
        assert_eq!(gauges.len(), 3, "unexpected number of gauges");

        let simple = gauges
            .iter()
            .find_map(|g| match g {
                GaugeDefinition::Simple(def) => Some(def),
                _ => None,
            })
            .expect("expected simple gauge definition");
        assert_eq!(simple.trace_type, "StorageMetrics");
        assert_eq!(simple.gauge_name, "ss_version");
        assert_eq!(simple.field_name, "Version");
        assert_eq!(simple.description, "Storage server version");

        let counter_total = gauges
            .iter()
            .find_map(|g| match g {
                GaugeDefinition::CounterTotal(def) => Some(def),
                _ => None,
            })
            .expect("expected counter total gauge definition");
        assert_eq!(counter_total.trace_type, "StorageMetrics");
        assert_eq!(counter_total.gauge_name, "ss_bytes_durable");
        assert_eq!(counter_total.field_name, "BytesDurable");

        let elapsed_rate = gauges
            .iter()
            .find_map(|g| match g {
                GaugeDefinition::ElapsedRate(def) => Some(def),
                _ => None,
            })
            .expect("expected elapsed rate gauge definition");
        assert_eq!(elapsed_rate.trace_type, "ProxyMetrics");
        assert_eq!(elapsed_rate.field_name, "CPUSeconds");
    }

    #[test]
    fn expands_histogram_percentiles_with_suffixes() {
        let file = write_config(
            r#"
            [[histogram_percentile_gauge]]
            group = "StorageServer"
            op = "Read"
            percentiles = [0.5, 0.99]
            gauge_name = "ss_read_latency_seconds"
            description = "Read latency"
            "#,
        );

        let gauges = read_gauge_config_file(file.path())
            .expect("histogram gauges should parse successfully");

        assert_eq!(gauges.len(), 2, "expected gauges for two percentiles");

        match &gauges[0] {
            GaugeDefinition::HistogramPercentile(def) => {
                assert_eq!(def.group, "StorageServer");
                assert_eq!(def.op, "Read");
                assert_eq!(def.percentile, 0.5);
                assert_eq!(def.gauge_name, "ss_read_latency_seconds_p50");
                assert_eq!(def.description, "Read latency (p50)");
            }
            other => panic!("expected histogram gauge, got {other:?}"),
        }

        match &gauges[1] {
            GaugeDefinition::HistogramPercentile(def) => {
                assert_eq!(def.percentile, 0.99);
                assert_eq!(def.gauge_name, "ss_read_latency_seconds_p99");
            }
            other => panic!("expected histogram gauge, got {other:?}"),
        }
    }

    #[test]
    fn errors_when_no_recognized_sections() {
        let file = write_config(
            r#"
            [unrelated]
            value = 1
            "#,
        );

        let error =
            read_gauge_config_file(file.path()).expect_err("should error without known sections");
        assert!(
            error
                .to_string()
                .contains("did not contain any recognized sections"),
            "unexpected error message: {error}"
        );
    }

    #[test]
    fn rejects_invalid_percentiles() {
        let file = write_config(
            r#"
            [[histogram_percentile_gauge]]
            group = "StorageServer"
            op = "Read"
            percentiles = [1.5]
            gauge_name = "ss_read_latency_seconds"
            description = "Read latency"
            "#,
        );

        let error =
            read_gauge_config_file(file.path()).expect_err("should reject invalid percentile");
        let mut found = false;
        for cause in error.chain() {
            if cause.to_string().contains("between 0.0 and 1.0") {
                found = true;
                break;
            }
        }
        assert!(found, "unexpected error chain: {error:?}");
    }

    #[test]
    fn percentile_suffix_formats_values() {
        assert_eq!(percentile_suffix(0.5), "p50");
        assert_eq!(percentile_suffix(0.995), "p99_5");
        assert_eq!(percentile_suffix(0.000_123), "p0_0123");
    }
}
