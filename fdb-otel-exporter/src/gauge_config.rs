use anyhow::{bail, Context, Result};
use serde::de::{self, Deserializer};
use serde::Deserialize;
use std::{collections::HashSet, fs, path::Path};
use toml::Value;

const GAUGE_CONFIG_SECTIONS: [&str; 5] = [
    "simple_gauge",
    "counter_total_gauge",
    "counter_rate_gauge",
    "elapsed_rate_gauge",
    "histogram_percentile_gauge",
];

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
#[serde(deny_unknown_fields)]
struct GaugeConfigEntry {
    trace_type: String,
    gauge_name: String,
    field_name: String,
    description: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct HistogramGaugeConfigEntry {
    group: String,
    op: String,
    #[serde(deserialize_with = "deserialize_percentiles")]
    percentiles: Vec<f64>,
    gauge_name: String,
    description: String,
}

fn validate_required_string(
    value: &str,
    field: &str,
    section: &str,
    index: usize,
    toml_config: &Path,
) -> Result<()> {
    if value.trim().is_empty() {
        bail!(
            "{} entry {} in {} has an empty required field `{}`",
            section,
            index,
            toml_config.display(),
            field
        );
    }

    Ok(())
}

fn validate_prometheus_metric_name(
    name: &str,
    section: &str,
    index: usize,
    toml_config: &Path,
) -> Result<()> {
    let mut characters = name.chars();
    let has_valid_first_character = characters
        .next()
        .is_some_and(|character| character.is_ascii_alphabetic() || matches!(character, '_' | ':'));
    let has_only_valid_characters = characters
        .all(|character| character.is_ascii_alphanumeric() || matches!(character, '_' | ':'));

    if !has_valid_first_character || !has_only_valid_characters {
        bail!(
            "{} entry {} in {} has invalid Prometheus metric name `{}`; expected [a-zA-Z_:][a-zA-Z0-9_:]*",
            section,
            index,
            toml_config.display(),
            name
        );
    }

    Ok(())
}

fn validate_standard_entry(
    entry: &GaugeConfigEntry,
    section: &str,
    index: usize,
    toml_config: &Path,
) -> Result<()> {
    validate_required_string(&entry.trace_type, "trace_type", section, index, toml_config)?;
    validate_required_string(&entry.gauge_name, "gauge_name", section, index, toml_config)?;
    validate_required_string(&entry.field_name, "field_name", section, index, toml_config)?;
    validate_required_string(
        &entry.description,
        "description",
        section,
        index,
        toml_config,
    )?;
    validate_prometheus_metric_name(&entry.gauge_name, section, index, toml_config)
}

fn validate_histogram_entry(
    entry: &HistogramGaugeConfigEntry,
    section: &str,
    index: usize,
    toml_config: &Path,
) -> Result<()> {
    validate_required_string(&entry.group, "group", section, index, toml_config)?;
    validate_required_string(&entry.op, "op", section, index, toml_config)?;
    validate_required_string(&entry.gauge_name, "gauge_name", section, index, toml_config)?;
    validate_required_string(
        &entry.description,
        "description",
        section,
        index,
        toml_config,
    )?;
    validate_prometheus_metric_name(&entry.gauge_name, section, index, toml_config)
}

fn register_gauge_name(
    gauge_names: &mut HashSet<String>,
    gauge_name: &str,
    toml_config: &Path,
) -> Result<()> {
    if !gauge_names.insert(gauge_name.to_owned()) {
        bail!(
            "duplicate generated gauge name `{}` in {} (check repeated definitions and histogram percentile suffix collisions)",
            gauge_name,
            toml_config.display()
        );
    }

    Ok(())
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

    if table.is_empty() {
        bail!(
            "gauge config file {} did not contain any recognized sections",
            toml_config.display()
        );
    }

    for section in table.keys() {
        if !GAUGE_CONFIG_SECTIONS.contains(&section.as_str()) {
            bail!(
                "unknown gauge config section `{}` in {}; expected one of: {}",
                section,
                toml_config.display(),
                GAUGE_CONFIG_SECTIONS.join(", ")
            );
        }
    }

    let mut gauges = Vec::new();
    let mut gauge_names = HashSet::new();

    for (section, entries) in table {
        match section.as_str() {
            "histogram_percentile_gauge" => {
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

                    validate_histogram_entry(&entry, section, index, toml_config)?;

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

                        register_gauge_name(&mut gauge_names, &gauge_name, toml_config)?;

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
                let gauge_type = GaugeType::from_section_name(section)
                    .expect("top-level gauge config sections were validated");

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

                    validate_standard_entry(&entry, section, index, toml_config)?;
                    register_gauge_name(&mut gauge_names, &entry.gauge_name, toml_config)?;

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

    Ok(gauges)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::de::value::Error as DeValueError;
    use tempfile::NamedTempFile;

    fn write_config(contents: &str) -> NamedTempFile {
        let file = NamedTempFile::new().expect("create temp config");
        std::fs::write(file.path(), contents.trim_start()).expect("write config");
        file
    }

    fn error_chain_contains(error: &anyhow::Error, needle: &str) -> bool {
        error
            .chain()
            .any(|cause| cause.to_string().contains(needle))
    }

    #[test]
    fn returns_empty_gauges_for_blank_file() {
        let file = write_config("");
        let gauges =
            read_gauge_config_file(file.path()).expect("blank config should return empty gauges");
        assert!(gauges.is_empty(), "expected empty gauge list");
    }

    #[test]
    fn surfaces_toml_parse_error_context() {
        let file = write_config("invalid = [");

        let error = read_gauge_config_file(file.path())
            .expect_err("invalid TOML should surface parse error");
        assert!(
            error_chain_contains(&error, "failed to parse gauge config file"),
            "missing parse context: {error}"
        );
    }

    #[test]
    fn errors_when_top_level_is_not_table() {
        let value = Value::Integer(42);
        let error = parse_typed_gauge_configs(&value, std::path::Path::new("inline.toml"))
            .expect_err("non-table root should error");
        assert!(
            error_chain_contains(
                &error,
                "expected gauge config file inline.toml to be a TOML table"
            ),
            "missing table context: {error}"
        );
    }

    #[test]
    fn errors_when_histogram_section_is_not_array() {
        let file = write_config(
            r#"
            [histogram_percentile_gauge]
            group = "StorageServer"
            op = "Read"
            percentiles = [0.5]
            gauge_name = "ss_read_latency_seconds"
            description = "Read latency"
            "#,
        );

        let error = read_gauge_config_file(file.path())
            .expect_err("non-array histogram section should error");
        assert!(
            error_chain_contains(
                &error,
                "expected histogram_percentile_gauge section to be an array"
            ),
            "missing histogram array context: {error}"
        );
    }

    #[test]
    fn errors_when_standard_section_is_not_array() {
        let file = write_config(
            r#"
            [simple_gauge]
            trace_type = "StorageMetrics"
            gauge_name = "ss_version"
            field_name = "Version"
            description = "Storage server version"
            "#,
        );

        let error = read_gauge_config_file(file.path())
            .expect_err("non-array standard section should error");
        assert!(
            error_chain_contains(&error, "expected simple_gauge section to be an array"),
            "missing standard array context: {error}"
        );
    }

    #[test]
    fn errors_when_standard_entry_is_missing_fields() {
        let file = write_config(
            r#"
            [[simple_gauge]]
            trace_type = "StorageMetrics"
            gauge_name = "ss_version"
            description = "Storage server version"
            "#,
        );

        let error = read_gauge_config_file(file.path()).expect_err("missing field should error");
        assert!(
            error_chain_contains(&error, "failed to parse simple_gauge entry 0"),
            "missing entry parse context: {error}"
        );
    }

    #[test]
    fn rejects_unknown_section_alongside_valid_section() {
        let file = write_config(
            r#"
            [[simple_gauge]]
            trace_type = "StorageMetrics"
            gauge_name = "ss_version"
            field_name = "Version"
            description = "Storage server version"

            [[typo_gauge]]
            trace_type = "StorageMetrics"
            gauge_name = "ignored_metric"
            field_name = "Ignored"
            description = "This must not be silently ignored"
            "#,
        );

        let error = read_gauge_config_file(file.path())
            .expect_err("unknown section should not be ignored when a valid section exists");
        assert!(
            error_chain_contains(&error, "unknown gauge config section `typo_gauge`"),
            "missing unknown section message: {error}"
        );
    }

    #[test]
    fn rejects_unknown_fields_in_each_entry_type() {
        let configs = [
            (
                "standard",
                r#"
                [[simple_gauge]]
                trace_type = "StorageMetrics"
                gauge_name = "ss_version"
                field_name = "Version"
                description = "Storage server version"
                unit = "version"
                "#,
                "unknown field `unit`",
            ),
            (
                "histogram",
                r#"
                [[histogram_percentile_gauge]]
                group = "StorageServer"
                op = "Read"
                percentiles = [0.5]
                gauge_name = "ss_read_latency_seconds"
                description = "Read latency"
                trace_type = "Histogram"
                "#,
                "unknown field `trace_type`",
            ),
        ];

        for (entry_type, config, expected_message) in configs {
            let file = write_config(config);
            let error = read_gauge_config_file(file.path()).unwrap_err();
            assert!(
                error_chain_contains(&error, expected_message),
                "missing unknown field message for {entry_type}: {error:?}"
            );
        }
    }

    #[test]
    fn rejects_whitespace_only_required_strings() {
        let configs = [
            (
                "trace_type",
                r#"
                [[simple_gauge]]
                trace_type = "   "
                gauge_name = "ss_version"
                field_name = "Version"
                description = "Storage server version"
                "#,
            ),
            (
                "op",
                r#"
                [[histogram_percentile_gauge]]
                group = "StorageServer"
                op = "\t"
                percentiles = [0.5]
                gauge_name = "ss_read_latency_seconds"
                description = "Read latency"
                "#,
            ),
            (
                "description",
                r#"
                [[counter_rate_gauge]]
                trace_type = "StorageMetrics"
                gauge_name = "ss_bytes_input_rate"
                field_name = "BytesInput"
                description = ""
                "#,
            ),
        ];

        for (field, config) in configs {
            let file = write_config(config);
            let error = read_gauge_config_file(file.path()).unwrap_err();
            assert!(
                error_chain_contains(&error, &format!("empty required field `{field}`")),
                "missing empty field message for {field}: {error:?}"
            );
        }
    }

    #[test]
    fn rejects_invalid_prometheus_metric_names() {
        for gauge_name in ["9starts_with_digit", "contains-dash", "contains space"] {
            let file = write_config(&format!(
                r#"
                [[simple_gauge]]
                trace_type = "StorageMetrics"
                gauge_name = "{gauge_name}"
                field_name = "Version"
                description = "Storage server version"
                "#
            ));

            let error = read_gauge_config_file(file.path()).unwrap_err();
            assert!(
                error_chain_contains(&error, "invalid Prometheus metric name"),
                "missing metric name validation error for `{gauge_name}`: {error:?}"
            );
        }
    }

    #[test]
    fn rejects_duplicate_gauge_names_across_sections() {
        let file = write_config(
            r#"
            [[simple_gauge]]
            trace_type = "StorageMetrics"
            gauge_name = "duplicate_metric"
            field_name = "Version"
            description = "Storage server version"

            [[counter_total_gauge]]
            trace_type = "StorageMetrics"
            gauge_name = "duplicate_metric"
            field_name = "BytesDurable"
            description = "Durable bytes"
            "#,
        );

        let error = read_gauge_config_file(file.path())
            .expect_err("duplicate generated gauge names should be rejected");
        assert!(
            error_chain_contains(&error, "duplicate generated gauge name `duplicate_metric`"),
            "missing duplicate gauge message: {error}"
        );
    }

    #[test]
    fn rejects_histogram_percentiles_with_colliding_suffixes() {
        let file = write_config(
            r#"
            [[histogram_percentile_gauge]]
            group = "StorageServer"
            op = "Read"
            percentiles = [0.5, 0.500000001]
            gauge_name = "ss_read_latency_seconds"
            description = "Read latency"
            "#,
        );

        let error = read_gauge_config_file(file.path())
            .expect_err("percentiles that generate the same suffix should be rejected");
        assert!(
            error_chain_contains(
                &error,
                "duplicate generated gauge name `ss_read_latency_seconds_p50`"
            ),
            "missing percentile collision message: {error}"
        );
    }

    #[test]
    fn rejects_empty_percentile_list() {
        let file = write_config(
            r#"
            [[histogram_percentile_gauge]]
            group = "StorageServer"
            op = "Read"
            percentiles = []
            gauge_name = "ss_read_latency_seconds"
            description = "Read latency"
            "#,
        );

        let error =
            read_gauge_config_file(file.path()).expect_err("empty percentile list should error");
        assert!(
            error_chain_contains(&error, "percentiles list cannot be empty"),
            "missing empty percentile message: {error}"
        );
    }

    #[test]
    fn validate_percentile_rejects_non_finite_values() {
        let error =
            validate_percentile::<DeValueError>(f64::NAN).expect_err("NaN percentile should error");
        assert_eq!(error.to_string(), "percentile must be finite");
    }

    #[test]
    fn expands_single_histogram_percentile_without_suffix() {
        let file = write_config(
            r#"
            [[histogram_percentile_gauge]]
            group = "StorageServer"
            op = "Read"
            percentiles = [0.9]
            gauge_name = "ss_read_latency_seconds"
            description = "Read latency"
            "#,
        );

        let gauges =
            read_gauge_config_file(file.path()).expect("single percentile histogram should parse");
        assert_eq!(gauges.len(), 1, "expected single histogram gauge");

        match &gauges[0] {
            GaugeDefinition::HistogramPercentile(def) => {
                assert_eq!(def.gauge_name, "ss_read_latency_seconds");
                assert_eq!(def.description, "Read latency");
                assert_eq!(def.percentile, 0.9);
            }
            other => panic!("expected histogram gauge, got {other:?}"),
        }
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
            error.to_string().contains("unknown gauge config section"),
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
