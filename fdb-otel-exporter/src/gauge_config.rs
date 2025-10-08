use anyhow::{bail, Context, Result};
use serde::de::{self, Deserializer};
use serde::Deserialize;
use std::{fs, path::Path};
use toml::Value;

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
        .map(|value| validate_percentile::<D::Error>(value))
        .collect()
}

fn validate_percentile<E: de::Error>(value: f64) -> Result<f64, E> {
    if !value.is_finite() {
        return Err(de::Error::custom("percentile must be finite"));
    }

    if !(0.0..=1.0).contains(&value) {
        return Err(de::Error::custom(format!(
            "percentile {} must be between 0.0 and 1.0",
            value
        )));
    }

    Ok(value)
}

fn percentile_suffix(percentile: f64) -> String {
    let display = percentile_display(percentile).replace('.', "_");
    format!("p{}", display)
}

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

                    for (idx, percentile) in percentiles.into_iter().enumerate() {
                        let gauge_name = if total == 1 || idx == 0 {
                            base_gauge_name.clone()
                        } else {
                            format!("{}_{}", base_gauge_name, percentile_suffix(percentile))
                        };

                        let description = if total == 1 || idx == 0 {
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
