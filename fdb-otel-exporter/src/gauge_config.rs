use anyhow::{Context, Result};
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
    percentile: f64,
    gauge_name: String,
    description: String,
}

#[derive(Debug, Clone, Deserialize)]
struct LegacyGaugeConfig {
    trace_type: String,
    gauge_name: String,
    field_name: String,
    #[serde(default)]
    gauge_type: GaugeType,
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

    if let Some(gauges) = parse_typed_gauge_configs(&parsed_value, toml_config)? {
        return Ok(gauges);
    }

    #[derive(Deserialize)]
    struct GaugeConfigWrapper {
        gauge: Vec<LegacyGaugeConfig>,
    }

    let gauges: Vec<LegacyGaugeConfig> = toml::from_str::<GaugeConfigWrapper>(&contents)
        .map(|wrapper| wrapper.gauge)
        .or_else(|_| toml::from_str::<Vec<LegacyGaugeConfig>>(&contents))
        .with_context(|| {
            format!(
                "failed to parse gauge config file {}",
                toml_config.display()
            )
        })?;

    gauges
        .into_iter()
        .map(|config| config.into_definition())
        .collect()
}

fn parse_typed_gauge_configs(
    value: &Value,
    toml_config: &Path,
) -> Result<Option<Vec<GaugeDefinition>>> {
    let Some(table) = value.as_table() else {
        return Ok(None);
    };

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

                for (index, entry) in array.iter().enumerate() {
                    let entry: HistogramGaugeConfigEntry =
                        entry.clone().try_into().with_context(|| {
                            format!(
                                "failed to parse {} entry {} in {}",
                                section,
                                index,
                                toml_config.display()
                            )
                        })?;

                    gauges.push(GaugeDefinition::HistogramPercentile(
                        HistogramPercentileGaugeDefinition {
                            group: entry.group,
                            op: entry.op,
                            percentile: entry.percentile,
                            gauge_name: entry.gauge_name,
                            description: entry.description,
                        },
                    ));
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
        Ok(Some(gauges))
    } else {
        Ok(None)
    }
}

impl LegacyGaugeConfig {
    fn into_definition(self) -> Result<GaugeDefinition> {
        let standard = StandardGaugeDefinition {
            trace_type: self.trace_type,
            gauge_name: self.gauge_name,
            field_name: self.field_name,
            description: self.description,
        };

        Ok(match self.gauge_type {
            GaugeType::Simple => GaugeDefinition::Simple(standard),
            GaugeType::CounterTotal => GaugeDefinition::CounterTotal(standard),
            GaugeType::CounterRate => GaugeDefinition::CounterRate(standard),
            GaugeType::ElapsedRate => GaugeDefinition::ElapsedRate(standard),
        })
    }
}
