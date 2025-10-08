use anyhow::{Context, Result};
use serde::Deserialize;
use std::{fs, path::Path};
use toml::Value;

#[derive(Debug, Clone, Deserialize, Default)]
pub enum GaugeType {
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

#[derive(Debug, Clone, Deserialize)]
pub struct GaugeConfig {
    pub trace_type: String,
    pub gauge_name: String,
    pub field_name: String,
    #[serde(default)]
    pub gauge_type: GaugeType,
    pub description: String,
}

#[derive(Debug, Clone, Deserialize)]
struct GaugeConfigEntry {
    trace_type: String,
    gauge_name: String,
    field_name: String,
    description: String,
}

pub fn read_gauge_config_file(toml_config: &Path) -> Result<Vec<GaugeConfig>> {
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
        gauge: Vec<GaugeConfig>,
    }

    let gauges: Vec<GaugeConfig> = toml::from_str::<GaugeConfigWrapper>(&contents)
        .map(|wrapper| wrapper.gauge)
        .or_else(|_| toml::from_str::<Vec<GaugeConfig>>(&contents))
        .with_context(|| {
            format!(
                "failed to parse gauge config file {}",
                toml_config.display()
            )
        })?;

    Ok(gauges)
}

fn parse_typed_gauge_configs(
    value: &Value,
    toml_config: &Path,
) -> Result<Option<Vec<GaugeConfig>>> {
    let Some(table) = value.as_table() else {
        return Ok(None);
    };

    let mut gauges = Vec::new();
    let mut recognized_any = false;

    for (section, entries) in table {
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

            gauges.push(GaugeConfig {
                trace_type: entry.trace_type,
                gauge_name: entry.gauge_name,
                field_name: entry.field_name,
                gauge_type: gauge_type.clone(),
                description: entry.description,
            });
        }
    }

    if recognized_any {
        Ok(Some(gauges))
    } else {
        Ok(None)
    }
}
