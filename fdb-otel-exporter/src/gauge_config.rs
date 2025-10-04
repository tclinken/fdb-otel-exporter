use anyhow::{Context, Result};
use serde::Deserialize;
use std::{fs, path::Path};

#[derive(Debug, Clone, Deserialize, Default)]
pub enum GaugeType {
    #[default]
    Simple,
    CounterTotal,
    CounterRate,
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

pub fn read_gauge_config_file(toml_config: &Path) -> Result<Vec<GaugeConfig>> {
    let contents = fs::read_to_string(toml_config)
        .with_context(|| format!("failed to read gauge config file {}", toml_config.display()))?;

    if contents.trim().is_empty() {
        return Ok(Vec::new());
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
