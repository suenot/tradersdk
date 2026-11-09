//! Bot configuration loading.

use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

/// Bot configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BotConfig {
    pub name: String,
    pub exchange: String,
    #[serde(default = "default_market_type")]
    pub market_type: String,
    #[serde(default)]
    pub testnet: bool,
    #[serde(default)]
    pub api_key: String,
    #[serde(default)]
    pub api_secret: String,
    #[serde(default)]
    pub api_passphrase: String,
    #[serde(default)]
    pub enabled: bool,
}

fn default_market_type() -> String {
    "swap".to_string()
}

/// Root configuration.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Config {
    #[serde(default)]
    pub bots: Vec<BotConfig>,
}

impl Config {
    /// Load configuration from a JSON file.
    pub fn load<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let content = fs::read_to_string(path)?;
        let config: Config = serde_json::from_str(&content)?;
        Ok(config)
    }

    /// Get a bot by name.
    pub fn get_bot(&self, name: &str) -> Option<&BotConfig> {
        self.bots.iter().find(|b| b.name == name)
    }
}
