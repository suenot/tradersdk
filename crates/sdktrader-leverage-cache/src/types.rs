use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Structure of the leverage cache JSON file
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LeverageCacheFile {
    #[serde(default)]
    pub exchanges: HashMap<String, ExchangeLeverageData>,
    #[serde(default)]
    pub set_leverages: HashMap<String, HashMap<String, SetLeverageEntry>>,
    pub updated_at: Option<String>,
}

/// Leverage data for a single exchange
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ExchangeLeverageData {
    #[serde(default)]
    pub symbols: HashMap<String, SymbolLeverageData>,
}

/// Leverage data for a single symbol
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymbolLeverageData {
    pub max_leverage: Option<u32>,
}

/// Entry recording a leverage that was actually set on exchange
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetLeverageEntry {
    pub leverage: Option<u32>,
}

/// Cache info for status display
#[derive(Debug, Clone, Serialize)]
pub struct CacheInfo {
    pub cache_file: String,
    pub exchanges: Vec<String>,
    pub total_symbols: usize,
    pub updated_at: Option<String>,
    pub last_refresh: Option<String>,
    pub refresh_interval_seconds: f64,
    pub default_leverage: u32,
}
