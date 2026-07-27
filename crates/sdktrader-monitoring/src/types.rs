use serde::{Deserialize, Serialize};

/// Type of change detected between two market snapshots.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MarketChangeType {
    /// Market is present in the new snapshot but absent from the previous one.
    Added,
    /// Market is present in the previous snapshot but absent from the new one.
    Removed,
}

impl std::fmt::Display for MarketChangeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MarketChangeType::Added => write!(f, "added"),
            MarketChangeType::Removed => write!(f, "removed"),
        }
    }
}

/// A detected change in an exchange's set of tradable markets.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketChange {
    pub change_type: MarketChangeType,
    /// Market identifier as reported by the exchange, e.g. `"BTC-USDT"`.
    pub market: String,
    /// Human-readable asset name, when the exchange provides one.
    pub display_name: String,
    pub timestamp_ms: i64,
}

/// A tradable market as reported by an exchange.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketInfo {
    /// Market identifier as reported by the exchange, e.g. `"BTC-USDT"`.
    pub market: String,
    /// Human-readable asset name, when the exchange provides one.
    pub display_name: String,
}

impl MarketInfo {
    pub fn new(market: impl Into<String>, display_name: impl Into<String>) -> Self {
        Self {
            market: market.into(),
            display_name: display_name.into(),
        }
    }
}
