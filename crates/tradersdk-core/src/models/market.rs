//! Market types and symbol information.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Market type for trading.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum MarketType {
    #[default]
    Spot,
    Swap,
}

impl From<&str> for MarketType {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "swap" | "futures" | "perp" | "perpetual" => Self::Swap,
            _ => Self::Spot,
        }
    }
}

impl std::fmt::Display for MarketType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MarketType::Spot => write!(f, "spot"),
            MarketType::Swap => write!(f, "swap"),
        }
    }
}

/// Candlestick interval.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub enum Interval {
    #[serde(rename = "1m")]
    M1,
    #[serde(rename = "5m")]
    M5,
    #[serde(rename = "15m")]
    M15,
    #[serde(rename = "30m")]
    M30,
    #[serde(rename = "1h")]
    H1,
    #[serde(rename = "4h")]
    H4,
    #[serde(rename = "1d")]
    D1,
    #[serde(rename = "1w")]
    W1,
}

impl Interval {
    /// Convert to exchange-specific string.
    pub fn to_string(&self) -> &'static str {
        match self {
            Interval::M1 => "1",
            Interval::M5 => "5",
            Interval::M15 => "15",
            Interval::M30 => "30",
            Interval::H1 => "60",
            Interval::H4 => "240",
            Interval::D1 => "D",
            Interval::W1 => "W",
        }
    }

    /// Get interval duration in seconds.
    pub fn to_seconds(&self) -> u64 {
        match self {
            Interval::M1 => 60,
            Interval::M5 => 300,
            Interval::M15 => 900,
            Interval::M30 => 1800,
            Interval::H1 => 3600,
            Interval::H4 => 14400,
            Interval::D1 => 86400,
            Interval::W1 => 604800,
        }
    }
}

/// Symbol trading information.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub struct SymbolInfo {
    /// Trading symbol (e.g., "BTCUSDT").
    pub symbol: String,
    /// Base asset (e.g., "BTC").
    pub base_asset: String,
    /// Quote asset (e.g., "USDT").
    pub quote_asset: String,
    /// Price decimal precision.
    pub price_precision: u8,
    /// Quantity decimal precision.
    pub quantity_precision: u8,
    /// Minimum order quantity.
    #[cfg_attr(feature = "json-schema", schemars(with = "String"))]
    pub min_quantity: Decimal,
    /// Maximum order quantity.
    #[cfg_attr(feature = "json-schema", schemars(with = "Option<String>"))]
    pub max_quantity: Option<Decimal>,
    /// Minimum notional value.
    #[cfg_attr(feature = "json-schema", schemars(with = "String"))]
    pub min_notional: Decimal,
    /// Price tick size.
    #[cfg_attr(feature = "json-schema", schemars(with = "String"))]
    pub tick_size: Decimal,
    /// Quantity step size.
    #[cfg_attr(feature = "json-schema", schemars(with = "String"))]
    pub step_size: Decimal,
    /// Maximum allowed leverage.
    pub max_leverage: u32,
}

impl Default for SymbolInfo {
    fn default() -> Self {
        Self {
            symbol: String::new(),
            base_asset: String::new(),
            quote_asset: String::new(),
            price_precision: 8,
            quantity_precision: 8,
            min_quantity: Decimal::ZERO,
            max_quantity: None,
            min_notional: Decimal::ZERO,
            tick_size: Decimal::new(1, 8), // 0.00000001
            step_size: Decimal::new(1, 8),
            max_leverage: 125,
        }
    }
}

/// Exchange information.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub struct ExchangeInfo {
    /// Exchange name.
    pub name: String,
    /// Available symbols.
    pub symbols: Vec<SymbolInfo>,
}

impl ExchangeInfo {
    /// Find symbol info by symbol name.
    pub fn get_symbol(&self, symbol: &str) -> Option<&SymbolInfo> {
        self.symbols.iter().find(|s| s.symbol == symbol)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_market_type_from_str() {
        assert_eq!(MarketType::from("spot"), MarketType::Spot);
        assert_eq!(MarketType::from("swap"), MarketType::Swap);
        assert_eq!(MarketType::from("futures"), MarketType::Swap);
        assert_eq!(MarketType::from("perp"), MarketType::Swap);
        assert_eq!(MarketType::from("perpetual"), MarketType::Swap);
    }

    #[test]
    fn test_interval_to_seconds() {
        assert_eq!(Interval::M1.to_seconds(), 60);
        assert_eq!(Interval::H1.to_seconds(), 3600);
        assert_eq!(Interval::D1.to_seconds(), 86400);
    }
}
