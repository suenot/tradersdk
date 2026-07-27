//! Ticker and price data models.

use serde::{Deserialize, Serialize};

/// Ticker data with Open Interest and Funding Rate.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub struct TickerInfo {
    /// Trading symbol.
    pub symbol: String,
    /// Last traded price.
    pub last_price: f64,
    /// Best bid price.
    pub bid_price: Option<f64>,
    /// Best ask price.
    pub ask_price: Option<f64>,
    /// 24h high price.
    pub high_24h: Option<f64>,
    /// 24h low price.
    pub low_24h: Option<f64>,
    /// 24h volume in base currency.
    pub volume_24h: Option<f64>,
    /// 24h turnover in quote currency.
    pub turnover_24h: Option<f64>,
    /// 24h price change percentage.
    pub change_24h: Option<f64>,
    /// Open Interest in contracts/coins.
    pub open_interest: Option<f64>,
    /// Open Interest value in USD.
    pub open_interest_value: Option<f64>,
    /// Current funding rate (for perpetual futures).
    pub funding_rate: Option<f64>,
    /// Next funding time (Unix timestamp ms).
    pub next_funding_time: Option<i64>,
    /// Mark price.
    pub mark_price: Option<f64>,
    /// Index price.
    pub index_price: Option<f64>,
    /// Data timestamp.
    pub timestamp: i64,
}

impl TickerInfo {
    /// Create a simple ticker with just price.
    pub fn simple(symbol: &str, price: f64, timestamp: i64) -> Self {
        Self {
            symbol: symbol.to_string(),
            last_price: price,
            bid_price: None,
            ask_price: None,
            high_24h: None,
            low_24h: None,
            volume_24h: None,
            turnover_24h: None,
            change_24h: None,
            open_interest: None,
            open_interest_value: None,
            funding_rate: None,
            next_funding_time: None,
            mark_price: None,
            index_price: None,
            timestamp,
        }
    }

    /// Get mid price (average of bid and ask).
    pub fn mid_price(&self) -> Option<f64> {
        match (self.bid_price, self.ask_price) {
            (Some(bid), Some(ask)) => Some((bid + ask) / 2.0),
            _ => None,
        }
    }

    /// Get spread (ask - bid).
    pub fn spread(&self) -> Option<f64> {
        match (self.bid_price, self.ask_price) {
            (Some(bid), Some(ask)) => Some(ask - bid),
            _ => None,
        }
    }

    /// Get spread percentage.
    pub fn spread_percent(&self) -> Option<f64> {
        match (self.spread(), self.mid_price()) {
            (Some(spread), Some(mid)) if mid > 0.0 => Some((spread / mid) * 100.0),
            _ => None,
        }
    }
}

impl Default for TickerInfo {
    fn default() -> Self {
        Self {
            symbol: String::new(),
            last_price: 0.0,
            bid_price: None,
            ask_price: None,
            high_24h: None,
            low_24h: None,
            volume_24h: None,
            turnover_24h: None,
            change_24h: None,
            open_interest: None,
            open_interest_value: None,
            funding_rate: None,
            next_funding_time: None,
            mark_price: None,
            index_price: None,
            timestamp: 0,
        }
    }
}

/// Funding rate history entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub struct FundingRateEntry {
    /// Trading symbol.
    pub symbol: String,
    /// Funding rate.
    pub funding_rate: f64,
    /// Funding time (timestamp ms).
    pub funding_time: i64,
}

/// Open Interest history entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub struct OpenInterestEntry {
    /// Trading symbol.
    pub symbol: String,
    /// Open interest amount.
    pub open_interest: f64,
    /// Open interest value in USD.
    pub open_interest_value: f64,
    /// Timestamp.
    pub timestamp: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ticker_spread() {
        let ticker = TickerInfo {
            symbol: "BTCUSDT".to_string(),
            last_price: 50000.0,
            bid_price: Some(49990.0),
            ask_price: Some(50010.0),
            ..Default::default()
        };

        assert_eq!(ticker.spread(), Some(20.0));
        assert_eq!(ticker.mid_price(), Some(50000.0));
    }
}
