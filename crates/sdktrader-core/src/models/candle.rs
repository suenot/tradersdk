//! OHLCV candlestick data.

use serde::{Deserialize, Serialize};

/// OHLCV candlestick data.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub struct Candle {
    /// Candle open timestamp (ms).
    pub timestamp: i64,
    /// Open price.
    pub open: f64,
    /// High price.
    pub high: f64,
    /// Low price.
    pub low: f64,
    /// Close price.
    pub close: f64,
    /// Volume in base currency.
    pub volume: f64,
    /// Turnover in quote currency (optional).
    pub turnover: Option<f64>,
}

impl Candle {
    /// Create a new candle.
    pub fn new(
        timestamp: i64,
        open: f64,
        high: f64,
        low: f64,
        close: f64,
        volume: f64,
    ) -> Self {
        Self {
            timestamp,
            open,
            high,
            low,
            close,
            volume,
            turnover: None,
        }
    }

    /// Check if candle is bullish (close > open).
    pub fn is_bullish(&self) -> bool {
        self.close > self.open
    }

    /// Check if candle is bearish (close < open).
    pub fn is_bearish(&self) -> bool {
        self.close < self.open
    }

    /// Check if candle is doji (close ~= open).
    pub fn is_doji(&self, threshold: f64) -> bool {
        let body = (self.close - self.open).abs();
        let range = self.high - self.low;
        if range == 0.0 {
            return true;
        }
        body / range < threshold
    }

    /// Get candle body size.
    pub fn body(&self) -> f64 {
        (self.close - self.open).abs()
    }

    /// Get candle range (high - low).
    pub fn range(&self) -> f64 {
        self.high - self.low
    }

    /// Get upper shadow size.
    pub fn upper_shadow(&self) -> f64 {
        self.high - self.open.max(self.close)
    }

    /// Get lower shadow size.
    pub fn lower_shadow(&self) -> f64 {
        self.open.min(self.close) - self.low
    }

    /// Get body percentage of range.
    pub fn body_percentage(&self) -> f64 {
        if self.range() == 0.0 {
            return 0.0;
        }
        (self.body() / self.range()) * 100.0
    }

    /// Get price change.
    pub fn change(&self) -> f64 {
        self.close - self.open
    }

    /// Get price change percentage.
    pub fn change_percent(&self) -> f64 {
        if self.open == 0.0 {
            return 0.0;
        }
        (self.change() / self.open) * 100.0
    }

    /// Get typical price (HLC/3).
    pub fn typical_price(&self) -> f64 {
        (self.high + self.low + self.close) / 3.0
    }

    /// Get VWAP estimate (typical price * volume).
    pub fn vwap(&self) -> f64 {
        self.typical_price() * self.volume
    }
}

impl Default for Candle {
    fn default() -> Self {
        Self {
            timestamp: 0,
            open: 0.0,
            high: 0.0,
            low: 0.0,
            close: 0.0,
            volume: 0.0,
            turnover: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_candle_bullish_bearish() {
        let bullish = Candle::new(0, 100.0, 110.0, 95.0, 105.0, 1000.0);
        let bearish = Candle::new(0, 105.0, 110.0, 95.0, 100.0, 1000.0);

        assert!(bullish.is_bullish());
        assert!(!bullish.is_bearish());
        assert!(!bearish.is_bullish());
        assert!(bearish.is_bearish());
    }

    #[test]
    fn test_candle_body_and_shadows() {
        let candle = Candle::new(0, 100.0, 120.0, 90.0, 110.0, 1000.0);

        assert_eq!(candle.body(), 10.0); // |110 - 100|
        assert_eq!(candle.range(), 30.0); // 120 - 90
        assert_eq!(candle.upper_shadow(), 10.0); // 120 - 110
        assert_eq!(candle.lower_shadow(), 10.0); // 100 - 90
    }

    #[test]
    fn test_candle_change_percent() {
        let candle = Candle::new(0, 100.0, 110.0, 95.0, 105.0, 1000.0);

        assert_eq!(candle.change(), 5.0);
        assert_eq!(candle.change_percent(), 5.0);
    }
}
