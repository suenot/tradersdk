//! Orderbook data models.

use serde::{Deserialize, Serialize};

/// Orderbook entry (price, quantity).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub struct OrderbookLevel {
    /// Price level.
    pub price: f64,
    /// Quantity at this level.
    pub quantity: f64,
}

impl OrderbookLevel {
    /// Create a new orderbook level.
    pub fn new(price: f64, quantity: f64) -> Self {
        Self { price, quantity }
    }

    /// Get total value at this level.
    pub fn value(&self) -> f64 {
        self.price * self.quantity
    }
}

/// Orderbook snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub struct Orderbook {
    /// Trading symbol.
    pub symbol: String,
    /// Bid levels (sorted by price descending).
    pub bids: Vec<OrderbookLevel>,
    /// Ask levels (sorted by price ascending).
    pub asks: Vec<OrderbookLevel>,
    /// Snapshot timestamp.
    pub timestamp: i64,
}

impl Orderbook {
    /// Create a new orderbook.
    pub fn new(symbol: &str, timestamp: i64) -> Self {
        Self {
            symbol: symbol.to_string(),
            bids: vec![],
            asks: vec![],
            timestamp,
        }
    }

    /// Get best bid price.
    pub fn best_bid(&self) -> Option<f64> {
        self.bids.first().map(|l| l.price)
    }

    /// Get best ask price.
    pub fn best_ask(&self) -> Option<f64> {
        self.asks.first().map(|l| l.price)
    }

    /// Get mid price.
    pub fn mid_price(&self) -> Option<f64> {
        match (self.best_bid(), self.best_ask()) {
            (Some(bid), Some(ask)) => Some((bid + ask) / 2.0),
            _ => None,
        }
    }

    /// Get spread.
    pub fn spread(&self) -> Option<f64> {
        match (self.best_bid(), self.best_ask()) {
            (Some(bid), Some(ask)) => Some(ask - bid),
            _ => None,
        }
    }

    /// Get spread in basis points.
    pub fn spread_bps(&self) -> Option<f64> {
        match (self.spread(), self.mid_price()) {
            (Some(spread), Some(mid)) if mid > 0.0 => Some((spread / mid) * 10000.0),
            _ => None,
        }
    }

    /// Get total bid depth (sum of all bid quantities).
    pub fn bid_depth(&self) -> f64 {
        self.bids.iter().map(|l| l.quantity).sum()
    }

    /// Get total ask depth (sum of all ask quantities).
    pub fn ask_depth(&self) -> f64 {
        self.asks.iter().map(|l| l.quantity).sum()
    }

    /// Get bid depth up to a certain price level.
    pub fn bid_depth_to_price(&self, price: f64) -> f64 {
        self.bids
            .iter()
            .filter(|l| l.price >= price)
            .map(|l| l.quantity)
            .sum()
    }

    /// Get ask depth up to a certain price level.
    pub fn ask_depth_to_price(&self, price: f64) -> f64 {
        self.asks
            .iter()
            .filter(|l| l.price <= price)
            .map(|l| l.quantity)
            .sum()
    }

    /// Calculate average price for a market buy of given size.
    pub fn avg_buy_price(&self, quantity: f64) -> Option<f64> {
        let mut remaining = quantity;
        let mut total_cost = 0.0;

        for level in &self.asks {
            if remaining <= 0.0 {
                break;
            }
            let fill_qty = remaining.min(level.quantity);
            total_cost += fill_qty * level.price;
            remaining -= fill_qty;
        }

        if remaining > 0.0 {
            return None; // Not enough liquidity
        }

        Some(total_cost / quantity)
    }

    /// Calculate average price for a market sell of given size.
    pub fn avg_sell_price(&self, quantity: f64) -> Option<f64> {
        let mut remaining = quantity;
        let mut total_value = 0.0;

        for level in &self.bids {
            if remaining <= 0.0 {
                break;
            }
            let fill_qty = remaining.min(level.quantity);
            total_value += fill_qty * level.price;
            remaining -= fill_qty;
        }

        if remaining > 0.0 {
            return None; // Not enough liquidity
        }

        Some(total_value / quantity)
    }
}

impl Default for Orderbook {
    fn default() -> Self {
        Self {
            symbol: String::new(),
            bids: vec![],
            asks: vec![],
            timestamp: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_orderbook() -> Orderbook {
        Orderbook {
            symbol: "BTCUSDT".to_string(),
            bids: vec![
                OrderbookLevel::new(49990.0, 1.0),
                OrderbookLevel::new(49980.0, 2.0),
                OrderbookLevel::new(49970.0, 3.0),
            ],
            asks: vec![
                OrderbookLevel::new(50010.0, 1.0),
                OrderbookLevel::new(50020.0, 2.0),
                OrderbookLevel::new(50030.0, 3.0),
            ],
            timestamp: 1234567890,
        }
    }

    #[test]
    fn test_orderbook_spread() {
        let ob = create_test_orderbook();

        assert_eq!(ob.best_bid(), Some(49990.0));
        assert_eq!(ob.best_ask(), Some(50010.0));
        assert_eq!(ob.spread(), Some(20.0));
        assert_eq!(ob.mid_price(), Some(50000.0));
    }

    #[test]
    fn test_orderbook_depth() {
        let ob = create_test_orderbook();

        assert_eq!(ob.bid_depth(), 6.0);
        assert_eq!(ob.ask_depth(), 6.0);
    }

    #[test]
    fn test_avg_buy_price() {
        let ob = create_test_orderbook();

        // Buy 1 BTC - should get best ask
        assert_eq!(ob.avg_buy_price(1.0), Some(50010.0));

        // Buy 2 BTC - 1 at 50010 + 1 at 50020 = 100030 / 2 = 50015
        assert_eq!(ob.avg_buy_price(2.0), Some(50015.0));
    }
}
