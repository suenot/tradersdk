//! PnL (Profit and Loss) tracking.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A single trade record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeRecord {
    /// Trade ID.
    pub id: String,
    /// Trading symbol.
    pub symbol: String,
    /// Trade side (buy/sell).
    pub side: String,
    /// Trade quantity.
    pub quantity: Decimal,
    /// Entry price.
    pub entry_price: Decimal,
    /// Exit price (if closed).
    pub exit_price: Option<Decimal>,
    /// Entry timestamp (ms).
    pub entry_time: i64,
    /// Exit timestamp (ms, if closed).
    pub exit_time: Option<i64>,
    /// Realized PnL (if closed).
    pub realized_pnl: Option<Decimal>,
    /// Fees paid.
    pub fees: Decimal,
    /// Exchange name.
    pub exchange: String,
}

impl TradeRecord {
    /// Create a new trade record.
    pub fn new(
        id: &str,
        symbol: &str,
        side: &str,
        quantity: Decimal,
        entry_price: Decimal,
        exchange: &str,
    ) -> Self {
        Self {
            id: id.to_string(),
            symbol: symbol.to_string(),
            side: side.to_string(),
            quantity,
            entry_price,
            exit_price: None,
            entry_time: chrono::Utc::now().timestamp_millis(),
            exit_time: None,
            realized_pnl: None,
            fees: Decimal::ZERO,
            exchange: exchange.to_string(),
        }
    }

    /// Close the trade and calculate PnL.
    pub fn close(&mut self, exit_price: Decimal) {
        self.exit_price = Some(exit_price);
        self.exit_time = Some(chrono::Utc::now().timestamp_millis());

        let price_diff = exit_price - self.entry_price;
        let pnl = if self.side == "buy" || self.side == "long" {
            price_diff * self.quantity
        } else {
            -price_diff * self.quantity
        };

        self.realized_pnl = Some(pnl - self.fees);
    }

    /// Check if trade is closed.
    pub fn is_closed(&self) -> bool {
        self.exit_price.is_some()
    }

    /// Get trade duration in milliseconds.
    pub fn duration_ms(&self) -> Option<i64> {
        self.exit_time.map(|exit| exit - self.entry_time)
    }
}

/// PnL summary for a symbol or period.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PnLSummary {
    /// Total number of trades.
    pub total_trades: u32,
    /// Number of winning trades.
    pub winning_trades: u32,
    /// Number of losing trades.
    pub losing_trades: u32,
    /// Total realized PnL.
    pub total_pnl: Decimal,
    /// Total fees paid.
    pub total_fees: Decimal,
    /// Net PnL (after fees).
    pub net_pnl: Decimal,
    /// Win rate percentage.
    pub win_rate: f64,
    /// Average win amount.
    pub avg_win: Decimal,
    /// Average loss amount.
    pub avg_loss: Decimal,
    /// Largest win.
    pub largest_win: Decimal,
    /// Largest loss.
    pub largest_loss: Decimal,
    /// Profit factor (gross profit / gross loss).
    pub profit_factor: f64,
}

/// PnL tracker for trading performance analysis.
#[derive(Default)]
pub struct PnLTracker {
    trades: Vec<TradeRecord>,
    summaries: HashMap<String, PnLSummary>,
}

impl PnLTracker {
    /// Create a new PnL tracker.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a trade record.
    pub fn add_trade(&mut self, trade: TradeRecord) {
        let symbol = trade.symbol.clone();
        self.trades.push(trade);
        self.update_summary(&symbol);
    }

    /// Get all trades.
    pub fn trades(&self) -> &[TradeRecord] {
        &self.trades
    }

    /// Get trades for a specific symbol.
    pub fn trades_for_symbol(&self, symbol: &str) -> Vec<&TradeRecord> {
        self.trades.iter().filter(|t| t.symbol == symbol).collect()
    }

    /// Get PnL summary for a symbol.
    pub fn get_summary(&self, symbol: &str) -> Option<&PnLSummary> {
        self.summaries.get(symbol)
    }

    /// Get overall PnL summary.
    pub fn get_total_summary(&self) -> PnLSummary {
        let mut summary = PnLSummary::default();

        for trade in &self.trades {
            if !trade.is_closed() {
                continue;
            }

            summary.total_trades += 1;
            summary.total_fees += trade.fees;

            if let Some(pnl) = trade.realized_pnl {
                summary.total_pnl += pnl;

                if pnl > Decimal::ZERO {
                    summary.winning_trades += 1;
                    if pnl > summary.largest_win {
                        summary.largest_win = pnl;
                    }
                } else if pnl < Decimal::ZERO {
                    summary.losing_trades += 1;
                    if pnl < summary.largest_loss {
                        summary.largest_loss = pnl;
                    }
                }
            }
        }

        summary.net_pnl = summary.total_pnl - summary.total_fees;

        if summary.total_trades > 0 {
            summary.win_rate =
                (summary.winning_trades as f64 / summary.total_trades as f64) * 100.0;

            if summary.winning_trades > 0 {
                // Calculate average win from winning trades
                let total_wins: Decimal = self
                    .trades
                    .iter()
                    .filter_map(|t| t.realized_pnl)
                    .filter(|p| *p > Decimal::ZERO)
                    .sum();
                summary.avg_win = total_wins / Decimal::from(summary.winning_trades);
            }

            if summary.losing_trades > 0 {
                // Calculate average loss from losing trades
                let total_losses: Decimal = self
                    .trades
                    .iter()
                    .filter_map(|t| t.realized_pnl)
                    .filter(|p| *p < Decimal::ZERO)
                    .sum();
                summary.avg_loss = total_losses / Decimal::from(summary.losing_trades);
            }

            // Profit factor
            let gross_profit: Decimal = self
                .trades
                .iter()
                .filter_map(|t| t.realized_pnl)
                .filter(|p| *p > Decimal::ZERO)
                .sum();
            let gross_loss: Decimal = self
                .trades
                .iter()
                .filter_map(|t| t.realized_pnl)
                .filter(|p| *p < Decimal::ZERO)
                .map(|p| p.abs())
                .sum();

            if gross_loss > Decimal::ZERO {
                use rust_decimal::prelude::ToPrimitive;
                summary.profit_factor = gross_profit.to_f64().unwrap_or(0.0)
                    / gross_loss.to_f64().unwrap_or(1.0);
            }
        }

        summary
    }

    /// Update summary for a symbol.
    fn update_summary(&mut self, symbol: &str) {
        let trades: Vec<_> = self.trades.iter().filter(|t| t.symbol == symbol).collect();

        let mut summary = PnLSummary::default();

        for trade in trades {
            if !trade.is_closed() {
                continue;
            }

            summary.total_trades += 1;
            summary.total_fees += trade.fees;

            if let Some(pnl) = trade.realized_pnl {
                summary.total_pnl += pnl;

                if pnl > Decimal::ZERO {
                    summary.winning_trades += 1;
                    if pnl > summary.largest_win {
                        summary.largest_win = pnl;
                    }
                } else if pnl < Decimal::ZERO {
                    summary.losing_trades += 1;
                    if pnl < summary.largest_loss {
                        summary.largest_loss = pnl;
                    }
                }
            }
        }

        summary.net_pnl = summary.total_pnl - summary.total_fees;

        if summary.total_trades > 0 {
            summary.win_rate =
                (summary.winning_trades as f64 / summary.total_trades as f64) * 100.0;
        }

        self.summaries.insert(symbol.to_string(), summary);
    }

    /// Clear all trades and summaries.
    pub fn clear(&mut self) {
        self.trades.clear();
        self.summaries.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trade_record_close() {
        let mut trade = TradeRecord::new(
            "1",
            "BTCUSDT",
            "buy",
            Decimal::from(1),
            Decimal::from(50000),
            "binance",
        );

        trade.close(Decimal::from(51000));

        assert!(trade.is_closed());
        assert_eq!(trade.realized_pnl, Some(Decimal::from(1000)));
    }

    #[test]
    fn test_pnl_tracker() {
        let mut tracker = PnLTracker::new();

        let mut trade1 = TradeRecord::new(
            "1",
            "BTCUSDT",
            "buy",
            Decimal::from(1),
            Decimal::from(50000),
            "binance",
        );
        trade1.close(Decimal::from(51000)); // +1000

        let mut trade2 = TradeRecord::new(
            "2",
            "BTCUSDT",
            "buy",
            Decimal::from(1),
            Decimal::from(51000),
            "binance",
        );
        trade2.close(Decimal::from(50500)); // -500

        tracker.add_trade(trade1);
        tracker.add_trade(trade2);

        let summary = tracker.get_total_summary();
        assert_eq!(summary.total_trades, 2);
        assert_eq!(summary.winning_trades, 1);
        assert_eq!(summary.losing_trades, 1);
        assert_eq!(summary.total_pnl, Decimal::from(500));
    }
}
