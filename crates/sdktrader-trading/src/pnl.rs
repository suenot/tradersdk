//! PnL (Profit and Loss) calculators.
//!
//! Ported from Python `trading/pnl_calculator.py` and `trading/position_exit_manager.py` FIFOCalculator.
//! Pure computation — no exchange API calls.

use sdktrader_core::models::PositionSide;
use serde::{Deserialize, Serialize};

/// Simple PnL calculation result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PnlResult {
    /// Absolute PnL in USDT.
    pub pnl_usdt: f64,
    /// PnL as percentage of entry value (without leverage).
    pub pnl_percent: f64,
    /// ROI percentage (PnL percent * leverage).
    pub roi_percent: f64,
    /// Total entry value (entry_price * quantity).
    pub entry_value: f64,
    /// Total exit value (exit_price * quantity).
    pub exit_value: f64,
}

/// Fill information for FIFO matching.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FillInfo {
    /// Execution price.
    pub price: f64,
    /// Fill quantity in base currency.
    pub quantity: f64,
    /// Optional fill timestamp in milliseconds.
    pub timestamp_ms: Option<i64>,
    /// Optional order ID.
    pub order_id: Option<String>,
}

/// A single FIFO match between an entry fill and an exit fill.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FifoMatch {
    /// Entry price of this match.
    pub entry_price: f64,
    /// Exit price of this match.
    pub exit_price: f64,
    /// Matched quantity.
    pub quantity: f64,
    /// Entry value (entry_price * quantity).
    pub entry_value: f64,
    /// Exit value (exit_price * quantity).
    pub exit_value: f64,
    /// PnL in USDT for this match.
    pub pnl_usdt: f64,
    /// PnL as percentage of entry value.
    pub pnl_percent: f64,
}

/// Aggregate FIFO PnL result across all matches.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FifoPnlResult {
    /// Individual FIFO matches.
    pub matches: Vec<FifoMatch>,
    /// Total PnL in USDT.
    pub pnl_usdt: f64,
    /// Total PnL percentage (of total entry value).
    pub pnl_percent: f64,
    /// ROI percentage (pnl_percent * leverage).
    pub roi_percent: f64,
    /// Sum of all entry values.
    pub total_entry_value: f64,
    /// Sum of all exit values.
    pub total_exit_value: f64,
    /// Number of entry fills provided.
    pub entry_count: usize,
    /// Number of exit fills provided.
    pub exit_count: usize,
    /// Number of FIFO matches produced.
    pub match_count: usize,
}

/// Calculate PnL for a single position.
///
/// # Arguments
/// * `entry_price` - Entry price per unit.
/// * `exit_price`  - Exit price per unit.
/// * `quantity`    - Position size in base currency.
/// * `direction`   - `"long"` or `"short"` (parsed via `PositionSide::from`).
/// * `leverage`    - Leverage multiplier.
///
/// # Returns
/// A [`PnlResult`] with absolute and percentage PnL.
pub fn calculate_pnl(
    entry_price: f64,
    exit_price: f64,
    quantity: f64,
    direction: &str,
    leverage: u32,
) -> PnlResult {
    let entry_value = entry_price * quantity;
    let exit_value = exit_price * quantity;

    let side = PositionSide::from(direction);
    let pnl_usdt = match side {
        PositionSide::Long => exit_value - entry_value,
        PositionSide::Short => entry_value - exit_value,
    };

    let pnl_percent = if entry_value > 0.0 {
        (pnl_usdt / entry_value) * 100.0
    } else {
        0.0
    };

    let roi_percent = pnl_percent * leverage as f64;

    PnlResult {
        pnl_usdt,
        pnl_percent,
        roi_percent,
        entry_value,
        exit_value,
    }
}

/// Calculate aggregate PnL for multiple entry orders closed at a single exit price.
///
/// Each element of `orders` is `(entry_price, quantity)`.
///
/// # Arguments
/// * `orders`     - Slice of `(entry_price, quantity)` tuples.
/// * `exit_price` - Common exit price.
/// * `direction`  - `"long"` or `"short"`.
/// * `leverage`   - Leverage multiplier.
pub fn calculate_aggregate_pnl(
    orders: &[(f64, f64)],
    exit_price: f64,
    direction: &str,
    leverage: u32,
) -> PnlResult {
    let mut total_pnl_usdt = 0.0;
    let mut total_entry_value = 0.0;
    let mut total_exit_value = 0.0;

    for &(entry_price, quantity) in orders {
        if entry_price == 0.0 || quantity == 0.0 {
            continue;
        }
        let r = calculate_pnl(entry_price, exit_price, quantity, direction, leverage);
        total_pnl_usdt += r.pnl_usdt;
        total_entry_value += r.entry_value;
        total_exit_value += r.exit_value;
    }

    let pnl_percent = if total_entry_value > 0.0 {
        (total_pnl_usdt / total_entry_value) * 100.0
    } else {
        0.0
    };

    let roi_percent = pnl_percent * leverage as f64;

    PnlResult {
        pnl_usdt: total_pnl_usdt,
        pnl_percent,
        roi_percent,
        entry_value: total_entry_value,
        exit_value: total_exit_value,
    }
}

/// Calculate PnL using FIFO (First-In, First-Out) matching.
///
/// Entry fills are matched against exit fills in order. Partial fills are
/// split across multiple matches when quantities differ.
///
/// # Arguments
/// * `entry_fills` - Chronologically ordered entry fills.
/// * `exit_fills`  - Chronologically ordered exit fills.
/// * `direction`   - `"long"` or `"short"`.
/// * `leverage`    - Leverage multiplier.
pub fn calculate_fifo_pnl(
    entry_fills: &[FillInfo],
    exit_fills: &[FillInfo],
    direction: &str,
    leverage: u32,
) -> FifoPnlResult {
    let side = PositionSide::from(direction);

    // Clone quantities so we can mutate them during matching.
    let mut entry_remaining: Vec<f64> = entry_fills.iter().map(|f| f.quantity).collect();
    let mut exit_remaining: Vec<f64> = exit_fills.iter().map(|f| f.quantity).collect();

    let mut matches = Vec::new();
    let mut total_pnl_usdt = 0.0;
    let mut total_entry_value = 0.0;
    let mut total_exit_value = 0.0;

    let mut entry_idx = 0usize;
    let mut exit_idx = 0usize;

    while entry_idx < entry_fills.len() && exit_idx < exit_fills.len() {
        let entry_qty = entry_remaining[entry_idx];
        let exit_qty = exit_remaining[exit_idx];

        let matched_qty = entry_qty.min(exit_qty);

        if matched_qty > 0.0 {
            let entry_price = entry_fills[entry_idx].price;
            let exit_price = exit_fills[exit_idx].price;

            let entry_value = entry_price * matched_qty;
            let exit_value = exit_price * matched_qty;

            let pnl = match side {
                PositionSide::Long => exit_value - entry_value,
                PositionSide::Short => entry_value - exit_value,
            };

            let pnl_percent = if entry_value > 0.0 {
                (pnl / entry_value) * 100.0
            } else {
                0.0
            };

            matches.push(FifoMatch {
                entry_price,
                exit_price,
                quantity: matched_qty,
                entry_value,
                exit_value,
                pnl_usdt: pnl,
                pnl_percent,
            });

            total_pnl_usdt += pnl;
            total_entry_value += entry_value;
            total_exit_value += exit_value;

            entry_remaining[entry_idx] -= matched_qty;
            exit_remaining[exit_idx] -= matched_qty;
        }

        if entry_remaining[entry_idx] <= 0.0 {
            entry_idx += 1;
        }
        if exit_remaining[exit_idx] <= 0.0 {
            exit_idx += 1;
        }
    }

    let pnl_percent = if total_entry_value > 0.0 {
        (total_pnl_usdt / total_entry_value) * 100.0
    } else {
        0.0
    };

    let roi_percent = pnl_percent * leverage as f64;
    let match_count = matches.len();

    FifoPnlResult {
        matches,
        pnl_usdt: total_pnl_usdt,
        pnl_percent,
        roi_percent,
        total_entry_value,
        total_exit_value,
        entry_count: entry_fills.len(),
        exit_count: exit_fills.len(),
        match_count,
    }
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── calculate_pnl ───────────────────────────────────────────────

    #[test]
    fn test_pnl_long_profit() {
        let r = calculate_pnl(100.0, 110.0, 10.0, "long", 1);
        assert!((r.pnl_usdt - 100.0).abs() < 1e-9);
        assert!((r.pnl_percent - 10.0).abs() < 1e-9);
        assert!((r.roi_percent - 10.0).abs() < 1e-9);
        assert!((r.entry_value - 1000.0).abs() < 1e-9);
        assert!((r.exit_value - 1100.0).abs() < 1e-9);
    }

    #[test]
    fn test_pnl_long_loss() {
        let r = calculate_pnl(100.0, 90.0, 10.0, "long", 1);
        assert!((r.pnl_usdt - (-100.0)).abs() < 1e-9);
        assert!((r.pnl_percent - (-10.0)).abs() < 1e-9);
    }

    #[test]
    fn test_pnl_short_profit() {
        let r = calculate_pnl(100.0, 90.0, 10.0, "short", 1);
        assert!((r.pnl_usdt - 100.0).abs() < 1e-9);
        assert!((r.pnl_percent - 10.0).abs() < 1e-9);
    }

    #[test]
    fn test_pnl_short_loss() {
        let r = calculate_pnl(100.0, 110.0, 10.0, "short", 1);
        assert!((r.pnl_usdt - (-100.0)).abs() < 1e-9);
        assert!((r.pnl_percent - (-10.0)).abs() < 1e-9);
    }

    #[test]
    fn test_pnl_with_leverage() {
        let r = calculate_pnl(100.0, 110.0, 10.0, "long", 5);
        assert!((r.pnl_usdt - 100.0).abs() < 1e-9);
        assert!((r.pnl_percent - 10.0).abs() < 1e-9);
        assert!((r.roi_percent - 50.0).abs() < 1e-9);
    }

    #[test]
    fn test_pnl_zero_entry_price() {
        let r = calculate_pnl(0.0, 100.0, 10.0, "long", 1);
        assert!((r.pnl_percent - 0.0).abs() < 1e-9);
    }

    #[test]
    fn test_pnl_zero_quantity() {
        let r = calculate_pnl(100.0, 110.0, 0.0, "long", 1);
        assert!((r.pnl_usdt - 0.0).abs() < 1e-9);
    }

    #[test]
    fn test_pnl_same_price() {
        let r = calculate_pnl(100.0, 100.0, 10.0, "long", 1);
        assert!((r.pnl_usdt - 0.0).abs() < 1e-9);
        assert!((r.pnl_percent - 0.0).abs() < 1e-9);
    }

    // ── calculate_aggregate_pnl ─────────────────────────────────────

    #[test]
    fn test_aggregate_pnl_long() {
        let orders = vec![(100.0, 5.0), (110.0, 5.0)];
        let r = calculate_aggregate_pnl(&orders, 120.0, "long", 1);
        // (120*5 - 100*5) + (120*5 - 110*5) = 100 + 50 = 150
        assert!((r.pnl_usdt - 150.0).abs() < 1e-9);
        // entry_value = 500 + 550 = 1050
        assert!((r.entry_value - 1050.0).abs() < 1e-9);
    }

    #[test]
    fn test_aggregate_pnl_short() {
        let orders = vec![(100.0, 5.0), (110.0, 5.0)];
        let r = calculate_aggregate_pnl(&orders, 90.0, "short", 1);
        // (100*5 - 90*5) + (110*5 - 90*5) = 50 + 100 = 150
        assert!((r.pnl_usdt - 150.0).abs() < 1e-9);
    }

    #[test]
    fn test_aggregate_pnl_skips_zero() {
        let orders = vec![(0.0, 5.0), (100.0, 10.0)];
        let r = calculate_aggregate_pnl(&orders, 110.0, "long", 1);
        // Only second order counted
        assert!((r.pnl_usdt - 100.0).abs() < 1e-9);
        assert!((r.entry_value - 1000.0).abs() < 1e-9);
    }

    #[test]
    fn test_aggregate_pnl_with_leverage() {
        let orders = vec![(100.0, 10.0)];
        let r = calculate_aggregate_pnl(&orders, 110.0, "long", 10);
        assert!((r.pnl_usdt - 100.0).abs() < 1e-9);
        assert!((r.roi_percent - 100.0).abs() < 1e-9);
    }

    #[test]
    fn test_aggregate_pnl_empty() {
        let orders: Vec<(f64, f64)> = vec![];
        let r = calculate_aggregate_pnl(&orders, 100.0, "long", 1);
        assert!((r.pnl_usdt - 0.0).abs() < 1e-9);
    }

    // ── calculate_fifo_pnl ──────────────────────────────────────────

    fn fill(price: f64, quantity: f64) -> FillInfo {
        FillInfo {
            price,
            quantity,
            timestamp_ms: None,
            order_id: None,
        }
    }

    #[test]
    fn test_fifo_single_match_long() {
        let entries = [fill(100.0, 10.0)];
        let exits = [fill(110.0, 10.0)];
        let r = calculate_fifo_pnl(&entries, &exits, "long", 1);

        assert_eq!(r.match_count, 1);
        assert!((r.pnl_usdt - 100.0).abs() < 1e-9);
        assert!((r.pnl_percent - 10.0).abs() < 1e-9);
        assert_eq!(r.entry_count, 1);
        assert_eq!(r.exit_count, 1);
    }

    #[test]
    fn test_fifo_single_match_short() {
        let entries = [fill(100.0, 10.0)];
        let exits = [fill(90.0, 10.0)];
        let r = calculate_fifo_pnl(&entries, &exits, "short", 1);

        assert_eq!(r.match_count, 1);
        assert!((r.pnl_usdt - 100.0).abs() < 1e-9);
    }

    #[test]
    fn test_fifo_multiple_matches() {
        let entries = [fill(100.0, 5.0), fill(110.0, 5.0)];
        let exits = [fill(120.0, 10.0)];
        let r = calculate_fifo_pnl(&entries, &exits, "long", 1);

        assert_eq!(r.match_count, 2);
        // Match 1: 5 @ 100 -> 5 @ 120 = +100
        // Match 2: 5 @ 110 -> 5 @ 120 = +50
        assert!((r.pnl_usdt - 150.0).abs() < 1e-9);
        assert_eq!(r.entry_count, 2);
        assert_eq!(r.exit_count, 1);
    }

    #[test]
    fn test_fifo_partial_fills() {
        // Entry: 8 units at 100
        // Exit: 3 at 110, then 5 at 120
        let entries = [fill(100.0, 8.0)];
        let exits = [fill(110.0, 3.0), fill(120.0, 5.0)];
        let r = calculate_fifo_pnl(&entries, &exits, "long", 1);

        assert_eq!(r.match_count, 2);
        // Match 1: 3 @ 100 -> 3 @ 110 = +30
        // Match 2: 5 @ 100 -> 5 @ 120 = +100
        assert!((r.pnl_usdt - 130.0).abs() < 1e-9);
    }

    #[test]
    fn test_fifo_with_leverage() {
        let entries = [fill(100.0, 10.0)];
        let exits = [fill(110.0, 10.0)];
        let r = calculate_fifo_pnl(&entries, &exits, "long", 5);

        assert!((r.pnl_usdt - 100.0).abs() < 1e-9);
        assert!((r.roi_percent - 50.0).abs() < 1e-9);
    }

    #[test]
    fn test_fifo_more_entries_than_exits() {
        let entries = [fill(100.0, 10.0), fill(110.0, 5.0)];
        let exits = [fill(120.0, 8.0)];
        let r = calculate_fifo_pnl(&entries, &exits, "long", 1);

        // Only 8 units matched total (all from first entry)
        assert_eq!(r.match_count, 1);
        assert!((r.pnl_usdt - 160.0).abs() < 1e-9); // 8 * (120-100) = 160
    }

    #[test]
    fn test_fifo_more_exits_than_entries() {
        let entries = [fill(100.0, 5.0)];
        let exits = [fill(110.0, 10.0)];
        let r = calculate_fifo_pnl(&entries, &exits, "long", 1);

        // Only 5 units matched
        assert_eq!(r.match_count, 1);
        assert!((r.pnl_usdt - 50.0).abs() < 1e-9); // 5 * (110-100) = 50
    }

    #[test]
    fn test_fifo_empty_fills() {
        let entries: Vec<FillInfo> = vec![];
        let exits: Vec<FillInfo> = vec![];
        let r = calculate_fifo_pnl(&entries, &exits, "long", 1);

        assert_eq!(r.match_count, 0);
        assert!((r.pnl_usdt - 0.0).abs() < 1e-9);
    }

    #[test]
    fn test_fifo_complex_scenario() {
        // 3 entries, 2 exits with partial matching
        let entries = [fill(100.0, 3.0), fill(105.0, 4.0), fill(110.0, 3.0)];
        let exits = [fill(120.0, 5.0), fill(115.0, 5.0)];
        let r = calculate_fifo_pnl(&entries, &exits, "long", 2);

        // Match 1: 3 @ 100 -> 3 @ 120 = +60
        // Match 2: 2 @ 105 -> 2 @ 120 = +30
        // Match 3: 2 @ 105 -> 2 @ 115 = +20
        // Match 4: 3 @ 110 -> 3 @ 115 = +15
        // Total PnL = 125
        assert_eq!(r.match_count, 4);
        assert!((r.pnl_usdt - 125.0).abs() < 1e-9);
        assert_eq!(r.entry_count, 3);
        assert_eq!(r.exit_count, 2);
        // roi = pnl_percent * 2
        let expected_pnl_pct = (125.0 / r.total_entry_value) * 100.0;
        assert!((r.roi_percent - expected_pnl_pct * 2.0).abs() < 1e-6);
    }
}
