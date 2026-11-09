//! Limit order grid calculation for position exits.
//!
//! Ported from Python `position_exit_manager.py` method `calculate_limit_orders_grid`.
//! Pure computation — no exchange API calls.

use serde::{Deserialize, Serialize};
use tradersdk_core::models::PositionSide;

/// Configuration for the limit order grid.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GridRule {
    /// Minimum USDT per order (default 30).
    pub min_order_lot: f64,
    /// Maximum number of orders in the grid (default 10).
    pub max_lots: u32,
    /// Start percentage from entry price (default 10).
    pub percent_start: f64,
    /// End percentage from entry price (default 50).
    pub percent_end: f64,
}

impl Default for GridRule {
    fn default() -> Self {
        Self {
            min_order_lot: 30.0,
            max_lots: 10,
            percent_start: 10.0,
            percent_end: 50.0,
        }
    }
}

/// A single calculated grid order.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GridOrder {
    /// Limit price for this order.
    pub price: f64,
    /// Size in USDT for this order.
    pub usdt_size: f64,
    /// Percentage offset from entry price.
    pub percent: f64,
}

/// Calculate a grid of limit orders for position exit.
///
/// Orders are distributed linearly from `percent_start` to `percent_end`
/// away from the entry price. For long positions the orders are placed
/// above entry (sell orders); for short positions they are placed below
/// entry (buy orders).
///
/// The number of orders is determined by `position_usdt / min_order_lot`,
/// clamped to `[1, max_lots]`.
///
/// # Arguments
/// * `entry_price`   - Entry price of the position.
/// * `position_usdt` - Position size in USDT.
/// * `direction`     - `"long"` or `"short"`.
/// * `rule`          - Grid configuration parameters.
///
/// # Returns
/// A `Vec<GridOrder>` with the calculated limit orders.
///
/// Returns an empty vec if `entry_price` is zero or `position_usdt` is non-positive.
pub fn calculate_limit_orders_grid(
    entry_price: f64,
    position_usdt: f64,
    direction: &str,
    rule: &GridRule,
) -> Vec<GridOrder> {
    if entry_price == 0.0 || position_usdt <= 0.0 {
        return Vec::new();
    }

    let side = PositionSide::from(direction);

    // Determine the number of orders.
    let num_orders = if rule.min_order_lot > 0.0 {
        let n = (position_usdt / rule.min_order_lot) as u32;
        n.max(1).min(rule.max_lots)
    } else {
        rule.max_lots.max(1)
    };

    let usdt_per_order = position_usdt / num_orders as f64;

    let mut orders = Vec::with_capacity(num_orders as usize);

    for i in 0..num_orders {
        // Linear interpolation between percent_start and percent_end.
        let percent = if num_orders == 1 {
            (rule.percent_start + rule.percent_end) / 2.0
        } else {
            rule.percent_start
                + (rule.percent_end - rule.percent_start) * i as f64 / (num_orders - 1) as f64
        };

        let price = match side {
            PositionSide::Long => entry_price * (1.0 + percent / 100.0),
            PositionSide::Short => entry_price * (1.0 - percent / 100.0),
        };

        orders.push(GridOrder {
            price,
            usdt_size: usdt_per_order,
            percent,
        });
    }

    orders
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn default_rule() -> GridRule {
        GridRule::default()
    }

    // ── Basic tests ─────────────────────────────────────────────────

    #[test]
    fn test_single_order_long() {
        let rule = GridRule {
            min_order_lot: 100.0,
            max_lots: 10,
            percent_start: 10.0,
            percent_end: 50.0,
        };
        // 50 / 100 = 0 -> clamped to 1
        let orders = calculate_limit_orders_grid(100.0, 50.0, "long", &rule);
        assert_eq!(orders.len(), 1);
        // Single order uses midpoint: (10 + 50) / 2 = 30%
        assert!((orders[0].percent - 30.0).abs() < 1e-9);
        assert!((orders[0].price - 130.0).abs() < 1e-9); // 100 * 1.30
        assert!((orders[0].usdt_size - 50.0).abs() < 1e-9);
    }

    #[test]
    fn test_single_order_short() {
        let rule = GridRule {
            min_order_lot: 100.0,
            max_lots: 10,
            percent_start: 10.0,
            percent_end: 50.0,
        };
        let orders = calculate_limit_orders_grid(100.0, 50.0, "short", &rule);
        assert_eq!(orders.len(), 1);
        assert!((orders[0].percent - 30.0).abs() < 1e-9);
        assert!((orders[0].price - 70.0).abs() < 1e-9); // 100 * (1 - 0.30)
    }

    #[test]
    fn test_multiple_orders_long() {
        let rule = GridRule {
            min_order_lot: 30.0,
            max_lots: 10,
            percent_start: 10.0,
            percent_end: 50.0,
        };
        // 300 / 30 = 10 orders
        let orders = calculate_limit_orders_grid(100.0, 300.0, "long", &rule);
        assert_eq!(orders.len(), 10);

        // First order at percent_start
        assert!((orders[0].percent - 10.0).abs() < 1e-9);
        assert!((orders[0].price - 110.0).abs() < 1e-9);

        // Last order at percent_end
        assert!((orders[9].percent - 50.0).abs() < 1e-9);
        assert!((orders[9].price - 150.0).abs() < 1e-9);

        // Each order has equal USDT size
        for o in &orders {
            assert!((o.usdt_size - 30.0).abs() < 1e-9);
        }
    }

    #[test]
    fn test_multiple_orders_short() {
        let rule = GridRule {
            min_order_lot: 30.0,
            max_lots: 10,
            percent_start: 10.0,
            percent_end: 50.0,
        };
        let orders = calculate_limit_orders_grid(100.0, 300.0, "short", &rule);
        assert_eq!(orders.len(), 10);

        // First order below entry
        assert!((orders[0].price - 90.0).abs() < 1e-9); // 100 * (1 - 0.10)
                                                        // Last order below entry
        assert!((orders[9].price - 50.0).abs() < 1e-9); // 100 * (1 - 0.50)
    }

    #[test]
    fn test_max_lots_cap() {
        let rule = GridRule {
            min_order_lot: 10.0,
            max_lots: 5,
            percent_start: 10.0,
            percent_end: 50.0,
        };
        // 1000 / 10 = 100, but capped at max_lots = 5
        let orders = calculate_limit_orders_grid(100.0, 1000.0, "long", &rule);
        assert_eq!(orders.len(), 5);
        assert!((orders[0].usdt_size - 200.0).abs() < 1e-9); // 1000 / 5
    }

    #[test]
    fn test_usdt_size_distribution() {
        let rule = GridRule {
            min_order_lot: 50.0,
            max_lots: 10,
            percent_start: 5.0,
            percent_end: 25.0,
        };
        let orders = calculate_limit_orders_grid(100.0, 200.0, "long", &rule);
        // 200 / 50 = 4 orders
        assert_eq!(orders.len(), 4);
        let total_usdt: f64 = orders.iter().map(|o| o.usdt_size).sum();
        assert!((total_usdt - 200.0).abs() < 1e-9);
    }

    // ── Edge cases ──────────────────────────────────────────────────

    #[test]
    fn test_zero_entry_price() {
        let orders = calculate_limit_orders_grid(0.0, 100.0, "long", &default_rule());
        assert!(orders.is_empty());
    }

    #[test]
    fn test_zero_position_usdt() {
        let orders = calculate_limit_orders_grid(100.0, 0.0, "long", &default_rule());
        assert!(orders.is_empty());
    }

    #[test]
    fn test_negative_position_usdt() {
        let orders = calculate_limit_orders_grid(100.0, -50.0, "long", &default_rule());
        assert!(orders.is_empty());
    }

    #[test]
    fn test_default_rule() {
        let rule = GridRule::default();
        assert!((rule.min_order_lot - 30.0).abs() < 1e-9);
        assert_eq!(rule.max_lots, 10);
        assert!((rule.percent_start - 10.0).abs() < 1e-9);
        assert!((rule.percent_end - 50.0).abs() < 1e-9);
    }

    #[test]
    fn test_percent_interpolation() {
        let rule = GridRule {
            min_order_lot: 25.0,
            max_lots: 10,
            percent_start: 0.0,
            percent_end: 100.0,
        };
        // 100 / 25 = 4 orders
        let orders = calculate_limit_orders_grid(100.0, 100.0, "long", &rule);
        assert_eq!(orders.len(), 4);
        // 0%, 33.33%, 66.67%, 100%
        assert!((orders[0].percent - 0.0).abs() < 1e-9);
        assert!((orders[3].percent - 100.0).abs() < 1e-9);
        // Verify linear spacing
        let step = orders[1].percent - orders[0].percent;
        for i in 1..orders.len() {
            assert!((orders[i].percent - orders[i - 1].percent - step).abs() < 1e-6);
        }
    }

    #[test]
    fn test_long_prices_ascending() {
        let rule = GridRule {
            min_order_lot: 30.0,
            max_lots: 5,
            percent_start: 10.0,
            percent_end: 50.0,
        };
        let orders = calculate_limit_orders_grid(100.0, 150.0, "long", &rule);
        for i in 1..orders.len() {
            assert!(
                orders[i].price > orders[i - 1].price,
                "Long grid prices should be ascending"
            );
        }
    }

    #[test]
    fn test_short_prices_descending() {
        let rule = GridRule {
            min_order_lot: 30.0,
            max_lots: 5,
            percent_start: 10.0,
            percent_end: 50.0,
        };
        let orders = calculate_limit_orders_grid(100.0, 150.0, "short", &rule);
        for i in 1..orders.len() {
            assert!(
                orders[i].price < orders[i - 1].price,
                "Short grid prices should be descending"
            );
        }
    }
}
