//! Position size and split order calculations.
//!
//! Ported from Python `trading/bot.py` position size calculation logic.
//! Pure computation — no exchange API calls.

use serde::{Deserialize, Serialize};

/// Calculate position size (in base currency) given a USDT margin amount,
/// leverage, and current price.
///
/// Formula: `quantity = (usdt_amount * leverage) / current_price`
///
/// # Arguments
/// * `usdt_amount`   - Margin amount in USDT.
/// * `leverage`      - Leverage multiplier.
/// * `current_price` - Current market price of the asset.
///
/// # Returns
/// Position size in base currency. Returns `0.0` if `current_price` is zero.
///
/// # Examples
/// ```
/// use sdktrader_trading::position::calculate_position_size;
///
/// let qty = calculate_position_size(100.0, 10, 50000.0);
/// assert!((qty - 0.02).abs() < 1e-9); // 100 * 10 / 50000 = 0.02
/// ```
pub fn calculate_position_size(usdt_amount: f64, leverage: u32, current_price: f64) -> f64 {
    if current_price == 0.0 {
        return 0.0;
    }
    (usdt_amount * leverage as f64) / current_price
}

/// Configuration for splitting a single order into multiple parts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SplitOrderConfig {
    /// Number of sub-orders to split into.
    pub count: u32,
    /// Delay between sub-orders in milliseconds.
    pub delay_ms: u64,
}

/// Calculate split order quantities (equal parts).
///
/// Distributes `total_quantity` evenly across `count` orders.
/// The last order absorbs any rounding remainder so the sum is exact.
///
/// # Arguments
/// * `total_quantity` - Total quantity to distribute.
/// * `count`          - Number of equal parts.
///
/// # Returns
/// A `Vec<f64>` of length `count` whose elements sum to `total_quantity`.
/// Returns an empty vec if `count` is zero.
///
/// # Examples
/// ```
/// use sdktrader_trading::position::calculate_split_orders;
///
/// let parts = calculate_split_orders(100.0, 3);
/// assert_eq!(parts.len(), 3);
/// let sum: f64 = parts.iter().sum();
/// assert!((sum - 100.0).abs() < 1e-9);
/// ```
pub fn calculate_split_orders(total_quantity: f64, count: u32) -> Vec<f64> {
    if count == 0 {
        return Vec::new();
    }
    let per_order = total_quantity / count as f64;
    let mut orders = vec![per_order; count as usize];

    // Correct rounding error in the last order.
    let partial_sum: f64 = orders[..orders.len() - 1].iter().sum();
    *orders.last_mut().unwrap() = total_quantity - partial_sum;

    orders
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── calculate_position_size ─────────────────────────────────────

    #[test]
    fn test_position_size_basic() {
        let qty = calculate_position_size(100.0, 10, 50000.0);
        assert!((qty - 0.02).abs() < 1e-9);
    }

    #[test]
    fn test_position_size_no_leverage() {
        let qty = calculate_position_size(1000.0, 1, 100.0);
        assert!((qty - 10.0).abs() < 1e-9);
    }

    #[test]
    fn test_position_size_zero_price() {
        let qty = calculate_position_size(100.0, 10, 0.0);
        assert!((qty - 0.0).abs() < 1e-9);
    }

    #[test]
    fn test_position_size_zero_amount() {
        let qty = calculate_position_size(0.0, 10, 100.0);
        assert!((qty - 0.0).abs() < 1e-9);
    }

    #[test]
    fn test_position_size_large_leverage() {
        let qty = calculate_position_size(10.0, 100, 200.0);
        // 10 * 100 / 200 = 5.0
        assert!((qty - 5.0).abs() < 1e-9);
    }

    // ── calculate_split_orders ──────────────────────────────────────

    #[test]
    fn test_split_orders_equal() {
        let parts = calculate_split_orders(100.0, 4);
        assert_eq!(parts.len(), 4);
        for &p in &parts {
            assert!((p - 25.0).abs() < 1e-9);
        }
    }

    #[test]
    fn test_split_orders_sum_exact() {
        let parts = calculate_split_orders(100.0, 3);
        assert_eq!(parts.len(), 3);
        let sum: f64 = parts.iter().sum();
        assert!((sum - 100.0).abs() < 1e-9);
    }

    #[test]
    fn test_split_orders_single() {
        let parts = calculate_split_orders(50.0, 1);
        assert_eq!(parts.len(), 1);
        assert!((parts[0] - 50.0).abs() < 1e-9);
    }

    #[test]
    fn test_split_orders_zero_count() {
        let parts = calculate_split_orders(100.0, 0);
        assert!(parts.is_empty());
    }

    #[test]
    fn test_split_orders_small_quantity() {
        let parts = calculate_split_orders(0.001, 3);
        assert_eq!(parts.len(), 3);
        let sum: f64 = parts.iter().sum();
        assert!((sum - 0.001).abs() < 1e-12);
    }

    #[test]
    fn test_split_orders_large_count() {
        let parts = calculate_split_orders(1000.0, 100);
        assert_eq!(parts.len(), 100);
        let sum: f64 = parts.iter().sum();
        assert!((sum - 1000.0).abs() < 1e-9);
    }
}
