//! Balance and account models.

use serde::{Deserialize, Serialize};

/// Individual asset balance.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub struct Balance {
    /// Asset name (e.g., "USDT", "BTC").
    pub asset: String,
    /// Free/available balance.
    pub free: f64,
    /// Locked balance (in orders).
    pub locked: f64,
}

impl Balance {
    /// Get total balance (free + locked).
    pub fn total(&self) -> f64 {
        self.free + self.locked
    }

    /// Check if balance is empty.
    pub fn is_empty(&self) -> bool {
        self.total() == 0.0
    }
}

/// Account balance summary.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub struct AccountBalance {
    /// Total balance/equity.
    pub total_balance: f64,
    /// Available balance for trading.
    pub available_balance: f64,
    /// Currency (e.g., "USDT").
    pub currency: String,
    /// Margin balance (for futures).
    pub margin_balance: Option<f64>,
    /// Used margin.
    pub used_margin: Option<f64>,
    /// Unrealized PnL.
    pub unrealized_pnl: Option<f64>,
    /// Realized PnL.
    pub realized_pnl: Option<f64>,
    /// Individual asset balances.
    pub balances: Vec<Balance>,
}

impl AccountBalance {
    /// Create a simple balance with just total and available.
    pub fn simple(total: f64, available: f64, currency: &str) -> Self {
        Self {
            total_balance: total,
            available_balance: available,
            currency: currency.to_string(),
            margin_balance: None,
            used_margin: None,
            unrealized_pnl: None,
            realized_pnl: None,
            balances: vec![],
        }
    }

    /// Get balance for specific asset.
    pub fn get_balance(&self, asset: &str) -> Option<&Balance> {
        let asset_upper = asset.to_uppercase();
        self.balances.iter().find(|b| b.asset.to_uppercase() == asset_upper)
    }

    /// Get free balance for specific asset.
    pub fn get_free_balance(&self, asset: &str) -> f64 {
        self.get_balance(asset).map(|b| b.free).unwrap_or(0.0)
    }

    /// Get margin usage percentage.
    pub fn margin_usage_percent(&self) -> Option<f64> {
        match (self.margin_balance, self.used_margin) {
            (Some(total), Some(used)) if total > 0.0 => Some((used / total) * 100.0),
            _ => None,
        }
    }
}

impl Default for AccountBalance {
    fn default() -> Self {
        Self {
            total_balance: 0.0,
            available_balance: 0.0,
            currency: "USDT".to_string(),
            margin_balance: None,
            used_margin: None,
            unrealized_pnl: None,
            realized_pnl: None,
            balances: vec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_balance_total() {
        let balance = Balance {
            asset: "USDT".to_string(),
            free: 1000.0,
            locked: 500.0,
        };

        assert_eq!(balance.total(), 1500.0);
        assert!(!balance.is_empty());
    }

    #[test]
    fn test_account_balance_get_asset() {
        let account = AccountBalance {
            balances: vec![
                Balance {
                    asset: "USDT".to_string(),
                    free: 1000.0,
                    locked: 0.0,
                },
                Balance {
                    asset: "BTC".to_string(),
                    free: 0.5,
                    locked: 0.1,
                },
            ],
            ..Default::default()
        };

        assert_eq!(account.get_free_balance("USDT"), 1000.0);
        assert_eq!(account.get_free_balance("BTC"), 0.5);
        assert_eq!(account.get_free_balance("ETH"), 0.0);
    }
}
