//! Position and margin models.

use serde::{Deserialize, Serialize};

/// Position side.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum PositionSide {
    Long,
    Short,
}

impl std::fmt::Display for PositionSide {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PositionSide::Long => write!(f, "long"),
            PositionSide::Short => write!(f, "short"),
        }
    }
}

impl From<&str> for PositionSide {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "short" | "sell" => Self::Short,
            _ => Self::Long,
        }
    }
}

/// Margin mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum MarginMode {
    #[default]
    Cross,
    Isolated,
}

impl std::fmt::Display for MarginMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MarginMode::Cross => write!(f, "cross"),
            MarginMode::Isolated => write!(f, "isolated"),
        }
    }
}

/// Trading position.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub struct Position {
    /// Trading symbol.
    pub symbol: String,
    /// Exchange name.
    pub exchange: String,
    /// Position side.
    pub side: PositionSide,
    /// Entry price.
    pub entry_price: f64,
    /// Position amount.
    pub amount: f64,
    /// Current mark price.
    pub mark_price: Option<f64>,
    /// Liquidation price.
    pub liquidation_price: Option<f64>,
    /// Position entry timestamp.
    pub entry_time: i64,
    /// Unrealized PnL.
    pub unrealized_pnl: f64,
    /// Realized PnL.
    pub realized_pnl: f64,
    /// Leverage.
    pub leverage: u32,
    /// Margin mode.
    pub margin_mode: MarginMode,
    /// Position margin.
    pub margin: Option<f64>,
    /// Whether position is open.
    pub is_open: bool,
}

impl Position {
    /// Calculate notional value.
    pub fn notional_value(&self) -> f64 {
        self.amount * self.entry_price
    }

    /// Calculate current PnL based on mark price.
    pub fn calculate_pnl(&self, current_price: f64) -> f64 {
        let price_diff = current_price - self.entry_price;
        let signed_diff = match self.side {
            PositionSide::Long => price_diff,
            PositionSide::Short => -price_diff,
        };
        signed_diff * self.amount
    }

    /// Calculate PnL percentage.
    pub fn pnl_percentage(&self, current_price: f64) -> f64 {
        let pnl = self.calculate_pnl(current_price);
        if self.notional_value() == 0.0 {
            return 0.0;
        }
        (pnl / self.notional_value()) * 100.0 * self.leverage as f64
    }

    /// Check if long position.
    pub fn is_long(&self) -> bool {
        self.side == PositionSide::Long
    }

    /// Check if short position.
    pub fn is_short(&self) -> bool {
        self.side == PositionSide::Short
    }
}

impl Default for Position {
    fn default() -> Self {
        Self {
            symbol: String::new(),
            exchange: String::new(),
            side: PositionSide::Long,
            entry_price: 0.0,
            amount: 0.0,
            mark_price: None,
            liquidation_price: None,
            entry_time: 0,
            unrealized_pnl: 0.0,
            realized_pnl: 0.0,
            leverage: 1,
            margin_mode: MarginMode::Cross,
            margin: None,
            is_open: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_position_pnl_long() {
        let pos = Position {
            side: PositionSide::Long,
            entry_price: 50000.0,
            amount: 1.0,
            ..Default::default()
        };

        // Price went up
        assert_eq!(pos.calculate_pnl(51000.0), 1000.0);
        // Price went down
        assert_eq!(pos.calculate_pnl(49000.0), -1000.0);
    }

    #[test]
    fn test_position_pnl_short() {
        let pos = Position {
            side: PositionSide::Short,
            entry_price: 50000.0,
            amount: 1.0,
            ..Default::default()
        };

        // Price went up (loss for short)
        assert_eq!(pos.calculate_pnl(51000.0), -1000.0);
        // Price went down (profit for short)
        assert_eq!(pos.calculate_pnl(49000.0), 1000.0);
    }
}
