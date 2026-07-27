//! Exchange capabilities matrix.
//!
//! Defines what each exchange implementation supports, similar to CCXT's `has` property.

use serde::{Deserialize, Serialize};

/// Whether a specific capability is supported by an exchange.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum CapabilityStatus {
    /// Fully supported natively.
    Supported,
    /// Supported via emulation or workaround.
    Emulated,
    /// Explicitly not supported.
    NotSupported,
    /// Not yet determined or not implemented.
    #[default]
    Unknown,
}

impl std::fmt::Display for CapabilityStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Supported => write!(f, "supported"),
            Self::Emulated => write!(f, "emulated"),
            Self::NotSupported => write!(f, "not_supported"),
            Self::Unknown => write!(f, "unknown"),
        }
    }
}

/// What an exchange implementation can and cannot do.
///
/// Each field represents a specific capability. Consumers should check
/// capabilities before calling optional methods.
///
/// # Example
/// ```rust
/// use sdktrader_core::models::capabilities::{ExchangeCapabilities, CapabilityStatus};
///
/// let caps = ExchangeCapabilities::default();
/// assert_eq!(caps.create_market_order, CapabilityStatus::Unknown);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub struct ExchangeCapabilities {
    // ═══════════════════════════════════════════════════════════════
    // MARKET TYPES
    // ═══════════════════════════════════════════════════════════════
    /// Spot trading support.
    pub spot: CapabilityStatus,
    /// Perpetual swap / futures trading support.
    pub swap: CapabilityStatus,
    /// Dated futures trading support.
    pub futures: CapabilityStatus,
    /// Options trading support.
    pub option: CapabilityStatus,
    /// Margin trading support.
    pub margin: CapabilityStatus,

    // ═══════════════════════════════════════════════════════════════
    // ORDER CREATION
    // ═══════════════════════════════════════════════════════════════
    /// Create market orders.
    pub create_market_order: CapabilityStatus,
    /// Create limit orders.
    pub create_limit_order: CapabilityStatus,
    /// Create stop-market orders.
    pub create_stop_market_order: CapabilityStatus,
    /// Create stop-limit orders.
    pub create_stop_limit_order: CapabilityStatus,
    /// Create take-profit orders.
    pub create_take_profit_order: CapabilityStatus,
    /// Create trailing stop orders.
    pub create_trailing_stop_order: CapabilityStatus,
    /// Create post-only (maker) orders.
    pub create_post_only_order: CapabilityStatus,
    /// Create reduce-only orders.
    pub create_reduce_only_order: CapabilityStatus,
    /// Batch order creation (multiple orders at once).
    pub create_orders_batch: CapabilityStatus,

    // ═══════════════════════════════════════════════════════════════
    // ORDER MANAGEMENT
    // ═══════════════════════════════════════════════════════════════
    /// Cancel a single order.
    pub cancel_order: CapabilityStatus,
    /// Cancel all orders for a symbol.
    pub cancel_all_orders: CapabilityStatus,
    /// Edit/amend an existing order.
    pub edit_order: CapabilityStatus,

    // ═══════════════════════════════════════════════════════════════
    // ORDER QUERIES
    // ═══════════════════════════════════════════════════════════════
    /// Get status of a specific order.
    pub get_order_status: CapabilityStatus,
    /// Get all open orders.
    pub get_open_orders: CapabilityStatus,
    /// Get order history.
    pub get_order_history: CapabilityStatus,
    /// Get trades for a specific order.
    pub get_order_trades: CapabilityStatus,

    // ═══════════════════════════════════════════════════════════════
    // POSITION MANAGEMENT
    // ═══════════════════════════════════════════════════════════════
    /// Set leverage for a symbol.
    pub set_leverage: CapabilityStatus,
    /// Set margin mode (cross/isolated).
    pub set_margin_mode: CapabilityStatus,
    /// Set position mode (hedge/one-way).
    pub set_position_mode: CapabilityStatus,
    /// Close a position.
    pub close_position: CapabilityStatus,
    /// Get a single position.
    pub get_position: CapabilityStatus,
    /// Get all positions.
    pub get_positions: CapabilityStatus,

    // ═══════════════════════════════════════════════════════════════
    // MARKET DATA (REST)
    // ═══════════════════════════════════════════════════════════════
    /// Get current price for a symbol.
    pub get_current_price: CapabilityStatus,
    /// Get full ticker info.
    pub get_ticker: CapabilityStatus,
    /// Get orderbook snapshot.
    pub get_orderbook: CapabilityStatus,
    /// Get OHLCV candles.
    pub get_ohlcv: CapabilityStatus,
    /// Get recent trades.
    pub get_trades: CapabilityStatus,
    /// Get funding rate (current).
    pub get_funding_rate: CapabilityStatus,
    /// Get funding rate history.
    pub get_funding_rate_history: CapabilityStatus,
    /// Get open interest.
    pub get_open_interest: CapabilityStatus,
    /// Get open interest history.
    pub get_open_interest_history: CapabilityStatus,

    // ═══════════════════════════════════════════════════════════════
    // ACCOUNT
    // ═══════════════════════════════════════════════════════════════
    /// Get account balance.
    pub get_balance: CapabilityStatus,
    /// Get server time.
    pub get_server_time: CapabilityStatus,
    /// Test connection.
    pub test_connection: CapabilityStatus,

    // ═══════════════════════════════════════════════════════════════
    // WEBSOCKET
    // ═══════════════════════════════════════════════════════════════
    /// WebSocket orderbook subscription.
    pub ws_orderbook: CapabilityStatus,
    /// WebSocket trades subscription.
    pub ws_trades: CapabilityStatus,
    /// WebSocket ticker subscription.
    pub ws_ticker: CapabilityStatus,
    /// WebSocket private order updates.
    pub ws_orders: CapabilityStatus,
    /// WebSocket private position updates.
    pub ws_positions: CapabilityStatus,
    /// WebSocket private balance updates.
    pub ws_balance: CapabilityStatus,

    // ═══════════════════════════════════════════════════════════════
    // FAST ENTRY
    // ═══════════════════════════════════════════════════════════════
    /// Execute a fast market entry with optional TP/SL.
    pub execute_fast_entry: CapabilityStatus,
    /// Symbol formatting.
    pub format_symbol: CapabilityStatus,

    // ═══════════════════════════════════════════════════════════════
    // TRANSFERS
    // ═══════════════════════════════════════════════════════════════
    /// Withdraw funds.
    pub withdraw: CapabilityStatus,
    /// Deposit address generation.
    pub get_deposit_address: CapabilityStatus,
    /// Internal transfers between accounts.
    pub transfer: CapabilityStatus,
}

impl ExchangeCapabilities {
    /// Count how many capabilities are supported (Supported or Emulated).
    pub fn supported_count(&self) -> usize {
        let json = serde_json::to_value(self).unwrap_or_default();
        if let serde_json::Value::Object(map) = json {
            map.values()
                .filter(|v| {
                    v.as_str()
                        .map(|s| s == "supported" || s == "emulated")
                        .unwrap_or(false)
                })
                .count()
        } else {
            0
        }
    }

    /// Count total capabilities.
    pub fn total_count(&self) -> usize {
        let json = serde_json::to_value(self).unwrap_or_default();
        if let serde_json::Value::Object(map) = json {
            map.len()
        } else {
            0
        }
    }

    /// Check if a named capability is supported.
    pub fn is_supported(&self, name: &str) -> bool {
        let json = serde_json::to_value(self).unwrap_or_default();
        if let serde_json::Value::Object(map) = json {
            map.get(name)
                .and_then(|v| v.as_str())
                .map(|s| s == "supported" || s == "emulated")
                .unwrap_or(false)
        } else {
            false
        }
    }
}

/// Declarative macro to construct [`ExchangeCapabilities`] with less boilerplate.
///
/// Only the fields that differ from `Unknown` need to be listed; the rest are
/// filled in via `..Default::default()`.
///
/// # Example
/// ```rust
/// let caps = sdktrader_core::exchange_capabilities! {
///     spot: Supported,
///     swap: Supported,
///     ws_orderbook: NotSupported,
/// };
/// assert_eq!(caps.spot, sdktrader_core::models::CapabilityStatus::Supported);
/// assert_eq!(caps.ws_orderbook, sdktrader_core::models::CapabilityStatus::NotSupported);
/// assert_eq!(caps.futures, sdktrader_core::models::CapabilityStatus::Unknown);
/// ```
#[macro_export]
macro_rules! exchange_capabilities {
    ($($field:ident : $status:ident),* $(,)?) => {
        {
            #[allow(unused_imports)]
            use $crate::models::CapabilityStatus::*;
            $crate::models::ExchangeCapabilities {
                $($field: $status,)*
                ..Default::default()
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_capabilities_are_unknown() {
        let caps = ExchangeCapabilities::default();
        assert_eq!(caps.spot, CapabilityStatus::Unknown);
        assert_eq!(caps.create_market_order, CapabilityStatus::Unknown);
        assert_eq!(caps.ws_orderbook, CapabilityStatus::Unknown);
    }

    #[test]
    fn test_capability_status_display() {
        assert_eq!(CapabilityStatus::Supported.to_string(), "supported");
        assert_eq!(CapabilityStatus::NotSupported.to_string(), "not_supported");
    }

    #[test]
    fn test_supported_count() {
        let mut caps = ExchangeCapabilities::default();
        assert_eq!(caps.supported_count(), 0);

        caps.spot = CapabilityStatus::Supported;
        caps.create_market_order = CapabilityStatus::Supported;
        caps.get_ticker = CapabilityStatus::Emulated;
        assert_eq!(caps.supported_count(), 3);
    }

    #[test]
    fn test_is_supported() {
        let mut caps = ExchangeCapabilities::default();
        assert!(!caps.is_supported("spot"));

        caps.spot = CapabilityStatus::Supported;
        assert!(caps.is_supported("spot"));

        caps.get_ticker = CapabilityStatus::Emulated;
        assert!(caps.is_supported("get_ticker"));
    }

    #[test]
    fn test_serialization_roundtrip() {
        let caps = ExchangeCapabilities {
            spot: CapabilityStatus::Supported,
            swap: CapabilityStatus::NotSupported,
            ..Default::default()
        };

        let json = serde_json::to_string(&caps).unwrap();
        let deserialized: ExchangeCapabilities = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.spot, CapabilityStatus::Supported);
        assert_eq!(deserialized.swap, CapabilityStatus::NotSupported);
        assert_eq!(deserialized.futures, CapabilityStatus::Unknown);
    }

    #[test]
    fn test_exchange_capabilities_macro() {
        let caps = exchange_capabilities! {
            spot: Supported,
            swap: Supported,
            ws_orderbook: NotSupported,
        };
        assert_eq!(caps.spot, CapabilityStatus::Supported);
        assert_eq!(caps.swap, CapabilityStatus::Supported);
        assert_eq!(caps.ws_orderbook, CapabilityStatus::NotSupported);
        assert_eq!(caps.futures, CapabilityStatus::Unknown);
        assert_eq!(caps.create_market_order, CapabilityStatus::Unknown);
    }

    #[test]
    fn test_exchange_capabilities_macro_empty() {
        let caps = exchange_capabilities! {};
        assert_eq!(caps.spot, CapabilityStatus::Unknown);
    }

    #[cfg(feature = "json-schema")]
    #[test]
    fn test_json_schema_generation() {
        let schema = schemars::schema_for!(ExchangeCapabilities);
        let json = serde_json::to_string_pretty(&schema).unwrap();
        assert!(json.contains("ExchangeCapabilities"));
        assert!(json.contains("spot"));
        assert!(json.contains("create_market_order"));
    }
}
