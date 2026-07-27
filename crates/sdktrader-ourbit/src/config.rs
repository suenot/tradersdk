//! Ourbit API configuration and constants.
//!
//! Ourbit only supports futures trading via REST API.
//! No spot trading, no testnet, no WebSocket API.

// ═══════════════════════════════════════════════════════════════════════════
// REST API URLs
// ═══════════════════════════════════════════════════════════════════════════

/// Futures mainnet REST API URL.
pub const FUTURES_MAINNET_URL: &str = "https://contract.ourbit.com";

// ═══════════════════════════════════════════════════════════════════════════
// RECONNECT SETTINGS
// ═══════════════════════════════════════════════════════════════════════════

/// Initial reconnect delay in seconds.
pub const INITIAL_RECONNECT_DELAY_SECS: u64 = 1;
/// Maximum reconnect delay in seconds.
pub const MAX_RECONNECT_DELAY_SECS: u64 = 60;

// ═══════════════════════════════════════════════════════════════════════════
// ORDER SIDE CONSTANTS
// ═══════════════════════════════════════════════════════════════════════════

/// Order side: Open Long position.
pub const ORDER_SIDE_OPEN_LONG: i32 = 1;
/// Order side: Close Short position.
pub const ORDER_SIDE_CLOSE_SHORT: i32 = 2;
/// Order side: Open Short position.
pub const ORDER_SIDE_OPEN_SHORT: i32 = 3;
/// Order side: Close Long position.
pub const ORDER_SIDE_CLOSE_LONG: i32 = 4;

// ═══════════════════════════════════════════════════════════════════════════
// ORDER TYPE CONSTANTS
// ═══════════════════════════════════════════════════════════════════════════

/// Order type: Limit order.
pub const ORDER_TYPE_LIMIT: i32 = 1;
/// Order type: Post-only order (maker only).
pub const ORDER_TYPE_POST_ONLY: i32 = 2;
/// Order type: Immediate or Cancel (IOC).
pub const ORDER_TYPE_IOC: i32 = 3;
/// Order type: Fill or Kill (FOK).
pub const ORDER_TYPE_FOK: i32 = 4;
/// Order type: Market order.
pub const ORDER_TYPE_MARKET: i32 = 5;

// ═══════════════════════════════════════════════════════════════════════════
// API ENDPOINTS
// ═══════════════════════════════════════════════════════════════════════════

/// REST API endpoints for different operations.
pub mod endpoints {
    /// Get ticker information for all contracts.
    pub const TICKER: &str = "/api/v1/contract/ticker";
    /// Get contract details/exchange info.
    pub const EXCHANGE_INFO: &str = "/api/v1/contract/detail";
    /// Get orderbook depth. Path: /{symbol}/{limit}
    pub const ORDERBOOK: &str = "/api/v1/contract/depth_commits";
    /// Get kline/candlestick data. Path: /{symbol}
    pub const KLINES: &str = "/api/v1/contract/kline";

    /// Submit a new order.
    pub const ORDER_SUBMIT: &str = "/api/v1/private/order/submit";
    /// Cancel an existing order.
    pub const ORDER_CANCEL: &str = "/api/v1/private/order/cancel";
    /// Get order details. Path: /{orderId}
    pub const ORDER_GET: &str = "/api/v1/private/order/get";
    /// Get open orders list. Path: /{symbol}
    pub const OPEN_ORDERS: &str = "/api/v1/private/order/list/open_orders";
    /// Change position leverage.
    pub const LEVERAGE: &str = "/api/v1/private/position/change_leverage";

    /// Get all account assets.
    pub const ASSETS: &str = "/api/v1/private/account/assets";
    /// Get single asset balance. Path: /{asset}
    pub const ASSET: &str = "/api/v1/private/account/asset";
    /// Get historical positions.
    pub const POSITIONS: &str = "/api/v1/private/position/list/history_positions";
}
