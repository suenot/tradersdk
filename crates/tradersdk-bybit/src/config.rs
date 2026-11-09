//! Bybit API configuration and constants.

// ═══════════════════════════════════════════════════════════════════════════
// REST API URLs
// ═══════════════════════════════════════════════════════════════════════════

/// Mainnet REST API URL.
pub const MAINNET_URL: &str = "https://api.bybit.com";
/// Testnet REST API URL.
pub const TESTNET_URL: &str = "https://api-testnet.bybit.com";

// ═══════════════════════════════════════════════════════════════════════════
// WEBSOCKET URLS - MARKET DATA STREAMS
// ═══════════════════════════════════════════════════════════════════════════

/// Mainnet WebSocket URL for linear perpetual market data.
pub const MAINNET_WS_URL: &str = "wss://stream.bybit.com/v5/public/linear";
/// Testnet WebSocket URL for linear perpetual market data.
pub const TESTNET_WS_URL: &str = "wss://stream-testnet.bybit.com/v5/public/linear";
/// Mainnet WebSocket URL for spot market data.
pub const MAINNET_WS_SPOT_URL: &str = "wss://stream.bybit.com/v5/public/spot";
/// Testnet WebSocket URL for spot market data.
pub const TESTNET_WS_SPOT_URL: &str = "wss://stream-testnet.bybit.com/v5/public/spot";

// ═══════════════════════════════════════════════════════════════════════════
// WEBSOCKET URLS - TRADING API
// ═══════════════════════════════════════════════════════════════════════════

/// Mainnet WebSocket URL for trading operations.
pub const MAINNET_WS_TRADE_URL: &str = "wss://stream.bybit.com/v5/trade";
/// Testnet WebSocket URL for trading operations.
pub const TESTNET_WS_TRADE_URL: &str = "wss://stream-testnet.bybit.com/v5/trade";

// ═══════════════════════════════════════════════════════════════════════════
// WEBSOCKET URLS - PRIVATE/USER DATA STREAM
// ═══════════════════════════════════════════════════════════════════════════

/// Mainnet WebSocket URL for private user data stream.
pub const MAINNET_WS_PRIVATE_URL: &str = "wss://stream.bybit.com/v5/private";
/// Testnet WebSocket URL for private user data stream.
pub const TESTNET_WS_PRIVATE_URL: &str = "wss://stream-testnet.bybit.com/v5/private";

// ═══════════════════════════════════════════════════════════════════════════
// RECONNECT SETTINGS
// ═══════════════════════════════════════════════════════════════════════════

/// Initial reconnect delay in seconds.
pub const INITIAL_RECONNECT_DELAY_SECS: u64 = 1;
/// Maximum reconnect delay in seconds.
pub const MAX_RECONNECT_DELAY_SECS: u64 = 30;

// ═══════════════════════════════════════════════════════════════════════════
// API ENDPOINTS
// ═══════════════════════════════════════════════════════════════════════════

/// REST API endpoints for different operations.
pub mod endpoints {
    // Server endpoints
    /// Server time endpoint.
    pub const SERVER_TIME: &str = "/v5/market/time";

    // Market data endpoints
    /// Market tickers endpoint.
    pub const TICKERS: &str = "/v5/market/tickers";
    /// Market orderbook endpoint.
    pub const ORDERBOOK: &str = "/v5/market/orderbook";
    /// Funding rate history endpoint.
    pub const FUNDING_HISTORY: &str = "/v5/market/funding/history";
    /// Open interest endpoint.
    pub const OPEN_INTEREST: &str = "/v5/market/open-interest";

    // Account endpoints
    /// Wallet balance endpoint.
    pub const WALLET_BALANCE: &str = "/v5/account/wallet-balance";

    // Position endpoints
    /// Position list endpoint.
    pub const POSITION_LIST: &str = "/v5/position/list";
    /// Set leverage endpoint.
    pub const SET_LEVERAGE: &str = "/v5/position/set-leverage";
    /// Trading stop (TP/SL) endpoint.
    pub const TRADING_STOP: &str = "/v5/position/trading-stop";

    // Order endpoints
    /// Real-time order status endpoint.
    pub const ORDER_REALTIME: &str = "/v5/order/realtime";
    /// Create order endpoint.
    pub const ORDER_CREATE: &str = "/v5/order/create";
    /// Cancel order endpoint.
    pub const ORDER_CANCEL: &str = "/v5/order/cancel";
    /// Amend order endpoint.
    pub const ORDER_AMEND: &str = "/v5/order/amend";
}
