//! Binance API configuration and constants.

// ═══════════════════════════════════════════════════════════════════════════
// REST API URLs
// ═══════════════════════════════════════════════════════════════════════════

/// Spot mainnet REST API URL.
pub const SPOT_MAINNET_URL: &str = "https://api.binance.com";
/// Spot testnet REST API URL.
pub const SPOT_TESTNET_URL: &str = "https://testnet.binance.vision";
/// Futures mainnet REST API URL.
pub const FUTURES_MAINNET_URL: &str = "https://fapi.binance.com";
/// Futures testnet REST API URL.
pub const FUTURES_TESTNET_URL: &str = "https://testnet.binancefuture.com";

// ═══════════════════════════════════════════════════════════════════════════
// WEBSOCKET URLS - MARKET DATA STREAMS
// ═══════════════════════════════════════════════════════════════════════════

/// Futures mainnet WebSocket URL for market data.
pub const FUTURES_WS_MAINNET: &str = "wss://fstream.binance.com/ws";
/// Futures testnet WebSocket URL for market data.
pub const FUTURES_WS_TESTNET: &str = "wss://stream.binancefuture.com/ws";
/// Spot mainnet WebSocket URL for market data.
pub const SPOT_WS_MAINNET: &str = "wss://stream.binance.com:9443/ws";
/// Spot testnet WebSocket URL for market data.
pub const SPOT_WS_TESTNET: &str = "wss://testnet.binance.vision/ws";

// ═══════════════════════════════════════════════════════════════════════════
// WEBSOCKET URLS - TRADING API
// ═══════════════════════════════════════════════════════════════════════════

/// Spot mainnet WebSocket API URL for trading.
pub const SPOT_WS_API_MAINNET: &str = "wss://ws-api.binance.com:443/ws-api/v3";
/// Spot testnet WebSocket API URL for trading.
pub const SPOT_WS_API_TESTNET: &str = "wss://ws-api.testnet.binance.vision/ws-api/v3";
/// Futures mainnet WebSocket API URL for trading.
pub const FUTURES_WS_API_MAINNET: &str = "wss://ws-fapi.binance.com/ws-fapi/v1";
/// Futures testnet WebSocket API URL for trading.
pub const FUTURES_WS_API_TESTNET: &str = "wss://testnet.binancefuture.com/ws-fapi/v1";

// ═══════════════════════════════════════════════════════════════════════════
// WEBSOCKET URLS - USER DATA STREAM
// ═══════════════════════════════════════════════════════════════════════════

/// Spot mainnet WebSocket URL for user data stream.
pub const SPOT_USER_WS_MAINNET: &str = "wss://stream.binance.com:9443/ws";
/// Spot testnet WebSocket URL for user data stream.
pub const SPOT_USER_WS_TESTNET: &str = "wss://testnet.binance.vision/ws";
/// Futures mainnet WebSocket URL for user data stream.
pub const FUTURES_USER_WS_MAINNET: &str = "wss://fstream.binance.com/ws";
/// Futures testnet WebSocket URL for user data stream.
pub const FUTURES_USER_WS_TESTNET: &str = "wss://stream.binancefuture.com/ws";

// ═══════════════════════════════════════════════════════════════════════════
// RECONNECT SETTINGS
// ═══════════════════════════════════════════════════════════════════════════

/// Initial reconnect delay in seconds.
pub const INITIAL_RECONNECT_DELAY_SECS: u64 = 1;
/// Maximum reconnect delay in seconds.
pub const MAX_RECONNECT_DELAY_SECS: u64 = 30;
/// Keepalive interval for listenKey renewal (30 minutes).
pub const LISTEN_KEY_KEEPALIVE_SECS: u64 = 30 * 60;

// ═══════════════════════════════════════════════════════════════════════════
// API ENDPOINTS
// ═══════════════════════════════════════════════════════════════════════════

/// REST API endpoints for different operations.
pub mod endpoints {
    // Spot endpoints
    pub const SPOT_PING: &str = "/api/v3/ping";
    pub const SPOT_TIME: &str = "/api/v3/time";
    pub const SPOT_ACCOUNT: &str = "/api/v3/account";
    pub const SPOT_TICKER_PRICE: &str = "/api/v3/ticker/price";
    pub const SPOT_TICKER_24HR: &str = "/api/v3/ticker/24hr";
    pub const SPOT_DEPTH: &str = "/api/v3/depth";
    pub const SPOT_ORDER: &str = "/api/v3/order";
    pub const SPOT_LISTEN_KEY: &str = "/api/v3/userDataStream";

    // Futures endpoints
    pub const FUTURES_PING: &str = "/fapi/v1/ping";
    pub const FUTURES_TIME: &str = "/fapi/v1/time";
    pub const FUTURES_BALANCE: &str = "/fapi/v2/balance";
    pub const FUTURES_TICKER_PRICE: &str = "/fapi/v1/ticker/price";
    pub const FUTURES_TICKER_24HR: &str = "/fapi/v1/ticker/24hr";
    pub const FUTURES_PREMIUM_INDEX: &str = "/fapi/v1/premiumIndex";
    pub const FUTURES_OPEN_INTEREST: &str = "/fapi/v1/openInterest";
    pub const FUTURES_DEPTH: &str = "/fapi/v1/depth";
    pub const FUTURES_ORDER: &str = "/fapi/v1/order";
    pub const FUTURES_LEVERAGE: &str = "/fapi/v1/leverage";
    pub const FUTURES_POSITION_RISK: &str = "/fapi/v2/positionRisk";
    pub const FUTURES_FUNDING_RATE: &str = "/fapi/v1/fundingRate";
    pub const FUTURES_OPEN_INTEREST_HIST: &str = "/futures/data/openInterestHist";
    pub const FUTURES_LISTEN_KEY: &str = "/fapi/v1/listenKey";
}
