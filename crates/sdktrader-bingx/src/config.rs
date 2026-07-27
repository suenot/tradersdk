//! BingX API configuration and constants.

// ═══════════════════════════════════════════════════════════════════════════
// REST API URLs
// ═══════════════════════════════════════════════════════════════════════════

/// Mainnet REST API URL.
pub const MAINNET_URL: &str = "https://open-api.bingx.com";
/// Testnet REST API URL (VST = Virtual Standard Trading).
pub const TESTNET_URL: &str = "https://open-api-vst.bingx.com";

// ═══════════════════════════════════════════════════════════════════════════
// WEBSOCKET URLS - MARKET DATA STREAMS
// ═══════════════════════════════════════════════════════════════════════════

/// Mainnet WebSocket URL for swap/perpetual market data.
pub const MAINNET_WS_SWAP_URL: &str = "wss://open-api-swap.bingx.com/swap-market";
/// Mainnet WebSocket URL for spot market data.
pub const MAINNET_WS_SPOT_URL: &str = "wss://open-api-ws.bingx.com/market";
/// Testnet WebSocket URL for swap/perpetual market data.
pub const TESTNET_WS_SWAP_URL: &str = "wss://open-api-vst.bingx.com/swap-market";

// ═══════════════════════════════════════════════════════════════════════════
// WEBSOCKET URLS - PRIVATE/USER DATA STREAM
// ═══════════════════════════════════════════════════════════════════════════

/// Mainnet WebSocket URL for private user data stream.
pub const MAINNET_WS_USER_URL: &str = "wss://open-api-ws.bingx.com/market";

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
    // ═══════════════════════════════════════════════════════════════════════
    // SWAP (PERPETUAL FUTURES) ENDPOINTS
    // ═══════════════════════════════════════════════════════════════════════

    /// Swap server time endpoint.
    pub const SWAP_SERVER_TIME: &str = "/openApi/swap/v2/server/time";
    /// Swap account balance endpoint.
    pub const SWAP_BALANCE: &str = "/openApi/swap/v2/user/balance";
    /// Swap current price endpoint.
    pub const SWAP_PRICE: &str = "/openApi/swap/v2/quote/price";
    /// Swap 24h ticker endpoint.
    pub const SWAP_TICKER: &str = "/openApi/swap/v2/quote/ticker";
    /// Swap orderbook depth endpoint.
    pub const SWAP_DEPTH: &str = "/openApi/swap/v2/quote/depth";
    /// Swap place/query order endpoint.
    pub const SWAP_ORDER: &str = "/openApi/swap/v2/trade/order";
    /// Swap set leverage endpoint.
    pub const SWAP_LEVERAGE: &str = "/openApi/swap/v2/trade/leverage";
    /// Swap positions endpoint.
    pub const SWAP_POSITIONS: &str = "/openApi/swap/v2/user/positions";
    /// Swap close all positions endpoint.
    pub const SWAP_CLOSE_ALL_POSITIONS: &str = "/openApi/swap/v2/trade/closeAllPositions";
    /// Swap funding rate endpoint.
    pub const SWAP_FUNDING_RATE: &str = "/openApi/swap/v2/quote/fundingRate";
    /// Swap open interest endpoint.
    pub const SWAP_OPEN_INTEREST: &str = "/openApi/swap/v2/quote/openInterest";

    // ═══════════════════════════════════════════════════════════════════════
    // SPOT ENDPOINTS
    // ═══════════════════════════════════════════════════════════════════════

    /// Spot account balance endpoint.
    pub const SPOT_BALANCE: &str = "/openApi/spot/v1/account/balance";
    /// Spot current price endpoint.
    pub const SPOT_PRICE: &str = "/openApi/spot/v1/ticker/price";
    /// Spot 24h ticker endpoint.
    pub const SPOT_TICKER: &str = "/openApi/spot/v1/ticker/24hr";
    /// Spot orderbook depth endpoint.
    pub const SPOT_DEPTH: &str = "/openApi/spot/v1/market/depth";
    /// Spot place order endpoint.
    pub const SPOT_ORDER: &str = "/openApi/spot/v1/trade/order";
    /// Spot cancel order endpoint.
    pub const SPOT_CANCEL: &str = "/openApi/spot/v1/trade/cancel";
    /// Spot query order endpoint.
    pub const SPOT_QUERY: &str = "/openApi/spot/v1/trade/query";

    // ═══════════════════════════════════════════════════════════════════════
    // USER DATA STREAM ENDPOINTS
    // ═══════════════════════════════════════════════════════════════════════

    /// User data stream (listenKey) endpoint.
    pub const USER_DATA_STREAM: &str = "/openApi/user/auth/userDataStream";
}
