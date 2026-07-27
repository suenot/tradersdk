//! MEXC configuration constants.

// === Spot API URLs ===
pub const SPOT_MAINNET_URL: &str = "https://api.mexc.com";
pub const SPOT_WS_URL: &str = "wss://wbs.mexc.com/ws";

// === Futures API URLs ===
pub const FUTURES_MAINNET_URL: &str = "https://contract.mexc.com";
pub const FUTURES_WS_URL: &str = "wss://contract.mexc.com/ws";

// === Reconnection settings ===
pub const INITIAL_RECONNECT_DELAY_SECS: u64 = 1;
pub const MAX_RECONNECT_DELAY_SECS: u64 = 60;

// === API Endpoints ===
pub mod endpoints {
    pub mod spot {
        pub const PING: &str = "/api/v3/ping";
        pub const TIME: &str = "/api/v3/time";
        pub const ACCOUNT: &str = "/api/v3/account";
        pub const ORDER: &str = "/api/v3/order";
        pub const TICKER_PRICE: &str = "/api/v3/ticker/price";
        pub const TICKER_24HR: &str = "/api/v3/ticker/24hr";
        pub const DEPTH: &str = "/api/v3/depth";
        pub const USER_DATA_STREAM: &str = "/api/v3/userDataStream";
    }

    pub mod futures {
        pub const PING: &str = "/api/v1/contract/ping";
        pub const TICKER: &str = "/api/v1/contract/ticker";
        pub const DEPTH: &str = "/api/v1/contract/depth";
        pub const FUNDING_RATE_HISTORY: &str = "/api/v1/contract/funding_rate/history";
        pub const ASSETS: &str = "/api/v1/private/account/assets";
        pub const ORDER_SUBMIT: &str = "/api/v1/private/order/submit";
        pub const ORDER_CANCEL: &str = "/api/v1/private/order/cancel";
        pub const ORDER_GET: &str = "/api/v1/private/order/get";
        pub const POSITIONS: &str = "/api/v1/private/position/open_positions";
        pub const CHANGE_LEVERAGE: &str = "/api/v1/private/position/change_leverage";
        pub const LISTEN_KEY: &str = "/api/v1/private/user/listen-key";
    }
}
