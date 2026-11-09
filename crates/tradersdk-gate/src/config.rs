//! Gate.io configuration constants.

// === API URLs ===
pub const SPOT_MAINNET_URL: &str = "https://api.gateio.ws/api/v4";
pub const FUTURES_MAINNET_URL: &str = "https://api.gateio.ws/api/v4";

// === WebSocket URLs ===
pub const SPOT_WS_URL: &str = "wss://api.gateio.ws/ws/v4/";
pub const FUTURES_WS_URL: &str = "wss://fx-ws.gateio.ws/v4/ws/usdt";

// === WebSocket Trading URLs (for order operations) ===
pub const SPOT_WS_TRADE_URL: &str = "wss://api.gateio.ws/ws/v4/";
pub const FUTURES_WS_TRADE_URL: &str = "wss://fx-ws.gateio.ws/v4/ws/usdt";

// === Reconnection settings ===
pub const INITIAL_RECONNECT_DELAY_SECS: u64 = 1;
pub const MAX_RECONNECT_DELAY_SECS: u64 = 30;

// === API Endpoints ===
pub mod endpoints {
    pub mod spot {
        pub const TIME: &str = "/spot/time";
        pub const ACCOUNTS: &str = "/spot/accounts";
        pub const ORDERS: &str = "/spot/orders";
        pub const TICKERS: &str = "/spot/tickers";
        pub const ORDER_BOOK: &str = "/spot/order_book";
    }

    pub mod futures {
        pub const CONTRACTS: &str = "/futures/usdt/contracts";
        pub const ACCOUNTS: &str = "/futures/usdt/accounts";
        pub const ORDERS: &str = "/futures/usdt/orders";
        pub const TICKERS: &str = "/futures/usdt/tickers";
        pub const POSITIONS: &str = "/futures/usdt/positions";
        pub const ORDER_BOOK: &str = "/futures/usdt/order_book";
        pub const FUNDING_RATE: &str = "/futures/usdt/funding_rate";
    }
}

// === WebSocket Channels ===
pub mod ws_channels {
    pub mod spot {
        pub const ORDER_BOOK: &str = "spot.order_book";
        pub const TRADES: &str = "spot.trades";
        pub const TICKERS: &str = "spot.tickers";
        pub const ORDERS: &str = "spot.orders";
        pub const ORDER_PLACE: &str = "spot.order_place";
        pub const ORDER_CANCEL: &str = "spot.order_cancel";
        pub const ORDER_AMEND: &str = "spot.order_amend";
        pub const PING: &str = "spot.ping";
    }

    pub mod futures {
        pub const ORDER_BOOK: &str = "futures.order_book";
        pub const TRADES: &str = "futures.trades";
        pub const TICKERS: &str = "futures.tickers";
        pub const ORDERS: &str = "futures.orders";
        pub const ORDER_PLACE: &str = "futures.order_place";
        pub const ORDER_CANCEL: &str = "futures.order_cancel";
        pub const ORDER_AMEND: &str = "futures.order_amend";
        pub const PING: &str = "futures.ping";
    }
}
