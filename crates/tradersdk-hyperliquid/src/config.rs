//! Hyperliquid configuration constants.

// === REST API URLs ===
pub const MAINNET_URL: &str = "https://api.hyperliquid.xyz";
pub const TESTNET_URL: &str = "https://api.hyperliquid-testnet.xyz";

// === WebSocket URLs ===
pub const MAINNET_WS_URL: &str = "wss://api.hyperliquid.xyz/ws";
pub const TESTNET_WS_URL: &str = "wss://api.hyperliquid-testnet.xyz/ws";

// === Reconnection settings ===
pub const INITIAL_RECONNECT_DELAY_SECS: u64 = 1;
pub const MAX_RECONNECT_DELAY_SECS: u64 = 30;

// === Exchange-specific constants ===
/// Funding interval is 1 hour on Hyperliquid (in milliseconds)
pub const FUNDING_INTERVAL_MS: i64 = 3600 * 1000;

// === API Endpoints ===
pub mod endpoints {
    /// Info endpoint (market data, account info)
    pub const INFO: &str = "/info";
    /// Exchange endpoint (trading operations)
    pub const EXCHANGE: &str = "/exchange";
}
