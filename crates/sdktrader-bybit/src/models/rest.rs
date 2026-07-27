//! Bybit REST API response models.
//!
//! This module contains data structures for parsing responses from
//! Bybit's REST API endpoints.

use serde::Deserialize;

/// Generic Bybit API response wrapper.
///
/// All Bybit REST API responses follow this structure with a return code,
/// message, and optional result payload.
///
/// # Example Response
/// ```json
/// {
///     "retCode": 0,
///     "retMsg": "OK",
///     "result": { ... }
/// }
/// ```
#[derive(Debug, Deserialize)]
pub struct BybitResponse<T> {
    /// Return code. 0 indicates success, non-zero indicates an error.
    #[serde(rename = "retCode")]
    pub ret_code: i32,

    /// Return message describing the result or error.
    #[serde(rename = "retMsg")]
    pub ret_msg: String,

    /// Optional result payload containing the requested data.
    pub result: Option<T>,
}

impl<T> BybitResponse<T> {
    /// Check if the API response indicates success.
    ///
    /// Returns `true` if `ret_code` is 0, indicating a successful request.
    pub fn is_success(&self) -> bool {
        self.ret_code == 0
    }
}

/// Server time response from `/v5/market/time` endpoint.
///
/// Used to synchronize local time with Bybit server time for
/// request signing and timestamp validation.
#[derive(Debug, Deserialize)]
pub struct ServerTimeResult {
    /// Server time in seconds as a string.
    #[serde(rename = "timeSecond")]
    pub time_second: String,

    /// Server time in nanoseconds as a string (for higher precision).
    #[serde(rename = "timeNano")]
    pub time_nano: String,
}

/// Wallet balance response from `/v5/account/wallet-balance` endpoint.
///
/// Contains a list of account balances for different account types.
#[derive(Debug, Deserialize)]
pub struct WalletBalanceResult {
    /// List of wallet accounts with their balances.
    pub list: Vec<WalletAccount>,
}

/// Individual wallet account balance information.
///
/// Represents the balance of a single account type (e.g., UNIFIED, CONTRACT).
#[derive(Debug, Deserialize)]
pub struct WalletAccount {
    /// Total available balance for trading (as string for precision).
    #[serde(rename = "totalAvailableBalance")]
    pub total_available_balance: String,

    /// Account type: UNIFIED, CONTRACT, SPOT, etc.
    #[serde(rename = "accountType")]
    pub account_type: String,
}

/// Order creation response data from `/v5/order/create` endpoint.
///
/// Contains the order ID of the newly created order.
#[derive(Debug, Deserialize)]
pub struct OrderResultData {
    /// Unique identifier for the created order.
    #[serde(rename = "orderId")]
    pub order_id: String,
}

/// Ticker response from `/v5/market/tickers` endpoint.
///
/// Contains a list of ticker data for requested symbols.
#[derive(Debug, Deserialize)]
pub struct TickerResult {
    /// List of ticker information for each requested symbol.
    pub list: Vec<TickerData>,
}

/// Detailed ticker information for a single trading pair.
///
/// Contains current price, spread, volume, and derivatives-specific
/// data like funding rate and open interest.
#[derive(Debug, Deserialize)]
pub struct TickerData {
    /// Trading symbol (e.g., "BTCUSDT").
    pub symbol: String,

    /// Last traded price.
    #[serde(rename = "lastPrice")]
    pub last_price: String,

    /// Best bid price (highest buy order).
    #[serde(rename = "bid1Price")]
    pub bid1_price: Option<String>,

    /// Best ask price (lowest sell order).
    #[serde(rename = "ask1Price")]
    pub ask1_price: Option<String>,

    /// Highest price in the last 24 hours.
    #[serde(rename = "highPrice24h")]
    pub high_price_24h: Option<String>,

    /// Lowest price in the last 24 hours.
    #[serde(rename = "lowPrice24h")]
    pub low_price_24h: Option<String>,

    /// Trading volume in the last 24 hours (in base currency).
    #[serde(rename = "volume24h")]
    pub volume_24h: Option<String>,

    /// Trading turnover in the last 24 hours (in quote currency).
    #[serde(rename = "turnover24h")]
    pub turnover_24h: Option<String>,

    /// Open interest in contracts (derivatives only).
    #[serde(rename = "openInterest")]
    pub open_interest: Option<String>,

    /// Open interest value in USD (derivatives only).
    #[serde(rename = "openInterestValue")]
    pub open_interest_value: Option<String>,

    /// Current funding rate (perpetual futures only).
    #[serde(rename = "fundingRate")]
    pub funding_rate: Option<String>,

    /// Next funding time in milliseconds (perpetual futures only).
    #[serde(rename = "nextFundingTime")]
    pub next_funding_time: Option<String>,

    /// Mark price used for liquidation calculations.
    #[serde(rename = "markPrice")]
    pub mark_price: Option<String>,

    /// Index price based on spot prices from multiple exchanges.
    #[serde(rename = "indexPrice")]
    pub index_price: Option<String>,
}

/// Orderbook response from `/v5/market/orderbook` endpoint.
///
/// Contains bid and ask levels with prices and quantities.
#[derive(Debug, Deserialize)]
pub struct OrderbookResult {
    /// Trading symbol (may be empty in some responses).
    #[serde(default)]
    pub s: String,

    /// Bid levels as [price, quantity] string pairs, sorted by price descending.
    pub b: Vec<Vec<String>>,

    /// Ask levels as [price, quantity] string pairs, sorted by price ascending.
    pub a: Vec<Vec<String>>,

    /// Timestamp of the orderbook snapshot in milliseconds.
    pub ts: i64,
}

/// Funding rate history response from `/v5/market/funding/history` endpoint.
///
/// Contains historical funding rate data for perpetual futures.
#[derive(Debug, Deserialize)]
pub struct FundingHistoryResult {
    /// List of historical funding rate entries.
    pub list: Vec<FundingEntry>,
}

/// Single funding rate history entry.
///
/// Represents one funding rate settlement event.
#[derive(Debug, Deserialize)]
pub struct FundingEntry {
    /// Trading symbol (e.g., "BTCUSDT").
    pub symbol: String,

    /// Funding rate as a decimal string (e.g., "0.0001" for 0.01%).
    #[serde(rename = "fundingRate")]
    pub funding_rate: String,

    /// Timestamp when this funding rate was applied, in milliseconds.
    #[serde(rename = "fundingRateTimestamp")]
    pub funding_rate_timestamp: String,
}

/// Open interest history response from `/v5/market/open-interest` endpoint.
///
/// Contains historical open interest data for derivatives.
#[derive(Debug, Deserialize)]
pub struct OpenInterestResult {
    /// List of historical open interest entries.
    pub list: Vec<OpenInterestEntry>,
}

/// Single open interest history entry.
///
/// Represents open interest at a specific point in time.
#[derive(Debug, Deserialize)]
pub struct OpenInterestEntry {
    /// Open interest in contracts.
    #[serde(rename = "openInterest")]
    pub open_interest: String,

    /// Timestamp of this data point in milliseconds.
    pub timestamp: String,
}

/// Position information from `/v5/position/list` endpoint.
///
/// Contains details about an open position including size, entry price,
/// and unrealized P&L.
#[derive(Debug, Clone, Deserialize)]
pub struct PositionInfo {
    /// Trading symbol (e.g., "BTCUSDT").
    pub symbol: String,

    /// Position side: "Buy" for long, "Sell" for short.
    pub side: String,

    /// Position size in contracts (as string for precision).
    pub size: String,

    /// Average entry price of the position.
    #[serde(rename = "avgPrice")]
    pub avg_price: String,

    /// Position index for hedge mode: 0 for one-way, 1 for buy-side, 2 for sell-side.
    #[serde(rename = "positionIdx")]
    pub position_idx: i32,

    /// Unrealized profit and loss.
    #[serde(rename = "unrealisedPnl")]
    pub unrealised_pnl: Option<String>,

    /// Current mark price of the position.
    #[serde(rename = "markPrice")]
    pub mark_price: Option<String>,

    /// Leverage applied to this position.
    pub leverage: Option<String>,
}

/// Position list response from `/v5/position/list` endpoint.
///
/// Wraps a list of positions for the queried symbol(s).
#[derive(Debug, Deserialize)]
pub struct PositionListResult {
    /// List of position information.
    pub list: Vec<PositionInfo>,
}
