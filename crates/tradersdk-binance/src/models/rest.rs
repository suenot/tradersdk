//! REST API response models for Binance.

use serde::Deserialize;

/// Server time response.
#[derive(Debug, Deserialize)]
pub struct ServerTimeResponse {
    #[serde(rename = "serverTime")]
    pub server_time: i64,
}

/// Simple ticker price response.
#[derive(Debug, Deserialize)]
pub struct TickerPriceResponse {
    pub symbol: String,
    pub price: String,
}

/// Order response from REST API.
#[derive(Debug, Deserialize)]
pub struct OrderResponse {
    #[serde(rename = "orderId")]
    pub order_id: i64,
    pub symbol: String,
    pub status: Option<String>,
    #[serde(rename = "executedQty")]
    pub executed_qty: Option<String>,
    #[serde(rename = "origQty")]
    pub orig_qty: Option<String>,
    #[serde(rename = "avgPrice")]
    pub avg_price: Option<String>,
    pub price: Option<String>,
    pub side: Option<String>,
    #[serde(rename = "type")]
    pub order_type: Option<String>,
    #[serde(rename = "time")]
    pub time: Option<i64>,
    #[serde(rename = "updateTime")]
    pub update_time: Option<i64>,
}

/// Balance response (for futures).
#[derive(Debug, Deserialize)]
pub struct BalanceResponse {
    #[serde(rename = "availableBalance")]
    pub available_balance: Option<String>,
    pub balance: Option<String>,
    pub asset: Option<String>,
}

/// Spot account balance entry.
#[derive(Debug, Deserialize)]
pub struct SpotBalanceEntry {
    pub asset: String,
    pub free: String,
    pub locked: String,
}

/// Spot account response.
#[derive(Debug, Deserialize)]
pub struct SpotAccountResponse {
    pub balances: Vec<SpotBalanceEntry>,
}

/// Position risk response (futures).
#[derive(Debug, Deserialize)]
pub struct PositionRiskResponse {
    pub symbol: String,
    #[serde(rename = "positionAmt")]
    pub position_amt: String,
    #[serde(rename = "entryPrice")]
    pub entry_price: Option<String>,
    #[serde(rename = "markPrice")]
    pub mark_price: Option<String>,
    #[serde(rename = "unRealizedProfit")]
    pub unrealized_profit: Option<String>,
    pub leverage: Option<String>,
}

/// Funding rate response.
#[derive(Debug, Deserialize)]
pub struct FundingRateResponse {
    pub symbol: String,
    #[serde(rename = "fundingRate")]
    pub funding_rate: String,
    #[serde(rename = "fundingTime")]
    pub funding_time: i64,
}

/// Open interest history response.
#[derive(Debug, Deserialize)]
pub struct OpenInterestHistResponse {
    pub symbol: String,
    #[serde(rename = "sumOpenInterest")]
    pub sum_open_interest: String,
    #[serde(rename = "sumOpenInterestValue")]
    pub sum_open_interest_value: String,
    pub timestamp: i64,
}

/// Premium index response (mark price, index price, funding).
#[derive(Debug, Deserialize)]
pub struct PremiumIndexResponse {
    pub symbol: String,
    #[serde(rename = "markPrice")]
    pub mark_price: String,
    #[serde(rename = "indexPrice")]
    pub index_price: String,
    #[serde(rename = "lastFundingRate")]
    pub last_funding_rate: Option<String>,
    #[serde(rename = "nextFundingTime")]
    pub next_funding_time: Option<i64>,
}

/// 24hr ticker response.
#[derive(Debug, Deserialize)]
pub struct Ticker24hrResponse {
    pub symbol: String,
    #[serde(rename = "lastPrice")]
    pub last_price: String,
    #[serde(rename = "bidPrice")]
    pub bid_price: Option<String>,
    #[serde(rename = "askPrice")]
    pub ask_price: Option<String>,
    #[serde(rename = "highPrice")]
    pub high_price: Option<String>,
    #[serde(rename = "lowPrice")]
    pub low_price: Option<String>,
    pub volume: Option<String>,
    #[serde(rename = "quoteVolume")]
    pub quote_volume: Option<String>,
    #[serde(rename = "priceChangePercent")]
    pub price_change_percent: Option<String>,
}

/// Open interest response.
#[derive(Debug, Deserialize)]
pub struct OpenInterestResponse {
    pub symbol: String,
    #[serde(rename = "openInterest")]
    pub open_interest: String,
}

/// Leverage response.
#[derive(Debug, Deserialize)]
pub struct LeverageResponse {
    pub leverage: i32,
    #[serde(rename = "maxNotionalValue")]
    pub max_notional_value: Option<String>,
    pub symbol: Option<String>,
}

/// ListenKey response.
#[derive(Debug, Deserialize)]
pub struct ListenKeyResponse {
    #[serde(rename = "listenKey")]
    pub listen_key: String,
}
