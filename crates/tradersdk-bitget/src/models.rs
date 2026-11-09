//! Bitget-specific response models.

use serde::{Deserialize, Serialize};

/// Bitget API response wrapper.
#[derive(Debug, Deserialize)]
pub struct BitgetResponse<T> {
    pub code: String,
    pub msg: String,
    #[serde(rename = "requestTime")]
    pub request_time: Option<i64>,
    pub data: Option<T>,
}

impl<T> BitgetResponse<T> {
    pub fn is_success(&self) -> bool {
        self.code == "00000"
    }
}

/// Bitget server time response.
#[derive(Debug, Deserialize)]
pub struct ServerTimeData {
    #[serde(rename = "serverTime")]
    pub server_time: String,
}

/// Bitget spot ticker data.
#[derive(Debug, Deserialize)]
pub struct SpotTickerData {
    pub symbol: String,
    #[serde(rename = "lastPr")]
    pub last_pr: Option<String>,
    #[serde(rename = "bidPr")]
    pub bid_pr: Option<String>,
    #[serde(rename = "askPr")]
    pub ask_pr: Option<String>,
    #[serde(rename = "high24h")]
    pub high_24h: Option<String>,
    #[serde(rename = "low24h")]
    pub low_24h: Option<String>,
    #[serde(rename = "baseVolume")]
    pub base_volume: Option<String>,
    #[serde(rename = "quoteVolume")]
    pub quote_volume: Option<String>,
    #[serde(rename = "change24h")]
    pub change_24h: Option<String>,
    pub ts: Option<String>,
}

/// Bitget futures/perpetual ticker data.
#[derive(Debug, Deserialize)]
pub struct FuturesTickerData {
    pub symbol: String,
    #[serde(rename = "lastPr")]
    pub last_pr: Option<String>,
    #[serde(rename = "bidPr")]
    pub bid_pr: Option<String>,
    #[serde(rename = "askPr")]
    pub ask_pr: Option<String>,
    #[serde(rename = "high24h")]
    pub high_24h: Option<String>,
    #[serde(rename = "low24h")]
    pub low_24h: Option<String>,
    #[serde(rename = "baseVolume")]
    pub base_volume: Option<String>,
    #[serde(rename = "quoteVolume")]
    pub quote_volume: Option<String>,
    #[serde(rename = "usdtVolume")]
    pub usdt_volume: Option<String>,
    #[serde(rename = "change24h")]
    pub change_24h: Option<String>,
    #[serde(rename = "fundingRate")]
    pub funding_rate: Option<String>,
    #[serde(rename = "indexPrice")]
    pub index_price: Option<String>,
    #[serde(rename = "markPrice")]
    pub mark_price: Option<String>,
    #[serde(rename = "holdingAmount")]
    pub holding_amount: Option<String>,
    #[serde(rename = "nextFundingTime")]
    pub next_funding_time: Option<String>,
    pub ts: Option<String>,
}

/// Bitget funding rate history entry.
#[derive(Debug, Deserialize)]
pub struct FundingRateHistoryEntry {
    pub symbol: String,
    #[serde(rename = "fundingRate")]
    pub funding_rate: String,
    #[serde(rename = "fundingTime")]
    pub funding_time: Option<String>,
    #[serde(rename = "fundingRateTimestamp")]
    pub funding_rate_timestamp: Option<String>,
}

/// Bitget open interest data.
#[derive(Debug, Deserialize)]
pub struct OpenInterestData {
    #[serde(rename = "openInterestList")]
    pub open_interest_list: Option<Vec<BitgetOpenInterestEntry>>,
    pub ts: Option<String>,
}

/// Bitget-specific open interest entry (renamed to avoid conflict with tradersdk_core::models::OpenInterestEntry).
#[derive(Debug, Deserialize)]
pub struct BitgetOpenInterestEntry {
    pub symbol: String,
    pub size: Option<String>,
    #[serde(rename = "openInterest")]
    pub open_interest: Option<String>,
}

/// Bitget account balance.
#[derive(Debug, Deserialize)]
pub struct AccountData {
    #[serde(rename = "marginCoin")]
    pub margin_coin: Option<String>,
    #[serde(rename = "accountEquity")]
    pub account_equity: Option<String>,
    pub available: Option<String>,
    #[serde(rename = "crossedAvailableBalance")]
    pub crossed_available_balance: Option<String>,
    #[serde(rename = "isolatedAvailableBalance")]
    pub isolated_available_balance: Option<String>,
    pub locked: Option<String>,
    #[serde(rename = "unrealizedPL")]
    pub unrealized_pl: Option<String>,
}

/// Bitget spot account assets.
#[derive(Debug, Deserialize)]
pub struct SpotAssetData {
    pub coin: String,
    pub available: Option<String>,
    pub frozen: Option<String>,
    pub locked: Option<String>,
    #[serde(rename = "usdtValue")]
    pub usdt_value: Option<String>,
}

/// Bitget position data.
#[derive(Debug, Deserialize)]
pub struct PositionData {
    pub symbol: String,
    #[serde(rename = "holdSide")]
    pub hold_side: Option<String>,
    #[serde(rename = "marginSize")]
    pub margin_size: Option<String>,
    pub available: Option<String>,
    #[serde(rename = "openPriceAvg")]
    pub open_price_avg: Option<String>,
    #[serde(rename = "markPrice")]
    pub mark_price: Option<String>,
    #[serde(rename = "unrealizedPL")]
    pub unrealized_pl: Option<String>,
    pub leverage: Option<String>,
    pub total: Option<String>,
    #[serde(rename = "achievedProfits")]
    pub achieved_profits: Option<String>,
    #[serde(rename = "liquidationPrice")]
    pub liquidation_price: Option<String>,
    #[serde(rename = "marginMode")]
    pub margin_mode: Option<String>,
    #[serde(rename = "cTime")]
    pub c_time: Option<String>,
}

/// Bitget order data.
#[derive(Debug, Deserialize)]
pub struct OrderData {
    #[serde(rename = "orderId")]
    pub order_id: String,
    #[serde(rename = "clientOid")]
    pub client_oid: Option<String>,
    pub symbol: String,
    pub status: Option<String>,
    #[serde(rename = "orderType")]
    pub order_type: Option<String>,
    pub side: Option<String>,
    pub price: Option<String>,
    #[serde(rename = "size")]
    pub size: Option<String>,
    #[serde(rename = "baseVolume")]
    pub base_volume: Option<String>,
    #[serde(rename = "quoteVolume")]
    pub quote_volume: Option<String>,
    #[serde(rename = "priceAvg")]
    pub price_avg: Option<String>,
    pub fee: Option<String>,
    #[serde(rename = "feeCcy")]
    pub fee_ccy: Option<String>,
    #[serde(rename = "cTime")]
    pub c_time: Option<String>,
    #[serde(rename = "uTime")]
    pub u_time: Option<String>,
}

/// Bitget futures order data.
#[derive(Debug, Deserialize)]
pub struct FuturesOrderData {
    #[serde(rename = "orderId")]
    pub order_id: String,
    #[serde(rename = "clientOid")]
    pub client_oid: Option<String>,
    pub symbol: String,
    pub state: Option<String>,
    #[serde(rename = "orderType")]
    pub order_type: Option<String>,
    pub side: Option<String>,
    pub price: Option<String>,
    #[serde(rename = "size")]
    pub size: Option<String>,
    #[serde(rename = "filledQty")]
    pub filled_qty: Option<String>,
    #[serde(rename = "priceAvg")]
    pub price_avg: Option<String>,
    pub fee: Option<String>,
    #[serde(rename = "feeCcy")]
    pub fee_ccy: Option<String>,
    #[serde(rename = "reduceOnly")]
    pub reduce_only: Option<String>,
    #[serde(rename = "cTime")]
    pub c_time: Option<String>,
    #[serde(rename = "uTime")]
    pub u_time: Option<String>,
}

/// Bitget orderbook data.
#[derive(Debug, Deserialize)]
pub struct OrderbookData {
    pub bids: Vec<Vec<String>>,
    pub asks: Vec<Vec<String>>,
    pub ts: Option<String>,
}

/// WebSocket login request for Bitget.
#[derive(Debug, Serialize)]
pub struct WsLoginRequest {
    pub op: String,
    pub args: Vec<WsLoginArgs>,
}

#[derive(Debug, Serialize)]
pub struct WsLoginArgs {
    #[serde(rename = "apiKey")]
    pub api_key: String,
    pub passphrase: String,
    pub timestamp: String,
    pub sign: String,
}

/// WebSocket login response from Bitget.
#[derive(Debug, Deserialize)]
pub struct WsLoginResponse {
    pub event: String,
    pub code: Option<String>,
    pub msg: Option<String>,
}

/// WebSocket subscribe request for Bitget.
#[derive(Debug, Serialize)]
pub struct WsSubscribeRequest {
    pub op: String,
    pub args: Vec<WsSubscribeArgs>,
}

#[derive(Debug, Serialize)]
pub struct WsSubscribeArgs {
    #[serde(rename = "instType")]
    pub inst_type: String,
    pub channel: String,
    #[serde(rename = "instId")]
    pub inst_id: String,
}

/// WebSocket public message.
#[derive(Debug, Deserialize)]
pub struct WsMessage {
    pub action: Option<String>,
    pub arg: Option<WsMessageArg>,
    pub data: Option<serde_json::Value>,
    pub ts: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct WsMessageArg {
    #[serde(rename = "instType")]
    pub inst_type: Option<String>,
    pub channel: Option<String>,
    #[serde(rename = "instId")]
    pub inst_id: Option<String>,
}

/// WebSocket trade request for Bitget.
#[derive(Debug, Serialize)]
pub struct WsTradeRequest {
    pub id: String,
    pub op: String,
    pub args: Vec<WsTradeArgs>,
}

#[derive(Debug, Serialize)]
pub struct WsTradeArgs {
    #[serde(rename = "instType")]
    pub inst_type: String,
    pub channel: String,
    #[serde(flatten)]
    pub data: serde_json::Value,
}

/// WebSocket trade response from Bitget.
#[derive(Debug, Deserialize)]
pub struct WsTradeResponse {
    #[allow(dead_code)]
    pub id: Option<String>,
    #[allow(dead_code)]
    pub op: Option<String>,
    pub code: String,
    pub msg: Option<String>,
    pub data: Option<serde_json::Value>,
}

/// Bitget current funding rate data.
#[derive(Debug, Deserialize)]
pub struct CurrentFundingRateData {
    pub symbol: String,
    #[serde(rename = "fundingRate")]
    pub funding_rate: String,
    #[serde(rename = "fundingRateInterval")]
    pub funding_rate_interval: Option<String>,
    #[serde(rename = "nextUpdate")]
    pub next_update: Option<String>,
    #[serde(rename = "minFundingRate")]
    pub min_funding_rate: Option<String>,
    #[serde(rename = "maxFundingRate")]
    pub max_funding_rate: Option<String>,
}
