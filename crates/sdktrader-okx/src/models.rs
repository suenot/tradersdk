//! OKX-specific response models.

use serde::{Deserialize, Serialize};

/// OKX API response wrapper.
#[derive(Debug, Deserialize)]
pub struct OkxResponse<T> {
    pub code: String,
    pub msg: String,
    pub data: Option<T>,
}

impl<T> OkxResponse<T> {
    pub fn is_success(&self) -> bool {
        self.code == "0"
    }
}

/// OKX ticker data from /api/v5/market/ticker.
#[derive(Debug, Deserialize)]
pub struct OkxTickerData {
    #[serde(rename = "instId")]
    pub inst_id: String,
    #[serde(rename = "last")]
    pub last: String,
    #[serde(rename = "bidPx")]
    pub bid_px: Option<String>,
    #[serde(rename = "askPx")]
    pub ask_px: Option<String>,
    #[serde(rename = "high24h")]
    pub high_24h: Option<String>,
    #[serde(rename = "low24h")]
    pub low_24h: Option<String>,
    #[serde(rename = "vol24h")]
    pub vol_24h: Option<String>,
    #[serde(rename = "volCcy24h")]
    pub vol_ccy_24h: Option<String>,
    pub ts: Option<String>,
}

/// OKX funding rate data from /api/v5/public/funding-rate.
#[derive(Debug, Deserialize)]
pub struct OkxFundingRateData {
    #[serde(rename = "instId")]
    pub inst_id: String,
    #[serde(rename = "instType")]
    pub inst_type: String,
    #[serde(rename = "fundingRate")]
    pub funding_rate: String,
    #[serde(rename = "nextFundingRate")]
    pub next_funding_rate: Option<String>,
    #[serde(rename = "fundingTime")]
    pub funding_time: String,
    #[serde(rename = "nextFundingTime")]
    pub next_funding_time: Option<String>,
}

/// OKX funding rate history data from /api/v5/public/funding-rate-history.
#[derive(Debug, Deserialize)]
pub struct OkxFundingRateHistoryData {
    #[serde(rename = "instId")]
    pub inst_id: String,
    #[serde(rename = "instType")]
    pub inst_type: String,
    #[serde(rename = "fundingRate")]
    pub funding_rate: String,
    #[serde(rename = "realizedRate")]
    pub realized_rate: String,
    #[serde(rename = "fundingTime")]
    pub funding_time: String,
}

/// OKX open interest data from /api/v5/public/open-interest.
#[derive(Debug, Deserialize)]
pub struct OkxOpenInterestData {
    #[serde(rename = "instId")]
    pub inst_id: String,
    #[serde(rename = "instType")]
    pub inst_type: String,
    /// Open interest in contracts.
    pub oi: String,
    /// Open interest in coin.
    #[serde(rename = "oiCcy")]
    pub oi_ccy: Option<String>,
    /// Open interest in USD.
    #[serde(rename = "oiUsd")]
    pub oi_usd: Option<String>,
    pub ts: String,
}

/// OKX mark price data from /api/v5/public/mark-price.
#[derive(Debug, Deserialize)]
pub struct OkxMarkPriceData {
    #[serde(rename = "instId")]
    pub inst_id: String,
    #[serde(rename = "instType")]
    pub inst_type: String,
    #[serde(rename = "markPx")]
    pub mark_px: String,
    pub ts: String,
}

/// OKX index ticker data from /api/v5/market/index-tickers.
#[derive(Debug, Deserialize)]
pub struct OkxIndexTickerData {
    #[serde(rename = "instId")]
    pub inst_id: String,
    #[serde(rename = "idxPx")]
    pub idx_px: String,
    pub ts: String,
}

/// OKX position data from /api/v5/account/positions.
#[derive(Debug, Clone, Deserialize)]
pub struct OkxPositionData {
    #[serde(rename = "instId")]
    pub inst_id: String,
    #[serde(rename = "instType")]
    pub inst_type: String,
    #[serde(rename = "mgnMode")]
    pub mgn_mode: String,
    #[serde(rename = "posId")]
    pub pos_id: String,
    #[serde(rename = "posSide")]
    pub pos_side: String,
    pub pos: String,
    #[serde(rename = "availPos")]
    pub avail_pos: Option<String>,
    #[serde(rename = "avgPx")]
    pub avg_px: String,
    #[serde(rename = "upl")]
    pub upl: Option<String>,
    pub lever: Option<String>,
    #[serde(rename = "markPx")]
    pub mark_px: Option<String>,
    #[serde(rename = "liqPx")]
    pub liq_px: Option<String>,
    #[serde(rename = "cTime")]
    pub c_time: Option<String>,
    #[serde(rename = "uTime")]
    pub u_time: Option<String>,
}

/// WebSocket login request.
#[derive(Debug, Serialize)]
pub struct WsLoginRequest {
    pub op: String,
    pub args: Vec<WsLoginArgs>,
}

/// WebSocket login arguments.
#[derive(Debug, Serialize)]
pub struct WsLoginArgs {
    #[serde(rename = "apiKey")]
    pub api_key: String,
    pub passphrase: String,
    pub timestamp: String,
    pub sign: String,
}

/// WebSocket login response.
#[derive(Debug, Deserialize)]
pub struct WsLoginResponse {
    #[serde(rename = "event")]
    pub event: String,
    pub code: String,
    pub msg: Option<String>,
    #[serde(rename = "connId")]
    pub conn_id: Option<String>,
}

/// WebSocket order request.
#[derive(Debug, Serialize)]
pub struct WsOrderRequest {
    pub id: String,
    pub op: String,
    pub args: Vec<serde_json::Value>,
}

/// WebSocket order response.
#[derive(Debug, Deserialize)]
pub struct WsOrderResponse {
    pub id: Option<String>,
    #[serde(rename = "event")]
    pub event: Option<String>,
    pub code: String,
    pub msg: Option<String>,
    pub data: Option<Vec<WsOrderData>>,
    #[serde(rename = "connId")]
    pub conn_id: Option<String>,
}

/// WebSocket order data.
#[derive(Debug, Deserialize)]
pub struct WsOrderData {
    #[serde(rename = "ordId")]
    pub ord_id: Option<String>,
    #[serde(rename = "clOrdId")]
    pub cl_ord_id: Option<String>,
    #[serde(rename = "sCode")]
    pub s_code: Option<String>,
    #[serde(rename = "sMsg")]
    pub s_msg: Option<String>,
}

/// WebSocket public message (for orderbook, trades, ticker).
#[derive(Debug, Deserialize)]
pub struct WsPublicMessage {
    pub arg: Option<WsChannelArg>,
    pub action: Option<String>,
    pub data: Option<serde_json::Value>,
}

/// WebSocket channel argument.
#[derive(Debug, Deserialize)]
pub struct WsChannelArg {
    pub channel: String,
    #[serde(rename = "instId")]
    pub inst_id: Option<String>,
}
