//! Phemex-specific response models.

use serde::{Deserialize, Serialize};

/// Phemex API response wrapper.
#[derive(Debug, Deserialize)]
pub struct PhemexResponse<T> {
    pub code: i32,
    #[serde(default)]
    pub msg: String,
    pub data: Option<T>,
}

impl<T> PhemexResponse<T> {
    pub fn is_success(&self) -> bool {
        self.code == 0
    }
}

/// Phemex API response with result field (for market data endpoints).
#[derive(Debug, Deserialize)]
pub struct PhemexResultResponse<T> {
    pub error: Option<serde_json::Value>,
    pub id: Option<i64>,
    pub result: Option<T>,
}

impl<T> PhemexResultResponse<T> {
    pub fn is_success(&self) -> bool {
        self.error.is_none() || self.error.as_ref().map(|e| e.is_null()).unwrap_or(true)
    }
}

/// Phemex server time response.
#[derive(Debug, Deserialize)]
pub struct ServerTimeData {
    #[serde(rename = "serverTime")]
    pub server_time: i64,
}

/// Phemex wallet balance response (for spot).
#[derive(Debug, Deserialize)]
pub struct SpotWalletData {
    pub userID: i64,
    #[serde(default)]
    pub wallets: Vec<SpotWallet>,
}

#[derive(Debug, Deserialize)]
pub struct SpotWallet {
    pub currency: String,
    #[serde(rename = "balanceEv")]
    pub balance_ev: i64,
    #[serde(rename = "lockedTradingBalanceEv")]
    pub locked_trading_balance_ev: i64,
    #[serde(rename = "lockedWithdrawEv")]
    pub locked_withdraw_ev: i64,
}

/// Phemex account positions response (for futures).
#[derive(Debug, Deserialize)]
pub struct AccountPositionsData {
    pub account: AccountInfo,
    #[serde(default)]
    pub positions: Vec<PositionInfo>,
}

#[derive(Debug, Deserialize)]
pub struct AccountInfo {
    #[serde(rename = "accountBalanceEv")]
    pub account_balance_ev: Option<i64>,
    #[serde(rename = "accountBalanceRv")]
    pub account_balance_rv: Option<String>,
    #[serde(rename = "availBalanceEv")]
    pub avail_balance_ev: Option<i64>,
    #[serde(rename = "availBalanceRv")]
    pub avail_balance_rv: Option<String>,
    pub currency: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PositionInfo {
    pub symbol: String,
    pub side: String,
    #[serde(rename = "positionStatus")]
    pub position_status: Option<String>,
    #[serde(rename = "avgEntryPriceEp")]
    pub avg_entry_price_ep: Option<i64>,
    #[serde(rename = "avgEntryPriceRp")]
    pub avg_entry_price_rp: Option<String>,
    pub size: Option<i64>,
    #[serde(rename = "sizeRq")]
    pub size_rq: Option<String>,
    #[serde(rename = "unrealisedPnlEv")]
    pub unrealised_pnl_ev: Option<i64>,
    #[serde(rename = "unrealisedPnlRv")]
    pub unrealised_pnl_rv: Option<String>,
    pub leverage: Option<i64>,
    #[serde(rename = "leverageRr")]
    pub leverage_rr: Option<String>,
    #[serde(rename = "posCostRv")]
    pub pos_cost_rv: Option<String>,
}

/// Phemex ticker response (spot).
#[derive(Debug, Deserialize)]
pub struct SpotTickerData {
    pub symbol: String,
    #[serde(rename = "lastEp")]
    pub last_ep: Option<i64>,
    #[serde(rename = "bidEp")]
    pub bid_ep: Option<i64>,
    #[serde(rename = "askEp")]
    pub ask_ep: Option<i64>,
    #[serde(rename = "highEp")]
    pub high_ep: Option<i64>,
    #[serde(rename = "lowEp")]
    pub low_ep: Option<i64>,
    #[serde(rename = "openEp")]
    pub open_ep: Option<i64>,
    #[serde(rename = "volumeEv")]
    pub volume_ev: Option<i64>,
    #[serde(rename = "turnoverEv")]
    pub turnover_ev: Option<i64>,
    pub timestamp: Option<i64>,
}

/// Phemex ticker response (perpetual v2).
#[derive(Debug, Deserialize)]
pub struct PerpTickerData {
    pub symbol: String,
    #[serde(rename = "closeRp")]
    pub close_rp: Option<String>,
    #[serde(rename = "highRp")]
    pub high_rp: Option<String>,
    #[serde(rename = "lowRp")]
    pub low_rp: Option<String>,
    #[serde(rename = "openRp")]
    pub open_rp: Option<String>,
    #[serde(rename = "indexPriceRp")]
    pub index_price_rp: Option<String>,
    #[serde(rename = "markPriceRp")]
    pub mark_price_rp: Option<String>,
    #[serde(rename = "fundingRateRr")]
    pub funding_rate_rr: Option<String>,
    #[serde(rename = "predFundingRateRr")]
    pub pred_funding_rate_rr: Option<String>,
    #[serde(rename = "openInterestRv")]
    pub open_interest_rv: Option<String>,
    #[serde(rename = "volumeRq")]
    pub volume_rq: Option<String>,
    #[serde(rename = "turnoverRv")]
    pub turnover_rv: Option<String>,
    pub timestamp: Option<String>,
}

/// Phemex orderbook response.
#[derive(Debug, Deserialize)]
pub struct OrderbookData {
    #[serde(default)]
    pub book: OrderbookBook,
    #[serde(default)]
    pub symbol: String,
    #[serde(default)]
    pub timestamp: i64,
}

#[derive(Debug, Default, Deserialize)]
pub struct OrderbookBook {
    #[serde(default)]
    pub bids: Vec<Vec<serde_json::Value>>,
    #[serde(default)]
    pub asks: Vec<Vec<serde_json::Value>>,
}

/// Phemex order response.
#[derive(Debug, Deserialize)]
pub struct OrderData {
    #[serde(rename = "orderID")]
    pub order_id: Option<String>,
    #[serde(rename = "clOrdID")]
    pub cl_ord_id: Option<String>,
    pub symbol: Option<String>,
    pub side: Option<String>,
    #[serde(rename = "ordType")]
    pub ord_type: Option<String>,
    #[serde(rename = "ordStatus")]
    pub ord_status: Option<String>,
    #[serde(rename = "priceEp")]
    pub price_ep: Option<i64>,
    #[serde(rename = "priceRp")]
    pub price_rp: Option<String>,
    #[serde(rename = "orderQty")]
    pub order_qty: Option<i64>,
    #[serde(rename = "orderQtyRq")]
    pub order_qty_rq: Option<String>,
    #[serde(rename = "cumQty")]
    pub cum_qty: Option<i64>,
    #[serde(rename = "cumQtyRq")]
    pub cum_qty_rq: Option<String>,
    #[serde(rename = "avgPriceEp")]
    pub avg_price_ep: Option<i64>,
    #[serde(rename = "avgPriceRp")]
    pub avg_price_rp: Option<String>,
    #[serde(rename = "cumValueEv")]
    pub cum_value_ev: Option<i64>,
    #[serde(rename = "cumFeeEv")]
    pub cum_fee_ev: Option<i64>,
    #[serde(rename = "createTimeNs")]
    pub create_time_ns: Option<i64>,
}

/// Phemex funding rate history response.
#[derive(Debug, Deserialize)]
pub struct FundingRateHistoryData {
    pub rows: Vec<PhemexFundingRateEntry>,
}

/// Phemex-specific funding rate entry (renamed to avoid clash with core FundingRateEntry).
#[derive(Debug, Deserialize)]
pub struct PhemexFundingRateEntry {
    pub symbol: String,
    #[serde(rename = "fundingRateEr")]
    pub funding_rate_er: Option<i64>,
    #[serde(rename = "fundingRateRr")]
    pub funding_rate_rr: Option<String>,
    #[serde(rename = "fundingTime")]
    pub funding_time: i64,
}

/// WebSocket authentication request.
#[derive(Debug, Serialize)]
pub struct WsAuthRequest {
    pub id: i64,
    pub method: String,
    pub params: Vec<String>,
}

/// WebSocket subscription request.
#[derive(Debug, Serialize)]
pub struct WsSubscribeRequest {
    pub id: i64,
    pub method: String,
    pub params: Vec<String>,
}

/// WebSocket general response.
#[derive(Debug, Deserialize)]
pub struct WsResponse {
    pub id: Option<i64>,
    pub error: Option<serde_json::Value>,
    pub result: Option<serde_json::Value>,
}

/// WebSocket message (for data streams).
#[derive(Debug, Deserialize)]
pub struct WsMessage {
    #[serde(default)]
    pub sequence: i64,
    #[serde(default)]
    pub symbol: String,
    #[serde(rename = "type")]
    pub msg_type: Option<String>,
    #[serde(default)]
    pub trades: Option<Vec<serde_json::Value>>,
    #[serde(default)]
    pub book: Option<OrderbookBook>,
    #[serde(default)]
    pub timestamp: i64,
}

/// WebSocket order update message.
#[derive(Debug, Deserialize)]
pub struct WsOrderUpdate {
    #[serde(rename = "orderID")]
    pub order_id: Option<String>,
    #[serde(rename = "clOrdID")]
    pub cl_ord_id: Option<String>,
    pub symbol: Option<String>,
    pub side: Option<String>,
    #[serde(rename = "ordType")]
    pub ord_type: Option<String>,
    #[serde(rename = "ordStatus")]
    pub ord_status: Option<String>,
    #[serde(rename = "priceRp")]
    pub price_rp: Option<String>,
    #[serde(rename = "orderQtyRq")]
    pub order_qty_rq: Option<String>,
    #[serde(rename = "cumQtyRq")]
    pub cum_qty_rq: Option<String>,
    #[serde(rename = "avgPriceRp")]
    pub avg_price_rp: Option<String>,
    #[serde(rename = "execFeeRv")]
    pub exec_fee_rv: Option<String>,
    #[serde(rename = "transactTimeNs")]
    pub transact_time_ns: Option<i64>,
    #[serde(rename = "createTimeNs")]
    pub create_time_ns: Option<i64>,
}

/// Phemex products response for linear perpetuals.
#[derive(Debug, Deserialize)]
pub struct ProductInfo {
    pub symbol: String,
    #[serde(rename = "priceScale")]
    pub price_scale: Option<i32>,
    #[serde(rename = "ratioScale")]
    pub ratio_scale: Option<i32>,
    #[serde(rename = "valueScale")]
    pub value_scale: Option<i32>,
    #[serde(rename = "qtyStepSize")]
    pub qty_step_size: Option<String>,
    #[serde(rename = "tickSize")]
    pub tick_size: Option<String>,
    #[serde(rename = "maxLeverage")]
    pub max_leverage: Option<i32>,
}
