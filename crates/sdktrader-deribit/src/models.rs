//! Deribit-specific response models.

use serde::{Deserialize, Serialize};

/// Deribit JSON-RPC API response wrapper.
#[derive(Debug, Deserialize)]
pub struct DeribitResponse<T> {
    pub jsonrpc: String,
    pub result: Option<T>,
    pub error: Option<DeribitError>,
    #[serde(rename = "usIn")]
    pub us_in: Option<i64>,
    #[serde(rename = "usOut")]
    pub us_out: Option<i64>,
    #[serde(rename = "usDiff")]
    pub us_diff: Option<i64>,
    pub testnet: Option<bool>,
}

impl<T> DeribitResponse<T> {
    pub fn is_success(&self) -> bool {
        self.error.is_none() && self.result.is_some()
    }
}

/// Deribit error structure.
#[derive(Debug, Deserialize)]
pub struct DeribitError {
    pub code: i32,
    pub message: String,
    pub data: Option<DeribitErrorData>,
}

/// Additional error data.
#[derive(Debug, Deserialize)]
pub struct DeribitErrorData {
    pub reason: Option<String>,
    pub param: Option<String>,
}

/// Server time response (result is a timestamp in milliseconds).
pub type ServerTimeResult = i64;

/// Orderbook response structure.
#[derive(Debug, Deserialize)]
pub struct OrderbookResult {
    pub timestamp: i64,
    pub state: String,
    pub settlement_price: Option<f64>,
    pub open_interest: Option<f64>,
    pub min_price: Option<f64>,
    pub max_price: Option<f64>,
    pub mark_price: Option<f64>,
    pub last_price: Option<f64>,
    pub instrument_name: String,
    pub index_price: Option<f64>,
    pub funding_8h: Option<f64>,
    pub current_funding: Option<f64>,
    pub change_id: i64,
    pub bids: Vec<Vec<f64>>,
    pub asks: Vec<Vec<f64>>,
    pub best_bid_price: Option<f64>,
    pub best_bid_amount: Option<f64>,
    pub best_ask_price: Option<f64>,
    pub best_ask_amount: Option<f64>,
}

/// Ticker response structure.
#[derive(Debug, Deserialize)]
pub struct TickerResult {
    pub timestamp: i64,
    pub state: String,
    pub settlement_price: Option<f64>,
    pub open_interest: Option<f64>,
    pub min_price: Option<f64>,
    pub max_price: Option<f64>,
    pub mark_price: Option<f64>,
    pub last_price: Option<f64>,
    pub instrument_name: String,
    pub index_price: Option<f64>,
    pub funding_8h: Option<f64>,
    pub current_funding: Option<f64>,
    pub best_bid_price: Option<f64>,
    pub best_bid_amount: Option<f64>,
    pub best_ask_price: Option<f64>,
    pub best_ask_amount: Option<f64>,
    pub stats: Option<TickerStats>,
    #[serde(rename = "estimated_delivery_price")]
    pub estimated_delivery_price: Option<f64>,
}

/// Ticker statistics.
#[derive(Debug, Deserialize)]
pub struct TickerStats {
    pub volume: Option<f64>,
    pub low: Option<f64>,
    pub high: Option<f64>,
    pub price_change: Option<f64>,
    pub volume_usd: Option<f64>,
}

/// Account summary response structure.
#[derive(Debug, Deserialize)]
pub struct AccountSummaryResult {
    pub currency: String,
    pub balance: f64,
    pub equity: f64,
    pub margin_balance: f64,
    pub available_funds: f64,
    pub available_withdrawal_funds: f64,
    pub initial_margin: f64,
    pub maintenance_margin: f64,
    pub session_upl: Option<f64>,
    pub session_rpl: Option<f64>,
    pub session_funding: Option<f64>,
    pub total_pl: Option<f64>,
    pub options_pl: Option<f64>,
    pub futures_pl: Option<f64>,
    pub delta_total: Option<f64>,
    pub options_gamma: Option<f64>,
    pub options_delta: Option<f64>,
    pub options_theta: Option<f64>,
    pub options_vega: Option<f64>,
}

/// Deribit-specific order result (from REST API response).
#[derive(Debug, Deserialize)]
pub struct DeribitOrderResult {
    pub order: OrderData,
    pub trades: Vec<TradeData>,
}

/// Order data structure.
#[derive(Debug, Clone, Deserialize)]
pub struct OrderData {
    pub order_id: String,
    pub order_state: String,
    pub order_type: String,
    pub instrument_name: String,
    pub direction: String,
    pub amount: f64,
    pub filled_amount: f64,
    pub price: Option<f64>,
    pub average_price: Option<f64>,
    pub time_in_force: String,
    pub post_only: bool,
    pub reduce_only: bool,
    pub commission: Option<f64>,
    pub creation_timestamp: i64,
    pub last_update_timestamp: i64,
    pub label: Option<String>,
    pub max_show: Option<f64>,
    pub is_liquidation: Option<bool>,
    pub api: Option<bool>,
    pub profit_loss: Option<f64>,
    pub trigger_price: Option<f64>,
}

/// Trade data structure.
#[derive(Debug, Clone, Deserialize)]
pub struct TradeData {
    pub trade_id: String,
    pub trade_seq: i64,
    pub timestamp: i64,
    pub instrument_name: String,
    pub direction: String,
    pub price: f64,
    pub amount: f64,
    pub order_id: String,
    pub order_type: String,
    pub fee: f64,
    pub fee_currency: String,
    pub liquidity: String,
    pub index_price: Option<f64>,
    pub mark_price: Option<f64>,
    pub tick_direction: Option<i32>,
    pub state: Option<String>,
    pub self_trade: Option<bool>,
    pub reduce_only: Option<bool>,
    pub post_only: Option<bool>,
    pub iv: Option<f64>,
}

/// Position data structure.
#[derive(Debug, Clone, Deserialize)]
pub struct PositionData {
    pub instrument_name: String,
    pub direction: String,
    pub size: f64,
    pub size_currency: Option<f64>,
    pub average_price: f64,
    pub mark_price: f64,
    pub index_price: f64,
    pub estimated_liquidation_price: Option<f64>,
    pub initial_margin: f64,
    pub maintenance_margin: f64,
    pub floating_profit_loss: f64,
    pub realized_profit_loss: f64,
    pub total_profit_loss: f64,
    pub leverage: i32,
    pub delta: Option<f64>,
    pub kind: String,
    pub settlement_price: Option<f64>,
    pub open_orders_margin: Option<f64>,
}

/// Positions list result (for get_positions endpoint).
pub type PositionsResult = Vec<PositionData>;

/// Instrument data structure.
#[derive(Debug, Clone, Deserialize)]
pub struct InstrumentData {
    pub instrument_name: String,
    pub kind: String,
    pub base_currency: String,
    pub quote_currency: String,
    pub settlement_currency: Option<String>,
    pub counter_currency: Option<String>,
    pub contract_size: f64,
    pub tick_size: f64,
    pub min_trade_amount: f64,
    pub is_active: bool,
    pub settlement_period: Option<String>,
    pub expiration_timestamp: Option<i64>,
    pub creation_timestamp: i64,
    pub taker_commission: f64,
    pub maker_commission: f64,
    pub max_leverage: Option<i32>,
    pub future_type: Option<String>,
    pub option_type: Option<String>,
    pub strike: Option<f64>,
}

/// Instruments list result.
pub type InstrumentsResult = Vec<InstrumentData>;

/// Funding rate history entry.
#[derive(Debug, Clone, Deserialize)]
pub struct FundingRateHistoryEntry {
    pub timestamp: i64,
    pub index_price: f64,
    pub prev_index_price: Option<f64>,
    pub interest_8h: f64,
    pub interest_1h: Option<f64>,
}

/// Funding rate history result.
pub type FundingRateHistoryResult = Vec<FundingRateHistoryEntry>;

/// Open orders result (list of orders).
pub type OpenOrdersResult = Vec<OrderData>;

/// Get order state result.
pub type GetOrderStateResult = OrderData;

/// Cancel order result.
pub type CancelOrderResult = OrderData;

/// WebSocket subscription request.
#[derive(Debug, Serialize)]
pub struct WsSubscribeRequest {
    pub jsonrpc: String,
    pub id: i64,
    pub method: String,
    pub params: WsSubscribeParams,
}

/// WebSocket subscription parameters.
#[derive(Debug, Serialize)]
pub struct WsSubscribeParams {
    pub channels: Vec<String>,
}

/// WebSocket authentication request.
#[derive(Debug, Serialize)]
pub struct WsAuthRequest {
    pub jsonrpc: String,
    pub id: i64,
    pub method: String,
    pub params: WsAuthParams,
}

/// WebSocket authentication parameters (client_credentials grant type).
#[derive(Debug, Serialize)]
pub struct WsAuthParams {
    pub grant_type: String,
    pub client_id: String,
    pub client_secret: String,
}

/// WebSocket authentication response.
#[derive(Debug, Deserialize)]
pub struct WsAuthResponse {
    pub jsonrpc: String,
    pub id: Option<i64>,
    pub result: Option<WsAuthResult>,
    pub error: Option<DeribitError>,
}

/// WebSocket authentication result.
#[derive(Debug, Deserialize)]
pub struct WsAuthResult {
    pub access_token: String,
    pub expires_in: i64,
    pub refresh_token: String,
    pub scope: String,
    pub token_type: String,
}

/// WebSocket message (for parsing incoming messages).
#[derive(Debug, Deserialize)]
pub struct WsMessage {
    pub jsonrpc: String,
    pub method: Option<String>,
    pub params: Option<WsMessageParams>,
    pub id: Option<i64>,
    pub result: Option<serde_json::Value>,
    pub error: Option<DeribitError>,
}

/// WebSocket message parameters.
#[derive(Debug, Deserialize)]
pub struct WsMessageParams {
    pub channel: Option<String>,
    pub data: Option<serde_json::Value>,
}

/// WebSocket order notification data.
#[derive(Debug, Deserialize)]
pub struct WsOrderNotification {
    pub order_id: String,
    pub order_state: String,
    pub order_type: String,
    pub instrument_name: String,
    pub direction: String,
    pub amount: f64,
    pub filled_amount: f64,
    pub price: Option<f64>,
    pub average_price: Option<f64>,
    pub commission: Option<f64>,
    pub creation_timestamp: i64,
    pub last_update_timestamp: i64,
    pub label: Option<String>,
    pub reduce_only: Option<bool>,
}

/// WebSocket trade notification data.
#[derive(Debug, Deserialize)]
pub struct WsTradeNotification {
    pub trade_id: String,
    pub trade_seq: i64,
    pub timestamp: i64,
    pub instrument_name: String,
    pub direction: String,
    pub price: f64,
    pub amount: f64,
    pub order_id: String,
    pub fee: f64,
    pub fee_currency: String,
}
