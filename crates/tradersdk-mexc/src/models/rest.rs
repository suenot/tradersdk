//! MEXC REST API response models.

use serde::Deserialize;

// === Spot API Models ===

#[derive(Debug, Deserialize)]
pub struct SpotBalanceResponse {
    pub balances: Vec<SpotBalance>,
}

#[derive(Debug, Deserialize)]
pub struct SpotBalance {
    pub asset: String,
    pub free: String,
    pub locked: String,
}

#[derive(Debug, Deserialize)]
pub struct SpotOrderResponse {
    #[serde(rename = "symbol")]
    pub symbol: String,
    #[serde(rename = "orderId")]
    pub order_id: String,
    #[serde(rename = "executedQty")]
    pub executed_qty: Option<String>,
    #[serde(rename = "cummulativeQuoteQty")]
    pub cummulative_quote_qty: Option<String>,
    pub price: Option<String>,
    pub status: Option<String>,
    pub side: Option<String>,
    #[serde(rename = "type")]
    pub order_type: Option<String>,
    #[serde(rename = "origQty")]
    pub orig_qty: Option<String>,
    pub time: Option<i64>,
}

// === Futures API Models ===

#[derive(Debug, Deserialize)]
pub struct FuturesResponse<T> {
    pub success: bool,
    #[serde(rename = "code")]
    pub code: Option<i32>,
    pub data: Option<T>,
    pub message: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct FuturesBalance {
    pub currency: String,
    #[serde(rename = "availableBalance")]
    pub available_balance: f64,
    #[serde(rename = "frozenBalance")]
    pub frozen_balance: f64,
}

#[derive(Debug, Deserialize)]
pub struct FuturesOrderResult {
    #[serde(rename = "orderId")]
    pub order_id: i64,
    #[serde(rename = "symbol")]
    pub symbol: String,
    #[serde(rename = "dealAvgPrice")]
    pub deal_avg_price: Option<f64>,
    #[serde(rename = "dealVol")]
    pub deal_vol: Option<f64>,
}

#[derive(Debug, Deserialize)]
pub struct FuturesPosition {
    pub symbol: String,
    #[serde(rename = "holdVol")]
    pub hold_vol: f64,
    #[serde(rename = "positionType")]
    pub position_type: i32, // 1: long, 2: short
    #[serde(rename = "openAvgPrice")]
    pub open_avg_price: f64,
}

#[derive(Debug, Deserialize)]
pub struct FuturesOrderInfo {
    #[serde(rename = "orderId")]
    pub order_id: i64,
    #[serde(rename = "symbol")]
    pub symbol: String,
    pub side: i32,
    #[serde(rename = "orderType")]
    pub order_type: i32,
    pub price: f64,
    pub vol: f64,
    #[serde(rename = "dealVol")]
    pub deal_vol: f64,
    #[serde(rename = "dealAvgPrice")]
    pub deal_avg_price: Option<f64>,
    pub state: i32,
    #[serde(rename = "createTime")]
    pub create_time: i64,
}
