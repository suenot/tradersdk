use serde::Deserialize;

/// Balance response from Ourbit.
#[derive(Debug, Deserialize)]
pub struct OurbitBalance {
    pub asset: String,
    pub balance: String,
    #[serde(rename = "availableBalance")]
    pub available_balance: String,
    #[serde(rename = "frozenBalance")]
    pub frozen_balance: Option<String>,
}

/// Position response from Ourbit.
#[derive(Debug, Deserialize)]
pub struct OurbitPosition {
    pub symbol: String,
    #[serde(rename = "positionType")]
    pub position_type: i32,
    #[serde(rename = "positionAmt")]
    pub position_amt: String,
    #[serde(rename = "entryPrice")]
    pub entry_price: String,
    pub leverage: i32,
}

/// Order response from Ourbit.
#[derive(Debug, Deserialize)]
pub struct OurbitOrder {
    #[serde(rename = "orderId")]
    pub order_id: String,
    pub symbol: String,
    pub side: i32,
    #[serde(rename = "type")]
    pub order_type: i32,
    pub vol: String,
    #[serde(rename = "dealVol")]
    pub deal_vol: Option<String>,
    pub price: Option<String>,
    #[serde(rename = "avgPrice")]
    pub avg_price: Option<String>,
    pub state: i32,
}

/// Ticker response from Ourbit.
#[derive(Debug, Deserialize)]
pub struct OurbitTicker {
    pub symbol: String,
    #[serde(rename = "lastPrice")]
    pub last_price: String,
    #[serde(rename = "bid1")]
    pub bid1: Option<String>,
    #[serde(rename = "ask1")]
    pub ask1: Option<String>,
    pub volume24: Option<String>,
    #[serde(rename = "riseFallRate")]
    pub rise_fall_rate: Option<String>,
}

/// Generic API response wrapper from Ourbit.
#[derive(Debug, Deserialize)]
pub struct OurbitResponse<T> {
    pub success: bool,
    pub code: i32,
    pub data: Option<T>,
    pub message: Option<String>,
}
