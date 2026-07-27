//! Gate.io REST API response models.

use serde::Deserialize;

/// Spot account balance response.
#[derive(Debug, Deserialize)]
pub struct SpotAccountBalance {
    /// Currency code (e.g., "USDT", "BTC").
    pub currency: String,
    /// Available balance.
    pub available: String,
    /// Locked balance (in orders).
    pub locked: String,
}

/// Futures account balance response.
#[derive(Debug, Deserialize)]
pub struct FuturesAccountBalance {
    /// Total balance.
    pub total: String,
    /// Available balance.
    pub available: String,
    /// Currency code.
    pub currency: String,
}

/// Spot order response.
#[derive(Debug, Deserialize)]
pub struct SpotOrderResponse {
    /// Order ID.
    pub id: String,
    /// Trading pair (e.g., "BTC_USDT").
    pub currency_pair: String,
    /// Order type ("limit", "market").
    #[serde(rename = "type")]
    pub order_type: String,
    /// Order side ("buy", "sell").
    pub side: String,
    /// Order amount.
    pub amount: String,
    /// Order price (None for market orders).
    pub price: Option<String>,
    /// Average deal price.
    pub avg_deal_price: Option<String>,
    /// Total filled amount.
    pub filled_total: Option<String>,
}

/// Futures order response.
#[derive(Debug, Deserialize)]
pub struct FuturesOrderResponse {
    /// Order ID.
    pub id: i64,
    /// Contract name (e.g., "BTC_USDT").
    pub contract: String,
    /// Order size (positive for long, negative for short).
    pub size: i64,
    /// Order price.
    pub price: Option<String>,
    /// Average fill price.
    pub fill_price: Option<String>,
}
