//! Hyperliquid REST API response models.
//!
//! Hyperliquid uses a different API structure than most CEXes.
//! All requests go to /info (POST) with a "type" field specifying the query.

use serde::Deserialize;

/// Asset metadata from the "meta" endpoint.
#[derive(Debug, Clone, Deserialize)]
pub struct AssetMeta {
    pub name: String,
    #[serde(rename = "szDecimals")]
    pub sz_decimals: i32,
    #[serde(rename = "maxLeverage")]
    pub max_leverage: Option<i32>,
    #[serde(rename = "onlyIsolated")]
    pub only_isolated: Option<bool>,
}

/// Asset context from "metaAndAssetCtxs" endpoint.
/// Contains funding rate, open interest, mark price, etc.
#[derive(Debug, Clone, Deserialize)]
pub struct AssetContext {
    /// Current funding rate (hourly, multiply by 24*365 for annual).
    pub funding: Option<String>,
    /// Open interest in contracts.
    #[serde(rename = "openInterest")]
    pub open_interest: Option<String>,
    /// Previous day's funding rate.
    #[serde(rename = "prevDayPx")]
    pub prev_day_px: Option<String>,
    /// 24h volume.
    #[serde(rename = "dayNtlVlm")]
    pub day_ntl_vlm: Option<String>,
    /// Premium for funding calculation.
    pub premium: Option<String>,
    /// Oracle price (index price).
    #[serde(rename = "oraclePx")]
    pub oracle_px: Option<String>,
    /// Mark price.
    #[serde(rename = "markPx")]
    pub mark_px: Option<String>,
    /// Mid price.
    #[serde(rename = "midPx")]
    pub mid_px: Option<String>,
    /// Impact prices for large orders.
    #[serde(rename = "impactPxs")]
    pub impact_pxs: Option<Vec<String>>,
}

/// Combined meta and asset contexts response.
/// This is the response from "metaAndAssetCtxs".
#[derive(Debug, Clone, Deserialize)]
pub struct MetaAndAssetCtxs {
    /// Array of [meta, asset_contexts].
    /// First element is universe metadata, second is array of asset contexts.
    #[serde(flatten)]
    pub data: serde_json::Value,
}

/// Universe metadata response.
#[derive(Debug, Clone, Deserialize)]
pub struct UniverseMeta {
    pub universe: Vec<AssetMeta>,
}

/// L2 orderbook response.
#[derive(Debug, Clone, Deserialize)]
pub struct L2BookResponse {
    pub levels: Vec<Vec<L2Level>>,
}

/// Single level in L2 orderbook.
#[derive(Debug, Clone, Deserialize)]
pub struct L2Level {
    /// Price.
    pub px: String,
    /// Size.
    pub sz: String,
    /// Number of orders at this level.
    #[serde(rename = "n")]
    pub n_orders: Option<i32>,
}

/// Funding history entry.
#[derive(Debug, Clone, Deserialize)]
pub struct FundingHistoryEntry {
    pub coin: String,
    #[serde(rename = "fundingRate")]
    pub funding_rate: String,
    pub premium: Option<String>,
    pub time: i64,
}

/// Clearinghouse state (account balance).
#[derive(Debug, Clone, Deserialize)]
pub struct ClearinghouseState {
    #[serde(rename = "marginSummary")]
    pub margin_summary: MarginSummary,
    #[serde(rename = "crossMarginSummary")]
    pub cross_margin_summary: Option<MarginSummary>,
    #[serde(rename = "assetPositions")]
    pub asset_positions: Option<Vec<AssetPosition>>,
}

/// Margin summary.
#[derive(Debug, Clone, Deserialize)]
pub struct MarginSummary {
    #[serde(rename = "accountValue")]
    pub account_value: String,
    #[serde(rename = "totalMarginUsed")]
    pub total_margin_used: Option<String>,
    #[serde(rename = "totalNtlPos")]
    pub total_ntl_pos: Option<String>,
    #[serde(rename = "totalRawUsd")]
    pub total_raw_usd: Option<String>,
}

/// Asset position.
#[derive(Debug, Clone, Deserialize)]
pub struct AssetPosition {
    pub position: PositionData,
}

/// Position data.
#[derive(Debug, Clone, Deserialize)]
pub struct PositionData {
    pub coin: String,
    /// Size (positive for long, negative for short).
    pub szi: String,
    /// Entry price.
    #[serde(rename = "entryPx")]
    pub entry_px: Option<String>,
    /// Position value.
    #[serde(rename = "positionValue")]
    pub position_value: Option<String>,
    /// Unrealized PnL.
    #[serde(rename = "unrealizedPnl")]
    pub unrealized_pnl: Option<String>,
    /// Margin used.
    #[serde(rename = "marginUsed")]
    pub margin_used: Option<String>,
    /// Leverage settings.
    pub leverage: Option<LeverageData>,
}

/// Leverage data.
#[derive(Debug, Clone, Deserialize)]
pub struct LeverageData {
    #[serde(rename = "type")]
    pub leverage_type: String,
    pub value: Option<i32>,
    #[serde(rename = "rawUsd")]
    pub raw_usd: Option<String>,
}

/// Order status response.
#[derive(Debug, Clone, Deserialize)]
pub struct OrderStatusResponse {
    pub order: Option<OrderData>,
    pub status: String,
    #[serde(rename = "statusTimestamp")]
    pub status_timestamp: Option<i64>,
}

/// Order data.
#[derive(Debug, Clone, Deserialize)]
pub struct OrderData {
    pub coin: String,
    pub side: String,
    #[serde(rename = "limitPx")]
    pub limit_px: String,
    pub sz: String,
    pub oid: i64,
    pub timestamp: i64,
    #[serde(rename = "origSz")]
    pub orig_sz: Option<String>,
    pub cloid: Option<String>,
}

/// Open orders response.
#[derive(Debug, Clone, Deserialize)]
pub struct OpenOrder {
    pub coin: String,
    pub side: String,
    #[serde(rename = "limitPx")]
    pub limit_px: String,
    pub sz: String,
    pub oid: i64,
    pub timestamp: i64,
    #[serde(rename = "origSz")]
    pub orig_sz: Option<String>,
    pub cloid: Option<String>,
    #[serde(rename = "reduceOnly")]
    pub reduce_only: Option<bool>,
    #[serde(rename = "orderType")]
    pub order_type: Option<String>,
    pub tif: Option<String>,
}

/// Exchange response for order operations.
#[derive(Debug, Clone, Deserialize)]
pub struct ExchangeResponse {
    pub status: String,
    pub response: Option<ExchangeResponseData>,
}

/// Exchange response data.
#[derive(Debug, Clone, Deserialize)]
pub struct ExchangeResponseData {
    #[serde(rename = "type")]
    pub response_type: String,
    pub data: Option<OrderResponseData>,
}

/// Order response data.
#[derive(Debug, Clone, Deserialize)]
pub struct OrderResponseData {
    pub statuses: Vec<serde_json::Value>,
}

/// Order placement status (result of placing an order).
///
/// Note: This is distinct from `sdktrader_core::models::OrderStatus` which is an enum
/// representing order lifecycle states (New, Filled, Cancelled, etc.).
#[derive(Debug, Clone, Deserialize)]
pub struct OrderPlacementStatus {
    pub resting: Option<RestingOrder>,
    pub filled: Option<FilledOrder>,
    pub error: Option<String>,
}

/// Resting (open) order.
#[derive(Debug, Clone, Deserialize)]
pub struct RestingOrder {
    pub oid: i64,
    pub cloid: Option<String>,
}

/// Filled order.
#[derive(Debug, Clone, Deserialize)]
pub struct FilledOrder {
    #[serde(rename = "totalSz")]
    pub total_sz: String,
    #[serde(rename = "avgPx")]
    pub avg_px: String,
    pub oid: i64,
    pub cloid: Option<String>,
}
