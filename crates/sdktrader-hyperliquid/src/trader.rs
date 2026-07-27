//! Hyperliquid trader implementation using service-based architecture.

use anyhow::{Context, Result};
use async_trait::async_trait;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use sdktrader_core::models::{
    AccountBalance, ExchangeCapabilities, FundingRateEntry, MarketType, OpenInterestEntry,
    OrderInfo, OrderResult, OrderSide, OrderType, OrderUpdateEvent, Orderbook, OrderbookLevel,
    TickerInfo, TimeInForce, WsTestResult,
};
use sdktrader_core::utils::timestamp_ms;
use sdktrader_core::ExchangeTrader;

use crate::config::FUNDING_INTERVAL_MS;
use crate::helpers::{format_price, format_size, parse_order_status, to_hl_symbol};
use crate::services::{MarketStreamService, RestService, UserDataService};

/// Hyperliquid exchange trader using service-based architecture.
pub struct HyperliquidTrader {
    rest_service: RestService,
    market_stream: MarketStreamService,
    user_data: UserDataService,
    market_type: MarketType,
}

impl HyperliquidTrader {
    pub fn new(api_key: &str, api_secret: &str, testnet: bool, market_type: MarketType) -> Self {
        Self {
            rest_service: RestService::new(api_key, api_secret, testnet),
            market_stream: MarketStreamService::new(testnet),
            user_data: UserDataService::new(api_key, testnet),
            market_type,
        }
    }

    async fn place_order_internal(
        &self,
        symbol: &str,
        side: OrderSide,
        order_type: OrderType,
        quantity: f64,
        price: Option<f64>,
        reduce_only: bool,
    ) -> Result<OrderResult> {
        let hl_symbol = to_hl_symbol(symbol);
        let asset_index = self.rest_service.get_asset_index(&hl_symbol).await?;
        let sz_decimals = self.rest_service.get_sz_decimals(&hl_symbol).await?;
        let is_buy = matches!(side, OrderSide::Buy);
        let sz = format_size(quantity, sz_decimals);
        let px = match order_type {
            OrderType::Market => {
                let current_price = self.get_current_price(symbol).await?;
                let slippage = 0.05;
                let price_with_slippage = if is_buy {
                    current_price * (1.0 + slippage)
                } else {
                    current_price * (1.0 - slippage)
                };
                format_price(price_with_slippage)
            }
            OrderType::Limit => match price {
                Some(p) => format_price(p),
                None => return Err(anyhow::anyhow!("Limit order requires a price")),
            },
            _ => return Err(anyhow::anyhow!("Unsupported order type: {:?}", order_type)),
        };
        let order_type_obj = match order_type {
            OrderType::Market => serde_json::json!({"limit": {"tif": "Ioc"}}),
            OrderType::Limit => serde_json::json!({"limit": {"tif": "Gtc"}}),
            _ => return Err(anyhow::anyhow!("Unsupported order type")),
        };
        let order = serde_json::json!({"a": asset_index, "b": is_buy, "p": px, "s": sz, "r": reduce_only, "t": order_type_obj});
        let action = serde_json::json!({"type": "order", "orders": [order], "grouping": "na"});
        let nonce = timestamp_ms();
        let response = self.rest_service.exchange_request(&action, nonce).await?;
        debug!("Hyperliquid order response: {:?}", response);
        let status = response["status"].as_str().unwrap_or("");
        if status != "ok" {
            let error_msg = response["response"]["data"]["statuses"][0]["error"]
                .as_str()
                .or_else(|| response.get("error").and_then(|e| e.as_str()))
                .unwrap_or("Unknown error")
                .to_string();
            return Ok(OrderResult::failure(symbol.to_string(), side, error_msg));
        }
        let statuses = response["response"]["data"]["statuses"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("Invalid response: missing statuses"))?;
        if let Some(first_status) = statuses.first() {
            let order_id = first_status["resting"]["oid"]
                .as_i64()
                .or_else(|| first_status["filled"]["oid"].as_i64())
                .map(|id| id.to_string());
            let avg_price = first_status["filled"]["avgPx"]
                .as_str()
                .and_then(|s| s.parse::<f64>().ok());
            let filled_sz = first_status["filled"]["totalSz"]
                .as_str()
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);
            if let Some(error) = first_status["error"].as_str() {
                return Ok(OrderResult::failure(
                    symbol.to_string(),
                    side,
                    error.to_string(),
                ));
            }
            let raw_map = response
                .as_object()
                .map(|obj| obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect());
            return Ok(OrderResult {
                success: true,
                order_id,
                symbol: symbol.to_string(),
                side,
                order_type,
                quantity: Some(quantity),
                entry_price: avg_price.or(price),
                position_size: Some(filled_sz),
                position_usdt: avg_price.map(|p| filled_sz * p),
                error: None,
                raw_response: raw_map,
            });
        }
        Ok(OrderResult::failure(
            symbol.to_string(),
            side,
            "No order status in response".to_string(),
        ))
    }

    #[allow(dead_code)]
    async fn get_open_orders(&self, symbol: Option<&str>) -> Result<Vec<serde_json::Value>> {
        let payload =
            serde_json::json!({"type": "openOrders", "user": self.rest_service.wallet_address()});
        let response = self.rest_service.info_request(&payload).await?;
        let orders = response.as_array().cloned().unwrap_or_default();
        if let Some(sym) = symbol {
            let hl_symbol = to_hl_symbol(sym);
            Ok(orders
                .into_iter()
                .filter(|o| o["coin"].as_str() == Some(&hl_symbol))
                .collect())
        } else {
            Ok(orders)
        }
    }

    async fn get_positions(&self) -> Result<Vec<serde_json::Value>> {
        let payload = serde_json::json!({"type": "clearinghouseState", "user": self.rest_service.wallet_address()});
        let response = self.rest_service.info_request(&payload).await?;
        let positions = response["assetPositions"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        Ok(positions)
    }
}

#[async_trait]
impl ExchangeTrader for HyperliquidTrader {
    fn exchange_name(&self) -> &str {
        "hyperliquid"
    }
    fn market_type(&self) -> MarketType {
        self.market_type
    }

    fn capabilities(&self) -> ExchangeCapabilities {
        sdktrader_core::exchange_capabilities! {
            swap: Supported,
            create_market_order: Supported,
            create_limit_order: Supported,
            cancel_order: Supported,
            get_order_status: Supported,
            set_leverage: Supported,
            close_position: Supported,
            get_current_price: Supported,
            get_ticker: Supported,
            get_orderbook: Supported,
            get_balance: Supported,
            get_server_time: Supported,
            test_connection: Supported,
            get_funding_rate_history: Supported,
            get_open_interest_history: Supported,
            execute_fast_entry: Supported,
            format_symbol: Supported,
            ws_orderbook: Supported,
            ws_trades: Supported,
            ws_ticker: Supported,
            ws_orders: Supported,
        }
    }

    async fn test_connection(&self) -> Result<bool> {
        self.rest_service.test_connection().await
    }

    async fn get_server_time(&self) -> Result<i64> {
        let payload = serde_json::json!({"type": "exchangeStatus"});
        let response = self.rest_service.info_request(&payload).await?;
        response["time"]
            .as_str()
            .and_then(|s| s.parse::<i64>().ok())
            .or_else(|| response["time"].as_i64())
            .ok_or_else(|| anyhow::anyhow!("Failed to get server time"))
    }

    async fn get_balance(&self) -> Result<AccountBalance> {
        let payload = serde_json::json!({"type": "clearinghouseState", "user": self.rest_service.wallet_address()});
        let response = self.rest_service.info_request(&payload).await?;
        let account_value = response["marginSummary"]["accountValue"]
            .as_str()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        let total_margin_used = response["marginSummary"]["totalMarginUsed"]
            .as_str()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        Ok(AccountBalance::simple(
            account_value - total_margin_used,
            account_value,
            "USDC",
        ))
    }

    async fn get_current_price(&self, symbol: &str) -> Result<f64> {
        let hl_symbol = to_hl_symbol(symbol);
        let payload = serde_json::json!({"type": "allMids"});
        let response = self.rest_service.info_request(&payload).await?;
        response[&hl_symbol]
            .as_str()
            .and_then(|s| s.parse::<f64>().ok())
            .ok_or_else(|| anyhow::anyhow!("Price not found for {} (tried {})", symbol, hl_symbol))
    }

    async fn get_ticker(&self, symbol: &str) -> Result<TickerInfo> {
        let hl_symbol = to_hl_symbol(symbol);
        let payload = serde_json::json!({"type": "metaAndAssetCtxs"});
        let response = self.rest_service.info_request(&payload).await?;
        let meta_array = response
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("Invalid metaAndAssetCtxs response"))?;
        if meta_array.len() < 2 {
            return Err(anyhow::anyhow!(
                "Invalid metaAndAssetCtxs response: missing data"
            ));
        }
        let universe = meta_array[0]["universe"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("Missing universe"))?;
        let asset_ctxs = meta_array[1]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("Missing asset contexts"))?;
        let asset_idx = universe
            .iter()
            .position(|a| a["name"].as_str() == Some(&hl_symbol))
            .ok_or_else(|| anyhow::anyhow!("Asset not found: {}", hl_symbol))?;
        let ctx = &asset_ctxs[asset_idx];
        let mid_px = ctx["midPx"]
            .as_str()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        let mark_px = ctx["markPx"].as_str().and_then(|s| s.parse::<f64>().ok());
        let oracle_px = ctx["oraclePx"].as_str().and_then(|s| s.parse::<f64>().ok());
        let funding_rate = ctx["funding"].as_str().and_then(|s| s.parse::<f64>().ok());
        let open_interest = ctx["openInterest"]
            .as_str()
            .and_then(|s| s.parse::<f64>().ok());
        let day_ntl_vlm = ctx["dayNtlVlm"]
            .as_str()
            .and_then(|s| s.parse::<f64>().ok());
        let prev_day_px = ctx["prevDayPx"]
            .as_str()
            .and_then(|s| s.parse::<f64>().ok());
        let change_24h = prev_day_px.map(|prev| {
            if prev > 0.0 {
                ((mid_px - prev) / prev) * 100.0
            } else {
                0.0
            }
        });
        let open_interest_value = match (open_interest, mark_px) {
            (Some(oi), Some(price)) => Some(oi * price),
            _ => None,
        };
        let now = timestamp_ms();
        let next_funding_time = ((now / FUNDING_INTERVAL_MS) + 1) * FUNDING_INTERVAL_MS;
        let (bid_price, ask_price) = if let Some(impact_pxs) = ctx["impactPxs"].as_array() {
            (
                impact_pxs
                    .first()
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse().ok()),
                impact_pxs
                    .get(1)
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse().ok()),
            )
        } else {
            (None, None)
        };
        Ok(TickerInfo {
            symbol: symbol.to_string(),
            last_price: mid_px,
            bid_price,
            ask_price,
            high_24h: None,
            low_24h: None,
            volume_24h: day_ntl_vlm,
            turnover_24h: day_ntl_vlm,
            change_24h,
            open_interest,
            open_interest_value,
            funding_rate,
            next_funding_time: Some(next_funding_time),
            mark_price: mark_px,
            index_price: oracle_px,
            timestamp: timestamp_ms(),
        })
    }

    async fn get_orderbook(&self, symbol: &str, _depth: u32) -> Result<Orderbook> {
        let hl_symbol = to_hl_symbol(symbol);
        let payload = serde_json::json!({"type": "l2Book", "coin": hl_symbol});
        let response = self.rest_service.info_request(&payload).await?;
        let levels = &response["levels"];
        let bids: Vec<OrderbookLevel> = levels[0]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|level| {
                        Some(OrderbookLevel {
                            price: level["px"].as_str()?.parse().ok()?,
                            quantity: level["sz"].as_str()?.parse().ok()?,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        let asks: Vec<OrderbookLevel> = levels[1]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|level| {
                        Some(OrderbookLevel {
                            price: level["px"].as_str()?.parse().ok()?,
                            quantity: level["sz"].as_str()?.parse().ok()?,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(Orderbook {
            symbol: symbol.to_string(),
            bids,
            asks,
            timestamp: timestamp_ms(),
        })
    }

    async fn get_funding_rate_history(
        &self,
        symbol: &str,
        limit: u32,
    ) -> Result<Vec<FundingRateEntry>> {
        let hl_symbol = to_hl_symbol(symbol);
        let end_time = timestamp_ms();
        let start_time = end_time - (limit as i64 * FUNDING_INTERVAL_MS);
        let payload = serde_json::json!({"type": "fundingHistory", "coin": hl_symbol, "startTime": start_time});
        let response = self.rest_service.info_request(&payload).await?;
        let entries: Vec<FundingRateEntry> = response
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|e| {
                        Some(FundingRateEntry {
                            symbol: symbol.to_string(),
                            funding_rate: e["fundingRate"].as_str()?.parse().ok()?,
                            funding_time: e["time"].as_i64()?,
                        })
                    })
                    .take(limit as usize)
                    .collect()
            })
            .unwrap_or_default();
        Ok(entries)
    }

    async fn get_open_interest_history(
        &self,
        symbol: &str,
        _interval: &str,
        _limit: u32,
    ) -> Result<Vec<OpenInterestEntry>> {
        let ticker = self.get_ticker(symbol).await?;
        Ok(vec![OpenInterestEntry {
            symbol: symbol.to_string(),
            open_interest: ticker.open_interest.unwrap_or(0.0),
            open_interest_value: ticker.open_interest_value.unwrap_or(0.0),
            timestamp: ticker.timestamp,
        }])
    }

    async fn set_leverage(&self, symbol: Option<&str>, leverage: u32) -> Result<bool> {
        let Some(sym) = symbol else {
            return Ok(true);
        };
        let hl_symbol = to_hl_symbol(sym);
        let asset_index = self.rest_service.get_asset_index(&hl_symbol).await?;
        let action = serde_json::json!({"type": "updateLeverage", "asset": asset_index, "isCross": true, "leverage": leverage});
        let nonce = timestamp_ms();
        let response = self.rest_service.exchange_request(&action, nonce).await?;
        let status = response["status"].as_str().unwrap_or("");
        if status == "ok" {
            info!("Hyperliquid: set leverage {} for {}", leverage, sym);
            Ok(true)
        } else {
            let error = response["response"]["data"]["error"]
                .as_str()
                .unwrap_or("Unknown error");
            warn!("Failed to set leverage for {}: {}", sym, error);
            Ok(false)
        }
    }

    async fn execute_fast_entry(
        &self,
        symbol: &str,
        direction: &str,
        usdt_amount: f64,
        leverage: u32,
        _tp_percent: Option<f64>,
        _sl_percent: Option<f64>,
        current_price: Option<f64>,
    ) -> Result<OrderResult> {
        let price = match current_price {
            Some(p) => p,
            None => self.get_current_price(symbol).await?,
        };
        let quantity = (usdt_amount * leverage as f64) / price;
        let side = if direction.to_lowercase() == "long" {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        };
        if let Err(e) = self.set_leverage(Some(symbol), leverage).await {
            warn!("Failed to set leverage: {}", e);
        }
        self.place_order_internal(symbol, side, OrderType::Market, quantity, None, false)
            .await
    }

    async fn place_market_order(
        &self,
        symbol: &str,
        side: OrderSide,
        quantity: f64,
    ) -> Result<OrderResult> {
        self.place_order_internal(symbol, side, OrderType::Market, quantity, None, false)
            .await
    }

    async fn place_limit_order(
        &self,
        symbol: &str,
        side: OrderSide,
        quantity: f64,
        price: f64,
        reduce_only: bool,
    ) -> Result<OrderResult> {
        self.place_order_internal(
            symbol,
            side,
            OrderType::Limit,
            quantity,
            Some(price),
            reduce_only,
        )
        .await
    }

    async fn cancel_order(&self, symbol: &str, order_id: &str) -> Result<bool> {
        let hl_symbol = to_hl_symbol(symbol);
        let asset_index = self.rest_service.get_asset_index(&hl_symbol).await?;
        let oid: i64 = order_id.parse().context("Invalid order ID")?;
        let action =
            serde_json::json!({"type": "cancel", "cancels": [{"a": asset_index, "o": oid}]});
        let nonce = timestamp_ms();
        let response = self.rest_service.exchange_request(&action, nonce).await?;
        let status = response["status"].as_str().unwrap_or("");
        if status == "ok" {
            info!("Hyperliquid: cancelled order {} for {}", order_id, symbol);
            Ok(true)
        } else {
            let error = response["response"]["data"]["statuses"][0]
                .as_str()
                .unwrap_or("Unknown error");
            warn!("Failed to cancel order {}: {}", order_id, error);
            Ok(false)
        }
    }

    async fn get_order_status(&self, symbol: &str, order_id: &str) -> Result<OrderInfo> {
        let oid: i64 = order_id.parse().context("Invalid order ID")?;
        let payload = serde_json::json!({"type": "orderStatus", "user": self.rest_service.wallet_address(), "oid": oid});
        let response = self.rest_service.info_request(&payload).await?;
        let order = &response["order"];
        let status_str = response["status"].as_str().unwrap_or("unknown");
        let side = if order["side"].as_str() == Some("B") {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        };
        let order_type = if order["orderType"].as_str() == Some("Market") {
            OrderType::Market
        } else {
            OrderType::Limit
        };
        let price = order["limitPx"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);
        let quantity = order["origSz"]
            .as_str()
            .or_else(|| order["sz"].as_str())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);
        let filled_quantity = order["filled"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);
        let timestamp = order["timestamp"].as_i64().unwrap_or(0);
        Ok(OrderInfo {
            order_id: order_id.to_string(),
            client_order_id: order["cloid"].as_str().map(|s| s.to_string()),
            symbol: symbol.to_string(),
            status: parse_order_status(status_str),
            side,
            order_type,
            price,
            quantity,
            filled_quantity,
            remaining_quantity: quantity - filled_quantity,
            avg_price: order["avgPx"].as_str().and_then(|s| s.parse().ok()),
            timestamp,
            time_in_force: TimeInForce::GTC,
            reduce_only: order["reduceOnly"].as_bool().unwrap_or(false),
        })
    }

    async fn close_position(&self, symbol: &str) -> Result<OrderResult> {
        let hl_symbol = to_hl_symbol(symbol);
        let positions = self.get_positions().await?;
        let position = positions
            .iter()
            .find(|p| p["position"]["coin"].as_str() == Some(&hl_symbol));
        let Some(pos) = position else {
            return Ok(OrderResult::failure(
                symbol.to_string(),
                OrderSide::Sell,
                "No position found".to_string(),
            ));
        };
        let size: f64 = pos["position"]["szi"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);
        if size.abs() < 0.000001 {
            return Ok(OrderResult::failure(
                symbol.to_string(),
                OrderSide::Sell,
                "Position size is zero".to_string(),
            ));
        }
        let (side, quantity) = if size > 0.0 {
            (OrderSide::Sell, size)
        } else {
            (OrderSide::Buy, size.abs())
        };
        self.place_order_internal(symbol, side, OrderType::Market, quantity, None, true)
            .await
    }

    fn format_symbol(&self, base_symbol: &str) -> String {
        to_hl_symbol(base_symbol)
    }

    async fn test_ws_orderbook(
        &self,
        symbol: &str,
        depth: u32,
        timeout_ms: u64,
    ) -> Result<WsTestResult> {
        self.market_stream
            .test_ws_orderbook(symbol, depth, timeout_ms)
            .await
    }

    async fn test_ws_trades(&self, symbol: &str, timeout_ms: u64) -> Result<WsTestResult> {
        self.market_stream.test_ws_trades(symbol, timeout_ms).await
    }

    async fn test_ws_ticker(&self, symbol: &str, timeout_ms: u64) -> Result<WsTestResult> {
        self.market_stream.test_ws_ticker(symbol, timeout_ms).await
    }

    async fn subscribe_order_updates(
        &self,
        sender: broadcast::Sender<OrderUpdateEvent>,
    ) -> Result<JoinHandle<()>> {
        self.user_data.subscribe_order_updates(sender).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_trader() {
        let trader = HyperliquidTrader::new("0x123", "abc", false, MarketType::Swap);
        assert_eq!(trader.exchange_name(), "hyperliquid");
        assert_eq!(trader.market_type(), MarketType::Swap);
    }

    #[test]
    fn test_new_trader_testnet() {
        let trader = HyperliquidTrader::new("0x123", "abc", true, MarketType::Swap);
        assert_eq!(trader.exchange_name(), "hyperliquid");
    }

    #[test]
    fn test_format_symbol() {
        let trader = HyperliquidTrader::new("0x123", "abc", false, MarketType::Swap);
        assert_eq!(trader.format_symbol("BTCUSDT"), "BTC");
        assert_eq!(trader.format_symbol("BTC/USDT"), "BTC");
        assert_eq!(trader.format_symbol("BTC"), "BTC");
        assert_eq!(trader.format_symbol("ETHPERP"), "ETH");
        assert_eq!(trader.format_symbol("btc"), "BTC");
    }

    #[test]
    fn test_market_type() {
        let swap_trader = HyperliquidTrader::new("0x123", "abc", false, MarketType::Swap);
        assert_eq!(swap_trader.market_type(), MarketType::Swap);
        let spot_trader = HyperliquidTrader::new("0x123", "abc", false, MarketType::Spot);
        assert_eq!(spot_trader.market_type(), MarketType::Spot);
    }
}
