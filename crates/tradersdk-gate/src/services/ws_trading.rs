//! Gate.io WebSocket Trading service.
//!
//! Gate.io supports WebSocket trading for fast order placement (2x faster than REST):
//! - place_order - Place market or limit orders
//! - cancel_order - Cancel existing orders
//! - amend_order - Modify existing orders (price/quantity)

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, error, info};

use crate::config::{FUTURES_WS_TRADE_URL, SPOT_WS_TRADE_URL};
use crate::helpers::ws_auth_signature;
use crate::models::{
    FuturesOrderParams, OrderCancelParams, SpotOrderParams, WsAuth, WsRequest, WsResponse,
};
use tradersdk_core::models::{MarketType, OrderResult, OrderSide, OrderType};
use tradersdk_core::utils::timestamp_ms;

/// WebSocket stream type alias for cleaner code.
type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// Gate.io WebSocket Trading service.
///
/// Provides fast order operations via WebSocket connection.
/// Approximately 2x faster than REST API for latency-sensitive trading.
pub struct WsTradingService {
    api_key: String,
    api_secret: String,
    market_type: MarketType,
}

impl WsTradingService {
    /// Create a new WebSocket trading service.
    ///
    /// # Arguments
    /// * `api_key` - Gate.io API key
    /// * `api_secret` - Gate.io API secret
    /// * `market_type` - Spot or Swap (futures)
    pub fn new(api_key: &str, api_secret: &str, market_type: MarketType) -> Self {
        Self {
            api_key: api_key.to_string(),
            api_secret: api_secret.to_string(),
            market_type,
        }
    }

    /// Get WebSocket URL based on market type.
    fn ws_url(&self) -> &str {
        match self.market_type {
            MarketType::Spot => SPOT_WS_TRADE_URL,
            MarketType::Swap => FUTURES_WS_TRADE_URL,
        }
    }

    /// Create WebSocket connection for trading.
    ///
    /// Gate.io doesn't require explicit auth handshake like some other exchanges.
    /// Authentication is sent with each authenticated request.
    async fn create_connection(&self) -> Result<WsStream> {
        let ws_url = self.ws_url();
        let (ws_stream, _) = connect_async(ws_url)
            .await
            .context("Failed to connect to Gate.io trade WebSocket")?;

        info!("Gate.io WebSocket connected to {}", ws_url);

        Ok(ws_stream)
    }

    /// Build authentication object for a request.
    fn build_auth(&self, channel: &str, event: &str, time: i64) -> WsAuth {
        let signature = ws_auth_signature(&self.api_secret, channel, event, time);
        WsAuth {
            method: "api_key".to_string(),
            key: self.api_key.clone(),
            sign: signature,
        }
    }

    /// Place order via WebSocket.
    ///
    /// # Arguments
    /// * `symbol` - Trading pair (e.g., "BTC_USDT")
    /// * `side` - Order side (Buy/Sell)
    /// * `order_type` - Order type (Market/Limit)
    /// * `quantity` - Order quantity
    /// * `price` - Order price (required for limit orders)
    /// * `reduce_only` - Whether the order is reduce-only (futures only)
    ///
    /// # Returns
    /// OrderResult with order details or error information.
    pub async fn place_order(
        &self,
        symbol: &str,
        side: OrderSide,
        order_type: OrderType,
        quantity: f64,
        price: Option<f64>,
        reduce_only: bool,
    ) -> Result<OrderResult> {
        // Create WS connection
        let mut ws_stream = self.create_connection().await?;

        let time = timestamp_ms() / 1000;
        let req_id = format!("order_{}", timestamp_ms());

        let (channel, event, payload) = if self.market_type == MarketType::Spot {
            // Spot order
            let side_str = match side {
                OrderSide::Buy => "buy",
                OrderSide::Sell => "sell",
            };

            let order_params = SpotOrderParams {
                text: Some(req_id.clone()),
                currency_pair: symbol.to_string(),
                order_type: match order_type {
                    OrderType::Market => "market".to_string(),
                    OrderType::Limit => "limit".to_string(),
                    _ => "limit".to_string(),
                },
                account: None,
                side: side_str.to_string(),
                amount: format!("{:.8}", quantity),
                price: price.map(|p| format!("{:.8}", p)),
                time_in_force: match order_type {
                    OrderType::Market => Some("ioc".to_string()),
                    OrderType::Limit => Some("gtc".to_string()),
                    _ => Some("gtc".to_string()),
                },
                iceberg: None,
                auto_borrow: None,
                auto_repay: None,
            };

            (
                "spot.order_place",
                "api",
                serde_json::to_value(order_params)?,
            )
        } else {
            // Futures order
            let size = match side {
                OrderSide::Buy => quantity as i64,
                OrderSide::Sell => -(quantity as i64),
            };

            let order_params = FuturesOrderParams {
                contract: symbol.to_string(),
                size,
                price: match order_type {
                    OrderType::Market => Some("0".to_string()),
                    OrderType::Limit => price.map(|p| format!("{:.8}", p)),
                    _ => price.map(|p| format!("{:.8}", p)),
                },
                tif: match order_type {
                    OrderType::Market => Some("ioc".to_string()),
                    OrderType::Limit => Some("gtc".to_string()),
                    _ => Some("gtc".to_string()),
                },
                text: Some(req_id.clone()),
                reduce_only: if reduce_only { Some(true) } else { None },
                close: None,
                iceberg: None,
                auto_size: None,
            };

            (
                "futures.order_place",
                "api",
                serde_json::to_value(order_params)?,
            )
        };

        // Generate auth signature
        let auth = self.build_auth(channel, event, time);

        // Build request
        let order_req = WsRequest {
            time,
            id: Some(req_id.clone()),
            channel: channel.to_string(),
            event: event.to_string(),
            payload: Some(payload),
            auth: Some(auth),
        };

        let order_json = serde_json::to_string(&order_req)?;
        debug!("Sending Gate.io WS order: {}", order_json);

        // Send order
        ws_stream.send(Message::Text(order_json)).await?;

        // Wait for response
        let timeout = tokio::time::Duration::from_secs(10);
        let mut order_result = None;

        tokio::select! {
            msg = ws_stream.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        debug!("Gate.io order response: {}", text);
                        let resp: WsResponse = serde_json::from_str(&text)
                            .context("Failed to parse order response")?;

                        if let Some(error) = resp.error {
                            let error_msg = error.message.unwrap_or_else(|| "Unknown error".to_string());
                            order_result = Some(OrderResult::failure(
                                symbol.to_string(),
                                side,
                                format!("Code {}: {}", error.code.unwrap_or(0), error_msg),
                            ));
                        } else if let Some(result) = resp.result {
                            // Parse order ID from result
                            let order_id = if self.market_type == MarketType::Spot {
                                result["id"].as_str().map(|s| s.to_string())
                            } else {
                                result["id"].as_i64().map(|id| id.to_string())
                            };

                            if let Some(id) = order_id {
                                info!("Gate.io WS order placed: {}", id);
                                order_result = Some(OrderResult {
                                    success: true,
                                    order_id: Some(id),
                                    symbol: symbol.to_string(),
                                    side,
                                    order_type,
                                    quantity: Some(quantity),
                                    entry_price: price,
                                    position_size: Some(quantity),
                                    position_usdt: price.map(|p| quantity * p),
                                    error: None,
                                    raw_response: None,
                                });
                            } else {
                                order_result = Some(OrderResult::failure(
                                    symbol.to_string(),
                                    side,
                                    "No order ID in response".to_string(),
                                ));
                            }
                        }
                    }
                    Some(Ok(Message::Ping(data))) => {
                        let _ = ws_stream.send(Message::Pong(data)).await;
                    }
                    Some(Ok(_)) => {
                        order_result = Some(OrderResult::failure(
                            symbol.to_string(),
                            side,
                            "Unexpected message type".to_string(),
                        ));
                    }
                    Some(Err(e)) => {
                        order_result = Some(OrderResult::failure(
                            symbol.to_string(),
                            side,
                            format!("WebSocket error: {}", e),
                        ));
                    }
                    None => {
                        order_result = Some(OrderResult::failure(
                            symbol.to_string(),
                            side,
                            "WebSocket closed".to_string(),
                        ));
                    }
                }
            }
            _ = tokio::time::sleep(timeout) => {
                order_result = Some(OrderResult::failure(
                    symbol.to_string(),
                    side,
                    "Order timeout".to_string(),
                ));
            }
        }

        // Close connection
        let _ = ws_stream.close(None).await;

        order_result.ok_or_else(|| anyhow::anyhow!("No order result received"))
    }

    /// Place a market order via WebSocket.
    ///
    /// Convenience method for placing market orders.
    pub async fn place_market_order(
        &self,
        symbol: &str,
        side: OrderSide,
        quantity: f64,
    ) -> Result<OrderResult> {
        self.place_order(symbol, side, OrderType::Market, quantity, None, false)
            .await
    }

    /// Place a limit order via WebSocket.
    ///
    /// Convenience method for placing limit orders.
    pub async fn place_limit_order(
        &self,
        symbol: &str,
        side: OrderSide,
        quantity: f64,
        price: f64,
        reduce_only: bool,
    ) -> Result<OrderResult> {
        self.place_order(
            symbol,
            side,
            OrderType::Limit,
            quantity,
            Some(price),
            reduce_only,
        )
        .await
    }

    /// Cancel order via WebSocket.
    ///
    /// # Arguments
    /// * `symbol` - Trading pair (e.g., "BTC_USDT")
    /// * `order_id` - Order ID to cancel
    ///
    /// # Returns
    /// `true` if cancel was successful, `false` otherwise.
    pub async fn cancel_order(&self, symbol: &str, order_id: &str) -> Result<bool> {
        // Create WS connection
        let mut ws_stream = self.create_connection().await?;

        let time = timestamp_ms() / 1000;
        let req_id = format!("cancel_{}", timestamp_ms());

        let (channel, event, payload) = if self.market_type == MarketType::Spot {
            let cancel_params = OrderCancelParams {
                order_id: Some(order_id.to_string()),
                currency_pair: Some(symbol.to_string()),
                contract: None,
            };
            (
                "spot.order_cancel",
                "api",
                serde_json::to_value(cancel_params)?,
            )
        } else {
            let cancel_params = OrderCancelParams {
                order_id: Some(order_id.to_string()),
                currency_pair: None,
                contract: Some(symbol.to_string()),
            };
            (
                "futures.order_cancel",
                "api",
                serde_json::to_value(cancel_params)?,
            )
        };

        // Generate auth signature
        let auth = self.build_auth(channel, event, time);

        // Build request
        let cancel_req = WsRequest {
            time,
            id: Some(req_id),
            channel: channel.to_string(),
            event: event.to_string(),
            payload: Some(payload),
            auth: Some(auth),
        };

        let cancel_json = serde_json::to_string(&cancel_req)?;
        debug!("Sending Gate.io WS cancel: {}", cancel_json);

        // Send cancel request
        ws_stream.send(Message::Text(cancel_json)).await?;

        // Wait for response
        let timeout = tokio::time::Duration::from_secs(10);
        let mut success = false;

        tokio::select! {
            msg = ws_stream.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        debug!("Gate.io cancel response: {}", text);
                        let resp: WsResponse = serde_json::from_str(&text)
                            .context("Failed to parse cancel response")?;

                        if resp.error.is_none() && resp.result.is_some() {
                            info!("Gate.io WS order cancelled: {}", order_id);
                            success = true;
                        } else if let Some(error) = resp.error {
                            let error_msg = error.message.unwrap_or_else(|| "Unknown error".to_string());
                            error!("Failed to cancel order {}: {}", order_id, error_msg);
                        }
                    }
                    Some(Ok(Message::Ping(data))) => {
                        let _ = ws_stream.send(Message::Pong(data)).await;
                    }
                    Some(Ok(_)) => error!("Unexpected message type during cancel"),
                    Some(Err(e)) => error!("WebSocket error during cancel: {}", e),
                    None => error!("WebSocket closed during cancel"),
                }
            }
            _ = tokio::time::sleep(timeout) => {
                error!("Cancel order timeout for {}", order_id);
            }
        }

        // Close connection
        let _ = ws_stream.close(None).await;

        Ok(success)
    }

    /// Amend (modify) order via WebSocket.
    ///
    /// Allows changing the price and/or quantity of an existing order.
    ///
    /// # Arguments
    /// * `symbol` - Trading pair (e.g., "BTC_USDT")
    /// * `order_id` - Order ID to amend
    /// * `new_quantity` - New order quantity (optional)
    /// * `new_price` - New order price (optional)
    ///
    /// # Returns
    /// `true` if amend was successful, `false` otherwise.
    pub async fn amend_order(
        &self,
        symbol: &str,
        order_id: &str,
        new_quantity: Option<f64>,
        new_price: Option<f64>,
    ) -> Result<bool> {
        // Create WS connection
        let mut ws_stream = self.create_connection().await?;

        let time = timestamp_ms() / 1000;
        let req_id = format!("amend_{}", timestamp_ms());

        let (channel, event, payload) = if self.market_type == MarketType::Spot {
            // For spot, we need to build amend params
            let mut amend_data = serde_json::json!({
                "order_id": order_id,
                "currency_pair": symbol,
            });

            if let Some(qty) = new_quantity {
                amend_data["amount"] = serde_json::json!(format!("{:.8}", qty));
            }
            if let Some(price) = new_price {
                amend_data["price"] = serde_json::json!(format!("{:.8}", price));
            }

            ("spot.order_amend", "api", amend_data)
        } else {
            // For futures
            let mut amend_data = serde_json::json!({
                "order_id": order_id,
            });

            if let Some(qty) = new_quantity {
                amend_data["size"] = serde_json::json!(qty as i64);
            }
            if let Some(price) = new_price {
                amend_data["price"] = serde_json::json!(format!("{:.8}", price));
            }

            ("futures.order_amend", "api", amend_data)
        };

        // Generate auth signature
        let auth = self.build_auth(channel, event, time);

        // Build request
        let amend_req = WsRequest {
            time,
            id: Some(req_id),
            channel: channel.to_string(),
            event: event.to_string(),
            payload: Some(payload),
            auth: Some(auth),
        };

        let amend_json = serde_json::to_string(&amend_req)?;
        debug!("Sending Gate.io WS amend: {}", amend_json);

        // Send amend request
        ws_stream.send(Message::Text(amend_json)).await?;

        // Wait for response
        let timeout = tokio::time::Duration::from_secs(10);
        let mut success = false;

        tokio::select! {
            msg = ws_stream.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        debug!("Gate.io amend response: {}", text);
                        let resp: WsResponse = serde_json::from_str(&text)
                            .context("Failed to parse amend response")?;

                        if resp.error.is_none() && resp.result.is_some() {
                            info!("Gate.io WS order amended: {}", order_id);
                            success = true;
                        } else if let Some(error) = resp.error {
                            let error_msg = error.message.unwrap_or_else(|| "Unknown error".to_string());
                            error!("Failed to amend order {}: {}", order_id, error_msg);
                        }
                    }
                    Some(Ok(Message::Ping(data))) => {
                        let _ = ws_stream.send(Message::Pong(data)).await;
                    }
                    Some(Ok(_)) => error!("Unexpected message type during amend"),
                    Some(Err(e)) => error!("WebSocket error during amend: {}", e),
                    None => error!("WebSocket closed during amend"),
                }
            }
            _ = tokio::time::sleep(timeout) => {
                error!("Amend order timeout for {}", order_id);
            }
        }

        // Close connection
        let _ = ws_stream.close(None).await;

        Ok(success)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ws_url_spot() {
        let service = WsTradingService::new("key", "secret", MarketType::Spot);
        assert_eq!(service.ws_url(), SPOT_WS_TRADE_URL);
    }

    #[test]
    fn test_ws_url_futures() {
        let service = WsTradingService::new("key", "secret", MarketType::Swap);
        assert_eq!(service.ws_url(), FUTURES_WS_TRADE_URL);
    }

    #[test]
    fn test_build_auth() {
        let service = WsTradingService::new("test_key", "test_secret", MarketType::Spot);
        let auth = service.build_auth("spot.order_place", "api", 1234567890);

        assert_eq!(auth.method, "api_key");
        assert_eq!(auth.key, "test_key");
        // Signature should be a non-empty hex string
        assert!(!auth.sign.is_empty());
        assert!(auth.sign.chars().all(|c| c.is_ascii_hexdigit()));
    }
}
