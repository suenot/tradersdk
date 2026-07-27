//! WebSocket trading service for Binance.

use sdktrader_core::utils::{hmac_sign, timestamp_ms};
use sdktrader_core::models::{MarketType, OrderResult, OrderSide, OrderType};
use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, error, info};

use crate::config::{
    FUTURES_WS_API_MAINNET, FUTURES_WS_API_TESTNET, SPOT_WS_API_MAINNET, SPOT_WS_API_TESTNET,
};
use crate::models::{WsOrderData, WsRequest, WsResponse};

/// WebSocket trading service for Binance.
pub struct WsTradingService {
    api_key: String,
    api_secret: String,
    testnet: bool,
    market_type: MarketType,
}

impl WsTradingService {
    /// Create a new WebSocket trading service.
    pub fn new(api_key: &str, api_secret: &str, testnet: bool, market_type: MarketType) -> Self {
        Self {
            api_key: api_key.to_string(),
            api_secret: api_secret.to_string(),
            testnet,
            market_type,
        }
    }

    /// Get WebSocket trade URL based on market type and testnet setting.
    fn ws_trade_url(&self) -> &str {
        match (self.market_type, self.testnet) {
            (MarketType::Spot, false) => SPOT_WS_API_MAINNET,
            (MarketType::Spot, true) => SPOT_WS_API_TESTNET,
            (MarketType::Swap, false) => FUTURES_WS_API_MAINNET,
            (MarketType::Swap, true) => FUTURES_WS_API_TESTNET,
        }
    }

    /// Sign query string.
    fn sign(&self, query: &str) -> String {
        hmac_sign(&self.api_secret, query)
    }

    /// Create WebSocket connection for trading.
    async fn create_connection(
        &self,
    ) -> Result<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    > {
        let ws_url = self.ws_trade_url();
        let (ws_stream, _) = connect_async(ws_url)
            .await
            .context("Failed to connect to Binance trade WebSocket")?;

        debug!("Connected to Binance WebSocket: {}", ws_url);
        Ok(ws_stream)
    }

    /// Send a signed WebSocket request and wait for response.
    async fn send_request(
        &self,
        ws_stream: &mut tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        method: &str,
        params: serde_json::Value,
    ) -> Result<WsResponse> {
        let timestamp = timestamp_ms();
        let id = format!("{}_{}", method, timestamp);

        // Prepare params with timestamp and signature
        let mut params_obj = params.as_object().unwrap().clone();
        params_obj.insert("timestamp".to_string(), serde_json::json!(timestamp));
        params_obj.insert("apiKey".to_string(), serde_json::json!(self.api_key));

        // Create query string for signature
        let mut query_parts: Vec<String> = params_obj
            .iter()
            .map(|(k, v)| {
                let value_str = match v {
                    serde_json::Value::String(s) => s.clone(),
                    serde_json::Value::Number(n) => n.to_string(),
                    serde_json::Value::Bool(b) => b.to_string(),
                    _ => v.to_string(),
                };
                format!("{}={}", k, value_str)
            })
            .collect();
        query_parts.sort();
        let query_string = query_parts.join("&");

        // Sign the request
        let signature = self.sign(&query_string);
        params_obj.insert("signature".to_string(), serde_json::json!(signature));

        // Create request
        let request = WsRequest {
            id: id.clone(),
            method: method.to_string(),
            params: Some(serde_json::Value::Object(params_obj)),
        };

        let request_json = serde_json::to_string(&request)?;
        debug!("Sending WS request: {}", request_json);

        // Send request
        ws_stream.send(Message::Text(request_json)).await?;

        // Wait for response
        let timeout = tokio::time::Duration::from_secs(10);
        tokio::select! {
            msg = ws_stream.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        debug!("WS response: {}", text);
                        let response: WsResponse = serde_json::from_str(&text)
                            .context("Failed to parse WS response")?;

                        if response.id != id {
                            anyhow::bail!("Response ID mismatch: expected {}, got {}", id, response.id);
                        }

                        Ok(response)
                    }
                    Some(Ok(Message::Ping(data))) => {
                        let _ = ws_stream.send(Message::Pong(data)).await;
                        anyhow::bail!("Received ping instead of response")
                    }
                    Some(Ok(_)) => anyhow::bail!("Unexpected message type"),
                    Some(Err(e)) => anyhow::bail!("WebSocket error: {}", e),
                    None => anyhow::bail!("WebSocket closed"),
                }
            }
            _ = tokio::time::sleep(timeout) => {
                anyhow::bail!("WebSocket request timeout")
            }
        }
    }

    /// Place order via WebSocket.
    pub async fn place_order(
        &self,
        symbol: &str,
        side: OrderSide,
        order_type: OrderType,
        quantity: f64,
        price: Option<f64>,
        reduce_only: bool,
    ) -> Result<OrderResult> {
        let mut ws_stream = self.create_connection().await?;

        // Build order parameters
        let side_str = match side {
            OrderSide::Buy => "BUY",
            OrderSide::Sell => "SELL",
        };

        let type_str = match order_type {
            OrderType::Market => "MARKET",
            OrderType::Limit => "LIMIT",
            _ => "MARKET",
        };

        let mut params = serde_json::json!({
            "symbol": symbol,
            "side": side_str,
            "type": type_str,
            "quantity": format!("{:.8}", quantity),
        });

        // Add price for limit orders
        if let (OrderType::Limit, Some(p)) = (order_type, price) {
            params["price"] = serde_json::json!(format!("{:.8}", p));
            params["timeInForce"] = serde_json::json!("GTC");
        }

        // Add reduceOnly for futures
        if reduce_only && self.market_type == MarketType::Swap {
            params["reduceOnly"] = serde_json::json!("true");
        }

        // Send order request
        let method = "order.place";
        let response = self.send_request(&mut ws_stream, method, params).await?;

        // Close connection
        let _ = ws_stream.close(None).await;

        // Process response
        if response.status == 200 {
            if let Some(result) = response.result {
                let order_data: WsOrderData =
                    serde_json::from_value(result).context("Failed to parse order data")?;

                let order_id = order_data
                    .order_id
                    .ok_or_else(|| anyhow::anyhow!("No order ID in response"))?;

                info!("Binance WS order placed: {}", order_id);

                let entry_price = order_data
                    .avg_price
                    .as_ref()
                    .and_then(|p| p.parse::<f64>().ok())
                    .or(price);

                return Ok(OrderResult {
                    success: true,
                    order_id: Some(order_id.to_string()),
                    symbol: symbol.to_string(),
                    side,
                    order_type,
                    quantity: Some(quantity),
                    entry_price,
                    position_size: Some(quantity),
                    position_usdt: entry_price.map(|p| quantity * p),
                    error: None,
                    raw_response: None,
                });
            }
        }

        // Handle error
        let error_msg = response
            .error
            .map(|e| format!("{}: {}", e.code, e.msg))
            .unwrap_or_else(|| format!("Unknown error (status: {})", response.status));

        Ok(OrderResult::failure(symbol.to_string(), side, error_msg))
    }

    /// Cancel order via WebSocket.
    pub async fn cancel_order(&self, symbol: &str, order_id: &str) -> Result<bool> {
        let mut ws_stream = self.create_connection().await?;

        let params = serde_json::json!({
            "symbol": symbol,
            "orderId": order_id.parse::<i64>()?,
        });

        let method = "order.cancel";
        let response = self.send_request(&mut ws_stream, method, params).await?;

        // Close connection
        let _ = ws_stream.close(None).await;

        if response.status == 200 {
            info!("Binance WS order cancelled: {}", order_id);
            Ok(true)
        } else {
            let error_msg = response
                .error
                .map(|e| format!("{}: {}", e.code, e.msg))
                .unwrap_or_else(|| format!("Unknown error (status: {})", response.status));
            error!("Failed to cancel order {}: {}", order_id, error_msg);
            Ok(false)
        }
    }
}
