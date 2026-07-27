//! WebSocket trading service for Bybit.

use sdktrader_core::utils::{hmac_sign, timestamp_ms};
use sdktrader_core::models::{MarketType, OrderResult, OrderSide, OrderType};
use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, error, info};

use crate::config::{MAINNET_WS_TRADE_URL, TESTNET_WS_TRADE_URL};
use crate::models::{
    WsAuthRequest, WsAuthResponse, WsOrderHeader, WsOrderRequest, WsOrderResponse,
};

/// WebSocket trading service for Bybit.
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

    /// Get WebSocket trade URL based on testnet setting.
    pub fn ws_trade_url(&self) -> &str {
        if self.testnet {
            TESTNET_WS_TRADE_URL
        } else {
            MAINNET_WS_TRADE_URL
        }
    }

    /// Generate WebSocket authentication signature.
    pub fn ws_auth_signature(&self, expires: i64) -> String {
        let sign_str = format!("GET/realtime{}", expires);
        hmac_sign(&self.api_secret, &sign_str)
    }

    /// Get category string for API calls.
    pub fn category(&self) -> &str {
        match self.market_type {
            MarketType::Spot => "spot",
            MarketType::Swap => "linear",
        }
    }

    /// Create and authenticate WebSocket connection for trading.
    pub async fn create_ws_trade_connection(
        &self,
    ) -> Result<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    > {
        let ws_url = self.ws_trade_url();
        let (mut ws_stream, _) = connect_async(ws_url)
            .await
            .context("Failed to connect to Bybit trade WebSocket")?;

        // Authenticate
        let expires = (timestamp_ms() + 10000) / 1000;
        let signature = self.ws_auth_signature(expires);

        let auth_msg = WsAuthRequest {
            req_id: format!("auth_{}", timestamp_ms()),
            op: "auth".to_string(),
            args: vec![self.api_key.clone(), expires.to_string(), signature],
        };

        let auth_json = serde_json::to_string(&auth_msg)?;
        debug!("Sending WS auth: {}", auth_json);

        ws_stream.send(Message::Text(auth_json)).await?;

        // Wait for auth response
        let timeout = tokio::time::Duration::from_secs(5);
        tokio::select! {
            msg = ws_stream.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        debug!("Auth response: {}", text);
                        let auth_resp: WsAuthResponse = serde_json::from_str(&text)
                            .context("Failed to parse auth response")?;

                        if !auth_resp.success {
                            anyhow::bail!("WebSocket authentication failed: {:?}", auth_resp.ret_msg);
                        }

                        info!("WebSocket authenticated, connId: {:?}", auth_resp.conn_id);
                    }
                    Some(Ok(Message::Ping(_))) => debug!("Received ping during auth"),
                    Some(Ok(Message::Pong(_))) => debug!("Received pong during auth"),
                    Some(Ok(_)) => anyhow::bail!("Unexpected message type during auth"),
                    Some(Err(e)) => anyhow::bail!("WebSocket error during auth: {}", e),
                    None => anyhow::bail!("WebSocket closed during auth"),
                }
            }
            _ = tokio::time::sleep(timeout) => {
                anyhow::bail!("WebSocket authentication timeout");
            }
        }

        Ok(ws_stream)
    }

    /// Place order via WebSocket.
    pub async fn place_order_ws(
        &self,
        symbol: &str,
        side: OrderSide,
        order_type: OrderType,
        quantity: f64,
        price: Option<f64>,
        reduce_only: bool,
    ) -> Result<OrderResult> {
        let mut ws_stream = self.create_ws_trade_connection().await?;

        let timestamp = timestamp_ms();
        let req_id = format!("order_{}", timestamp);

        let mut order_params = serde_json::json!({
            "category": self.category(),
            "symbol": symbol,
            "side": match side {
                OrderSide::Buy => "Buy",
                OrderSide::Sell => "Sell",
            },
            "orderType": match order_type {
                OrderType::Market => "Market",
                OrderType::Limit => "Limit",
                _ => "Market",
            },
            "qty": format!("{:.8}", quantity),
        });

        if let (OrderType::Limit, Some(p)) = (order_type, price) {
            order_params["price"] = serde_json::json!(format!("{:.8}", p));
            order_params["timeInForce"] = serde_json::json!("GTC");
        }

        if reduce_only && self.market_type == MarketType::Swap {
            order_params["reduceOnly"] = serde_json::json!(true);
        }

        let order_req = WsOrderRequest {
            req_id: req_id.clone(),
            header: WsOrderHeader {
                timestamp: timestamp.to_string(),
                recv_window: "5000".to_string(),
            },
            op: "order.create".to_string(),
            args: vec![order_params],
        };

        let order_json = serde_json::to_string(&order_req)?;
        debug!("Sending WS order: {}", order_json);

        ws_stream.send(Message::Text(order_json)).await?;

        // Wait for response
        let timeout = tokio::time::Duration::from_secs(10);
        let mut order_result = None;

        tokio::select! {
            msg = ws_stream.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        debug!("Order response: {}", text);
                        let order_resp: WsOrderResponse = serde_json::from_str(&text)
                            .context("Failed to parse order response")?;

                        if order_resp.success {
                            if let Some(data) = order_resp.data {
                                if let Some(order_id) = data.order_id {
                                    info!("Bybit WS order placed: {}", order_id);
                                    order_result = Some(OrderResult {
                                        success: true,
                                        order_id: Some(order_id),
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
                                }
                            }
                        } else {
                            let error_msg = order_resp.ret_msg
                                .unwrap_or_else(|| "Unknown error".to_string());
                            order_result = Some(OrderResult::failure(
                                symbol.to_string(),
                                side,
                                error_msg,
                            ));
                        }
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

        let _ = ws_stream.close(None).await;
        order_result.ok_or_else(|| anyhow::anyhow!("No order result received"))
    }

    /// Cancel order via WebSocket.
    pub async fn cancel_order_ws(&self, symbol: &str, order_id: &str) -> Result<bool> {
        let mut ws_stream = self.create_ws_trade_connection().await?;

        let timestamp = timestamp_ms();
        let req_id = format!("cancel_{}", timestamp);

        let cancel_params = serde_json::json!({
            "category": self.category(),
            "symbol": symbol,
            "orderId": order_id,
        });

        let cancel_req = WsOrderRequest {
            req_id: req_id.clone(),
            header: WsOrderHeader {
                timestamp: timestamp.to_string(),
                recv_window: "5000".to_string(),
            },
            op: "order.cancel".to_string(),
            args: vec![cancel_params],
        };

        let cancel_json = serde_json::to_string(&cancel_req)?;
        debug!("Sending WS cancel: {}", cancel_json);

        ws_stream.send(Message::Text(cancel_json)).await?;

        let timeout = tokio::time::Duration::from_secs(10);
        let mut success = false;

        tokio::select! {
            msg = ws_stream.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        debug!("Cancel response: {}", text);
                        let cancel_resp: WsOrderResponse = serde_json::from_str(&text)
                            .context("Failed to parse cancel response")?;

                        if cancel_resp.success {
                            info!("Bybit WS order cancelled: {}", order_id);
                            success = true;
                        } else {
                            let error_msg = cancel_resp.ret_msg
                                .unwrap_or_else(|| "Unknown error".to_string());
                            error!("Failed to cancel order {}: {}", order_id, error_msg);
                        }
                    }
                    Some(Ok(Message::Ping(_))) => debug!("Received ping during cancel"),
                    Some(Ok(Message::Pong(_))) => debug!("Received pong during cancel"),
                    Some(Ok(_)) => error!("Unexpected message type during cancel"),
                    Some(Err(e)) => error!("WebSocket error during cancel: {}", e),
                    None => error!("WebSocket closed during cancel"),
                }
            }
            _ = tokio::time::sleep(timeout) => {
                error!("Cancel order timeout for {}", order_id);
            }
        }

        let _ = ws_stream.close(None).await;
        Ok(success)
    }

    /// Amend (modify) order via WebSocket.
    pub async fn amend_order_ws(
        &self,
        symbol: &str,
        order_id: &str,
        new_quantity: Option<f64>,
        new_price: Option<f64>,
    ) -> Result<bool> {
        let mut ws_stream = self.create_ws_trade_connection().await?;

        let timestamp = timestamp_ms();
        let req_id = format!("amend_{}", timestamp);

        let mut amend_params = serde_json::json!({
            "category": self.category(),
            "symbol": symbol,
            "orderId": order_id,
        });

        if let Some(qty) = new_quantity {
            amend_params["qty"] = serde_json::json!(format!("{:.8}", qty));
        }

        if let Some(price) = new_price {
            amend_params["price"] = serde_json::json!(format!("{:.8}", price));
        }

        let amend_req = WsOrderRequest {
            req_id: req_id.clone(),
            header: WsOrderHeader {
                timestamp: timestamp.to_string(),
                recv_window: "5000".to_string(),
            },
            op: "order.amend".to_string(),
            args: vec![amend_params],
        };

        let amend_json = serde_json::to_string(&amend_req)?;
        debug!("Sending WS amend: {}", amend_json);

        ws_stream.send(Message::Text(amend_json)).await?;

        let timeout = tokio::time::Duration::from_secs(10);
        let mut success = false;

        tokio::select! {
            msg = ws_stream.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        debug!("Amend response: {}", text);
                        let amend_resp: WsOrderResponse = serde_json::from_str(&text)
                            .context("Failed to parse amend response")?;

                        if amend_resp.success {
                            info!("Bybit WS order amended: {}", order_id);
                            success = true;
                        } else {
                            let error_msg = amend_resp.ret_msg
                                .unwrap_or_else(|| "Unknown error".to_string());
                            error!("Failed to amend order {}: {}", order_id, error_msg);
                        }
                    }
                    Some(Ok(Message::Ping(_))) => debug!("Received ping during amend"),
                    Some(Ok(Message::Pong(_))) => debug!("Received pong during amend"),
                    Some(Ok(_)) => error!("Unexpected message type during amend"),
                    Some(Err(e)) => error!("WebSocket error during amend: {}", e),
                    None => error!("WebSocket closed during amend"),
                }
            }
            _ = tokio::time::sleep(timeout) => {
                error!("Amend order timeout for {}", order_id);
            }
        }

        let _ = ws_stream.close(None).await;
        Ok(success)
    }
}
