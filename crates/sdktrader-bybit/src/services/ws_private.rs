//! Private WebSocket user data stream service for Bybit.

use sdktrader_core::utils::{hmac_sign, timestamp_ms};
use sdktrader_core::models::{OrderSide, OrderStatus, OrderType, OrderUpdateEvent};
use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, error, info, warn};

use crate::config::{
    INITIAL_RECONNECT_DELAY_SECS, MAINNET_WS_PRIVATE_URL, MAX_RECONNECT_DELAY_SECS,
    TESTNET_WS_PRIVATE_URL,
};

/// Calculate next reconnect delay with exponential backoff.
fn next_reconnect_delay(current_secs: u64) -> u64 {
    (current_secs * 2).min(MAX_RECONNECT_DELAY_SECS)
}

/// Initial reconnect delay.
fn initial_reconnect_delay() -> u64 {
    INITIAL_RECONNECT_DELAY_SECS
}

/// Parse order status string to OrderStatus enum.
fn parse_order_status(s: &str) -> OrderStatus {
    match s.to_lowercase().as_str() {
        "new" | "open" | "live" | "created" | "init" | "accepted" | "untriggered" | "triggered" => {
            OrderStatus::New
        }
        "partially_filled" | "partiallyfilled" | "partial_filled" | "partial" => {
            OrderStatus::PartiallyFilled
        }
        "filled" | "closed" | "full_fill" | "full" => OrderStatus::Filled,
        "cancelled" | "canceled" | "cancel" | "deactivated" => OrderStatus::Cancelled,
        "rejected" | "fail" | "failed" => OrderStatus::Rejected,
        "expired" | "expire" => OrderStatus::Expired,
        _ => {
            warn!("Unknown order status string: '{}'", s);
            OrderStatus::New
        }
    }
}

/// Log an order update event.
fn log_order_update(event: &OrderUpdateEvent) {
    let delta = event.received_at - event.exchange_timestamp;
    info!(
        "ORDER_UPDATE exchange={} order_id={} symbol={} status={:?} side={:?} type={:?} \
         filled={}/{} price={} avg_fill={} fee={} exchange_ts={} local_ts={} delta_ms={}",
        event.exchange,
        event.order_id,
        event.symbol,
        event.status,
        event.side,
        event.order_type,
        event.filled_quantity,
        event.quantity,
        event.price,
        event.avg_fill_price.unwrap_or(0.0),
        event.fee.unwrap_or(0.0),
        event.exchange_timestamp,
        event.received_at,
        delta,
    );
}

/// User data stream service for Bybit (order updates, position updates, etc.).
pub struct UserDataService {
    api_key: String,
    api_secret: String,
    testnet: bool,
}

impl UserDataService {
    /// Create a new user data stream service.
    pub fn new(api_key: &str, api_secret: &str, testnet: bool) -> Self {
        Self {
            api_key: api_key.to_string(),
            api_secret: api_secret.to_string(),
            testnet,
        }
    }

    /// Get WebSocket URL for private stream.
    fn ws_url(&self) -> &str {
        if self.testnet {
            TESTNET_WS_PRIVATE_URL
        } else {
            MAINNET_WS_PRIVATE_URL
        }
    }

    /// Subscribe to order updates stream.
    pub fn subscribe_order_updates(
        &self,
        sender: broadcast::Sender<OrderUpdateEvent>,
    ) -> Result<JoinHandle<()>> {
        let api_key = self.api_key.clone();
        let api_secret = self.api_secret.clone();
        let ws_url = self.ws_url().to_string();
        let exchange_name = "bybit".to_string();

        let handle = tokio::spawn(async move {
            let mut reconnect_delay = initial_reconnect_delay();

            loop {
                info!("[{}] Connecting to private WS: {}", exchange_name, ws_url);

                match connect_async(&ws_url).await {
                    Ok((mut ws_stream, _)) => {
                        // Authenticate
                        let expires = (timestamp_ms() + 10000) / 1000;
                        let sign_str = format!("GET/realtime{}", expires);
                        let signature = hmac_sign(&api_secret, &sign_str);

                        let auth_msg = serde_json::json!({
                            "reqId": format!("auth_{}", timestamp_ms()),
                            "op": "auth",
                            "args": [api_key, expires.to_string(), signature]
                        });

                        if let Err(e) = ws_stream.send(Message::Text(auth_msg.to_string())).await {
                            error!("[{}] Failed to send auth: {}", exchange_name, e);
                            tokio::time::sleep(tokio::time::Duration::from_secs(reconnect_delay))
                                .await;
                            reconnect_delay = next_reconnect_delay(reconnect_delay);
                            continue;
                        }

                        // Wait for auth response
                        let mut authenticated = false;
                        if let Some(Ok(Message::Text(text))) = ws_stream.next().await {
                            if let Ok(resp) = serde_json::from_str::<serde_json::Value>(&text) {
                                if resp.get("success").and_then(|v| v.as_bool()) == Some(true) {
                                    info!("[{}] Private WS authenticated", exchange_name);
                                    authenticated = true;
                                } else {
                                    error!("[{}] Auth failed: {}", exchange_name, text);
                                }
                            }
                        }

                        if !authenticated {
                            tokio::time::sleep(tokio::time::Duration::from_secs(reconnect_delay))
                                .await;
                            reconnect_delay = next_reconnect_delay(reconnect_delay);
                            continue;
                        }

                        // Subscribe to order updates
                        let sub_msg = serde_json::json!({
                            "reqId": format!("sub_{}", timestamp_ms()),
                            "op": "subscribe",
                            "args": ["order"]
                        });

                        if let Err(e) = ws_stream.send(Message::Text(sub_msg.to_string())).await {
                            error!("[{}] Failed to subscribe: {}", exchange_name, e);
                            tokio::time::sleep(tokio::time::Duration::from_secs(reconnect_delay))
                                .await;
                            reconnect_delay = next_reconnect_delay(reconnect_delay);
                            continue;
                        }

                        info!("[{}] Subscribed to private order updates", exchange_name);
                        reconnect_delay = initial_reconnect_delay();

                        // Main message loop with ping interval
                        let mut ping_interval =
                            tokio::time::interval(tokio::time::Duration::from_secs(20));

                        loop {
                            tokio::select! {
                                msg = ws_stream.next() => {
                                    match msg {
                                        Some(Ok(Message::Text(text))) => {
                                            if let Ok(msg_json) = serde_json::from_str::<serde_json::Value>(&text) {
                                                // Skip subscription confirmations and pong responses
                                                if msg_json.get("op").is_some() {
                                                    debug!("[{}] WS op response: {}", exchange_name, text);
                                                    continue;
                                                }

                                                // Process order update topic
                                                let topic = msg_json.get("topic").and_then(|t| t.as_str());
                                                if topic == Some("order") {
                                                    if let Some(data_arr) = msg_json.get("data").and_then(|d| d.as_array()) {
                                                        for order_data in data_arr {
                                                            let event = parse_order_update(order_data, &exchange_name);
                                                            log_order_update(&event);
                                                            let _ = sender.send(event);
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        Some(Ok(Message::Ping(data))) => {
                                            let _ = ws_stream.send(Message::Pong(data)).await;
                                        }
                                        Some(Ok(Message::Close(_))) => {
                                            warn!("[{}] Private WS closed by server", exchange_name);
                                            break;
                                        }
                                        Some(Err(e)) => {
                                            error!("[{}] Private WS error: {}", exchange_name, e);
                                            break;
                                        }
                                        None => {
                                            warn!("[{}] Private WS stream ended", exchange_name);
                                            break;
                                        }
                                        _ => {}
                                    }
                                }
                                _ = ping_interval.tick() => {
                                    let ping = serde_json::json!({"op": "ping"});
                                    if let Err(e) = ws_stream.send(Message::Text(ping.to_string())).await {
                                        error!("[{}] Failed to send ping: {}", exchange_name, e);
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!("[{}] Failed to connect private WS: {}", exchange_name, e);
                    }
                }

                // Reconnect with backoff
                warn!(
                    "[{}] Reconnecting private WS in {}s...",
                    exchange_name, reconnect_delay
                );
                tokio::time::sleep(tokio::time::Duration::from_secs(reconnect_delay)).await;
                reconnect_delay = next_reconnect_delay(reconnect_delay);
            }
        });

        Ok(handle)
    }
}

/// Parse order update data from Bybit WebSocket message.
fn parse_order_update(order_data: &serde_json::Value, exchange_name: &str) -> OrderUpdateEvent {
    let order_id = order_data
        .get("orderId")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let symbol = order_data
        .get("symbol")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let status_str = order_data
        .get("orderStatus")
        .and_then(|v| v.as_str())
        .unwrap_or("New");
    let side_str = order_data
        .get("side")
        .and_then(|v| v.as_str())
        .unwrap_or("Buy");
    let order_type_str = order_data
        .get("orderType")
        .and_then(|v| v.as_str())
        .unwrap_or("Market");
    let price = order_data
        .get("price")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.0);
    let qty = order_data
        .get("qty")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.0);
    let filled_qty = order_data
        .get("cumExecQty")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.0);
    let avg_price = order_data
        .get("avgPrice")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<f64>().ok());
    let created_time = order_data
        .get("createdTime")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0);
    let cum_exec_fee = order_data
        .get("cumExecFee")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<f64>().ok());

    OrderUpdateEvent {
        exchange: exchange_name.to_string(),
        order_id,
        client_order_id: order_data
            .get("orderLinkId")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        symbol,
        status: parse_order_status(status_str),
        side: if side_str == "Buy" {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        },
        order_type: if order_type_str == "Market" {
            OrderType::Market
        } else {
            OrderType::Limit
        },
        price,
        quantity: qty,
        filled_quantity: filled_qty,
        avg_fill_price: avg_price,
        fee: cum_exec_fee,
        fee_currency: None,
        exchange_timestamp: created_time,
        received_at: timestamp_ms(),
        raw: Some(order_data.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_order_status() {
        assert_eq!(parse_order_status("New"), OrderStatus::New);
        assert_eq!(parse_order_status("Filled"), OrderStatus::Filled);
        assert_eq!(
            parse_order_status("PartiallyFilled"),
            OrderStatus::PartiallyFilled
        );
        assert_eq!(parse_order_status("Cancelled"), OrderStatus::Cancelled);
        assert_eq!(parse_order_status("Rejected"), OrderStatus::Rejected);
        assert_eq!(parse_order_status("Expired"), OrderStatus::Expired);
    }

    #[test]
    fn test_next_reconnect_delay() {
        assert_eq!(next_reconnect_delay(1), 2);
        assert_eq!(next_reconnect_delay(2), 4);
        assert_eq!(next_reconnect_delay(16), 30); // Capped at MAX
        assert_eq!(next_reconnect_delay(30), 30); // Already at max
    }

    #[test]
    fn test_parse_order_update() {
        let order_json = serde_json::json!({
            "orderId": "test-order-123",
            "symbol": "BTCUSDT",
            "orderStatus": "Filled",
            "side": "Buy",
            "orderType": "Market",
            "price": "50000.00",
            "qty": "0.001",
            "cumExecQty": "0.001",
            "avgPrice": "50001.50",
            "createdTime": "1700000000000",
            "cumExecFee": "0.05",
            "orderLinkId": "client-123"
        });

        let event = parse_order_update(&order_json, "bybit");

        assert_eq!(event.exchange, "bybit");
        assert_eq!(event.order_id, "test-order-123");
        assert_eq!(event.symbol, "BTCUSDT");
        assert_eq!(event.status, OrderStatus::Filled);
        assert_eq!(event.side, OrderSide::Buy);
        assert_eq!(event.order_type, OrderType::Market);
        assert_eq!(event.price, 50000.0);
        assert_eq!(event.quantity, 0.001);
        assert_eq!(event.filled_quantity, 0.001);
        assert_eq!(event.avg_fill_price, Some(50001.50));
        assert_eq!(event.fee, Some(0.05));
        assert_eq!(event.client_order_id, Some("client-123".to_string()));
    }
}
