//! Hyperliquid private user data WebSocket service.
//!
//! Note: Hyperliquid is unique - the order updates WebSocket does NOT require authentication!
//! It just uses the wallet address for subscription.

use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, error, info, warn};

use crate::config::{INITIAL_RECONNECT_DELAY_SECS, MAINNET_WS_URL, TESTNET_WS_URL};
use crate::helpers::{next_reconnect_delay, parse_order_status};
use sdktrader_core::models::{OrderSide, OrderType, OrderUpdateEvent};
use sdktrader_core::utils::timestamp_ms;

/// Log an order update event with exchange/local timestamp delta.
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

/// Hyperliquid private user data stream service.
/// Note: Hyperliquid doesn't require authentication for order updates - just wallet address.
pub struct UserDataService {
    wallet_address: String,
    testnet: bool,
}

impl UserDataService {
    pub fn new(wallet_address: &str, testnet: bool) -> Self {
        Self {
            wallet_address: wallet_address.to_string(),
            testnet,
        }
    }

    fn ws_url(&self) -> &str {
        if self.testnet {
            TESTNET_WS_URL
        } else {
            MAINNET_WS_URL
        }
    }

    /// Subscribe to order updates via WebSocket.
    /// Returns a JoinHandle for the background task.
    ///
    /// Note: Unlike most exchanges, Hyperliquid does NOT require authentication
    /// for the order updates WebSocket. It simply uses the wallet address.
    pub async fn subscribe_order_updates(
        &self,
        sender: broadcast::Sender<OrderUpdateEvent>,
    ) -> Result<JoinHandle<()>> {
        let wallet_address = self.wallet_address.clone();
        let testnet = self.testnet;
        let exchange_name = "hyperliquid".to_string();

        let handle = tokio::spawn(async move {
            let mut reconnect_delay = INITIAL_RECONNECT_DELAY_SECS;

            loop {
                let ws_url = if testnet {
                    TESTNET_WS_URL
                } else {
                    MAINNET_WS_URL
                };
                info!("[{}] Connecting to WS: {}", exchange_name, ws_url);

                match connect_async(ws_url).await {
                    Ok((mut ws_stream, _)) => {
                        // Subscribe to order updates (no auth needed, uses wallet address)
                        let sub_msg = serde_json::json!({
                            "method": "subscribe",
                            "subscription": {
                                "type": "orderUpdates",
                                "user": wallet_address
                            }
                        });

                        if let Err(e) = ws_stream.send(Message::Text(sub_msg.to_string())).await {
                            error!("[{}] Failed to subscribe: {}", exchange_name, e);
                            tokio::time::sleep(tokio::time::Duration::from_secs(reconnect_delay))
                                .await;
                            reconnect_delay = next_reconnect_delay(reconnect_delay);
                            continue;
                        }

                        info!(
                            "[{}] Subscribed to order updates for {}",
                            exchange_name, wallet_address
                        );
                        reconnect_delay = INITIAL_RECONNECT_DELAY_SECS;

                        let mut ping_interval =
                            tokio::time::interval(tokio::time::Duration::from_secs(30));

                        loop {
                            tokio::select! {
                                msg = ws_stream.next() => {
                                    match msg {
                                        Some(Ok(Message::Text(text))) => {
                                            if let Ok(msg_json) = serde_json::from_str::<serde_json::Value>(&text) {
                                                let channel = msg_json.get("channel").and_then(|v| v.as_str()).unwrap_or("");

                                                if channel == "orderUpdates" {
                                                    if let Some(data_arr) = msg_json.get("data").and_then(|d| d.as_array()) {
                                                        for o in data_arr {
                                                            let order_obj = o.get("order").unwrap_or(o);
                                                            let status_str = o.get("status").and_then(|v| v.as_str())
                                                                .or_else(|| order_obj.get("status").and_then(|v| v.as_str()))
                                                                .unwrap_or("open");

                                                            let event = OrderUpdateEvent {
                                                                exchange: exchange_name.clone(),
                                                                order_id: order_obj.get("oid").and_then(|v| v.as_i64())
                                                                    .map(|v| v.to_string())
                                                                    .or_else(|| order_obj.get("oid").and_then(|v| v.as_str()).map(|s| s.to_string()))
                                                                    .unwrap_or_default(),
                                                                client_order_id: order_obj.get("cloid").and_then(|v| v.as_str()).map(|s| s.to_string()),
                                                                symbol: order_obj.get("coin").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                                                                status: parse_order_status(status_str),
                                                                side: if order_obj.get("side").and_then(|v| v.as_str()) == Some("B") { OrderSide::Buy } else { OrderSide::Sell },
                                                                order_type: if order_obj.get("orderType").and_then(|v| v.as_str()) == Some("Market") { OrderType::Market } else { OrderType::Limit },
                                                                price: order_obj.get("limitPx").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0),
                                                                quantity: order_obj.get("sz").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0),
                                                                filled_quantity: order_obj.get("filled").and_then(|v| v.as_str())
                                                                    .or_else(|| order_obj.get("totalSz").and_then(|v| v.as_str()))
                                                                    .and_then(|s| s.parse().ok()).unwrap_or(0.0),
                                                                avg_fill_price: order_obj.get("avgPx").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()),
                                                                fee: None,
                                                                fee_currency: None,
                                                                exchange_timestamp: o.get("statusTimestamp").and_then(|v| v.as_i64()).unwrap_or_else(timestamp_ms),
                                                                received_at: timestamp_ms(),
                                                                raw: Some(o.clone()),
                                                            };
                                                            log_order_update(&event);
                                                            let _ = sender.send(event);
                                                        }
                                                    }
                                                } else if channel == "subscriptionResponse" {
                                                    debug!("[{}] Subscription confirmed: {}", exchange_name, text);
                                                }
                                            }
                                        }
                                        Some(Ok(Message::Ping(data))) => {
                                            let _ = ws_stream.send(Message::Pong(data)).await;
                                        }
                                        Some(Ok(Message::Close(_))) | Some(Err(_)) | None => {
                                            warn!("[{}] WS disconnected", exchange_name);
                                            break;
                                        }
                                        _ => {}
                                    }
                                }
                                _ = ping_interval.tick() => {
                                    let ping = serde_json::json!({"method": "ping"});
                                    if let Err(e) = ws_stream.send(Message::Text(ping.to_string())).await {
                                        error!("[{}] Ping failed: {}", exchange_name, e);
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!("[{}] Failed to connect WS: {}", exchange_name, e);
                    }
                }

                warn!(
                    "[{}] Reconnecting in {}s...",
                    exchange_name, reconnect_delay
                );
                tokio::time::sleep(tokio::time::Duration::from_secs(reconnect_delay)).await;
                reconnect_delay = next_reconnect_delay(reconnect_delay);
            }
        });

        Ok(handle)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_user_data_service_new() {
        let service = UserDataService::new("0x1234567890abcdef", false);
        assert_eq!(service.wallet_address, "0x1234567890abcdef");
        assert!(!service.testnet);
        assert_eq!(service.ws_url(), MAINNET_WS_URL);
    }

    #[test]
    fn test_user_data_service_testnet() {
        let service = UserDataService::new("0x1234567890abcdef", true);
        assert!(service.testnet);
        assert_eq!(service.ws_url(), TESTNET_WS_URL);
    }
}
