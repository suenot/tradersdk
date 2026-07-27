//! Gate.io private user data WebSocket service.
//!
//! Gate.io uses per-request authentication for private WebSocket:
//! - Subscribe with auth object containing api_key and HMAC-SHA512 signature
//! - Channel: spot.orders / futures.orders
//! - Payload: ["!all"] to subscribe to all symbols

use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use sha2::Sha512;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, error, info, warn};

use sdktrader_core::utils::timestamp_ms;
use sdktrader_core::models::{MarketType, OrderSide, OrderType, OrderUpdateEvent};
use crate::config::{
    SPOT_WS_TRADE_URL, FUTURES_WS_TRADE_URL, INITIAL_RECONNECT_DELAY_SECS,
};
use crate::helpers::{next_reconnect_delay, parse_order_status};

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

/// Gate.io private user data stream service.
///
/// Handles subscription to private order update channels via WebSocket.
/// Supports both spot (spot.orders) and futures (futures.orders) markets.
pub struct UserDataService {
    api_key: String,
    api_secret: String,
    market_type: MarketType,
}

impl UserDataService {
    /// Create a new UserDataService instance.
    ///
    /// # Arguments
    /// * `api_key` - Gate.io API key
    /// * `api_secret` - Gate.io API secret for HMAC-SHA512 signing
    /// * `market_type` - Market type (Spot or Swap/Futures)
    pub fn new(api_key: &str, api_secret: &str, market_type: MarketType) -> Self {
        Self {
            api_key: api_key.to_string(),
            api_secret: api_secret.to_string(),
            market_type,
        }
    }

    /// Generate HMAC-SHA512 signature for WebSocket authentication.
    ///
    /// Sign string format: channel=<channel>&event=<event>&time=<time>
    fn ws_auth_signature(&self, channel: &str, event: &str, time: i64) -> String {
        let sign_string = format!("channel={}&event={}&time={}", channel, event, time);

        type HmacSha512 = Hmac<Sha512>;
        let mut mac = HmacSha512::new_from_slice(self.api_secret.as_bytes())
            .expect("HMAC can take key of any size");
        mac.update(sign_string.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }

    /// Get WebSocket URL based on market type.
    fn ws_url(&self) -> &'static str {
        match self.market_type {
            MarketType::Spot => SPOT_WS_TRADE_URL,
            MarketType::Swap => FUTURES_WS_TRADE_URL,
        }
    }

    /// Get orders channel based on market type.
    fn orders_channel(&self) -> &'static str {
        match self.market_type {
            MarketType::Spot => "spot.orders",
            MarketType::Swap => "futures.orders",
        }
    }

    /// Get ping channel based on market type.
    fn ping_channel(&self) -> &'static str {
        match self.market_type {
            MarketType::Spot => "spot.ping",
            MarketType::Swap => "futures.ping",
        }
    }

    /// Subscribe to order updates via WebSocket.
    ///
    /// Spawns a background task that:
    /// - Connects to the private WebSocket endpoint
    /// - Subscribes to order updates with HMAC-SHA512 authentication
    /// - Parses order update messages and sends them to the broadcast channel
    /// - Handles reconnection with exponential backoff on disconnect
    ///
    /// # Arguments
    /// * `sender` - Broadcast sender for OrderUpdateEvent messages
    ///
    /// # Returns
    /// A JoinHandle for the spawned background task
    pub async fn subscribe_order_updates(
        &self,
        sender: broadcast::Sender<OrderUpdateEvent>,
    ) -> Result<JoinHandle<()>> {
        let api_key = self.api_key.clone();
        let api_secret = self.api_secret.clone();
        let market_type = self.market_type;
        let exchange_name = "gate".to_string();

        let handle = tokio::spawn(async move {
            let mut reconnect_delay = INITIAL_RECONNECT_DELAY_SECS;

            loop {
                let ws_url = match market_type {
                    MarketType::Spot => SPOT_WS_TRADE_URL,
                    MarketType::Swap => FUTURES_WS_TRADE_URL,
                };

                info!("[{}] Connecting to private WS: {}", exchange_name, ws_url);

                match connect_async(ws_url).await {
                    Ok((mut ws_stream, _)) => {
                        // Gate.io uses per-request auth, subscribe with auth
                        let channel = match market_type {
                            MarketType::Spot => "spot.orders",
                            MarketType::Swap => "futures.orders",
                        };
                        let time = timestamp_ms() / 1000;
                        let sign_string =
                            format!("channel={}&event={}&time={}", channel, "subscribe", time);

                        type HmacSha512 = Hmac<Sha512>;
                        let signature = {
                            let mut mac = HmacSha512::new_from_slice(api_secret.as_bytes())
                                .expect("HMAC can take key of any size");
                            mac.update(sign_string.as_bytes());
                            hex::encode(mac.finalize().into_bytes())
                        };

                        // Build subscription message with auth
                        // Payload ["!all"] subscribes to all trading pairs
                        let sub_msg = serde_json::json!({
                            "time": time,
                            "channel": channel,
                            "event": "subscribe",
                            "payload": ["!all"],
                            "auth": {
                                "method": "api_key",
                                "KEY": api_key,
                                "SIGN": signature
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
                            "[{}] Subscribed to private order updates ({})",
                            exchange_name, channel
                        );
                        reconnect_delay = INITIAL_RECONNECT_DELAY_SECS;

                        // Ping interval to keep connection alive
                        let mut ping_interval =
                            tokio::time::interval(tokio::time::Duration::from_secs(15));

                        loop {
                            tokio::select! {
                                msg = ws_stream.next() => {
                                    match msg {
                                        Some(Ok(Message::Text(text))) => {
                                            if let Ok(msg_json) = serde_json::from_str::<serde_json::Value>(&text) {
                                                let event_type = msg_json.get("event").and_then(|v| v.as_str()).unwrap_or("");

                                                // Skip subscription confirmations
                                                if event_type == "subscribe" || event_type == "unsubscribe" {
                                                    debug!("[{}] WS event: {}", exchange_name, text);
                                                    continue;
                                                }

                                                // Process order update events
                                                if event_type == "update" {
                                                    if let Some(result) = msg_json.get("result").and_then(|r| r.as_array()) {
                                                        for o in result {
                                                            // Extract order ID (can be string or integer)
                                                            let id = o.get("id").and_then(|v| v.as_str())
                                                                .or_else(|| o.get("id").and_then(|v| v.as_i64()).map(|_| ""))
                                                                .unwrap_or("");
                                                            let id_str = if id.is_empty() {
                                                                o.get("id").and_then(|v| v.as_i64()).map(|v| v.to_string()).unwrap_or_default()
                                                            } else {
                                                                id.to_string()
                                                            };

                                                            // Extract timestamp (can be float or integer)
                                                            let create_time = o.get("create_time")
                                                                .and_then(|v| v.as_f64())
                                                                .map(|t| (t * 1000.0) as i64)
                                                                .or_else(|| o.get("create_time").and_then(|v| v.as_i64()))
                                                                .unwrap_or(0);

                                                            // Parse order side
                                                            let side = match o.get("side").and_then(|v| v.as_str()) {
                                                                Some("buy") => OrderSide::Buy,
                                                                _ => OrderSide::Sell,
                                                            };

                                                            // Parse order type
                                                            let order_type = match o.get("type").and_then(|v| v.as_str()) {
                                                                Some("market") => OrderType::Market,
                                                                _ => OrderType::Limit,
                                                            };

                                                            // Parse status from "status" or "finish_as" field
                                                            let status = parse_order_status(
                                                                o.get("status").or_else(|| o.get("finish_as"))
                                                                    .and_then(|v| v.as_str()).unwrap_or("open")
                                                            );

                                                            // Parse quantity (can be string or number)
                                                            let quantity = o.get("amount").or_else(|| o.get("size"))
                                                                .and_then(|v| {
                                                                    v.as_str().and_then(|s| s.parse::<f64>().ok())
                                                                        .or_else(|| v.as_f64())
                                                                        .or_else(|| v.as_i64().map(|i| i.abs() as f64))
                                                                })
                                                                .unwrap_or(0.0);

                                                            // Build order update event
                                                            let event = OrderUpdateEvent {
                                                                exchange: exchange_name.clone(),
                                                                order_id: id_str,
                                                                client_order_id: o.get("text").and_then(|v| v.as_str()).map(|s| s.to_string()),
                                                                symbol: o.get("currency_pair")
                                                                    .or_else(|| o.get("contract"))
                                                                    .and_then(|v| v.as_str())
                                                                    .unwrap_or("").to_string(),
                                                                status,
                                                                side,
                                                                order_type,
                                                                price: o.get("price").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0),
                                                                quantity,
                                                                filled_quantity: o.get("filled_total").or_else(|| o.get("left"))
                                                                    .and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0),
                                                                avg_fill_price: o.get("avg_deal_price").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()),
                                                                fee: o.get("fee").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()),
                                                                fee_currency: o.get("fee_currency").and_then(|v| v.as_str()).map(|s| s.to_string()),
                                                                exchange_timestamp: create_time,
                                                                received_at: timestamp_ms(),
                                                                raw: Some(o.clone()),
                                                            };

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
                                        Some(Ok(Message::Close(_))) | Some(Err(_)) | None => {
                                            warn!("[{}] Private WS disconnected", exchange_name);
                                            break;
                                        }
                                        _ => {}
                                    }
                                }
                                _ = ping_interval.tick() => {
                                    // Send ping to keep connection alive
                                    let ping_channel = match market_type {
                                        MarketType::Spot => "spot.ping",
                                        MarketType::Swap => "futures.ping",
                                    };
                                    let ping_msg = serde_json::json!({
                                        "time": timestamp_ms() / 1000,
                                        "channel": ping_channel
                                    });
                                    if let Err(e) = ws_stream.send(Message::Text(ping_msg.to_string())).await {
                                        error!("[{}] Ping failed: {}", exchange_name, e);
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

                // Reconnect with exponential backoff
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
        let service = UserDataService::new("test_key", "test_secret", MarketType::Spot);
        assert_eq!(service.api_key, "test_key");
        assert_eq!(service.api_secret, "test_secret");
        assert_eq!(service.market_type, MarketType::Spot);
    }

    #[test]
    fn test_ws_url() {
        let spot_service = UserDataService::new("key", "secret", MarketType::Spot);
        assert_eq!(spot_service.ws_url(), SPOT_WS_TRADE_URL);

        let futures_service = UserDataService::new("key", "secret", MarketType::Swap);
        assert_eq!(futures_service.ws_url(), FUTURES_WS_TRADE_URL);
    }

    #[test]
    fn test_orders_channel() {
        let spot_service = UserDataService::new("key", "secret", MarketType::Spot);
        assert_eq!(spot_service.orders_channel(), "spot.orders");

        let futures_service = UserDataService::new("key", "secret", MarketType::Swap);
        assert_eq!(futures_service.orders_channel(), "futures.orders");
    }

    #[test]
    fn test_ping_channel() {
        let spot_service = UserDataService::new("key", "secret", MarketType::Spot);
        assert_eq!(spot_service.ping_channel(), "spot.ping");

        let futures_service = UserDataService::new("key", "secret", MarketType::Swap);
        assert_eq!(futures_service.ping_channel(), "futures.ping");
    }

    #[test]
    fn test_ws_auth_signature() {
        let service = UserDataService::new("key", "test_secret", MarketType::Spot);
        let signature = service.ws_auth_signature("spot.orders", "subscribe", 1234567890);

        // Verify signature is a valid hex string (128 chars for SHA512)
        assert_eq!(signature.len(), 128);
        assert!(signature.chars().all(|c| c.is_ascii_hexdigit()));
    }
}
