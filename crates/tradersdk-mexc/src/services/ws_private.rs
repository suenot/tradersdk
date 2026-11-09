//! MEXC private user data WebSocket service.

use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, error, info, warn};

use crate::config::{
    FUTURES_MAINNET_URL, FUTURES_WS_URL, INITIAL_RECONNECT_DELAY_SECS, SPOT_MAINNET_URL,
    SPOT_WS_URL,
};
use crate::helpers::{next_reconnect_delay, parse_order_status};
use tradersdk_core::models::{MarketType, OrderSide, OrderType, OrderUpdateEvent};
use tradersdk_core::utils::timestamp_ms;

/// Log order update event.
fn log_order_update(event: &OrderUpdateEvent) {
    info!(
        "[{}] Order update: {} {} {} @ {} | filled: {}/{} | status: {:?}",
        event.exchange,
        event.symbol,
        event.side,
        event.order_type,
        event.price,
        event.filled_quantity,
        event.quantity,
        event.status
    );
}

/// MEXC private user data stream service.
///
/// Provides real-time order updates via WebSocket.
/// Uses listenKey authentication:
/// 1. Create listenKey via POST to userDataStream endpoint
/// 2. Connect to WebSocket with listenKey in URL
/// 3. Renew listenKey every 30 minutes via PUT
pub struct UserDataService {
    api_key: String,
    market_type: MarketType,
}

impl UserDataService {
    /// Create a new UserDataService.
    ///
    /// # Arguments
    /// * `api_key` - MEXC API key for authentication
    /// * `market_type` - Spot or Futures market
    pub fn new(api_key: &str, market_type: MarketType) -> Self {
        Self {
            api_key: api_key.to_string(),
            market_type,
        }
    }

    /// Get the listenKey URL based on market type.
    fn get_listen_key_url(&self) -> String {
        if self.market_type == MarketType::Spot {
            format!("{}/api/v3/userDataStream", SPOT_MAINNET_URL)
        } else {
            format!("{}/api/v1/private/user/listen-key", FUTURES_MAINNET_URL)
        }
    }

    /// Get the WebSocket URL with listenKey.
    fn get_ws_url(&self, listen_key: &str) -> String {
        if self.market_type == MarketType::Spot {
            format!("{}?listenKey={}", SPOT_WS_URL, listen_key)
        } else {
            format!("{}?listenKey={}", FUTURES_WS_URL, listen_key)
        }
    }

    /// Subscribe to order updates via WebSocket.
    ///
    /// Returns a JoinHandle for the background task that maintains
    /// the WebSocket connection and broadcasts order updates.
    ///
    /// The service handles:
    /// - Creating and renewing listenKey every 30 minutes
    /// - Automatic reconnection with exponential backoff
    /// - Parsing executionReport events and broadcasting OrderUpdateEvent
    ///
    /// # Arguments
    /// * `sender` - Broadcast channel sender for OrderUpdateEvent
    ///
    /// # Returns
    /// * `JoinHandle<()>` - Handle to the spawned background task
    pub async fn subscribe_order_updates(
        &self,
        sender: broadcast::Sender<OrderUpdateEvent>,
    ) -> Result<JoinHandle<()>> {
        let api_key = self.api_key.clone();
        let market_type = self.market_type;
        let exchange_name = "mexc".to_string();

        let handle = tokio::spawn(async move {
            let client = Client::new();
            let mut reconnect_delay = INITIAL_RECONNECT_DELAY_SECS;

            loop {
                // Create listenKey
                let listen_key_url = if market_type == MarketType::Spot {
                    format!("{}/api/v3/userDataStream", SPOT_MAINNET_URL)
                } else {
                    format!("{}/api/v1/private/user/listen-key", FUTURES_MAINNET_URL)
                };

                let listen_key_resp = client
                    .post(&listen_key_url)
                    .header("X-MEXC-APIKEY", &api_key)
                    .send()
                    .await;

                let listen_key = match listen_key_resp {
                    Ok(resp) => {
                        let text = resp.text().await.unwrap_or_default();
                        match serde_json::from_str::<serde_json::Value>(&text) {
                            Ok(json) => json
                                .get("listenKey")
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string())
                                .unwrap_or_default(),
                            Err(_) => String::new(),
                        }
                    }
                    Err(e) => {
                        error!("[{}] Failed to create listenKey: {}", exchange_name, e);
                        tokio::time::sleep(tokio::time::Duration::from_secs(reconnect_delay)).await;
                        reconnect_delay = next_reconnect_delay(reconnect_delay);
                        continue;
                    }
                };

                if listen_key.is_empty() {
                    error!("[{}] Empty listenKey", exchange_name);
                    tokio::time::sleep(tokio::time::Duration::from_secs(reconnect_delay)).await;
                    reconnect_delay = next_reconnect_delay(reconnect_delay);
                    continue;
                }

                let ws_url = if market_type == MarketType::Spot {
                    format!("{}?listenKey={}", SPOT_WS_URL, listen_key)
                } else {
                    format!("{}?listenKey={}", FUTURES_WS_URL, listen_key)
                };

                info!("[{}] Connecting to user data stream", exchange_name);

                match connect_async(&ws_url).await {
                    Ok((mut ws_stream, _)) => {
                        info!("[{}] Connected to user data stream", exchange_name);
                        reconnect_delay = INITIAL_RECONNECT_DELAY_SECS;

                        // Renew listenKey every 30 minutes
                        let mut keepalive_interval =
                            tokio::time::interval(tokio::time::Duration::from_secs(30 * 60));
                        keepalive_interval.tick().await; // Skip first immediate tick

                        loop {
                            tokio::select! {
                                msg = ws_stream.next() => {
                                    match msg {
                                        Some(Ok(Message::Text(text))) => {
                                            if let Ok(ev) = serde_json::from_str::<serde_json::Value>(&text) {
                                                let event_type = ev.get("e").and_then(|v| v.as_str()).unwrap_or("");

                                                if event_type == "executionReport" {
                                                    let event = OrderUpdateEvent {
                                                        exchange: exchange_name.clone(),
                                                        order_id: ev.get("i").and_then(|v| v.as_str())
                                                            .or_else(|| ev.get("i").and_then(|v| v.as_i64()).map(|_| ""))
                                                            .unwrap_or("").to_string(),
                                                        client_order_id: ev.get("c").and_then(|v| v.as_str()).map(|s| s.to_string()),
                                                        symbol: ev.get("s").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                                                        status: parse_order_status(
                                                            ev.get("X").and_then(|v| v.as_str()).unwrap_or("NEW")
                                                        ),
                                                        side: if ev.get("S").and_then(|v| v.as_str()) == Some("BUY") { OrderSide::Buy } else { OrderSide::Sell },
                                                        order_type: if ev.get("o").and_then(|v| v.as_str()) == Some("MARKET") { OrderType::Market } else { OrderType::Limit },
                                                        price: ev.get("p").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0),
                                                        quantity: ev.get("q").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0),
                                                        filled_quantity: ev.get("z").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0),
                                                        avg_fill_price: ev.get("L").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()),
                                                        fee: ev.get("n").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()),
                                                        fee_currency: ev.get("N").and_then(|v| v.as_str()).map(|s| s.to_string()),
                                                        exchange_timestamp: ev.get("T").and_then(|v| v.as_i64()).unwrap_or(0),
                                                        received_at: timestamp_ms(),
                                                        raw: Some(ev.clone()),
                                                    };
                                                    log_order_update(&event);
                                                    let _ = sender.send(event);
                                                }
                                            }
                                        }
                                        Some(Ok(Message::Ping(data))) => {
                                            let _ = ws_stream.send(Message::Pong(data)).await;
                                        }
                                        Some(Ok(Message::Close(_))) | Some(Err(_)) | None => {
                                            warn!("[{}] User data stream disconnected", exchange_name);
                                            break;
                                        }
                                        _ => {}
                                    }
                                }
                                _ = keepalive_interval.tick() => {
                                    // Renew listenKey via PUT request
                                    let put_resp = client
                                        .put(&listen_key_url)
                                        .header("X-MEXC-APIKEY", &api_key)
                                        .send()
                                        .await;
                                    match put_resp {
                                        Ok(resp) if resp.status().is_success() => {
                                            debug!("[{}] listenKey renewed", exchange_name);
                                        }
                                        _ => {
                                            warn!("[{}] listenKey renewal failed", exchange_name);
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!(
                            "[{}] Failed to connect user data stream: {}",
                            exchange_name, e
                        );
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
    fn test_user_data_service_creation() {
        let service = UserDataService::new("test_api_key", MarketType::Spot);
        assert_eq!(service.api_key, "test_api_key");
        assert_eq!(service.market_type, MarketType::Spot);
    }

    #[test]
    fn test_get_listen_key_url_spot() {
        let service = UserDataService::new("test_key", MarketType::Spot);
        let url = service.get_listen_key_url();
        assert!(url.contains("api.mexc.com"));
        assert!(url.contains("userDataStream"));
    }

    #[test]
    fn test_get_listen_key_url_futures() {
        let service = UserDataService::new("test_key", MarketType::Swap);
        let url = service.get_listen_key_url();
        assert!(url.contains("contract.mexc.com"));
        assert!(url.contains("listen-key"));
    }

    #[test]
    fn test_get_ws_url_spot() {
        let service = UserDataService::new("test_key", MarketType::Spot);
        let url = service.get_ws_url("test_listen_key");
        assert!(url.contains("wbs.mexc.com"));
        assert!(url.contains("listenKey=test_listen_key"));
    }

    #[test]
    fn test_get_ws_url_futures() {
        let service = UserDataService::new("test_key", MarketType::Swap);
        let url = service.get_ws_url("test_listen_key");
        assert!(url.contains("contract.mexc.com"));
        assert!(url.contains("listenKey=test_listen_key"));
    }
}
