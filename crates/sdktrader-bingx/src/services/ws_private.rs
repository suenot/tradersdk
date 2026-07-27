//! Private WebSocket user data stream service for BingX.

use sdktrader_core::utils::{hmac_sign, timestamp_ms};
use sdktrader_core::models::{OrderSide, OrderStatus, OrderType, OrderUpdateEvent};
use anyhow::Result;
use flate2::read::GzDecoder;
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use std::io::Read;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, error, info, warn};

use super::super::config::{
    endpoints, INITIAL_RECONNECT_DELAY_SECS, MAINNET_URL, MAINNET_WS_USER_URL,
    MAX_RECONNECT_DELAY_SECS, TESTNET_URL,
};

/// listenKey renewal interval in seconds (30 minutes).
const LISTEN_KEY_RENEWAL_SECS: u64 = 30 * 60;

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
    match s.to_uppercase().as_str() {
        "NEW" | "PENDING" | "WORKING" => OrderStatus::New,
        "PARTIALLY_FILLED" => OrderStatus::PartiallyFilled,
        "FILLED" => OrderStatus::Filled,
        "CANCELLED" | "CANCELED" => OrderStatus::Cancelled,
        "REJECTED" | "FAILED" => OrderStatus::Rejected,
        "EXPIRED" => OrderStatus::Expired,
        _ => {
            warn!("Unknown BingX order status: '{}'", s);
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

/// User data stream service for BingX (order updates, position updates, etc.).
pub struct UserDataService {
    api_key: String,
    api_secret: String,
    testnet: bool,
    base_url: String,
}

impl UserDataService {
    /// Create a new user data stream service.
    pub fn new(api_key: &str, api_secret: &str, testnet: bool, base_url: Option<&str>) -> Self {
        let default_url = if testnet { TESTNET_URL } else { MAINNET_URL };
        Self {
            api_key: api_key.to_string(),
            api_secret: api_secret.to_string(),
            testnet,
            base_url: base_url.unwrap_or(default_url).to_string(),
        }
    }

    /// Get WebSocket URL for private stream with listenKey.
    fn ws_url(&self, listen_key: &str) -> String {
        if self.testnet {
            format!(
                "wss://open-api-vst.bingx.com/market?listenKey={}",
                listen_key
            )
        } else {
            format!("{}?listenKey={}", MAINNET_WS_USER_URL, listen_key)
        }
    }

    /// Subscribe to order updates stream.
    pub fn subscribe_order_updates(
        &self,
        sender: broadcast::Sender<OrderUpdateEvent>,
    ) -> Result<JoinHandle<()>> {
        let api_key = self.api_key.clone();
        let api_secret = self.api_secret.clone();
        let testnet = self.testnet;
        let base_url = self.base_url.clone();
        let exchange_name = "bingx".to_string();

        let handle = tokio::spawn(async move {
            let client = Client::new();
            let mut reconnect_delay = initial_reconnect_delay();

            loop {
                // Step 1: Create listenKey via POST /openApi/user/auth/userDataStream
                let listen_key = match create_listen_key(&client, &api_key, &api_secret, &base_url)
                    .await
                {
                    Ok(key) => key,
                    Err(e) => {
                        error!("[{}] Failed to create listenKey: {}", exchange_name, e);
                        tokio::time::sleep(tokio::time::Duration::from_secs(reconnect_delay)).await;
                        reconnect_delay = next_reconnect_delay(reconnect_delay);
                        continue;
                    }
                };

                if listen_key.is_empty() {
                    error!("[{}] Empty listenKey received", exchange_name);
                    tokio::time::sleep(tokio::time::Duration::from_secs(reconnect_delay)).await;
                    reconnect_delay = next_reconnect_delay(reconnect_delay);
                    continue;
                }

                // Step 2: Build WebSocket URL with listenKey
                let ws_url = if testnet {
                    format!(
                        "wss://open-api-vst.bingx.com/market?listenKey={}",
                        listen_key
                    )
                } else {
                    format!("{}?listenKey={}", MAINNET_WS_USER_URL, listen_key)
                };

                info!("[{}] Connecting to user data stream: {}", exchange_name, ws_url);

                // Step 3: Connect to WebSocket
                match connect_async(&ws_url).await {
                    Ok((mut ws_stream, _)) => {
                        info!("[{}] Connected to user data stream", exchange_name);
                        reconnect_delay = initial_reconnect_delay();

                        // Step 5: Set up listenKey renewal every 30 minutes
                        let mut keepalive_interval =
                            tokio::time::interval(tokio::time::Duration::from_secs(
                                LISTEN_KEY_RENEWAL_SECS,
                            ));
                        // Skip first tick
                        keepalive_interval.tick().await;

                        // Step 4: Main message loop
                        loop {
                            tokio::select! {
                                msg = ws_stream.next() => {
                                    match msg {
                                        Some(Ok(Message::Text(text))) => {
                                            // Step 4a: Handle Ping/Pong (BingX sends "Ping" as text)
                                            if text == "Ping" {
                                                let _ = ws_stream.send(Message::Text("Pong".to_string())).await;
                                                continue;
                                            }
                                            // Step 6: Parse order events
                                            if let Ok(ev) = serde_json::from_str::<serde_json::Value>(&text) {
                                                process_order_event(&ev, &exchange_name, &sender);
                                            }
                                        }
                                        // Step 3: Handle gzip-compressed Binary messages
                                        Some(Ok(Message::Binary(data))) => {
                                            let mut decoder = GzDecoder::new(&data[..]);
                                            let mut text = String::new();
                                            if decoder.read_to_string(&mut text).is_ok() {
                                                // Handle Ping in compressed messages
                                                if text == "Ping" {
                                                    let _ = ws_stream.send(Message::Text("Pong".to_string())).await;
                                                    continue;
                                                }
                                                if let Ok(ev) = serde_json::from_str::<serde_json::Value>(&text) {
                                                    process_order_event(&ev, &exchange_name, &sender);
                                                }
                                            }
                                        }
                                        Some(Ok(Message::Ping(data))) => {
                                            let _ = ws_stream.send(Message::Pong(data)).await;
                                        }
                                        Some(Ok(Message::Close(_))) => {
                                            warn!("[{}] User data stream closed by server", exchange_name);
                                            break;
                                        }
                                        Some(Err(e)) => {
                                            error!("[{}] User data stream error: {}", exchange_name, e);
                                            break;
                                        }
                                        None => {
                                            warn!("[{}] User data stream ended", exchange_name);
                                            break;
                                        }
                                        _ => {}
                                    }
                                }
                                // Step 5: Renew listenKey every 30 minutes via PUT
                                _ = keepalive_interval.tick() => {
                                    match renew_listen_key(&client, &api_key, &api_secret, &base_url, &listen_key).await {
                                        Ok(true) => {
                                            debug!("[{}] listenKey renewed successfully", exchange_name);
                                        }
                                        _ => {
                                            warn!("[{}] listenKey renewal failed, reconnecting...", exchange_name);
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

                // Step 7: Reconnect with exponential backoff
                warn!(
                    "[{}] Reconnecting user data stream in {}s...",
                    exchange_name, reconnect_delay
                );
                tokio::time::sleep(tokio::time::Duration::from_secs(reconnect_delay)).await;
                reconnect_delay = next_reconnect_delay(reconnect_delay);
            }
        });

        Ok(handle)
    }
}

/// Create a new listenKey via POST /openApi/user/auth/userDataStream.
async fn create_listen_key(
    client: &Client,
    api_key: &str,
    api_secret: &str,
    base_url: &str,
) -> Result<String> {
    let timestamp = timestamp_ms();
    let params = format!("timestamp={}", timestamp);
    let signature = hmac_sign(api_secret, &params);
    let url = format!(
        "{}{}?{}&signature={}",
        base_url,
        endpoints::USER_DATA_STREAM,
        params,
        signature
    );

    let response = client
        .post(&url)
        .header("X-BX-APIKEY", api_key)
        .send()
        .await?;

    let text = response.text().await?;
    let json: serde_json::Value = serde_json::from_str(&text)?;

    // listenKey can be at top level or in data field
    let listen_key = json
        .get("listenKey")
        .or_else(|| json.get("data").and_then(|d| d.get("listenKey")))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_default();

    Ok(listen_key)
}

/// Renew listenKey via PUT /openApi/user/auth/userDataStream.
async fn renew_listen_key(
    client: &Client,
    api_key: &str,
    api_secret: &str,
    base_url: &str,
    listen_key: &str,
) -> Result<bool> {
    let timestamp = timestamp_ms();
    let params = format!("timestamp={}", timestamp);
    let signature = hmac_sign(api_secret, &params);
    let url = format!(
        "{}{}?{}&signature={}&listenKey={}",
        base_url,
        endpoints::USER_DATA_STREAM,
        params,
        signature,
        listen_key
    );

    let response = client
        .put(&url)
        .header("X-BX-APIKEY", api_key)
        .send()
        .await?;

    Ok(response.status().is_success())
}

/// Process order event from WebSocket.
/// Parses ORDER_TRADE_UPDATE (futures) and executionReport (spot) events.
fn process_order_event(
    ev: &serde_json::Value,
    exchange_name: &str,
    sender: &broadcast::Sender<OrderUpdateEvent>,
) {
    let event_type = ev.get("e").and_then(|v| v.as_str()).unwrap_or("");

    // BingX uses ORDER_TRADE_UPDATE for futures, executionReport for spot
    if event_type != "ORDER_TRADE_UPDATE" && event_type != "executionReport" {
        return;
    }

    // Order data can be in "o" field or at root level
    let o = ev.get("o").unwrap_or(ev);

    // Extract order_id which can be string or number
    let order_id = o
        .get("i")
        .map(|v| match v {
            serde_json::Value::String(s) => s.clone(),
            serde_json::Value::Number(n) => n.to_string(),
            _ => String::new(),
        })
        .unwrap_or_default();

    let event = OrderUpdateEvent {
        exchange: exchange_name.to_string(),
        order_id,
        client_order_id: o.get("c").and_then(|v| v.as_str()).map(|s| s.to_string()),
        symbol: o
            .get("s")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        status: parse_order_status(o.get("X").and_then(|v| v.as_str()).unwrap_or("NEW")),
        side: if o.get("S").and_then(|v| v.as_str()) == Some("BUY") {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        },
        order_type: if o.get("o").and_then(|v| v.as_str()) == Some("MARKET") {
            OrderType::Market
        } else {
            OrderType::Limit
        },
        price: o
            .get("p")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0),
        quantity: o
            .get("q")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0),
        filled_quantity: o
            .get("z")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0),
        avg_fill_price: o
            .get("ap")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse().ok()),
        fee: o
            .get("n")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse().ok()),
        fee_currency: o.get("N").and_then(|v| v.as_str()).map(|s| s.to_string()),
        exchange_timestamp: ev.get("T").and_then(|v| v.as_i64()).unwrap_or(0),
        received_at: timestamp_ms(),
        raw: Some(ev.clone()),
    };

    log_order_update(&event);
    let _ = sender.send(event);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_order_status() {
        assert_eq!(parse_order_status("NEW"), OrderStatus::New);
        assert_eq!(parse_order_status("FILLED"), OrderStatus::Filled);
        assert_eq!(
            parse_order_status("PARTIALLY_FILLED"),
            OrderStatus::PartiallyFilled
        );
        assert_eq!(parse_order_status("CANCELLED"), OrderStatus::Cancelled);
        assert_eq!(parse_order_status("CANCELED"), OrderStatus::Cancelled);
        assert_eq!(parse_order_status("REJECTED"), OrderStatus::Rejected);
        assert_eq!(parse_order_status("EXPIRED"), OrderStatus::Expired);
    }

    #[test]
    fn test_next_reconnect_delay() {
        assert_eq!(next_reconnect_delay(1), 2);
        assert_eq!(next_reconnect_delay(2), 4);
        assert_eq!(next_reconnect_delay(16), 30); // Capped at MAX
        assert_eq!(next_reconnect_delay(30), 30); // Already at max
    }

    #[test]
    fn test_process_order_event_futures() {
        let (sender, _rx) = broadcast::channel(16);

        let event_json = serde_json::json!({
            "e": "ORDER_TRADE_UPDATE",
            "T": 1700000000000i64,
            "o": {
                "i": "123456789",
                "c": "client-order-123",
                "s": "BTC-USDT",
                "X": "FILLED",
                "S": "BUY",
                "o": "MARKET",
                "p": "50000.00",
                "q": "0.001",
                "z": "0.001",
                "ap": "50001.50",
                "n": "0.05",
                "N": "USDT"
            }
        });

        // This should not panic
        process_order_event(&event_json, "bingx", &sender);
    }

    #[test]
    fn test_process_order_event_spot() {
        let (sender, _rx) = broadcast::channel(16);

        let event_json = serde_json::json!({
            "e": "executionReport",
            "T": 1700000000000i64,
            "i": 987654321i64,
            "s": "BTC-USDT",
            "X": "NEW",
            "S": "SELL",
            "o": "LIMIT",
            "p": "51000.00",
            "q": "0.01",
            "z": "0.00"
        });

        // This should not panic
        process_order_event(&event_json, "bingx", &sender);
    }

    #[test]
    fn test_process_order_event_ignores_other_events() {
        let (sender, _rx) = broadcast::channel(16);

        let event_json = serde_json::json!({
            "e": "ACCOUNT_UPDATE",
            "T": 1700000000000i64
        });

        // This should not panic and should be ignored
        process_order_event(&event_json, "bingx", &sender);
    }
}
