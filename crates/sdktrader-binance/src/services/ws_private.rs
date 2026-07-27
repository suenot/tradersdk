//! Private WebSocket user data stream service for Binance.

use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use sdktrader_core::models::{MarketType, OrderSide, OrderType, OrderUpdateEvent};
use sdktrader_core::utils::timestamp_ms;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, error, info, warn};

use crate::config::{
    endpoints, FUTURES_USER_WS_MAINNET, FUTURES_USER_WS_TESTNET, LISTEN_KEY_KEEPALIVE_SECS,
    SPOT_USER_WS_MAINNET, SPOT_USER_WS_TESTNET,
};
use crate::helpers::{
    initial_reconnect_delay, log_order_update, next_reconnect_delay, parse_order_status,
};

/// User data stream service for Binance (order updates, position updates, etc.).
pub struct UserDataService {
    api_key: String,
    base_url: String,
    testnet: bool,
    market_type: MarketType,
}

impl UserDataService {
    /// Create a new user data stream service.
    pub fn new(api_key: &str, base_url: &str, testnet: bool, market_type: MarketType) -> Self {
        Self {
            api_key: api_key.to_string(),
            base_url: base_url.to_string(),
            testnet,
            market_type,
        }
    }

    /// Get WebSocket URL for user data stream.
    fn ws_url(&self) -> &str {
        match (self.market_type, self.testnet) {
            (MarketType::Spot, false) => SPOT_USER_WS_MAINNET,
            (MarketType::Spot, true) => SPOT_USER_WS_TESTNET,
            (MarketType::Swap, false) => FUTURES_USER_WS_MAINNET,
            (MarketType::Swap, true) => FUTURES_USER_WS_TESTNET,
        }
    }

    /// Get listen key endpoint.
    fn listen_key_endpoint(&self) -> &str {
        if self.market_type == MarketType::Spot {
            endpoints::SPOT_LISTEN_KEY
        } else {
            endpoints::FUTURES_LISTEN_KEY
        }
    }

    /// Subscribe to order updates stream.
    pub fn subscribe(&self, sender: broadcast::Sender<OrderUpdateEvent>) -> JoinHandle<()> {
        let api_key = self.api_key.clone();
        let base_url = self.base_url.clone();
        let _market_type = self.market_type;
        let ws_base = self.ws_url().to_string();
        let listen_key_endpoint = self.listen_key_endpoint().to_string();
        let exchange_name = "binance".to_string();

        tokio::spawn(async move {
            let client = Client::new();
            let mut reconnect_delay = initial_reconnect_delay();

            loop {
                // Step 1: Create listenKey via REST
                let listen_key_url = format!("{}{}", base_url, listen_key_endpoint);
                let listen_key_resp = client
                    .post(&listen_key_url)
                    .header("X-MBX-APIKEY", &api_key)
                    .send()
                    .await;

                let listen_key = match listen_key_resp {
                    Ok(resp) => {
                        let text = resp.text().await.unwrap_or_default();
                        match serde_json::from_str::<serde_json::Value>(&text) {
                            Ok(json) => {
                                if let Some(key) = json.get("listenKey").and_then(|v| v.as_str()) {
                                    key.to_string()
                                } else {
                                    error!(
                                        "[{}] No listenKey in response: {}",
                                        exchange_name, text
                                    );
                                    tokio::time::sleep(tokio::time::Duration::from_secs(
                                        reconnect_delay,
                                    ))
                                    .await;
                                    reconnect_delay = next_reconnect_delay(reconnect_delay);
                                    continue;
                                }
                            }
                            Err(e) => {
                                error!(
                                    "[{}] Failed to parse listenKey response: {}",
                                    exchange_name, e
                                );
                                tokio::time::sleep(tokio::time::Duration::from_secs(
                                    reconnect_delay,
                                ))
                                .await;
                                reconnect_delay = next_reconnect_delay(reconnect_delay);
                                continue;
                            }
                        }
                    }
                    Err(e) => {
                        error!("[{}] Failed to create listenKey: {}", exchange_name, e);
                        tokio::time::sleep(tokio::time::Duration::from_secs(reconnect_delay)).await;
                        reconnect_delay = next_reconnect_delay(reconnect_delay);
                        continue;
                    }
                };

                info!(
                    "[{}] Got listenKey, connecting to user data stream",
                    exchange_name
                );

                // Step 2: Connect to WS with listenKey
                let ws_url = format!("{}/{}", ws_base, listen_key);

                match connect_async(&ws_url).await {
                    Ok((mut ws_stream, _)) => {
                        info!("[{}] Connected to user data stream", exchange_name);
                        reconnect_delay = initial_reconnect_delay();

                        // Keepalive timer: renew listenKey every 30 minutes
                        let mut keepalive_interval = tokio::time::interval(
                            tokio::time::Duration::from_secs(LISTEN_KEY_KEEPALIVE_SECS),
                        );
                        keepalive_interval.tick().await; // skip first immediate tick

                        loop {
                            tokio::select! {
                                msg = ws_stream.next() => {
                                    match msg {
                                        Some(Ok(Message::Text(text))) => {
                                            if let Ok(event_json) = serde_json::from_str::<serde_json::Value>(&text) {
                                                let event_type = event_json.get("e").and_then(|v| v.as_str()).unwrap_or("");

                                                // Spot: executionReport, Futures: ORDER_TRADE_UPDATE
                                                if event_type == "executionReport" {
                                                    let event = parse_spot_execution_report(&event_json, &exchange_name);
                                                    log_order_update(&event);
                                                    let _ = sender.send(event);
                                                } else if event_type == "ORDER_TRADE_UPDATE" {
                                                    let event = parse_futures_order_update(&event_json, &exchange_name);
                                                    log_order_update(&event);
                                                    let _ = sender.send(event);
                                                }
                                            }
                                        }
                                        Some(Ok(Message::Ping(data))) => {
                                            let _ = ws_stream.send(Message::Pong(data)).await;
                                        }
                                        Some(Ok(Message::Close(_))) => {
                                            warn!("[{}] User data stream closed", exchange_name);
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
                                _ = keepalive_interval.tick() => {
                                    // Renew listenKey
                                    let put_resp = client
                                        .put(&listen_key_url)
                                        .header("X-MBX-APIKEY", &api_key)
                                        .send()
                                        .await;
                                    match put_resp {
                                        Ok(resp) if resp.status().is_success() => {
                                            debug!("[{}] listenKey renewed", exchange_name);
                                        }
                                        Ok(resp) => {
                                            warn!("[{}] listenKey renewal failed: {}", exchange_name, resp.status());
                                            break; // reconnect with new listenKey
                                        }
                                        Err(e) => {
                                            warn!("[{}] listenKey renewal error: {}", exchange_name, e);
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
                    "[{}] Reconnecting user data stream in {}s...",
                    exchange_name, reconnect_delay
                );
                tokio::time::sleep(tokio::time::Duration::from_secs(reconnect_delay)).await;
                reconnect_delay = next_reconnect_delay(reconnect_delay);
            }
        })
    }
}

/// Parse spot execution report event to OrderUpdateEvent.
fn parse_spot_execution_report(event_json: &serde_json::Value, exchange: &str) -> OrderUpdateEvent {
    OrderUpdateEvent {
        exchange: exchange.to_string(),
        order_id: event_json
            .get("i")
            .and_then(|v| v.as_i64())
            .map(|v| v.to_string())
            .unwrap_or_default(),
        client_order_id: event_json
            .get("c")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        symbol: event_json
            .get("s")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        status: parse_order_status(
            event_json
                .get("X")
                .and_then(|v| v.as_str())
                .unwrap_or("NEW"),
        ),
        side: if event_json.get("S").and_then(|v| v.as_str()) == Some("BUY") {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        },
        order_type: if event_json.get("o").and_then(|v| v.as_str()) == Some("MARKET") {
            OrderType::Market
        } else {
            OrderType::Limit
        },
        price: event_json
            .get("p")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0),
        quantity: event_json
            .get("q")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0),
        filled_quantity: event_json
            .get("z")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0),
        avg_fill_price: event_json
            .get("L")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse().ok()),
        fee: event_json
            .get("n")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse().ok()),
        fee_currency: event_json
            .get("N")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        exchange_timestamp: event_json.get("T").and_then(|v| v.as_i64()).unwrap_or(0),
        received_at: timestamp_ms(),
        raw: Some(event_json.clone()),
    }
}

/// Parse futures ORDER_TRADE_UPDATE event to OrderUpdateEvent.
fn parse_futures_order_update(event_json: &serde_json::Value, exchange: &str) -> OrderUpdateEvent {
    let o = event_json.get("o").unwrap_or(event_json);

    OrderUpdateEvent {
        exchange: exchange.to_string(),
        order_id: o
            .get("i")
            .and_then(|v| v.as_i64())
            .map(|v| v.to_string())
            .unwrap_or_default(),
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
        exchange_timestamp: event_json.get("T").and_then(|v| v.as_i64()).unwrap_or(0),
        received_at: timestamp_ms(),
        raw: Some(event_json.clone()),
    }
}
