//! Gate.io public market data WebSocket service.

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

use crate::config::{FUTURES_WS_URL, SPOT_WS_URL};
use crate::helpers::format_symbol;
use tradersdk_core::models::{MarketType, WsTestResult};
use tradersdk_core::utils::timestamp_ms;

/// Gate.io public market data stream service.
pub struct MarketStreamService {
    market_type: MarketType,
}

impl MarketStreamService {
    pub fn new(market_type: MarketType) -> Self {
        Self { market_type }
    }

    fn ws_url(&self) -> &str {
        match self.market_type {
            MarketType::Spot => SPOT_WS_URL,
            MarketType::Swap => FUTURES_WS_URL,
        }
    }

    /// Format symbol for Gate.io API.
    fn format_symbol(&self, symbol: &str) -> String {
        format_symbol(symbol, self.market_type)
    }

    /// Test WebSocket orderbook subscription.
    /// Channel: spot.order_book / futures.order_book
    pub async fn test_ws_orderbook(
        &self,
        symbol: &str,
        _depth: u32,
        timeout_ms: u64,
    ) -> Result<WsTestResult> {
        let formatted = self.format_symbol(symbol);
        let ws_url = self.ws_url();

        let start = std::time::Instant::now();
        let (mut ws_stream, _) = connect_async(ws_url)
            .await
            .context("Failed to connect to Gate.io WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        // Subscribe to orderbook
        // Spot: {"time": ..., "channel": "spot.order_book", "event": "subscribe", "payload": ["BTC_USDT", "20", "100ms"]}
        // Futures: {"time": ..., "channel": "futures.order_book", "event": "subscribe", "payload": ["BTC_USDT", "20", "100ms"]}
        let subscribe_msg = if self.market_type == MarketType::Spot {
            serde_json::json!({
                "time": timestamp_ms() / 1000,
                "channel": "spot.order_book",
                "event": "subscribe",
                "payload": [formatted, "20", "100ms"]
            })
        } else {
            serde_json::json!({
                "time": timestamp_ms() / 1000,
                "channel": "futures.order_book",
                "event": "subscribe",
                "payload": [formatted, "20", "100ms"]
            })
        };

        ws_stream
            .send(Message::Text(subscribe_msg.to_string()))
            .await?;

        let mut messages_received = 0u32;
        let mut data_sample = None;
        let timeout = tokio::time::Duration::from_millis(timeout_ms);

        loop {
            tokio::select! {
                msg = ws_stream.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            messages_received += 1;
                            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&text) {
                                if parsed.get("result").is_some() || parsed.get("bids").is_some() {
                                    data_sample = Some(parsed);
                                    break;
                                }
                            }
                        }
                        Some(Ok(Message::Ping(data))) => {
                            let _ = ws_stream.send(Message::Pong(data)).await;
                        }
                        Some(Ok(_)) => {}
                        Some(Err(e)) => {
                            return Err(anyhow::anyhow!("WebSocket error: {}", e));
                        }
                        None => break,
                    }
                }
                _ = tokio::time::sleep(timeout) => {
                    break;
                }
            }
        }

        let _ = ws_stream.close(None).await;

        Ok(WsTestResult {
            connected: true,
            latency_ms: connect_time,
            messages_received,
            subscription_type: "order_book".to_string(),
            data_sample,
        })
    }

    /// Test WebSocket trades subscription.
    /// Channel: spot.trades / futures.trades
    pub async fn test_ws_trades(&self, symbol: &str, timeout_ms: u64) -> Result<WsTestResult> {
        let formatted = self.format_symbol(symbol);
        let ws_url = self.ws_url();

        let start = std::time::Instant::now();
        let (mut ws_stream, _) = connect_async(ws_url)
            .await
            .context("Failed to connect to Gate.io WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        let subscribe_msg = if self.market_type == MarketType::Spot {
            serde_json::json!({
                "time": timestamp_ms() / 1000,
                "channel": "spot.trades",
                "event": "subscribe",
                "payload": [formatted]
            })
        } else {
            serde_json::json!({
                "time": timestamp_ms() / 1000,
                "channel": "futures.trades",
                "event": "subscribe",
                "payload": [formatted]
            })
        };

        ws_stream
            .send(Message::Text(subscribe_msg.to_string()))
            .await?;

        let mut messages_received = 0u32;
        let mut data_sample = None;
        let timeout = tokio::time::Duration::from_millis(timeout_ms);

        loop {
            tokio::select! {
                msg = ws_stream.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            messages_received += 1;
                            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&text) {
                                if parsed.get("result").is_some() {
                                    data_sample = Some(parsed);
                                    break;
                                }
                            }
                        }
                        Some(Ok(Message::Ping(data))) => {
                            let _ = ws_stream.send(Message::Pong(data)).await;
                        }
                        Some(Ok(_)) => {}
                        Some(Err(e)) => {
                            return Err(anyhow::anyhow!("WebSocket error: {}", e));
                        }
                        None => break,
                    }
                }
                _ = tokio::time::sleep(timeout) => {
                    break;
                }
            }
        }

        let _ = ws_stream.close(None).await;

        Ok(WsTestResult {
            connected: true,
            latency_ms: connect_time,
            messages_received,
            subscription_type: "trades".to_string(),
            data_sample,
        })
    }

    /// Test WebSocket ticker subscription.
    /// Channel: spot.tickers / futures.tickers
    pub async fn test_ws_ticker(&self, symbol: &str, timeout_ms: u64) -> Result<WsTestResult> {
        let formatted = self.format_symbol(symbol);
        let ws_url = self.ws_url();

        let start = std::time::Instant::now();
        let (mut ws_stream, _) = connect_async(ws_url)
            .await
            .context("Failed to connect to Gate.io WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        let subscribe_msg = if self.market_type == MarketType::Spot {
            serde_json::json!({
                "time": timestamp_ms() / 1000,
                "channel": "spot.tickers",
                "event": "subscribe",
                "payload": [formatted]
            })
        } else {
            serde_json::json!({
                "time": timestamp_ms() / 1000,
                "channel": "futures.tickers",
                "event": "subscribe",
                "payload": [formatted]
            })
        };

        ws_stream
            .send(Message::Text(subscribe_msg.to_string()))
            .await?;

        let mut messages_received = 0u32;
        let mut data_sample = None;
        let timeout = tokio::time::Duration::from_millis(timeout_ms);

        loop {
            tokio::select! {
                msg = ws_stream.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            messages_received += 1;
                            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&text) {
                                data_sample = Some(parsed);
                                break;
                            }
                        }
                        Some(Ok(Message::Ping(data))) => {
                            let _ = ws_stream.send(Message::Pong(data)).await;
                        }
                        Some(Ok(_)) => {}
                        Some(Err(e)) => {
                            return Err(anyhow::anyhow!("WebSocket error: {}", e));
                        }
                        None => break,
                    }
                }
                _ = tokio::time::sleep(timeout) => {
                    break;
                }
            }
        }

        let _ = ws_stream.close(None).await;

        Ok(WsTestResult {
            connected: true,
            latency_ms: connect_time,
            messages_received,
            subscription_type: "tickers".to_string(),
            data_sample,
        })
    }
}
