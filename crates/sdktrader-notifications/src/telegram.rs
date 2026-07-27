//! Telegram notifications.

use crate::{NotificationMessage, NotificationResult};
use anyhow::Result;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info};

/// Telegram notifier configuration.
#[derive(Debug, Clone)]
pub struct TelegramConfig {
    /// Telegram bot token.
    pub bot_token: String,
    /// Chat ID to send messages to.
    pub chat_id: i64,
    /// Whether to disable link previews.
    pub disable_web_preview: bool,
    /// Whether to disable notifications.
    pub disable_notification: bool,
}

impl TelegramConfig {
    /// Create a new Telegram config.
    pub fn new(bot_token: &str, chat_id: i64) -> Self {
        Self {
            bot_token: bot_token.to_string(),
            chat_id,
            disable_web_preview: true,
            disable_notification: false,
        }
    }
}

/// Telegram notifier.
pub struct TelegramNotifier {
    config: TelegramConfig,
    client: Client,
}

impl TelegramNotifier {
    /// Create a new Telegram notifier.
    pub fn new(bot_token: &str, chat_id: i64) -> Self {
        Self {
            config: TelegramConfig::new(bot_token, chat_id),
            client: Client::new(),
        }
    }

    /// Create from config.
    pub fn from_config(config: TelegramConfig) -> Self {
        Self {
            config,
            client: Client::new(),
        }
    }

    /// Get the API URL for sending messages.
    fn api_url(&self) -> String {
        format!(
            "https://api.telegram.org/bot{}/sendMessage",
            self.config.bot_token
        )
    }

    /// Send a message.
    pub async fn send(&self, message: &NotificationMessage) -> Result<NotificationResult> {
        let text = message.to_text();
        self.send_text(&text).await
    }

    /// Send raw text.
    pub async fn send_text(&self, text: &str) -> Result<NotificationResult> {
        let request = TelegramRequest {
            chat_id: self.config.chat_id,
            text: text.to_string(),
            parse_mode: Some("HTML".to_string()),
            disable_web_page_preview: Some(self.config.disable_web_preview),
            disable_notification: Some(self.config.disable_notification),
        };

        debug!("Sending Telegram message to chat {}", self.config.chat_id);

        let response = self
            .client
            .post(&self.api_url())
            .json(&request)
            .send()
            .await?;

        let response_text = response.text().await?;
        debug!("Telegram response: {}", response_text);

        let result: TelegramResponse = serde_json::from_str(&response_text)?;

        if result.ok {
            info!("Telegram message sent successfully");
            Ok(NotificationResult::success())
        } else {
            let error_msg = result.description.unwrap_or_else(|| "Unknown error".to_string());
            error!("Telegram error: {}", error_msg);
            Ok(NotificationResult::failure(&error_msg))
        }
    }

    /// Test connection by sending a test message.
    pub async fn test_connection(&self) -> bool {
        let message = NotificationMessage::info("Test", "Connection test successful");
        match self.send(&message).await {
            Ok(result) => result.success,
            Err(e) => {
                error!("Telegram test failed: {}", e);
                false
            }
        }
    }

    /// Send an info notification.
    pub async fn send_info(&self, title: &str, body: &str) -> Result<NotificationResult> {
        self.send(&NotificationMessage::info(title, body)).await
    }

    /// Send an error notification.
    pub async fn send_error(&self, title: &str, body: &str) -> Result<NotificationResult> {
        self.send(&NotificationMessage::error(title, body)).await
    }

    /// Send a trading alert.
    pub async fn send_trade_alert(
        &self,
        symbol: &str,
        side: &str,
        price: f64,
        amount: f64,
    ) -> Result<NotificationResult> {
        let message = format!(
            "\u{1f514} <b>Trade Alert</b>\n\n\
             Symbol: <code>{}</code>\n\
             Side: <b>{}</b>\n\
             Price: <code>{:.4}</code>\n\
             Amount: <code>{:.4}</code>",
            symbol, side, price, amount
        );

        self.send_text(&message).await
    }
}

#[derive(Debug, Serialize)]
struct TelegramRequest {
    chat_id: i64,
    text: String,
    parse_mode: Option<String>,
    disable_web_page_preview: Option<bool>,
    disable_notification: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct TelegramResponse {
    ok: bool,
    description: Option<String>,
    result: Option<serde_json::Value>,
}
