//! Notification manager for multiple channels.

use crate::{
    NotificationMessage, NotificationResult, TelegramConfig, TelegramNotifier,
    WebhookConfig, WebhookNotifier,
};
use anyhow::Result;
use std::collections::HashMap;
use tracing::{debug, error, info};

/// Channel type for notifications.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ChannelType {
    Telegram,
    Webhook,
}

/// A notification channel.
enum Channel {
    Telegram(TelegramNotifier),
    Webhook(WebhookNotifier),
}

/// Manages multiple notification channels.
pub struct NotificationManager {
    channels: HashMap<String, Channel>,
    enabled: bool,
}

impl NotificationManager {
    /// Create a new notification manager.
    pub fn new() -> Self {
        Self {
            channels: HashMap::new(),
            enabled: true,
        }
    }

    /// Enable or disable notifications.
    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }

    /// Add a Telegram channel.
    pub fn add_telegram(&mut self, name: &str, bot_token: &str, chat_id: i64) {
        let notifier = TelegramNotifier::new(bot_token, chat_id);
        self.channels
            .insert(name.to_string(), Channel::Telegram(notifier));
        info!("Added Telegram channel: {}", name);
    }

    /// Add a Telegram channel from config.
    pub fn add_telegram_from_config(&mut self, name: &str, config: TelegramConfig) {
        let notifier = TelegramNotifier::from_config(config);
        self.channels
            .insert(name.to_string(), Channel::Telegram(notifier));
        info!("Added Telegram channel: {}", name);
    }

    /// Add a webhook channel.
    pub fn add_webhook(&mut self, name: &str, url: &str) {
        let notifier = WebhookNotifier::new(url);
        self.channels
            .insert(name.to_string(), Channel::Webhook(notifier));
        info!("Added webhook channel: {}", name);
    }

    /// Add a webhook channel from config.
    pub fn add_webhook_from_config(&mut self, name: &str, config: WebhookConfig) {
        let notifier = WebhookNotifier::from_config(config);
        self.channels
            .insert(name.to_string(), Channel::Webhook(notifier));
        info!("Added webhook channel: {}", name);
    }

    /// Remove a channel.
    pub fn remove_channel(&mut self, name: &str) -> bool {
        self.channels.remove(name).is_some()
    }

    /// List all channel names.
    pub fn list_channels(&self) -> Vec<&str> {
        self.channels.keys().map(|s| s.as_str()).collect()
    }

    /// Send a notification to all channels.
    pub async fn send_all(&self, message: &NotificationMessage) -> Vec<(String, NotificationResult)> {
        if !self.enabled {
            debug!("Notifications disabled, skipping");
            return vec![];
        }

        let mut results = Vec::new();

        for (name, channel) in &self.channels {
            let result = match channel {
                Channel::Telegram(notifier) => notifier.send(message).await,
                Channel::Webhook(notifier) => notifier.send(message).await,
            };

            match result {
                Ok(r) => results.push((name.clone(), r)),
                Err(e) => {
                    error!("Failed to send to channel {}: {}", name, e);
                    results.push((name.clone(), NotificationResult::failure(&e.to_string())));
                }
            }
        }

        results
    }

    /// Send a notification to a specific channel.
    pub async fn send_to(&self, channel_name: &str, message: &NotificationMessage) -> Result<NotificationResult> {
        if !self.enabled {
            debug!("Notifications disabled, skipping");
            return Ok(NotificationResult::success());
        }

        let channel = self
            .channels
            .get(channel_name)
            .ok_or_else(|| anyhow::anyhow!("Channel not found: {}", channel_name))?;

        match channel {
            Channel::Telegram(notifier) => notifier.send(message).await,
            Channel::Webhook(notifier) => notifier.send(message).await,
        }
    }

    /// Send info notification to all channels.
    pub async fn notify_info(&self, title: &str, body: &str) {
        let message = NotificationMessage::info(title, body);
        self.send_all(&message).await;
    }

    /// Send warning notification to all channels.
    pub async fn notify_warning(&self, title: &str, body: &str) {
        let message = NotificationMessage::warning(title, body);
        self.send_all(&message).await;
    }

    /// Send error notification to all channels.
    pub async fn notify_error(&self, title: &str, body: &str) {
        let message = NotificationMessage::error(title, body);
        self.send_all(&message).await;
    }

    /// Send trade alert to all channels.
    pub async fn notify_trade(
        &self,
        symbol: &str,
        side: &str,
        price: f64,
        amount: f64,
    ) {
        let body = format!(
            "Symbol: {}\nSide: {}\nPrice: {:.4}\nAmount: {:.4}",
            symbol, side, price, amount
        );
        let message = NotificationMessage::info("Trade Alert", &body);
        self.send_all(&message).await;
    }
}

impl Default for NotificationManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_notification_manager() {
        let mut manager = NotificationManager::new();

        manager.add_webhook("test", "https://example.com/webhook");

        assert_eq!(manager.list_channels().len(), 1);
        assert!(manager.list_channels().contains(&"test"));

        manager.remove_channel("test");
        assert!(manager.list_channels().is_empty());
    }
}
