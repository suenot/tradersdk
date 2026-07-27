//! Webhook notifications.

use crate::{NotificationMessage, NotificationResult};
use anyhow::Result;
use reqwest::Client;
use serde::Serialize;
use std::collections::HashMap;
use std::time::Duration;
use tracing::{debug, error, info};

/// Webhook notifier configuration.
#[derive(Debug, Clone)]
pub struct WebhookConfig {
    /// Webhook URL.
    pub url: String,
    /// Optional secret for signing.
    pub secret: Option<String>,
    /// Request timeout.
    pub timeout: Duration,
    /// Number of retry attempts.
    pub retry_count: u32,
    /// Custom headers.
    pub headers: HashMap<String, String>,
}

impl WebhookConfig {
    /// Create a new webhook config.
    pub fn new(url: &str) -> Self {
        Self {
            url: url.to_string(),
            secret: None,
            timeout: Duration::from_secs(10),
            retry_count: 3,
            headers: HashMap::new(),
        }
    }

    /// Set secret for signing.
    pub fn with_secret(mut self, secret: &str) -> Self {
        self.secret = Some(secret.to_string());
        self
    }

    /// Set timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Add custom header.
    pub fn with_header(mut self, key: &str, value: &str) -> Self {
        self.headers.insert(key.to_string(), value.to_string());
        self
    }
}

/// Webhook notifier.
pub struct WebhookNotifier {
    config: WebhookConfig,
    client: Client,
}

impl WebhookNotifier {
    /// Create a new webhook notifier.
    pub fn new(url: &str) -> Self {
        Self {
            config: WebhookConfig::new(url),
            client: Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap_or_default(),
        }
    }

    /// Create from config.
    pub fn from_config(config: WebhookConfig) -> Self {
        let client = Client::builder()
            .timeout(config.timeout)
            .build()
            .unwrap_or_default();

        Self { config, client }
    }

    /// Send a notification.
    pub async fn send(&self, message: &NotificationMessage) -> Result<NotificationResult> {
        let payload = WebhookPayload {
            title: message.title.clone(),
            body: message.body.clone(),
            level: format!("{:?}", message.level),
            timestamp: message.timestamp,
            metadata: message.metadata.clone(),
        };

        self.send_payload(&payload).await
    }

    /// Send a custom payload.
    pub async fn send_payload<T: Serialize>(&self, payload: &T) -> Result<NotificationResult> {
        let mut attempts = 0;
        let max_attempts = self.config.retry_count + 1;

        while attempts < max_attempts {
            attempts += 1;

            debug!("Sending webhook (attempt {}/{})", attempts, max_attempts);

            let mut request = self.client.post(&self.config.url).json(payload);

            // Add custom headers
            for (key, value) in &self.config.headers {
                request = request.header(key, value);
            }

            // Add signature if secret is configured
            if let Some(ref secret) = self.config.secret {
                let body = serde_json::to_string(payload)?;
                let signature = sdktrader_core::utils::hmac_sign(secret, &body);
                request = request.header("X-Signature", signature);
            }

            match request.send().await {
                Ok(response) => {
                    let status = response.status();
                    let body = response.text().await.unwrap_or_default();

                    if status.is_success() {
                        info!("Webhook sent successfully");
                        return Ok(NotificationResult::success());
                    } else {
                        error!("Webhook error ({}): {}", status, body);

                        if attempts >= max_attempts {
                            return Ok(NotificationResult::failure(&format!(
                                "HTTP {}: {}",
                                status, body
                            )));
                        }
                    }
                }
                Err(e) => {
                    error!("Webhook request failed: {}", e);

                    if attempts >= max_attempts {
                        return Ok(NotificationResult::failure(&e.to_string()));
                    }
                }
            }

            // Wait before retry
            tokio::time::sleep(Duration::from_millis(500 * attempts as u64)).await;
        }

        Ok(NotificationResult::failure("Max retries exceeded"))
    }

    /// Send raw JSON.
    pub async fn send_json(&self, json: serde_json::Value) -> Result<NotificationResult> {
        self.send_payload(&json).await
    }
}

#[derive(Debug, Serialize)]
struct WebhookPayload {
    title: String,
    body: String,
    level: String,
    timestamp: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata: Option<serde_json::Value>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_webhook_config() {
        let config = WebhookConfig::new("https://example.com/webhook")
            .with_secret("my_secret")
            .with_timeout(Duration::from_secs(30))
            .with_header("X-Custom", "value");

        assert_eq!(config.url, "https://example.com/webhook");
        assert_eq!(config.secret, Some("my_secret".to_string()));
        assert_eq!(config.timeout, Duration::from_secs(30));
        assert_eq!(config.headers.get("X-Custom"), Some(&"value".to_string()));
    }
}
