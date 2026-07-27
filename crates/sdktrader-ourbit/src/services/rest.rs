//! Ourbit REST API service.

use anyhow::{Context, Result};
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde::Deserialize;
use sha2::Sha256;
use tracing::debug;

use crate::config::FUTURES_MAINNET_URL;
use sdktrader_core::utils::timestamp_ms;

type HmacSha256 = Hmac<Sha256>;

/// Ourbit REST API service.
pub struct RestService {
    client: Client,
    api_key: String,
    api_secret: String,
    base_url: String,
}

impl RestService {
    /// Create a new RestService with API credentials.
    pub fn new(api_key: &str, api_secret: &str) -> Self {
        Self {
            client: Client::new(),
            api_key: api_key.to_string(),
            api_secret: api_secret.to_string(),
            base_url: FUTURES_MAINNET_URL.to_string(),
        }
    }

    /// Generate Ourbit API signature (HMAC-SHA256).
    pub fn sign_request(&self, method: &str, params: &str, body: &str) -> (String, String) {
        let timestamp = timestamp_ms().to_string();

        let sign_string = match method {
            "POST" => format!("{}{}{}", self.api_key, timestamp, body),
            _ => format!("{}{}{}", self.api_key, timestamp, params),
        };

        debug!("Ourbit sign string: {}", sign_string);

        let mut mac = HmacSha256::new_from_slice(self.api_secret.as_bytes())
            .expect("HMAC can take key of any size");
        mac.update(sign_string.as_bytes());
        let signature = hex::encode(mac.finalize().into_bytes());

        (timestamp, signature)
    }

    /// Make a signed GET request.
    pub async fn signed_get<T: for<'de> Deserialize<'de>>(
        &self,
        endpoint: &str,
        params: &str,
    ) -> Result<T> {
        let (timestamp, signature) = self.sign_request("GET", params, "");

        let full_url = if params.is_empty() {
            format!("{}{}", self.base_url, endpoint)
        } else {
            format!("{}{}?{}", self.base_url, endpoint, params)
        };

        debug!("Ourbit signed GET: {}", full_url);

        let response = self
            .client
            .get(&full_url)
            .header("ApiKey", &self.api_key)
            .header("Request-Time", &timestamp)
            .header("Signature", signature)
            .header("Content-Type", "application/json")
            .send()
            .await
            .context("Failed to send request to Ourbit")?;

        let text = response.text().await?;
        debug!("Ourbit response: {}", text);

        serde_json::from_str(&text).context(format!("Failed to parse Ourbit response: {}", text))
    }

    /// Make a signed POST request.
    pub async fn signed_post<T: for<'de> Deserialize<'de>>(
        &self,
        endpoint: &str,
        body: &serde_json::Value,
    ) -> Result<T> {
        let body_str = serde_json::to_string(body)?;
        let (timestamp, signature) = self.sign_request("POST", "", &body_str);

        let full_url = format!("{}{}", self.base_url, endpoint);

        debug!("Ourbit signed POST: {} body: {}", full_url, body_str);

        let response = self
            .client
            .post(&full_url)
            .header("ApiKey", &self.api_key)
            .header("Request-Time", &timestamp)
            .header("Signature", signature)
            .header("Content-Type", "application/json")
            .body(body_str)
            .send()
            .await
            .context("Failed to send request to Ourbit")?;

        let text = response.text().await?;
        debug!("Ourbit response: {}", text);

        serde_json::from_str(&text).context(format!("Failed to parse Ourbit response: {}", text))
    }

    /// Make a public GET request (no authentication needed).
    pub async fn public_get<T: for<'de> Deserialize<'de>>(&self, endpoint: &str) -> Result<T> {
        let url = format!("{}{}", self.base_url, endpoint);

        debug!("Ourbit public GET: {}", url);

        let response = self.client.get(&url).send().await?;
        let text = response.text().await?;

        debug!("Ourbit response: {}", text);

        serde_json::from_str(&text).context(format!("Failed to parse Ourbit response: {}", text))
    }

    /// Get the base URL.
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Get the API key.
    pub fn api_key(&self) -> &str {
        &self.api_key
    }

    /// Get the API secret.
    pub fn api_secret(&self) -> &str {
        &self.api_secret
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sign_request_post() {
        let service = RestService::new("my_api_key", "my_secret");

        let body = r#"{"symbol":"BTC_USDT"}"#;
        let (timestamp, signature) = service.sign_request("POST", "", body);

        assert!(timestamp.parse::<i64>().is_ok());
        assert_eq!(signature.len(), 64);
        assert!(signature.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_sign_request_get() {
        let service = RestService::new("my_api_key", "my_secret");

        let params = "symbol=BTC_USDT";
        let (timestamp, signature) = service.sign_request("GET", params, "");

        assert!(timestamp.parse::<i64>().is_ok());
        assert_eq!(signature.len(), 64);
        assert!(signature.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_new_service() {
        let service = RestService::new("test_key", "test_secret");

        assert_eq!(service.api_key(), "test_key");
        assert_eq!(service.api_secret(), "test_secret");
        assert_eq!(service.base_url(), FUTURES_MAINNET_URL);
    }
}
