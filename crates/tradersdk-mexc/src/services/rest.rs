//! MEXC REST API service.
//!
//! MEXC has TWO different signing methods:
//! 1. Spot API v3: HMAC-SHA256 signature in URL params, X-MEXC-APIKEY header
//! 2. Futures API v1: Signature in headers (ApiKey, Request-Time, Signature)

use anyhow::{Context, Result};
use reqwest::Client;
use serde::Deserialize;
use tracing::debug;

use crate::config::{FUTURES_MAINNET_URL, SPOT_MAINNET_URL};
use tradersdk_core::models::MarketType;
use tradersdk_core::utils::{hmac_sign, timestamp_ms};

/// MEXC REST API service.
pub struct RestService {
    client: Client,
    api_key: String,
    api_secret: String,
    market_type: MarketType,
    base_url: String,
}

impl RestService {
    /// Create a new REST service.
    pub fn new(api_key: &str, api_secret: &str, market_type: MarketType) -> Self {
        let base_url = match market_type {
            MarketType::Spot => SPOT_MAINNET_URL,
            MarketType::Swap => FUTURES_MAINNET_URL,
        };

        Self {
            client: Client::new(),
            api_key: api_key.to_string(),
            api_secret: api_secret.to_string(),
            market_type,
            base_url: base_url.to_string(),
        }
    }

    /// Sign query parameters with HMAC-SHA256.
    fn sign(&self, query: &str) -> String {
        hmac_sign(&self.api_secret, query)
    }

    // === Spot API Methods ===
    // Spot API v3 uses:
    // - X-MEXC-APIKEY header for API key
    // - timestamp and signature as URL query parameters
    // - Signature is HMAC-SHA256 of the query string

    /// Make a signed GET request (for spot API).
    ///
    /// Spot signing: HMAC-SHA256 signature appended to URL params.
    /// Headers: X-MEXC-APIKEY
    pub async fn signed_get_spot<T: for<'de> Deserialize<'de>>(
        &self,
        endpoint: &str,
        params: &str,
    ) -> Result<T> {
        let timestamp = timestamp_ms();
        let query = if params.is_empty() {
            format!("timestamp={}", timestamp)
        } else {
            format!("{}&timestamp={}", params, timestamp)
        };
        let signature = self.sign(&query);
        let url = format!(
            "{}{}?{}&signature={}",
            self.base_url, endpoint, query, signature
        );

        let response = self
            .client
            .get(&url)
            .header("X-MEXC-APIKEY", &self.api_key)
            .header("Content-Type", "application/json")
            .send()
            .await
            .context("Failed to send request to MEXC")?;

        let text = response.text().await?;
        debug!("MEXC response: {}", text);

        serde_json::from_str(&text).context(format!("Failed to parse MEXC response: {}", text))
    }

    /// Make a signed POST request (for spot API).
    ///
    /// Spot signing: HMAC-SHA256 signature appended to URL params.
    /// Headers: X-MEXC-APIKEY
    pub async fn signed_post_spot<T: for<'de> Deserialize<'de>>(
        &self,
        endpoint: &str,
        params: &str,
    ) -> Result<T> {
        let timestamp = timestamp_ms();
        let query = if params.is_empty() {
            format!("timestamp={}", timestamp)
        } else {
            format!("{}&timestamp={}", params, timestamp)
        };
        let signature = self.sign(&query);
        let url = format!(
            "{}{}?{}&signature={}",
            self.base_url, endpoint, query, signature
        );

        let response = self
            .client
            .post(&url)
            .header("X-MEXC-APIKEY", &self.api_key)
            .header("Content-Type", "application/json")
            .send()
            .await
            .context("Failed to send request to MEXC")?;

        let text = response.text().await?;
        debug!("MEXC response: {}", text);

        serde_json::from_str(&text).context(format!("Failed to parse MEXC response: {}", text))
    }

    /// Make a signed DELETE request (for spot API).
    ///
    /// Spot signing: HMAC-SHA256 signature appended to URL params.
    /// Headers: X-MEXC-APIKEY
    pub async fn signed_delete_spot<T: for<'de> Deserialize<'de>>(
        &self,
        endpoint: &str,
        params: &str,
    ) -> Result<T> {
        let timestamp = timestamp_ms();
        let query = if params.is_empty() {
            format!("timestamp={}", timestamp)
        } else {
            format!("{}&timestamp={}", params, timestamp)
        };
        let signature = self.sign(&query);
        let url = format!(
            "{}{}?{}&signature={}",
            self.base_url, endpoint, query, signature
        );

        let response = self
            .client
            .delete(&url)
            .header("X-MEXC-APIKEY", &self.api_key)
            .header("Content-Type", "application/json")
            .send()
            .await
            .context("Failed to send request to MEXC")?;

        let text = response.text().await?;
        debug!("MEXC response: {}", text);

        serde_json::from_str(&text).context(format!("Failed to parse MEXC response: {}", text))
    }

    // === Futures API Methods ===
    // Futures API v1 uses different signing:
    // - ApiKey header for API key
    // - Request-Time header for timestamp
    // - Signature header containing HMAC-SHA256 of (apiKey + timestamp + params)
    // - Query params in URL (no signature in URL)

    /// Make a signed request for futures API (different signing method).
    ///
    /// Futures signing: Signature = HMAC-SHA256(apiKey + timestamp + params)
    /// Headers: ApiKey, Request-Time, Signature
    pub async fn signed_request_futures<T: for<'de> Deserialize<'de>>(
        &self,
        method: &str,
        endpoint: &str,
        params: &str,
    ) -> Result<T> {
        let timestamp = timestamp_ms();
        // Futures signature format: apiKey + timestamp + params
        let request_param = format!("{}{}{}", self.api_key, timestamp, params);
        let signature = self.sign(&request_param);

        let url = if params.is_empty() {
            format!("{}{}", self.base_url, endpoint)
        } else {
            format!("{}{}?{}", self.base_url, endpoint, params)
        };

        let response = match method {
            "GET" => {
                self.client
                    .get(&url)
                    .header("ApiKey", &self.api_key)
                    .header("Request-Time", timestamp.to_string())
                    .header("Signature", signature)
                    .header("Content-Type", "application/json")
                    .send()
                    .await?
            }
            "POST" => {
                self.client
                    .post(&url)
                    .header("ApiKey", &self.api_key)
                    .header("Request-Time", timestamp.to_string())
                    .header("Signature", signature)
                    .header("Content-Type", "application/json")
                    .send()
                    .await?
            }
            "DELETE" => {
                self.client
                    .delete(&url)
                    .header("ApiKey", &self.api_key)
                    .header("Request-Time", timestamp.to_string())
                    .header("Signature", signature)
                    .header("Content-Type", "application/json")
                    .send()
                    .await?
            }
            _ => anyhow::bail!("Unsupported HTTP method: {}", method),
        };

        let text = response.text().await?;
        debug!("MEXC futures response: {}", text);

        serde_json::from_str(&text)
            .context(format!("Failed to parse MEXC futures response: {}", text))
    }

    // === Public API Methods ===

    /// Make a public GET request (no auth).
    pub async fn public_get<T: for<'de> Deserialize<'de>>(&self, endpoint: &str) -> Result<T> {
        let url = format!("{}{}", self.base_url, endpoint);
        let response = self.client.get(&url).send().await?;
        let text = response.text().await?;
        debug!("MEXC response: {}", text);
        serde_json::from_str(&text).context(format!("Failed to parse: {}", text))
    }

    /// Make a public GET request with query parameters (no auth).
    pub async fn public_get_with_params<T: for<'de> Deserialize<'de>>(
        &self,
        endpoint: &str,
        params: &str,
    ) -> Result<T> {
        let url = if params.is_empty() {
            format!("{}{}", self.base_url, endpoint)
        } else {
            format!("{}{}?{}", self.base_url, endpoint, params)
        };
        let response = self.client.get(&url).send().await?;
        let text = response.text().await?;
        debug!("MEXC response: {}", text);
        serde_json::from_str(&text).context(format!("Failed to parse: {}", text))
    }

    // === Accessors ===

    /// Get the market type.
    pub fn market_type(&self) -> MarketType {
        self.market_type
    }

    /// Get the base URL.
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Get the API key.
    pub fn api_key(&self) -> &str {
        &self.api_key
    }

    /// Get the HTTP client reference.
    pub fn client(&self) -> &Client {
        &self.client
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rest_service_creation_spot() {
        let service = RestService::new("test_key", "test_secret", MarketType::Spot);
        assert_eq!(service.market_type(), MarketType::Spot);
        assert_eq!(service.base_url(), SPOT_MAINNET_URL);
        assert_eq!(service.api_key(), "test_key");
    }

    #[test]
    fn test_rest_service_creation_futures() {
        let service = RestService::new("test_key", "test_secret", MarketType::Swap);
        assert_eq!(service.market_type(), MarketType::Swap);
        assert_eq!(service.base_url(), FUTURES_MAINNET_URL);
    }
}
