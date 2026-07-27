//! REST API service for Bybit.

use sdktrader_core::utils::{hmac_sign, timestamp_ms};
use sdktrader_core::models::MarketType;
use anyhow::{Context, Result};
use reqwest::Client;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tracing::debug;

use crate::config::{MAINNET_URL, TESTNET_URL};

/// REST API service for Bybit.
pub struct RestService {
    api_key: String,
    api_secret: String,
    client: Client,
    base_url: String,
    market_type: MarketType,
    testnet: bool,
}

impl RestService {
    /// Create a new REST service.
    pub fn new(
        api_key: &str,
        api_secret: &str,
        testnet: bool,
        market_type: MarketType,
    ) -> Self {
        let base_url = if testnet { TESTNET_URL } else { MAINNET_URL };

        Self {
            api_key: api_key.to_string(),
            api_secret: api_secret.to_string(),
            client: Client::new(),
            base_url: base_url.to_string(),
            market_type,
            testnet,
        }
    }

    /// Get the HTTP client reference.
    pub fn client(&self) -> &Client {
        &self.client
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

    /// Get the market type.
    pub fn market_type(&self) -> MarketType {
        self.market_type
    }

    /// Check if using testnet.
    pub fn is_testnet(&self) -> bool {
        self.testnet
    }

    /// Get category string for API calls (spot/linear).
    pub fn category(&self) -> &str {
        match self.market_type {
            MarketType::Spot => "spot",
            MarketType::Swap => "linear",
        }
    }

    /// Sign a request with HMAC-SHA256.
    ///
    /// Bybit signature format: timestamp + api_key + recv_window + params
    pub fn sign_request(&self, params: &str, timestamp: i64) -> String {
        let recv_window = "5000";
        let sign_str = format!("{}{}{}{}", timestamp, self.api_key, recv_window, params);
        hmac_sign(&self.api_secret, &sign_str)
    }

    /// Make a public GET request (no signature).
    pub async fn public_get<T: DeserializeOwned>(&self, endpoint: &str) -> Result<T> {
        let url = format!("{}{}", self.base_url, endpoint);

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .context("Failed to send public request to Bybit")?;

        let text = response.text().await?;
        debug!("Bybit public response: {}", text);

        serde_json::from_str(&text).context("Failed to parse Bybit response")
    }

    /// Make a public GET request with query params.
    pub async fn public_get_with_params<T: DeserializeOwned>(
        &self,
        endpoint: &str,
        params: &str,
    ) -> Result<T> {
        let url = if params.is_empty() {
            format!("{}{}", self.base_url, endpoint)
        } else {
            format!("{}{}?{}", self.base_url, endpoint, params)
        };

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .context("Failed to send public request to Bybit")?;

        let text = response.text().await?;
        debug!("Bybit public response: {}", text);

        serde_json::from_str(&text).context("Failed to parse Bybit response")
    }

    /// Make a signed GET request.
    ///
    /// Bybit requires headers: X-BAPI-API-KEY, X-BAPI-SIGN, X-BAPI-TIMESTAMP, X-BAPI-RECV-WINDOW
    pub async fn signed_get<T: DeserializeOwned>(
        &self,
        endpoint: &str,
        params: &str,
    ) -> Result<T> {
        let timestamp = timestamp_ms();
        let signature = self.sign_request(params, timestamp);

        let url = if params.is_empty() {
            format!("{}{}", self.base_url, endpoint)
        } else {
            format!("{}{}?{}", self.base_url, endpoint, params)
        };

        let response = self
            .client
            .get(&url)
            .header("X-BAPI-API-KEY", &self.api_key)
            .header("X-BAPI-SIGN", signature)
            .header("X-BAPI-TIMESTAMP", timestamp.to_string())
            .header("X-BAPI-RECV-WINDOW", "5000")
            .send()
            .await
            .context("Failed to send signed GET request to Bybit")?;

        let text = response.text().await?;
        debug!("Bybit signed GET response: {}", text);

        serde_json::from_str(&text).context("Failed to parse Bybit response")
    }

    /// Make a signed POST request with JSON body.
    ///
    /// Bybit requires headers: X-BAPI-API-KEY, X-BAPI-SIGN, X-BAPI-TIMESTAMP, X-BAPI-RECV-WINDOW
    pub async fn signed_post<T: DeserializeOwned>(
        &self,
        endpoint: &str,
        body: &Value,
    ) -> Result<T> {
        let timestamp = timestamp_ms();
        let body_str = body.to_string();
        let signature = self.sign_request(&body_str, timestamp);

        let url = format!("{}{}", self.base_url, endpoint);

        let response = self
            .client
            .post(&url)
            .header("Content-Type", "application/json")
            .header("X-BAPI-API-KEY", &self.api_key)
            .header("X-BAPI-SIGN", signature)
            .header("X-BAPI-TIMESTAMP", timestamp.to_string())
            .header("X-BAPI-RECV-WINDOW", "5000")
            .body(body_str)
            .send()
            .await
            .context("Failed to send signed POST request to Bybit")?;

        let text = response.text().await?;
        debug!("Bybit signed POST response: {}", text);

        serde_json::from_str(&text).context("Failed to parse Bybit response")
    }

    /// Make a signed POST request with empty body.
    pub async fn signed_post_empty<T: DeserializeOwned>(&self, endpoint: &str) -> Result<T> {
        self.signed_post(endpoint, &serde_json::json!({})).await
    }

    /// Get the appropriate category-prefixed params string.
    pub fn with_category(&self, params: &str) -> String {
        if params.is_empty() {
            format!("category={}", self.category())
        } else {
            format!("category={}&{}", self.category(), params)
        }
    }
}
