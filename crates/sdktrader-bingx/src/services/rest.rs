//! REST API service for BingX.

use anyhow::{Context, Result};
use reqwest::Client;
use sdktrader_core::models::MarketType;
use sdktrader_core::utils::{hmac_sign, timestamp_ms};
use serde::de::DeserializeOwned;
use tracing::debug;

use super::super::config::{MAINNET_URL, TESTNET_URL};
use super::super::models::BingxResponse;

/// REST API service for BingX.
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
    pub fn new(api_key: &str, api_secret: &str, testnet: bool, market_type: MarketType) -> Self {
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

    /// Get API path prefix based on market type.
    ///
    /// Returns `/openApi/swap/v2` for Swap or `/openApi/spot/v1` for Spot.
    pub fn api_path(&self) -> &str {
        match self.market_type {
            MarketType::Swap => "/openApi/swap/v2",
            MarketType::Spot => "/openApi/spot/v1",
        }
    }

    /// Sign a request with HMAC-SHA256.
    ///
    /// BingX signature is computed over the query string parameters.
    pub fn sign(&self, params: &str) -> String {
        hmac_sign(&self.api_secret, params)
    }

    /// Make a public GET request (no signature).
    pub async fn public_get<T: DeserializeOwned>(&self, url: &str) -> Result<T> {
        let response = self
            .client
            .get(url)
            .send()
            .await
            .context("Failed to send public request to BingX")?;

        let text = response.text().await?;
        debug!("BingX public response: {}", text);

        serde_json::from_str(&text).context("Failed to parse BingX response")
    }

    /// Make a public GET request with endpoint (relative to base_url).
    pub async fn public_get_endpoint<T: DeserializeOwned>(&self, endpoint: &str) -> Result<T> {
        let url = format!("{}{}", self.base_url, endpoint);
        self.public_get(&url).await
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

        self.public_get(&url).await
    }

    /// Make a signed GET request.
    ///
    /// BingX requires: X-BX-APIKEY header, timestamp and signature in query params.
    pub async fn signed_get<T: DeserializeOwned>(
        &self,
        endpoint: &str,
        params: &str,
    ) -> Result<BingxResponse<T>> {
        let timestamp = timestamp_ms();
        let full_params = if params.is_empty() {
            format!("timestamp={}", timestamp)
        } else {
            format!("{}&timestamp={}", params, timestamp)
        };
        let signature = self.sign(&full_params);

        let url = format!(
            "{}{}?{}&signature={}",
            self.base_url, endpoint, full_params, signature
        );

        let response = self
            .client
            .get(&url)
            .header("X-BX-APIKEY", &self.api_key)
            .send()
            .await
            .context("Failed to send signed GET request to BingX")?;

        let text = response.text().await?;
        debug!("BingX signed GET response: {}", text);

        serde_json::from_str(&text).context(format!("Failed to parse BingX response: {}", text))
    }

    /// Make a signed POST request.
    ///
    /// BingX requires: X-BX-APIKEY header, timestamp and signature in query params.
    pub async fn signed_post<T: DeserializeOwned>(
        &self,
        endpoint: &str,
        params: &str,
    ) -> Result<BingxResponse<T>> {
        let timestamp = timestamp_ms();
        let full_params = if params.is_empty() {
            format!("timestamp={}", timestamp)
        } else {
            format!("{}&timestamp={}", params, timestamp)
        };
        let signature = self.sign(&full_params);

        let url = format!(
            "{}{}?{}&signature={}",
            self.base_url, endpoint, full_params, signature
        );

        let response = self
            .client
            .post(&url)
            .header("X-BX-APIKEY", &self.api_key)
            .send()
            .await
            .context("Failed to send signed POST request to BingX")?;

        let text = response.text().await?;
        debug!("BingX signed POST response: {}", text);

        serde_json::from_str(&text).context(format!("Failed to parse BingX response: {}", text))
    }

    /// Make a signed DELETE request.
    ///
    /// BingX requires: X-BX-APIKEY header, timestamp and signature in query params.
    pub async fn signed_delete<T: DeserializeOwned>(
        &self,
        endpoint: &str,
        params: &str,
    ) -> Result<BingxResponse<T>> {
        let timestamp = timestamp_ms();
        let full_params = if params.is_empty() {
            format!("timestamp={}", timestamp)
        } else {
            format!("{}&timestamp={}", params, timestamp)
        };
        let signature = self.sign(&full_params);

        let url = format!(
            "{}{}?{}&signature={}",
            self.base_url, endpoint, full_params, signature
        );

        let response = self
            .client
            .delete(&url)
            .header("X-BX-APIKEY", &self.api_key)
            .send()
            .await
            .context("Failed to send signed DELETE request to BingX")?;

        let text = response.text().await?;
        debug!("BingX signed DELETE response: {}", text);

        serde_json::from_str(&text).context(format!("Failed to parse BingX response: {}", text))
    }
}
