//! REST API service for Binance.

use anyhow::{Context, Result};
use reqwest::Client;
use sdktrader_core::models::MarketType;
use sdktrader_core::utils::{hmac_sign, timestamp_ms};
use serde::de::DeserializeOwned;
use tracing::debug;

use crate::config::{
    endpoints, FUTURES_MAINNET_URL, FUTURES_TESTNET_URL, SPOT_MAINNET_URL, SPOT_TESTNET_URL,
};

/// REST API service for Binance.
pub struct RestService {
    api_key: String,
    api_secret: String,
    client: Client,
    base_url: String,
    market_type: MarketType,
}

impl RestService {
    /// Create a new REST service.
    pub fn new(api_key: &str, api_secret: &str, testnet: bool, market_type: MarketType) -> Self {
        let base_url = match (market_type, testnet) {
            (MarketType::Spot, false) => SPOT_MAINNET_URL,
            (MarketType::Spot, true) => SPOT_TESTNET_URL,
            (MarketType::Swap, false) => FUTURES_MAINNET_URL,
            (MarketType::Swap, true) => FUTURES_TESTNET_URL,
        };

        Self {
            api_key: api_key.to_string(),
            api_secret: api_secret.to_string(),
            client: Client::new(),
            base_url: base_url.to_string(),
            market_type,
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

    /// Get the market type.
    pub fn market_type(&self) -> MarketType {
        self.market_type
    }

    /// Sign query parameters.
    fn sign(&self, query: &str) -> String {
        hmac_sign(&self.api_secret, query)
    }

    /// Make a public GET request (no signature).
    pub async fn public_get<T: DeserializeOwned>(&self, endpoint: &str) -> Result<T> {
        let url = format!("{}{}", self.base_url, endpoint);

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .context("Failed to send public request to Binance")?;

        let text = response.text().await?;
        debug!("Binance public response: {}", text);

        serde_json::from_str(&text).context("Failed to parse Binance response")
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
            .context("Failed to send public request to Binance")?;

        let text = response.text().await?;
        debug!("Binance public response: {}", text);

        serde_json::from_str(&text).context("Failed to parse Binance response")
    }

    /// Make a signed GET request.
    pub async fn signed_get<T: DeserializeOwned>(&self, endpoint: &str, params: &str) -> Result<T> {
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
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await
            .context("Failed to send signed GET request to Binance")?;

        let text = response.text().await?;
        debug!("Binance signed GET response: {}", text);

        serde_json::from_str(&text).context("Failed to parse Binance response")
    }

    /// Make a signed POST request.
    pub async fn signed_post<T: DeserializeOwned>(
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
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await
            .context("Failed to send signed POST request to Binance")?;

        let text = response.text().await?;
        debug!("Binance signed POST response: {}", text);

        serde_json::from_str(&text).context("Failed to parse Binance response")
    }

    /// Make a signed DELETE request.
    pub async fn signed_delete<T: DeserializeOwned>(
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
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await
            .context("Failed to send signed DELETE request to Binance")?;

        let text = response.text().await?;
        debug!("Binance signed DELETE response: {}", text);

        serde_json::from_str(&text).context("Failed to parse Binance response")
    }

    /// Make a signed PUT request (for listenKey renewal).
    pub async fn signed_put(&self, endpoint: &str) -> Result<()> {
        let url = format!("{}{}", self.base_url, endpoint);

        let response = self
            .client
            .put(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await
            .context("Failed to send signed PUT request to Binance")?;

        if response.status().is_success() {
            Ok(())
        } else {
            anyhow::bail!("PUT request failed with status: {}", response.status())
        }
    }

    /// Get the appropriate endpoint based on market type.
    pub fn endpoint_for<'a>(&self, spot: &'a str, futures: &'a str) -> &'a str {
        if self.market_type == MarketType::Spot {
            spot
        } else {
            futures
        }
    }

    /// Get ping endpoint.
    pub fn ping_endpoint(&self) -> &str {
        self.endpoint_for(endpoints::SPOT_PING, endpoints::FUTURES_PING)
    }

    /// Get time endpoint.
    pub fn time_endpoint(&self) -> &str {
        self.endpoint_for(endpoints::SPOT_TIME, endpoints::FUTURES_TIME)
    }

    /// Get account/balance endpoint.
    pub fn balance_endpoint(&self) -> &str {
        self.endpoint_for(endpoints::SPOT_ACCOUNT, endpoints::FUTURES_BALANCE)
    }

    /// Get ticker price endpoint.
    pub fn ticker_price_endpoint(&self) -> &str {
        self.endpoint_for(
            endpoints::SPOT_TICKER_PRICE,
            endpoints::FUTURES_TICKER_PRICE,
        )
    }

    /// Get 24hr ticker endpoint.
    pub fn ticker_24hr_endpoint(&self) -> &str {
        self.endpoint_for(endpoints::SPOT_TICKER_24HR, endpoints::FUTURES_TICKER_24HR)
    }

    /// Get depth endpoint.
    pub fn depth_endpoint(&self) -> &str {
        self.endpoint_for(endpoints::SPOT_DEPTH, endpoints::FUTURES_DEPTH)
    }

    /// Get order endpoint.
    pub fn order_endpoint(&self) -> &str {
        self.endpoint_for(endpoints::SPOT_ORDER, endpoints::FUTURES_ORDER)
    }

    /// Get listen key endpoint.
    pub fn listen_key_endpoint(&self) -> &str {
        self.endpoint_for(endpoints::SPOT_LISTEN_KEY, endpoints::FUTURES_LISTEN_KEY)
    }
}
