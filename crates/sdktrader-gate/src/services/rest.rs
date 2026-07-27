//! Gate.io REST API service.

use anyhow::{Context, Result};
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde::Deserialize;
use sha2::{Digest, Sha512};
use tracing::debug;

use sdktrader_core::utils::timestamp_ms;
use sdktrader_core::models::MarketType;
use crate::config::{SPOT_MAINNET_URL, FUTURES_MAINNET_URL};

/// Gate.io REST API service.
pub struct RestService {
    client: Client,
    api_key: String,
    api_secret: String,
    market_type: MarketType,
    base_url: String,
}

impl RestService {
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

    /// Generate Gate.io API v4 signature (HMAC-SHA512).
    /// Sign string format: method\nurl\nquery_string\nhashed_payload\ntimestamp
    fn sign(&self, method: &str, url: &str, query_string: &str, body: &str) -> (String, String) {
        let timestamp = (timestamp_ms() / 1000).to_string();

        // Hash body/payload with SHA512 (empty string if no body)
        let body_hash = hex::encode(Sha512::digest(body.as_bytes()));

        // Create sign string: METHOD\n/api/v4/path\nquery_string\nhashed_payload\ntimestamp
        let sign_string = format!(
            "{}\n{}\n{}\n{}\n{}",
            method, url, query_string, body_hash, timestamp
        );

        debug!("Gate.io sign string: {}", sign_string.replace("\n", "\\n"));

        // HMAC-SHA512 signature
        type HmacSha512 = Hmac<Sha512>;
        let mut mac = HmacSha512::new_from_slice(self.api_secret.as_bytes())
            .expect("HMAC can take key of any size");
        mac.update(sign_string.as_bytes());
        let signature = hex::encode(mac.finalize().into_bytes());

        (signature, timestamp)
    }

    /// Make a signed GET request.
    pub async fn signed_get<T: for<'de> Deserialize<'de>>(
        &self,
        endpoint: &str,
        query: &str,
    ) -> Result<T> {
        let url_path = endpoint;
        let (signature, timestamp) = self.sign("GET", url_path, query, "");

        let full_url = if query.is_empty() {
            format!("{}{}", self.base_url, endpoint)
        } else {
            format!("{}{}?{}", self.base_url, endpoint, query)
        };

        let response = self
            .client
            .get(&full_url)
            .header("KEY", &self.api_key)
            .header("SIGN", signature)
            .header("Timestamp", timestamp)
            .header("Content-Type", "application/json")
            .send()
            .await
            .context("Failed to send request to Gate.io")?;

        let text = response.text().await?;
        debug!("Gate.io response: {}", text);

        serde_json::from_str(&text).context(format!("Failed to parse Gate.io response: {}", text))
    }

    /// Make a signed POST request.
    pub async fn signed_post<T: for<'de> Deserialize<'de>>(
        &self,
        endpoint: &str,
        body: &serde_json::Value,
    ) -> Result<T> {
        let url_path = endpoint;
        let body_str = serde_json::to_string(body)?;
        let (signature, timestamp) = self.sign("POST", url_path, "", &body_str);

        let response = self
            .client
            .post(format!("{}{}", self.base_url, endpoint))
            .header("KEY", &self.api_key)
            .header("SIGN", signature)
            .header("Timestamp", timestamp)
            .header("Content-Type", "application/json")
            .body(body_str)
            .send()
            .await
            .context("Failed to send request to Gate.io")?;

        let text = response.text().await?;
        debug!("Gate.io response: {}", text);

        serde_json::from_str(&text).context(format!("Failed to parse Gate.io response: {}", text))
    }

    /// Make a signed DELETE request.
    pub async fn signed_delete<T: for<'de> Deserialize<'de>>(
        &self,
        endpoint: &str,
        query: &str,
    ) -> Result<T> {
        let url_path = endpoint;
        let (signature, timestamp) = self.sign("DELETE", url_path, query, "");

        let full_url = if query.is_empty() {
            format!("{}{}", self.base_url, endpoint)
        } else {
            format!("{}{}?{}", self.base_url, endpoint, query)
        };

        let response = self
            .client
            .delete(&full_url)
            .header("KEY", &self.api_key)
            .header("SIGN", signature)
            .header("Timestamp", timestamp)
            .header("Content-Type", "application/json")
            .send()
            .await
            .context("Failed to send request to Gate.io")?;

        let text = response.text().await?;
        debug!("Gate.io response: {}", text);

        serde_json::from_str(&text).context(format!("Failed to parse Gate.io response: {}", text))
    }

    /// Make a public GET request (no auth).
    pub async fn public_get<T: for<'de> Deserialize<'de>>(&self, endpoint: &str) -> Result<T> {
        let url = format!("{}{}", self.base_url, endpoint);
        let response = self.client.get(&url).send().await?;
        let text = response.text().await?;
        serde_json::from_str(&text).context(format!("Failed to parse: {}", text))
    }

    /// Make a public GET request with query parameters (no auth).
    pub async fn public_get_with_query<T: for<'de> Deserialize<'de>>(
        &self,
        endpoint: &str,
        query: &str,
    ) -> Result<T> {
        let url = if query.is_empty() {
            format!("{}{}", self.base_url, endpoint)
        } else {
            format!("{}{}?{}", self.base_url, endpoint, query)
        };
        let response = self.client.get(&url).send().await?;
        let text = response.text().await?;
        serde_json::from_str(&text).context(format!("Failed to parse: {}", text))
    }

    // Accessors
    pub fn market_type(&self) -> MarketType {
        self.market_type
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub fn api_key(&self) -> &str {
        &self.api_key
    }

    pub fn api_secret(&self) -> &str {
        &self.api_secret
    }
}
