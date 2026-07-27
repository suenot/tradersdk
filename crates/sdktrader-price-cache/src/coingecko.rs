use anyhow::{Context, Result};
use serde::Deserialize;
use tracing::{debug, info};

use crate::rate_limiter::RateLimiter;

/// Data returned by CoinGecko `/coins/markets` endpoint.
#[derive(Debug, Clone, Deserialize)]
pub struct CoinMarketData {
    pub id: String,
    pub symbol: String,
    pub name: String,
    pub current_price: Option<f64>,
    pub market_cap_rank: Option<u32>,
    pub price_change_percentage_24h: Option<f64>,
}

/// CoinGecko API client with rate limiting.
///
/// Supports both the free API and the Pro API (when an API key is provided).
/// All requests are rate-limited according to the configured calls-per-minute.
pub struct CoinGeckoClient {
    base_url: String,
    client: reqwest::Client,
    rate_limiter: RateLimiter,
}

impl CoinGeckoClient {
    /// Create a new CoinGecko client.
    ///
    /// # Arguments
    /// * `api_key` - Optional CoinGecko Pro API key. If provided, uses `pro-api.coingecko.com`.
    /// * `calls_per_minute` - Rate limit in calls per minute.
    pub fn new(api_key: Option<String>, calls_per_minute: u32) -> Self {
        let (base_url, client) = if let Some(ref key) = api_key {
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert(
                "x-cg-pro-api-key",
                reqwest::header::HeaderValue::from_str(key)
                    .unwrap_or_else(|_| reqwest::header::HeaderValue::from_static("")),
            );
            let client = reqwest::Client::builder()
                .default_headers(headers)
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .expect("Failed to build reqwest client");
            ("https://pro-api.coingecko.com/api/v3".to_string(), client)
        } else {
            let client = reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .expect("Failed to build reqwest client");
            ("https://api.coingecko.com/api/v3".to_string(), client)
        };

        info!(
            base_url = base_url.as_str(),
            calls_per_minute,
            has_api_key = api_key.is_some(),
            "CoinGecko client created"
        );

        Self {
            base_url,
            client,
            rate_limiter: RateLimiter::new(calls_per_minute),
        }
    }

    /// Fetch a single coin price by CoinGecko coin ID.
    ///
    /// Uses the `/simple/price` endpoint.
    ///
    /// # Arguments
    /// * `coin_id` - CoinGecko coin identifier (e.g., "bitcoin", "ethereum").
    ///
    /// # Returns
    /// The USD price of the coin.
    pub async fn fetch_price(&self, coin_id: &str) -> Result<f64> {
        self.rate_limiter.wait_if_needed().await;

        let url = format!("{}/simple/price", self.base_url);

        debug!(coin_id, "Fetching single coin price");

        let response = self
            .client
            .get(&url)
            .query(&[("ids", coin_id), ("vs_currencies", "usd")])
            .send()
            .await
            .with_context(|| format!("Failed to fetch price for {}", coin_id))?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!(
                "CoinGecko API error for {}: status={}, body={}",
                coin_id,
                status,
                body
            );
        }

        let data: serde_json::Value = response
            .json()
            .await
            .with_context(|| format!("Failed to parse price response for {}", coin_id))?;

        let price = data
            .get(coin_id)
            .and_then(|v| v.get("usd"))
            .and_then(|v| v.as_f64())
            .ok_or_else(|| anyhow::anyhow!("No USD price found for coin_id={}", coin_id))?;

        debug!(coin_id, price, "Fetched price");
        Ok(price)
    }

    /// Fetch a page of top coins from the `/coins/markets` endpoint.
    ///
    /// Coins are ordered by market cap descending.
    ///
    /// # Arguments
    /// * `page` - Page number (1-indexed).
    /// * `per_page` - Number of coins per page (max 250 on CoinGecko).
    ///
    /// # Returns
    /// A vector of `CoinMarketData` for the requested page.
    pub async fn fetch_coins_page(&self, page: u32, per_page: u32) -> Result<Vec<CoinMarketData>> {
        self.rate_limiter.wait_if_needed().await;

        let url = format!("{}/coins/markets", self.base_url);

        info!(page, per_page, "Fetching coins page");

        let response = self
            .client
            .get(&url)
            .query(&[
                ("vs_currency", "usd"),
                ("order", "market_cap_desc"),
                ("per_page", &per_page.to_string()),
                ("page", &page.to_string()),
                ("sparkline", "false"),
            ])
            .send()
            .await
            .with_context(|| format!("Failed to fetch coins page {}", page))?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!(
                "CoinGecko API error for page {}: status={}, body={}",
                page,
                status,
                body
            );
        }

        let coins: Vec<CoinMarketData> = response
            .json()
            .await
            .with_context(|| format!("Failed to parse coins page {}", page))?;

        info!(page, count = coins.len(), "Fetched coins page");
        Ok(coins)
    }

    /// Get the base URL being used.
    pub fn base_url(&self) -> &str {
        &self.base_url
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_creation_free() {
        let client = CoinGeckoClient::new(None, 30);
        assert_eq!(client.base_url(), "https://api.coingecko.com/api/v3");
    }

    #[test]
    fn test_client_creation_pro() {
        let client = CoinGeckoClient::new(Some("test-key".to_string()), 30);
        assert_eq!(client.base_url(), "https://pro-api.coingecko.com/api/v3");
    }

    #[test]
    fn test_coin_market_data_deserialization() {
        let json = r#"{
            "id": "bitcoin",
            "symbol": "btc",
            "name": "Bitcoin",
            "current_price": 42000.50,
            "market_cap_rank": 1,
            "price_change_percentage_24h": -2.5
        }"#;

        let data: CoinMarketData = serde_json::from_str(json).unwrap();
        assert_eq!(data.id, "bitcoin");
        assert_eq!(data.symbol, "btc");
        assert_eq!(data.name, "Bitcoin");
        assert_eq!(data.current_price, Some(42000.50));
        assert_eq!(data.market_cap_rank, Some(1));
        assert_eq!(data.price_change_percentage_24h, Some(-2.5));
    }

    #[test]
    fn test_coin_market_data_optional_fields() {
        let json = r#"{
            "id": "some-coin",
            "symbol": "sc",
            "name": "Some Coin",
            "current_price": null,
            "market_cap_rank": null,
            "price_change_percentage_24h": null
        }"#;

        let data: CoinMarketData = serde_json::from_str(json).unwrap();
        assert_eq!(data.current_price, None);
        assert_eq!(data.market_cap_rank, None);
        assert_eq!(data.price_change_percentage_24h, None);
    }
}
