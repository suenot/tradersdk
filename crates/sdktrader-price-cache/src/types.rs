use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Single price data entry with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceData {
    pub symbol: String,
    pub price_usd: f64,
    pub updated_at: f64, // unix timestamp
    pub name: Option<String>,
    pub market_cap_rank: Option<u32>,
    pub price_change_24h: Option<f64>,
}

/// Serializable price cache
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceCacheData {
    pub prices: HashMap<String, PriceData>,
    pub last_full_update: f64,
    pub version: u32,
}

impl Default for PriceCacheData {
    fn default() -> Self {
        Self {
            prices: HashMap::new(),
            last_full_update: 0.0,
            version: 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_price_data_serialization_roundtrip() {
        let data = PriceData {
            symbol: "BTC".to_string(),
            price_usd: 42000.50,
            updated_at: 1700000000.0,
            name: Some("Bitcoin".to_string()),
            market_cap_rank: Some(1),
            price_change_24h: Some(-2.5),
        };

        let json = serde_json::to_string(&data).unwrap();
        let deserialized: PriceData = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.symbol, "BTC");
        assert_eq!(deserialized.price_usd, 42000.50);
        assert_eq!(deserialized.updated_at, 1700000000.0);
        assert_eq!(deserialized.name, Some("Bitcoin".to_string()));
        assert_eq!(deserialized.market_cap_rank, Some(1));
        assert_eq!(deserialized.price_change_24h, Some(-2.5));
    }

    #[test]
    fn test_price_data_optional_fields() {
        let data = PriceData {
            symbol: "UNKNOWN".to_string(),
            price_usd: 0.001,
            updated_at: 1700000000.0,
            name: None,
            market_cap_rank: None,
            price_change_24h: None,
        };

        let json = serde_json::to_string(&data).unwrap();
        let deserialized: PriceData = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.name, None);
        assert_eq!(deserialized.market_cap_rank, None);
        assert_eq!(deserialized.price_change_24h, None);
    }

    #[test]
    fn test_price_cache_data_default() {
        let cache = PriceCacheData::default();
        assert!(cache.prices.is_empty());
        assert_eq!(cache.last_full_update, 0.0);
        assert_eq!(cache.version, 1);
    }

    #[test]
    fn test_price_cache_data_serialization_roundtrip() {
        let mut cache = PriceCacheData::default();
        cache.prices.insert(
            "ETH".to_string(),
            PriceData {
                symbol: "ETH".to_string(),
                price_usd: 2200.0,
                updated_at: 1700000000.0,
                name: Some("Ethereum".to_string()),
                market_cap_rank: Some(2),
                price_change_24h: Some(1.5),
            },
        );
        cache.last_full_update = 1700000000.0;

        let json = serde_json::to_string(&cache).unwrap();
        let deserialized: PriceCacheData = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.prices.len(), 1);
        assert_eq!(deserialized.prices["ETH"].price_usd, 2200.0);
        assert_eq!(deserialized.last_full_update, 1700000000.0);
        assert_eq!(deserialized.version, 1);
    }
}
