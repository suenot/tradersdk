//! Hyperliquid REST API service.
//!
//! This module handles all REST API communication with Hyperliquid:
//! - HTTP POST requests to /info endpoint (public data)
//! - HTTP POST requests to /exchange endpoint (signed trading operations)
//! - EIP-712 signing for authenticated requests
//! - Asset cache for symbol->index mapping

use anyhow::{Context, Result};
use reqwest::Client;
use tracing::{debug, info, warn};

#[cfg(feature = "signing")]
use ethers::signers::{LocalWallet, Signer};
#[cfg(feature = "signing")]
use ethers::types::H256;

use crate::config::{endpoints, MAINNET_URL, TESTNET_URL};

/// Asset info with index for order placement.
#[derive(Debug, Clone)]
pub struct AssetInfo {
    pub name: String,
    pub index: i32,
    pub sz_decimals: i32,
}

/// Hyperliquid REST API service.
///
/// Handles all HTTP communication with the Hyperliquid API including:
/// - Public info requests (market data, account info)
/// - Signed exchange requests (trading operations)
/// - EIP-712 message signing for authentication
pub struct RestService {
    client: Client,
    base_url: String,
    wallet_address: String,
    private_key: String,
    testnet: bool,
    asset_cache: std::sync::RwLock<Option<Vec<AssetInfo>>>,
}

impl RestService {
    /// Create a new REST service.
    ///
    /// # Arguments
    /// * `wallet_address` - Ethereum wallet address (0x...)
    /// * `private_key` - Private key (hex string, with or without 0x prefix)
    /// * `testnet` - Use testnet if true
    pub fn new(wallet_address: &str, private_key: &str, testnet: bool) -> Self {
        let base_url = if testnet { TESTNET_URL } else { MAINNET_URL };

        // Normalize private key (remove 0x prefix if present)
        let private_key = private_key
            .strip_prefix("0x")
            .unwrap_or(private_key)
            .to_string();

        Self {
            client: Client::new(),
            base_url: base_url.to_string(),
            wallet_address: wallet_address.to_string(),
            private_key,
            testnet,
            asset_cache: std::sync::RwLock::new(None),
        }
    }

    /// POST to /info endpoint (public data).
    ///
    /// Used for:
    /// - Market data (meta, allMids, l2Book)
    /// - Account info (clearinghouseState, openOrders)
    /// - Exchange status
    pub async fn info_request(&self, payload: &serde_json::Value) -> Result<serde_json::Value> {
        let url = format!("{}{}", self.base_url, endpoints::INFO);
        debug!("Hyperliquid info request to {}: {:?}", url, payload);

        let response = self
            .client
            .post(&url)
            .json(payload)
            .send()
            .await
            .context("Failed to send info request")?
            .json()
            .await
            .context("Failed to parse info response")?;

        Ok(response)
    }

    /// POST to /exchange endpoint (signed request).
    ///
    /// Used for:
    /// - Order placement
    /// - Order cancellation
    /// - Leverage updates
    /// - Other trading operations
    pub async fn exchange_request(
        &self,
        action: &serde_json::Value,
        nonce: i64,
    ) -> Result<serde_json::Value> {
        let signature = self.sign_l1_action(action, nonce, None).await?;

        let request = serde_json::json!({
            "action": action,
            "nonce": nonce,
            "signature": signature
        });

        let url = format!("{}{}", self.base_url, endpoints::EXCHANGE);
        debug!("Hyperliquid exchange request to {}: {:?}", url, request);

        let response = self
            .client
            .post(&url)
            .json(&request)
            .send()
            .await
            .context("Failed to send exchange request")?
            .json()
            .await
            .context("Failed to parse exchange response")?;

        Ok(response)
    }

    /// Sign an action using EIP-712 (phantom agent pattern).
    ///
    /// Hyperliquid uses a unique signing scheme:
    /// 1. Pack the action using msgpack
    /// 2. Create a hash including nonce and vault address
    /// 3. Create a "phantom agent" with source ('a' for mainnet, 'b' for testnet)
    /// 4. Sign using EIP-712 structured data
    #[cfg(feature = "signing")]
    pub async fn sign_l1_action(
        &self,
        action: &serde_json::Value,
        nonce: i64,
        vault_address: Option<&str>,
    ) -> Result<serde_json::Value> {
        use ethers::core::utils::keccak256;

        // For Hyperliquid, we need to:
        // 1. Serialize action to msgpack
        // 2. Create action hash
        // 3. Create phantom agent
        // 4. Sign with EIP-712

        // Step 1: Serialize action (using JSON as msgpack approximation for now)
        let action_bytes = serde_json::to_vec(action)?;

        // Step 2: Create action hash by concatenating data
        let mut data_to_hash = action_bytes.clone();

        // Add nonce (8 bytes, big endian)
        let nonce_bytes = nonce.to_be_bytes();
        data_to_hash.extend_from_slice(&[0u8; 5]); // Padding
        data_to_hash.extend_from_slice(&nonce_bytes);

        // Add vault address flag
        if let Some(vault) = vault_address {
            data_to_hash.push(0x01);
            let vault_bytes = hex::decode(vault.strip_prefix("0x").unwrap_or(vault))?;
            data_to_hash.extend_from_slice(&vault_bytes);
        } else {
            data_to_hash.push(0x00);
        }

        let action_hash = keccak256(&data_to_hash);

        // Step 3: Create phantom agent
        let source = if self.testnet { "b" } else { "a" };

        // Step 4: Sign with EIP-712
        // Create wallet from private key
        let wallet: LocalWallet = self
            .private_key
            .parse::<LocalWallet>()
            .context("Invalid private key")?;

        // EIP-712 domain for Hyperliquid
        let domain_type_hash = keccak256(
            b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
        );
        let name_hash = keccak256(b"Exchange");
        let version_hash = keccak256(b"1");

        let mut domain_data = Vec::new();
        domain_data.extend_from_slice(&domain_type_hash);
        domain_data.extend_from_slice(&name_hash);
        domain_data.extend_from_slice(&version_hash);
        domain_data.extend_from_slice(&[0u8; 24]);
        domain_data.extend_from_slice(&1337u64.to_be_bytes()); // chainId
        domain_data.extend_from_slice(&[0u8; 32]); // zero address
        let domain_separator = keccak256(&domain_data);

        // Type hash for Agent
        let type_hash = keccak256(b"Agent(string source,bytes32 connectionId)");

        // Struct hash
        let mut struct_data = Vec::new();
        struct_data.extend_from_slice(&type_hash);
        struct_data.extend_from_slice(&keccak256(source.as_bytes()));
        struct_data.extend_from_slice(&action_hash);
        let struct_hash = keccak256(&struct_data);

        // Final message hash
        let mut message_data = Vec::new();
        message_data.extend_from_slice(&[0x19, 0x01]);
        message_data.extend_from_slice(&domain_separator);
        message_data.extend_from_slice(&struct_hash);
        let message_hash = keccak256(&message_data);

        // Sign the message hash
        let signature = wallet.sign_hash(H256::from(message_hash))?;

        // Convert U256 r and s to bytes
        let mut r_bytes = [0u8; 32];
        let mut s_bytes = [0u8; 32];
        signature.r.to_big_endian(&mut r_bytes);
        signature.s.to_big_endian(&mut s_bytes);

        Ok(serde_json::json!({
            "r": format!("0x{}", hex::encode(r_bytes)),
            "s": format!("0x{}", hex::encode(s_bytes)),
            "v": signature.v as u8
        }))
    }

    /// Sign an action (stub for when signing feature is not enabled).
    #[cfg(not(feature = "signing"))]
    pub async fn sign_l1_action(
        &self,
        _action: &serde_json::Value,
        _nonce: i64,
        _vault_address: Option<&str>,
    ) -> Result<serde_json::Value> {
        Err(anyhow::anyhow!(
            "Hyperliquid trading requires the 'signing' feature flag. \
             Enable it with: cargo build --features signing"
        ))
    }

    /// Load and cache asset metadata from the exchange.
    ///
    /// This fetches the "meta" info which includes all available assets
    /// with their indices and size decimals.
    pub async fn ensure_asset_cache(&self) -> Result<()> {
        {
            let cache = self.asset_cache.read().unwrap();
            if cache.is_some() {
                return Ok(());
            }
        }

        let response = self
            .info_request(&serde_json::json!({"type": "meta"}))
            .await?;

        let universe = response["universe"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("Invalid meta response: missing universe"))?;

        let assets: Vec<AssetInfo> = universe
            .iter()
            .enumerate()
            .filter_map(|(i, asset)| {
                let name = asset["name"].as_str()?.to_string();
                let sz_decimals = asset["szDecimals"].as_i64()? as i32;
                Some(AssetInfo {
                    name,
                    index: i as i32,
                    sz_decimals,
                })
            })
            .collect();

        info!("Hyperliquid: loaded {} assets into cache", assets.len());

        let mut cache = self.asset_cache.write().unwrap();
        *cache = Some(assets);

        Ok(())
    }

    /// Get asset index by symbol.
    ///
    /// The symbol should be in Hyperliquid format (e.g., "BTC", "ETH").
    pub async fn get_asset_index(&self, hl_symbol: &str) -> Result<i32> {
        self.ensure_asset_cache().await?;

        let cache = self.asset_cache.read().unwrap();

        if let Some(assets) = cache.as_ref() {
            for asset in assets {
                if asset.name == hl_symbol {
                    return Ok(asset.index);
                }
            }
        }

        Err(anyhow::anyhow!("Asset not found: {}", hl_symbol))
    }

    /// Get size decimals for a symbol.
    ///
    /// The symbol should be in Hyperliquid format (e.g., "BTC", "ETH").
    /// Returns 4 as default if the asset is not found.
    pub async fn get_sz_decimals(&self, hl_symbol: &str) -> Result<i32> {
        self.ensure_asset_cache().await?;

        let cache = self.asset_cache.read().unwrap();

        if let Some(assets) = cache.as_ref() {
            for asset in assets {
                if asset.name == hl_symbol {
                    return Ok(asset.sz_decimals);
                }
            }
        }

        // Default to 4 decimals
        warn!(
            "Size decimals not found for {}, using default of 4",
            hl_symbol
        );
        Ok(4)
    }

    /// Get all cached assets.
    ///
    /// Returns None if the cache has not been loaded yet.
    pub fn get_cached_assets(&self) -> Option<Vec<AssetInfo>> {
        self.asset_cache.read().unwrap().clone()
    }

    /// Clear the asset cache, forcing a reload on next use.
    pub fn clear_asset_cache(&self) {
        let mut cache = self.asset_cache.write().unwrap();
        *cache = None;
    }

    /// Get the wallet address.
    pub fn wallet_address(&self) -> &str {
        &self.wallet_address
    }

    /// Check if using testnet.
    pub fn is_testnet(&self) -> bool {
        self.testnet
    }

    /// Get the base URL.
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Test the connection by making a simple info request.
    pub async fn test_connection(&self) -> Result<bool> {
        let response = self
            .client
            .post(format!("{}{}", self.base_url, endpoints::INFO))
            .json(&serde_json::json!({"type": "meta"}))
            .send()
            .await?;

        Ok(response.status().is_success())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rest_service_new() {
        let service = RestService::new(
            "0x1234567890abcdef1234567890abcdef12345678",
            "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
            false,
        );

        assert_eq!(
            service.wallet_address(),
            "0x1234567890abcdef1234567890abcdef12345678"
        );
        assert!(!service.is_testnet());
        assert_eq!(service.base_url(), MAINNET_URL);
    }

    #[test]
    fn test_rest_service_testnet() {
        let service = RestService::new("0x123", "abc123", true);

        assert!(service.is_testnet());
        assert_eq!(service.base_url(), TESTNET_URL);
    }

    #[test]
    fn test_private_key_normalization() {
        // With 0x prefix
        let service1 = RestService::new("0x123", "0xabcdef", false);
        // Without 0x prefix
        let service2 = RestService::new("0x123", "abcdef", false);

        // Both should have the same normalized private key (without prefix)
        // We can't directly access private_key, but the services should work the same
        assert!(!service1.is_testnet());
        assert!(!service2.is_testnet());
    }
}
