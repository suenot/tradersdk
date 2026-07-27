use std::collections::HashMap;

use sha2::{Digest, Sha256};

use crate::types::{AccountStats, UniqueAccount};

/// Input information for a bot's exchange account.
#[derive(Debug, Clone)]
pub struct BotAccountInfo {
    /// Bot name / identifier.
    pub name: String,
    /// Exchange name (e.g., "bybit", "binance").
    pub exchange: String,
    /// API key for the exchange account.
    pub api_key: String,
    /// API secret for the exchange account.
    pub api_secret: String,
}

/// Detects unique exchange accounts from bot configurations.
///
/// Multiple bots can share the same exchange account (same API credentials).
/// This detector groups bots by their unique account, identified by a hash
/// of the exchange name and API credentials.
pub struct AccountDetector {
    /// Map from account_id -> UniqueAccount.
    accounts: HashMap<String, UniqueAccount>,
    /// Map from bot_name -> account_id.
    bot_to_account: HashMap<String, String>,
}

impl AccountDetector {
    /// Create a new empty `AccountDetector`.
    pub fn new() -> Self {
        Self {
            accounts: HashMap::new(),
            bot_to_account: HashMap::new(),
        }
    }

    /// Generate a unique account ID from exchange name and API credentials.
    ///
    /// Algorithm: SHA-256 of `"{exchange}:{api_key}:{api_secret_first_8_chars}"`,
    /// then take first 16 hex characters.
    fn generate_account_id(exchange: &str, api_key: &str, api_secret: &str) -> String {
        let secret_prefix = if api_secret.len() >= 8 {
            &api_secret[..8]
        } else {
            api_secret
        };

        let input = format!("{}:{}:{}", exchange, api_key, secret_prefix);
        let mut hasher = Sha256::new();
        hasher.update(input.as_bytes());
        let result = hasher.finalize();
        let hex_string = hex::encode(result);

        hex_string[..16].to_string()
    }

    /// Detect unique accounts from bot configurations.
    ///
    /// Processes all bots and groups them by unique account identity.
    /// Returns a reference to the map of detected accounts.
    pub fn detect_accounts(&mut self, bots: &[BotAccountInfo]) -> &HashMap<String, UniqueAccount> {
        for bot in bots {
            let account_id =
                Self::generate_account_id(&bot.exchange, &bot.api_key, &bot.api_secret);

            self.bot_to_account
                .insert(bot.name.clone(), account_id.clone());

            let account =
                self.accounts
                    .entry(account_id.clone())
                    .or_insert_with(|| UniqueAccount {
                        exchange: bot.exchange.clone(),
                        account_id: account_id.clone(),
                        bot_names: Vec::new(),
                    });

            if !account.bot_names.contains(&bot.name) {
                account.bot_names.push(bot.name.clone());
            }
        }

        &self.accounts
    }

    /// Get the account ID for a specific bot.
    pub fn get_account_for_bot(&self, bot_name: &str) -> Option<&str> {
        self.bot_to_account.get(bot_name).map(|s| s.as_str())
    }

    /// Get accounts grouped by exchange.
    pub fn get_accounts_by_exchange(&self) -> HashMap<String, Vec<&UniqueAccount>> {
        let mut by_exchange: HashMap<String, Vec<&UniqueAccount>> = HashMap::new();
        for account in self.accounts.values() {
            by_exchange
                .entry(account.exchange.clone())
                .or_default()
                .push(account);
        }
        by_exchange
    }

    /// Get statistics about detected accounts.
    pub fn stats(&self) -> AccountStats {
        let total_accounts = self.accounts.len();
        let total_bots = self.bot_to_account.len();

        let mut accounts_per_exchange: HashMap<String, usize> = HashMap::new();
        for account in self.accounts.values() {
            *accounts_per_exchange
                .entry(account.exchange.clone())
                .or_insert(0) += 1;
        }

        AccountStats {
            total_accounts,
            total_bots,
            accounts_per_exchange,
        }
    }
}

impl Default for AccountDetector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_bot(name: &str, exchange: &str, key: &str, secret: &str) -> BotAccountInfo {
        BotAccountInfo {
            name: name.to_string(),
            exchange: exchange.to_string(),
            api_key: key.to_string(),
            api_secret: secret.to_string(),
        }
    }

    #[test]
    fn test_new_detector_is_empty() {
        let detector = AccountDetector::new();
        assert!(detector.accounts.is_empty());
        assert!(detector.bot_to_account.is_empty());
    }

    #[test]
    fn test_detect_single_account() {
        let mut detector = AccountDetector::new();
        let bots = vec![make_bot("bot1", "bybit", "key123", "secret123456")];

        let accounts = detector.detect_accounts(&bots);
        assert_eq!(accounts.len(), 1);

        let account = accounts.values().next().unwrap();
        assert_eq!(account.exchange, "bybit");
        assert_eq!(account.bot_names, vec!["bot1"]);
        assert_eq!(account.account_id.len(), 16);
    }

    #[test]
    fn test_detect_multiple_bots_same_account() {
        let mut detector = AccountDetector::new();
        let bots = vec![
            make_bot("bot1", "bybit", "key123", "secret123456"),
            make_bot("bot2", "bybit", "key123", "secret123456"),
            make_bot("bot3", "bybit", "key123", "secret123456"),
        ];

        let accounts = detector.detect_accounts(&bots);
        assert_eq!(accounts.len(), 1, "Should detect only one unique account");

        let account = accounts.values().next().unwrap();
        assert_eq!(account.bot_names.len(), 3);
        assert!(account.bot_names.contains(&"bot1".to_string()));
        assert!(account.bot_names.contains(&"bot2".to_string()));
        assert!(account.bot_names.contains(&"bot3".to_string()));
    }

    #[test]
    fn test_detect_different_accounts() {
        let mut detector = AccountDetector::new();
        let bots = vec![
            make_bot("bot1", "bybit", "key_a", "secret_a_12345"),
            make_bot("bot2", "bybit", "key_b", "secret_b_12345"),
            make_bot("bot3", "binance", "key_c", "secret_c_12345"),
        ];

        let accounts = detector.detect_accounts(&bots);
        assert_eq!(accounts.len(), 3, "Should detect three unique accounts");
    }

    #[test]
    fn test_same_key_different_exchanges() {
        let mut detector = AccountDetector::new();
        let bots = vec![
            make_bot("bot1", "bybit", "samekey", "samesecret12"),
            make_bot("bot2", "binance", "samekey", "samesecret12"),
        ];

        let accounts = detector.detect_accounts(&bots);
        assert_eq!(
            accounts.len(),
            2,
            "Same credentials on different exchanges should be different accounts"
        );
    }

    #[test]
    fn test_get_account_for_bot() {
        let mut detector = AccountDetector::new();
        let bots = vec![
            make_bot("bot1", "bybit", "key1", "secret1_long"),
            make_bot("bot2", "binance", "key2", "secret2_long"),
        ];

        detector.detect_accounts(&bots);

        assert!(detector.get_account_for_bot("bot1").is_some());
        assert!(detector.get_account_for_bot("bot2").is_some());
        assert!(detector.get_account_for_bot("bot_nonexistent").is_none());

        // Bot1 and bot2 should have different accounts
        assert_ne!(
            detector.get_account_for_bot("bot1"),
            detector.get_account_for_bot("bot2")
        );
    }

    #[test]
    fn test_get_accounts_by_exchange() {
        let mut detector = AccountDetector::new();
        let bots = vec![
            make_bot("bot1", "bybit", "key1", "secret1_long"),
            make_bot("bot2", "bybit", "key2", "secret2_long"),
            make_bot("bot3", "binance", "key3", "secret3_long"),
        ];

        detector.detect_accounts(&bots);
        let by_exchange = detector.get_accounts_by_exchange();

        assert_eq!(by_exchange.get("bybit").map(|v| v.len()), Some(2));
        assert_eq!(by_exchange.get("binance").map(|v| v.len()), Some(1));
    }

    #[test]
    fn test_stats() {
        let mut detector = AccountDetector::new();
        let bots = vec![
            make_bot("bot1", "bybit", "key1", "secret1_long"),
            make_bot("bot2", "bybit", "key1", "secret1_long"), // same account as bot1
            make_bot("bot3", "bybit", "key2", "secret2_long"),
            make_bot("bot4", "binance", "key3", "secret3_long"),
        ];

        detector.detect_accounts(&bots);
        let stats = detector.stats();

        assert_eq!(stats.total_accounts, 3); // 2 bybit + 1 binance
        assert_eq!(stats.total_bots, 4);
        assert_eq!(stats.accounts_per_exchange.get("bybit"), Some(&2));
        assert_eq!(stats.accounts_per_exchange.get("binance"), Some(&1));
    }

    #[test]
    fn test_account_id_deterministic() {
        let id1 = AccountDetector::generate_account_id("bybit", "key123", "secret12345678");
        let id2 = AccountDetector::generate_account_id("bybit", "key123", "secret12345678");
        assert_eq!(id1, id2, "Same inputs should produce same account ID");
        assert_eq!(id1.len(), 16, "Account ID should be 16 hex chars");
    }

    #[test]
    fn test_account_id_uses_first_8_chars_of_secret() {
        // Secrets with same first 8 chars should produce same ID
        let id1 = AccountDetector::generate_account_id("bybit", "key", "secret12_suffix_a");
        let id2 = AccountDetector::generate_account_id("bybit", "key", "secret12_suffix_b");
        assert_eq!(
            id1, id2,
            "Secrets with same first 8 chars should produce same ID"
        );

        // Different first 8 chars should produce different ID
        let id3 = AccountDetector::generate_account_id("bybit", "key", "other123_suffix");
        assert_ne!(
            id1, id3,
            "Different secret prefixes should produce different IDs"
        );
    }

    #[test]
    fn test_short_secret() {
        // Secret shorter than 8 chars should still work
        let id = AccountDetector::generate_account_id("bybit", "key", "short");
        assert_eq!(id.len(), 16);
    }

    #[test]
    fn test_default_trait() {
        let detector = AccountDetector::default();
        assert!(detector.accounts.is_empty());
    }

    #[test]
    fn test_detect_accounts_idempotent() {
        let mut detector = AccountDetector::new();
        let bots = vec![make_bot("bot1", "bybit", "key1", "secret1_long")];

        // Call twice with same data
        detector.detect_accounts(&bots);
        detector.detect_accounts(&bots);

        let account = detector.accounts.values().next().unwrap();
        assert_eq!(
            account.bot_names.len(),
            1,
            "Should not duplicate bot names on repeated calls"
        );
    }
}
