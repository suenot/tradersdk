use std::collections::HashMap;

use crate::types::{MarketChange, MarketChangeType, MarketInfo};

/// Detects markets added to or removed from an exchange by diffing snapshots.
///
/// Usage:
/// 1. Create with `MarketDiffDetector::new()`
/// 2. The first call to `detect_changes()` (or an explicit `load_initial()`)
///    populates the baseline and returns an empty vec, so a cold start never
///    reports the entire market list as new.
/// 3. Subsequent calls compare the new snapshot against the baseline and return
///    added and removed markets.
pub struct MarketDiffDetector {
    current: HashMap<String, MarketInfo>,
}

impl MarketDiffDetector {
    pub fn new() -> Self {
        Self {
            current: HashMap::new(),
        }
    }

    /// Load the baseline explicitly (first API response, no changes reported).
    pub fn load_initial(&mut self, markets: Vec<MarketInfo>) {
        self.current = markets
            .into_iter()
            .map(|m| (m.market.clone(), m))
            .collect();
        tracing::info!(count = self.current.len(), "Initial markets loaded");
    }

    /// Compare a new snapshot against the baseline and return the changes.
    ///
    /// Returns an empty `Vec` on the first call, when no baseline exists yet.
    pub fn detect_changes(&mut self, markets: Vec<MarketInfo>) -> Vec<MarketChange> {
        let timestamp_ms = chrono::Utc::now().timestamp_millis();

        let new_map: HashMap<String, MarketInfo> = markets
            .into_iter()
            .map(|m| (m.market.clone(), m))
            .collect();

        // First run: establish the baseline, report nothing.
        if self.current.is_empty() {
            tracing::info!(count = new_map.len(), "Initial snapshot cached");
            self.current = new_map;
            return Vec::new();
        }

        let mut changes = Vec::new();

        // Added: present in the new snapshot but absent from the baseline.
        for (market, info) in &new_map {
            if !self.current.contains_key(market) {
                tracing::info!(market = %market, name = %info.display_name, "Market added");
                changes.push(MarketChange {
                    change_type: MarketChangeType::Added,
                    market: info.market.clone(),
                    display_name: info.display_name.clone(),
                    timestamp_ms,
                });
            }
        }

        // Removed: present in the baseline but absent from the new snapshot.
        for (market, info) in &self.current {
            if !new_map.contains_key(market) {
                tracing::info!(market = %market, name = %info.display_name, "Market removed");
                changes.push(MarketChange {
                    change_type: MarketChangeType::Removed,
                    market: info.market.clone(),
                    display_name: info.display_name.clone(),
                    timestamp_ms,
                });
            }
        }

        // Advance the baseline only when something actually changed.
        if !changes.is_empty() {
            self.current = new_map;
        }

        changes
    }

    /// Number of markets currently cached.
    pub fn market_count(&self) -> usize {
        self.current.len()
    }

    /// Whether a baseline has been established.
    pub fn is_initialized(&self) -> bool {
        !self.current.is_empty()
    }
}

impl Default for MarketDiffDetector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_markets() -> Vec<MarketInfo> {
        vec![
            MarketInfo::new("USDT-BTC", "Bitcoin"),
            MarketInfo::new("USDT-ETH", "Ethereum"),
            MarketInfo::new("USDT-XRP", "Ripple"),
        ]
    }

    #[test]
    fn initial_load_reports_no_changes() {
        let mut detector = MarketDiffDetector::new();
        assert!(!detector.is_initialized());

        let changes = detector.detect_changes(base_markets());
        assert!(changes.is_empty(), "First call must return no changes");
        assert!(detector.is_initialized());
        assert_eq!(detector.market_count(), 3);
    }

    #[test]
    fn load_initial_establishes_baseline() {
        let mut detector = MarketDiffDetector::new();
        detector.load_initial(base_markets());
        assert!(detector.is_initialized());
        assert_eq!(detector.market_count(), 3);
    }

    #[test]
    fn added_market_detected() {
        let mut detector = MarketDiffDetector::new();
        detector.load_initial(base_markets());

        let mut updated = base_markets();
        updated.push(MarketInfo::new("USDT-SOL", "Solana"));

        let changes = detector.detect_changes(updated);
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].change_type, MarketChangeType::Added);
        assert_eq!(changes[0].market, "USDT-SOL");
        assert_eq!(changes[0].display_name, "Solana");
        assert_eq!(detector.market_count(), 4);
    }

    #[test]
    fn removed_market_detected() {
        let mut detector = MarketDiffDetector::new();
        detector.load_initial(base_markets());

        let updated = vec![
            MarketInfo::new("USDT-BTC", "Bitcoin"),
            MarketInfo::new("USDT-ETH", "Ethereum"),
        ];

        let changes = detector.detect_changes(updated);
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].change_type, MarketChangeType::Removed);
        assert_eq!(changes[0].market, "USDT-XRP");
        assert_eq!(detector.market_count(), 2);
    }

    #[test]
    fn simultaneous_add_and_remove() {
        let mut detector = MarketDiffDetector::new();
        detector.load_initial(base_markets());

        let updated = vec![
            MarketInfo::new("USDT-BTC", "Bitcoin"),
            MarketInfo::new("USDT-ETH", "Ethereum"),
            MarketInfo::new("USDT-SOL", "Solana"),
        ];

        let changes = detector.detect_changes(updated);
        assert_eq!(changes.len(), 2);

        let added: Vec<_> = changes
            .iter()
            .filter(|c| c.change_type == MarketChangeType::Added)
            .collect();
        let removed: Vec<_> = changes
            .iter()
            .filter(|c| c.change_type == MarketChangeType::Removed)
            .collect();

        assert_eq!(added.len(), 1);
        assert_eq!(added[0].market, "USDT-SOL");
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].market, "USDT-XRP");
    }

    #[test]
    fn identical_snapshot_reports_nothing() {
        let mut detector = MarketDiffDetector::new();
        detector.load_initial(base_markets());

        let changes = detector.detect_changes(base_markets());
        assert!(changes.is_empty());
        assert_eq!(detector.market_count(), 3);
    }

    #[test]
    fn sequential_updates() {
        let mut detector = MarketDiffDetector::new();
        detector.load_initial(base_markets());

        // Round 1: add SOL
        let mut round1 = base_markets();
        round1.push(MarketInfo::new("USDT-SOL", "Solana"));
        let changes1 = detector.detect_changes(round1.clone());
        assert_eq!(changes1.len(), 1);
        assert_eq!(changes1[0].market, "USDT-SOL");

        // Round 2: no change
        let changes2 = detector.detect_changes(round1);
        assert!(changes2.is_empty());

        // Round 3: add DOGE, remove BTC
        let round3 = vec![
            MarketInfo::new("USDT-ETH", "Ethereum"),
            MarketInfo::new("USDT-XRP", "Ripple"),
            MarketInfo::new("USDT-SOL", "Solana"),
            MarketInfo::new("USDT-DOGE", "Dogecoin"),
        ];
        let changes3 = detector.detect_changes(round3);
        assert_eq!(changes3.len(), 2);
    }
}
