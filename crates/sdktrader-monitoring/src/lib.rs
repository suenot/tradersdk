//! # sdktrader-monitoring
//!
//! Detects changes in the set of markets an exchange offers.
//!
//! - **Market diff detector** (`pairs_diff`): compares snapshots of an
//!   exchange's tradable markets and reports which were added or removed.
//!
//! The detector is transport-agnostic: fetch market snapshots however you like
//! (see `sdktrader-providers` for exchange clients and `sdktrader-proxy` for
//! proxy rotation) and hand the results to [`MarketDiffDetector`].
//!
//! ```
//! use sdktrader_monitoring::{MarketDiffDetector, MarketInfo, MarketChangeType};
//!
//! let mut detector = MarketDiffDetector::new();
//! detector.load_initial(vec![MarketInfo::new("USDT-BTC", "Bitcoin")]);
//!
//! let changes = detector.detect_changes(vec![
//!     MarketInfo::new("USDT-BTC", "Bitcoin"),
//!     MarketInfo::new("USDT-SOL", "Solana"),
//! ]);
//!
//! assert_eq!(changes.len(), 1);
//! assert_eq!(changes[0].change_type, MarketChangeType::Added);
//! assert_eq!(changes[0].market, "USDT-SOL");
//! ```

pub mod pairs_diff;
pub mod types;

pub use pairs_diff::MarketDiffDetector;
pub use types::{MarketChange, MarketChangeType, MarketInfo};
