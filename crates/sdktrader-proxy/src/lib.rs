//! Proxy rotation with cooldown and parallel request scheduling.
//!
//! This crate provides two main components:
//!
//! - [`ProxyManager`](proxy_manager::ProxyManager) -- manages a pool of proxy
//!   connections with per-proxy cooldown timers and random selection.
//! - [`RequestScheduler`](scheduler::RequestScheduler) -- an async scheduler
//!   that fires requests at regular intervals (with jitter), each through a
//!   proxy acquired from a `ProxyManager`.
//!
//! Proxy rotation and request scheduling utilities.
//!

pub mod proxy_manager;
pub mod scheduler;

// Re-export the most commonly used items at the crate root for convenience.
pub use proxy_manager::{ProxyManager, ProxyStats, HOST_CONNECTION};
pub use scheduler::{RequestResult, RequestScheduler, SchedulerConfig, SchedulerStats};
