//! Async request scheduler with jitter and proxy rotation.
//!
//! Request scheduler with per-host concurrency limits.
//!
//! The scheduler spawns a background tokio task that fires requests at
//! regular intervals (plus optional random jitter).  Each request gets a
//! proxy from the shared [`ProxyManager`] and is executed in a spawned
//! task, so a slow proxy never blocks subsequent requests.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use rand::Rng;
use serde::Serialize;
use tokio::sync::{mpsc, Mutex};
use tracing::{debug, error, info};

use crate::proxy_manager::ProxyManager;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Outcome of a single scheduled request.
#[derive(Debug)]
pub enum RequestResult<T> {
    /// The fetch function returned successfully.
    Success(T),
    /// The fetch function (or proxy acquisition) failed with an error message.
    Error(String),
}

/// Statistics snapshot for a running (or stopped) scheduler.
#[derive(Debug, Clone, Serialize)]
pub struct SchedulerStats {
    pub running: bool,
    pub interval_ms: u64,
    pub jitter_ms: u64,
    pub active_tasks: usize,
    pub queue_size: usize,
}

/// Configuration for [`RequestScheduler`].
pub struct SchedulerConfig {
    /// Base interval between request launches (milliseconds).
    pub interval_ms: u64,
    /// Random jitter added to each interval (0..jitter_ms milliseconds).
    pub jitter_ms: u64,
    /// Maximum number of concurrently executing fetch tasks.
    pub max_workers: usize,
}

// ---------------------------------------------------------------------------
// RequestScheduler
// ---------------------------------------------------------------------------

/// Async request scheduler that fires requests at regular intervals with jitter.
///
/// Generic over `T` -- the value type returned by the user-supplied fetch
/// function.
pub struct RequestScheduler<T: Send + 'static> {
    config: SchedulerConfig,
    proxy_manager: Arc<Mutex<ProxyManager>>,
    running: Arc<AtomicBool>,
    /// Tracks active task count (shared with spawned tasks).
    active_tasks: Arc<std::sync::atomic::AtomicUsize>,
    /// Phantom for `T`.
    _marker: std::marker::PhantomData<T>,
}

impl<T: Send + 'static> RequestScheduler<T> {
    /// Create a new scheduler.
    ///
    /// The scheduler does **not** start automatically -- call [`start`](Self::start).
    pub fn new(config: SchedulerConfig, proxy_manager: Arc<Mutex<ProxyManager>>) -> Self {
        info!(
            "RequestScheduler initialized: interval={}ms, jitter={}ms, max_workers={}",
            config.interval_ms, config.jitter_ms, config.max_workers,
        );

        Self {
            config,
            proxy_manager,
            running: Arc::new(AtomicBool::new(false)),
            active_tasks: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            _marker: std::marker::PhantomData,
        }
    }

    /// Start the scheduler.
    ///
    /// A background tokio task is spawned that, every `interval + jitter`
    /// milliseconds, acquires a proxy and calls `fetch_fn(proxy)` inside a
    /// new spawned task.  Results are sent through the returned
    /// [`mpsc::Receiver`].
    ///
    /// If the scheduler is already running the call is a no-op and the
    /// returned receiver will never yield items (the real receiver was
    /// returned on the first call).
    pub fn start<F, Fut>(&self, fetch_fn: F) -> mpsc::Receiver<RequestResult<T>>
    where
        F: Fn(String) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<T, String>> + Send + 'static,
    {
        // Channel for results.  Buffer = max_workers so producers don't
        // block unless the consumer is very slow.
        let (tx, rx) = mpsc::channel::<RequestResult<T>>(self.config.max_workers.max(1));

        if self.running.swap(true, Ordering::SeqCst) {
            // Already running -- return a dummy receiver.
            return rx;
        }

        let running = Arc::clone(&self.running);
        let pm = Arc::clone(&self.proxy_manager);
        let active = Arc::clone(&self.active_tasks);
        let interval_ms = self.config.interval_ms;
        let jitter_ms = self.config.jitter_ms;
        let max_workers = self.config.max_workers;
        let fetch_fn = Arc::new(fetch_fn);

        tokio::spawn(async move {
            info!("Request scheduler timer started");

            while running.load(Ordering::SeqCst) {
                // Calculate sleep duration with jitter.
                let jitter = if jitter_ms > 0 {
                    rand::thread_rng().gen_range(0..=jitter_ms)
                } else {
                    0
                };
                let sleep_ms = interval_ms + jitter;
                tokio::time::sleep(tokio::time::Duration::from_millis(sleep_ms)).await;

                if !running.load(Ordering::SeqCst) {
                    break;
                }

                // Respect max_workers limit.
                if active.load(Ordering::SeqCst) >= max_workers {
                    debug!("Max workers reached ({max_workers}), skipping tick");
                    continue;
                }

                // Acquire a proxy.
                let proxy = {
                    let mut pm_guard = pm.lock().await;
                    pm_guard.get_next_proxy()
                };

                let proxy = match proxy {
                    Some(p) => p,
                    None => {
                        error!("No proxies available, skipping tick");
                        let _ = tx
                            .send(RequestResult::Error("No proxies available".into()))
                            .await;
                        continue;
                    }
                };

                // Spawn a task for the actual request.
                let tx2 = tx.clone();
                let fetch_fn2 = Arc::clone(&fetch_fn);
                let active2 = Arc::clone(&active);

                active.fetch_add(1, Ordering::SeqCst);

                tokio::spawn(async move {
                    let result = fetch_fn2(proxy).await;
                    active2.fetch_sub(1, Ordering::SeqCst);

                    let msg = match result {
                        Ok(val) => {
                            debug!("Request completed successfully");
                            RequestResult::Success(val)
                        }
                        Err(e) => {
                            error!("Request failed: {e}");
                            RequestResult::Error(e)
                        }
                    };

                    // If the receiver has been dropped we silently ignore.
                    let _ = tx2.send(msg).await;
                });
            }

            info!("Request scheduler timer stopped");
        });

        rx
    }

    /// Stop the scheduler.
    ///
    /// This sets the `running` flag to `false`.  The background timer task
    /// will exit after the current sleep completes.  In-flight fetch tasks
    /// will run to completion and their results will still be sent through
    /// the channel.
    pub fn stop(&self) {
        if self.running.swap(false, Ordering::SeqCst) {
            info!("RequestScheduler stopping");
        }
    }

    /// Whether the scheduler loop is currently active.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Return a snapshot of scheduler statistics.
    ///
    /// `queue_size` is reported as `0` because the mpsc channel length is
    /// not directly observable; the consumer should track that externally if
    /// needed.
    pub fn stats(&self) -> SchedulerStats {
        SchedulerStats {
            running: self.is_running(),
            interval_ms: self.config.interval_ms,
            jitter_ms: self.config.jitter_ms,
            active_tasks: self.active_tasks.load(Ordering::SeqCst),
            queue_size: 0,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proxy_manager::ProxyManager;

    fn make_pm() -> Arc<Mutex<ProxyManager>> {
        let pm = ProxyManager::new(vec!["http:a:1:u:p:enabled".to_string()], 0, false);
        Arc::new(Mutex::new(pm))
    }

    #[test]
    fn test_scheduler_config_creation() {
        let cfg = SchedulerConfig {
            interval_ms: 500,
            jitter_ms: 100,
            max_workers: 4,
        };
        assert_eq!(cfg.interval_ms, 500);
        assert_eq!(cfg.jitter_ms, 100);
        assert_eq!(cfg.max_workers, 4);
    }

    #[test]
    fn test_scheduler_not_running_initially() {
        let pm = make_pm();
        let sched = RequestScheduler::<String>::new(
            SchedulerConfig {
                interval_ms: 100,
                jitter_ms: 0,
                max_workers: 2,
            },
            pm,
        );
        assert!(!sched.is_running());
    }

    #[tokio::test]
    async fn test_scheduler_start_stop() {
        let pm = make_pm();
        let sched = RequestScheduler::<String>::new(
            SchedulerConfig {
                interval_ms: 50,
                jitter_ms: 0,
                max_workers: 2,
            },
            pm,
        );

        let _rx = sched.start(|proxy| async move { Ok(format!("fetched via {proxy}")) });

        assert!(sched.is_running());

        sched.stop();

        assert!(!sched.is_running());
    }

    #[tokio::test]
    async fn test_scheduler_delivers_results() {
        let pm = make_pm();
        let sched = RequestScheduler::<String>::new(
            SchedulerConfig {
                interval_ms: 10,
                jitter_ms: 0,
                max_workers: 4,
            },
            pm,
        );

        let mut rx = sched.start(|proxy| async move { Ok(format!("ok:{proxy}")) });

        // Wait for at least one result.
        let result = tokio::time::timeout(tokio::time::Duration::from_secs(2), rx.recv()).await;

        sched.stop();

        let result = result.expect("timed out waiting for result");
        let result = result.expect("channel closed");
        match result {
            RequestResult::Success(val) => {
                assert!(val.starts_with("ok:"));
            }
            RequestResult::Error(e) => panic!("unexpected error: {e}"),
        }
    }

    #[tokio::test]
    async fn test_scheduler_stats() {
        let pm = make_pm();
        let sched = RequestScheduler::<String>::new(
            SchedulerConfig {
                interval_ms: 1000,
                jitter_ms: 50,
                max_workers: 8,
            },
            pm,
        );

        let stats = sched.stats();
        assert!(!stats.running);
        assert_eq!(stats.interval_ms, 1000);
        assert_eq!(stats.jitter_ms, 50);
        assert_eq!(stats.active_tasks, 0);
    }

    #[tokio::test]
    async fn test_scheduler_error_propagation() {
        let pm = make_pm();
        let sched = RequestScheduler::<String>::new(
            SchedulerConfig {
                interval_ms: 10,
                jitter_ms: 0,
                max_workers: 2,
            },
            pm,
        );

        let mut rx = sched.start(|_proxy| async move { Err("simulated failure".to_string()) });

        let result = tokio::time::timeout(tokio::time::Duration::from_secs(2), rx.recv()).await;

        sched.stop();

        let result = result.expect("timed out").expect("channel closed");
        match result {
            RequestResult::Error(msg) => assert_eq!(msg, "simulated failure"),
            RequestResult::Success(_) => panic!("expected error"),
        }
    }
}
