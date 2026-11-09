# TraderSDK

Unified Rust SDK for cryptocurrency trading. One trait, 20 exchanges, spot and perpetuals.

```rust
use tradersdk::prelude::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let credentials = ExchangeCredentials {
        api_key: "your_key".into(),
        api_secret: "your_secret".into(),
        passphrase: None,
        testnet: true,
    };

    let trader = create_trader("bybit", credentials, MarketType::Swap)?;

    let price = trader.get_current_price("BTCUSDT").await?;
    println!("BTC: {price}");

    Ok(())
}
```

Swap `"bybit"` for `"binance"`, `"okx"`, `"gate"` — the rest of your code stays the same.

## Install

Not published to crates.io yet — depend on the repository directly:

```toml
[dependencies]
tradersdk = { git = "https://github.com/suenot/tradersdk" }
```

Every exchange is behind a feature flag. The default `full` feature pulls all 20; compile only what you use:

```toml
tradersdk = { git = "https://github.com/suenot/tradersdk", default-features = false, features = ["bybit", "binance"] }
```

## Supported exchanges

| | | | |
|---|---|---|---|
| Binance | Bybit | OKX | Gate.io |
| Bitget | BingX | MEXC | KuCoin |
| HTX | Kraken | Deribit | Hyperliquid |
| Bitfinex | Bitmart | CoinEx | LBank |
| Phemex | XT | WEEX | Ourbit |

Coverage is not uniform — every adapter reports what it actually supports through
`trader.capabilities()`, so you can check before you call rather than discovering
at runtime.

## The trait

Everything is built on one trait. Implement it and your exchange works with the
rest of the SDK; call it and you are exchange-agnostic.

```rust
#[async_trait]
pub trait ExchangeTrader: Send + Sync {
    fn exchange_name(&self) -> &str;
    fn market_type(&self) -> MarketType;
    fn capabilities(&self) -> ExchangeCapabilities;

    // Connection
    async fn test_connection(&self) -> Result<bool>;
    async fn get_server_time(&self) -> Result<i64>;

    // Market data
    async fn get_current_price(&self, symbol: &str) -> Result<f64>;
    async fn get_ticker(&self, symbol: &str) -> Result<TickerInfo>;
    async fn get_orderbook(&self, symbol: &str, depth: u32) -> Result<Orderbook>;
    async fn get_funding_rate_history(&self, symbol: &str, limit: u32) -> Result<Vec<FundingRateEntry>>;
    async fn get_open_interest_history(&self, symbol: &str, interval: &str, limit: u32) -> Result<Vec<OpenInterestEntry>>;

    // Account
    async fn get_balance(&self) -> Result<AccountBalance>;
    async fn set_leverage(&self, symbol: Option<&str>, leverage: u32) -> Result<bool>;

    // Trading
    async fn place_market_order(&self, symbol: &str, side: OrderSide, quantity: f64) -> Result<OrderResult>;
    async fn place_limit_order(&self, symbol: &str, side: OrderSide, quantity: f64, price: f64, reduce_only: bool) -> Result<OrderResult>;
    async fn execute_fast_entry(
        &self,
        symbol: &str,
        direction: &str,
        usdt_amount: f64,
        leverage: u32,
        tp_percent: Option<f64>,
        sl_percent: Option<f64>,
        current_price: Option<f64>,
    ) -> Result<OrderResult>;
    async fn cancel_order(&self, symbol: &str, order_id: &str) -> Result<bool>;
    async fn get_order_status(&self, symbol: &str, order_id: &str) -> Result<OrderStatus>;
    async fn close_position(&self, symbol: &str) -> Result<OrderResult>;
}
```

`execute_fast_entry` places a market order and attaches take-profit / stop-loss in a
single call, on exchanges whose API allows it — meaningfully faster than placing
three separate orders.

## Workspace

The SDK is a workspace of small crates. Depend on the `tradersdk` facade for everything,
or pick individual crates to keep your build lean.

| Crate | Purpose |
|---|---|
| `tradersdk-core` | The `ExchangeTrader` trait, shared models, capability reporting |
| `tradersdk-providers` | Exchange registry and the `create_trader` factory |
| `tradersdk-<exchange>` | One crate per exchange adapter (20 of them) |
| `tradersdk-trading` | PnL calculators, position sizing, symbol formatting, exit grids |
| `tradersdk-proxy` | Proxy rotation with cooldown, request scheduling |
| `tradersdk-storage` | In-memory cache with TTL, file persistence |
| `tradersdk-price-cache` | Price cache with CoinGecko backfill and rate limiting |
| `tradersdk-leverage-cache` | Per-symbol leverage limits, cached |
| `tradersdk-monitoring` | Detects markets added to or removed from an exchange |
| `tradersdk-notifications` | Telegram and webhook alerts |
| `tradersdk-warming` | Connection pre-warming for latency-sensitive entries |
| `tradersdk-signal` | Signal parsing and routing |
| `tradersdk-api` | HTTP API for exercising adapters |
| `tradersdk-python` | Python bindings via PyO3 |
| `tradersdk-utils` | Retry, rate limiting, circuit breaking |
| `tradersdk-schema-export` | JSON Schema export for the shared models |

## Utilities

Caching:

```rust
use tradersdk::storage::*;

let cache: MemoryCache<String, f64> = MemoryCache::new()
    .with_ttl(Duration::from_secs(60))
    .with_max_size(1000);

cache.set("BTCUSDT".into(), 50_000.0);
let price = cache.get(&"BTCUSDT".into());
```

Alerts:

```rust
use tradersdk::notifications::*;

let mut manager = NotificationManager::new();
manager.add_telegram("alerts", "BOT_TOKEN", 123456789);
manager.add_webhook("discord", "https://discord.com/api/webhooks/...");

manager.notify_trade("BTCUSDT", "Buy", 50_000.0, 0.1).await;
```

Detecting new markets:

```rust
use tradersdk_monitoring::{MarketDiffDetector, MarketInfo};

let mut detector = MarketDiffDetector::new();
detector.load_initial(fetch_markets().await?);

// later
for change in detector.detect_changes(fetch_markets().await?) {
    println!("{} {}", change.change_type, change.market);
}
```

## Python

Bindings are built with PyO3 and require Python 3.9+.

```bash
cd crates/tradersdk-python
maturin develop
```

## Status

Pre-1.0. The trait is stable in shape but not yet frozen — minor releases may
break it while adapter coverage fills in. Pin an exact version if that matters
to you.

## License

MIT
