//! JSON Schema exporter for sdktrader types.
//!
//! Generates JSON Schema files for all core types.
//! These schemas are used to auto-generate Pydantic models for Python.
//!
//! Usage: cargo run -p sdktrader-schema-export -- [output_dir]

use schemars::schema_for;
use std::fs;
use std::path::Path;

fn main() {
    let output_dir = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "schemas".to_string());

    let output_path = Path::new(&output_dir);
    fs::create_dir_all(output_path).expect("Failed to create output directory");

    println!("Exporting JSON schemas to {}/", output_dir);

    // Order types
    export_schema::<sdktrader_core::models::OrderSide>(output_path, "order_side");
    export_schema::<sdktrader_core::models::OrderType>(output_path, "order_type");
    export_schema::<sdktrader_core::models::OrderStatus>(output_path, "order_status");
    export_schema::<sdktrader_core::models::TimeInForce>(output_path, "time_in_force");
    export_schema::<sdktrader_core::models::OrderResult>(output_path, "order_result");
    export_schema::<sdktrader_core::models::OrderInfo>(output_path, "order_info");

    // Position types
    export_schema::<sdktrader_core::models::PositionSide>(output_path, "position_side");
    export_schema::<sdktrader_core::models::MarginMode>(output_path, "margin_mode");
    export_schema::<sdktrader_core::models::Position>(output_path, "position");

    // Market types
    export_schema::<sdktrader_core::models::MarketType>(output_path, "market_type");
    export_schema::<sdktrader_core::models::Interval>(output_path, "interval");
    export_schema::<sdktrader_core::models::SymbolInfo>(output_path, "symbol_info");
    export_schema::<sdktrader_core::models::ExchangeInfo>(output_path, "exchange_info");

    // Balance types
    export_schema::<sdktrader_core::models::Balance>(output_path, "balance");
    export_schema::<sdktrader_core::models::AccountBalance>(output_path, "account_balance");

    // Orderbook types
    export_schema::<sdktrader_core::models::OrderbookLevel>(output_path, "orderbook_level");
    export_schema::<sdktrader_core::models::Orderbook>(output_path, "orderbook");

    // Ticker types
    export_schema::<sdktrader_core::models::TickerInfo>(output_path, "ticker_info");
    export_schema::<sdktrader_core::models::FundingRateEntry>(output_path, "funding_rate_entry");
    export_schema::<sdktrader_core::models::OpenInterestEntry>(output_path, "open_interest_entry");

    // Candle
    export_schema::<sdktrader_core::models::Candle>(output_path, "candle");

    // Events
    export_schema::<sdktrader_core::models::OrderUpdateEvent>(output_path, "order_update_event");
    export_schema::<sdktrader_core::models::TradeEvent>(output_path, "trade_event");
    export_schema::<sdktrader_core::models::WsTestResult>(output_path, "ws_test_result");
    export_schema::<sdktrader_core::models::OrderTestResult>(output_path, "order_test_result");

    // Capabilities
    export_schema::<sdktrader_core::models::CapabilityStatus>(output_path, "capability_status");
    export_schema::<sdktrader_core::models::ExchangeCapabilities>(output_path, "exchange_capabilities");

    println!("Done! Exported schemas to {}/", output_dir);
}

fn export_schema<T: schemars::JsonSchema>(output_path: &Path, name: &str) {
    let schema = schema_for!(T);
    let json = serde_json::to_string_pretty(&schema).expect("Failed to serialize schema");
    let file_path = output_path.join(format!("{}.json", name));
    fs::write(&file_path, &json).expect(&format!("Failed to write {}", file_path.display()));
    println!(
        "  {} -> {}",
        std::any::type_name::<T>().rsplit("::").next().unwrap_or("?"),
        file_path.display()
    );
}
