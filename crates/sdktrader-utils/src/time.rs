//! Time utilities.

use chrono::{DateTime, TimeZone, Utc};

/// Convert timestamp in milliseconds to DateTime<Utc>.
pub fn timestamp_to_datetime(timestamp_ms: i64) -> DateTime<Utc> {
    Utc.timestamp_millis_opt(timestamp_ms)
        .single()
        .unwrap_or_else(Utc::now)
}

/// Convert DateTime<Utc> to timestamp in milliseconds.
pub fn datetime_to_timestamp(dt: DateTime<Utc>) -> i64 {
    dt.timestamp_millis()
}

/// Get elapsed time in milliseconds since a timestamp.
pub fn elapsed_ms(since: i64) -> i64 {
    sdktrader_core::utils::timestamp_ms() - since
}

/// Format duration in milliseconds to human readable string.
pub fn format_duration_ms(ms: i64) -> String {
    if ms < 1000 {
        format!("{}ms", ms)
    } else if ms < 60000 {
        format!("{:.1}s", ms as f64 / 1000.0)
    } else if ms < 3600000 {
        format!("{:.1}m", ms as f64 / 60000.0)
    } else {
        format!("{:.1}h", ms as f64 / 3600000.0)
    }
}

/// Parse ISO 8601 datetime string.
pub fn parse_iso_datetime(s: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Datelike;

    #[test]
    fn test_timestamp_to_datetime() {
        let ts = 1704067200000i64; // 2024-01-01 00:00:00 UTC
        let dt = timestamp_to_datetime(ts);
        assert_eq!(dt.year(), 2024);
        assert_eq!(dt.month(), 1);
        assert_eq!(dt.day(), 1);
    }

    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration_ms(500), "500ms");
        assert_eq!(format_duration_ms(1500), "1.5s");
        assert_eq!(format_duration_ms(90000), "1.5m");
        assert_eq!(format_duration_ms(5400000), "1.5h");
    }
}
