use chrono::{DateTime, Timelike, Utc};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimePart {
    pub event_date: String,
    pub hour: u32,
}

pub fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

pub fn time_part(timestamp_ms: i64) -> TimePart {
    let timestamp =
        DateTime::<Utc>::from_timestamp_millis(timestamp_ms).unwrap_or(DateTime::<Utc>::UNIX_EPOCH);
    TimePart {
        event_date: timestamp.format("%Y-%m-%d").to_string(),
        hour: timestamp.hour(),
    }
}

pub fn floor_window(timestamp_ms: i64, window_ms: i64) -> i64 {
    if window_ms <= 0 {
        return timestamp_ms;
    }
    timestamp_ms.div_euclid(window_ms) * window_ms
}

pub fn run_id(prefix: &str, timestamp_ms: i64) -> String {
    let timestamp =
        DateTime::<Utc>::from_timestamp_millis(timestamp_ms).unwrap_or(DateTime::<Utc>::UNIX_EPOCH);
    format!("{}-{}", prefix, timestamp.format("%Y%m%dT%H%M%S%3fZ"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn floors_window_with_epoch_millis() {
        assert_eq!(floor_window(1778166029000, 900_000), 1778166000000);
    }

    #[test]
    fn builds_market_l1_time_part() {
        let part = time_part(0);
        assert_eq!(part.event_date, "1970-01-01");
        assert_eq!(part.hour, 0);
    }
}
