use std::time::{SystemTime, UNIX_EPOCH};

pub fn format_decimal(value: f64) -> String {
    let mut rendered = format!("{value:.12}");
    while rendered.contains('.') && rendered.ends_with('0') {
        rendered.pop();
    }
    if rendered.ends_with('.') {
        rendered.pop();
    }
    rendered
}

pub fn floor_units(value: f64, step: f64) -> Option<f64> {
    if !(value.is_finite() && step.is_finite() && value > 0.0 && step > 0.0) {
        return None;
    }

    let units = (value / step).floor();
    (units >= 1.0).then_some(units)
}

pub fn max_units_strict_below(limit: f64, step: f64) -> Option<f64> {
    if !(limit.is_finite() && step.is_finite() && limit > 0.0 && step > 0.0) {
        return None;
    }

    let ratio = limit / step;
    if ratio <= 1.0 {
        return None;
    }

    let rounded = ratio.round();
    let units = if (ratio - rounded).abs() <= 1e-9 {
        rounded - 1.0
    } else {
        ratio.floor()
    };

    (units >= 1.0).then_some(units)
}

pub fn unix_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub fn unix_now_ns() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}
