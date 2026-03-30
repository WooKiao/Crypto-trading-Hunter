use std::collections::VecDeque;

#[derive(Debug, Clone)]
pub struct WindowPoint {
    pub timestamp_ms: u64,
    pub price: f64,
    pub display: String,
}

#[derive(Debug, Default, Clone)]
pub struct SlidingWindow {
    events: VecDeque<WindowPoint>,
    min_deque: VecDeque<WindowPoint>,
    max_deque: VecDeque<WindowPoint>,
}

impl SlidingWindow {
    pub fn push(&mut self, point: WindowPoint, cutoff_ms: u64) {
        while let Some(back) = self.min_deque.back() {
            if back.price <= point.price {
                break;
            }
            self.min_deque.pop_back();
        }
        self.min_deque.push_back(point.clone());

        while let Some(back) = self.max_deque.back() {
            if back.price >= point.price {
                break;
            }
            self.max_deque.pop_back();
        }
        self.max_deque.push_back(point.clone());

        self.events.push_back(point);
        self.evict(cutoff_ms);
    }

    fn evict(&mut self, cutoff_ms: u64) {
        while let Some(front) = self.events.front() {
            if front.timestamp_ms >= cutoff_ms {
                break;
            }

            let expired = self.events.pop_front().expect("front exists");

            if self.min_deque.front().is_some_and(|front| {
                front.timestamp_ms == expired.timestamp_ms && front.display == expired.display
            }) {
                self.min_deque.pop_front();
            }

            if self.max_deque.front().is_some_and(|front| {
                front.timestamp_ms == expired.timestamp_ms && front.display == expired.display
            }) {
                self.max_deque.pop_front();
            }
        }
    }

    pub fn low(&self) -> Option<&str> {
        self.min_deque.front().map(|point| point.display.as_str())
    }

    pub fn high(&self) -> Option<&str> {
        self.max_deque.front().map(|point| point.display.as_str())
    }

    pub fn low_point(&self) -> Option<&WindowPoint> {
        self.min_deque.front()
    }

    pub fn high_point(&self) -> Option<&WindowPoint> {
        self.max_deque.front()
    }
}

pub fn weighted_average_recent(values: &VecDeque<f64>) -> Option<f64> {
    if values.is_empty() {
        return None;
    }

    let mut weighted_sum = 0.0_f64;
    let mut weight_sum = 0.0_f64;
    for (index, value) in values.iter().enumerate() {
        let weight = (index + 1) as f64;
        weighted_sum += value * weight;
        weight_sum += weight;
    }

    (weight_sum > 0.0).then_some(weighted_sum / weight_sum)
}
