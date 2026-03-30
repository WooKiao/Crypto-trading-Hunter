use crate::{
    app::App,
    models::{
        FeedTiming, RuntimeLatencyMetrics, ServiceEvent, SnapshotResponse, StreamControlState,
    },
    util::unix_now_ms,
};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{
    sync::{RwLock, watch},
    time,
};

pub(crate) type SharedState = Arc<RwLock<App>>;
pub(crate) type SharedReadModel = Arc<RwLock<SnapshotResponse>>;
pub(crate) type SharedMetrics = Arc<Mutex<RuntimeLatencyMetrics>>;

pub(crate) fn build_timing(exchange_timestamp_ms: Option<u64>) -> FeedTiming {
    FeedTiming {
        received_at: Instant::now(),
        exchange_timestamp_ms,
        exchange_latency_ms: exchange_timestamp_ms
            .map(|timestamp_ms| unix_now_ms().saturating_sub(timestamp_ms)),
    }
}

pub(crate) fn stream_enabled_for(control: &StreamControlState, exchange: &str) -> bool {
    control.get(exchange)
}

pub(crate) fn event_received_at(event: &ServiceEvent) -> Option<Instant> {
    match event {
        ServiceEvent::MarketStats(batch) => Some(batch.timing.received_at),
        ServiceEvent::Bbo(update) => Some(update.timing.received_at),
        _ => None,
    }
}

pub(crate) fn update_metrics<F>(metrics: &SharedMetrics, updater: F)
where
    F: FnOnce(&mut RuntimeLatencyMetrics),
{
    if let Ok(mut guard) = metrics.lock() {
        updater(&mut guard);
    }
}

pub(crate) async fn sync_read_model(
    state: &SharedState,
    metrics: &SharedMetrics,
    read_model: &SharedReadModel,
) {
    let latency_snapshot = metrics
        .lock()
        .map(|guard| guard.snapshot())
        .unwrap_or_default();
    let snapshot = state.read().await.snapshot(latency_snapshot);
    *read_model.write().await = snapshot;
}

pub(crate) async fn wait_or_shutdown(
    shutdown_rx: &mut watch::Receiver<bool>,
    duration: Duration,
) -> bool {
    tokio::select! {
        _ = shutdown_rx.changed() => true,
        _ = time::sleep(duration) => false,
    }
}
