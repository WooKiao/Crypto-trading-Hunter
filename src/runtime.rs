use crate::{
    app::{App, EventEffects},
    constants::{
        EVENT_BATCH_SIZE, GATE_EXCHANGE, LIGHTER_EXCHANGE, PERSISTENCE_BATCH_SIZE,
        READ_MODEL_REFRESH_INTERVAL,
    },
    database::{Database, PersistenceTask},
    gate_execution::ExecutionTask,
    lighter_execution::{LighterExecutionMarket, LighterExecutionTask},
    models::{InstrumentKey, OrderStatus, ServiceEvent, SimulatedOrderRecord},
    support::{
        SharedMetrics, SharedReadModel, SharedState, event_received_at, sync_read_model,
        update_metrics,
    },
};
use anyhow::Result;
use std::collections::HashMap;
use tokio::{
    sync::{mpsc, watch},
    time::{self, MissedTickBehavior},
};
use tracing::warn;

pub async fn run_state_manager(
    state: SharedState,
    metrics: SharedMetrics,
    mut event_rx: mpsc::Receiver<ServiceEvent>,
    persistence_tx: mpsc::UnboundedSender<PersistenceTask>,
    gate_execution_tx: Option<mpsc::UnboundedSender<ExecutionTask>>,
    lighter_execution_tx: Option<mpsc::UnboundedSender<LighterExecutionTask>>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<()> {
    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => break,
            maybe_event = event_rx.recv() => {
                let Some(event) = maybe_event else {
                    break;
                };
                let processing_started_at = std::time::Instant::now();
                let mut events = vec![event];
                while events.len() < EVENT_BATCH_SIZE {
                    match event_rx.try_recv() {
                        Ok(event) => events.push(event),
                        Err(_) => break,
                    }
                }

                let queue_delay_us_values = events
                    .iter()
                    .filter_map(|event| {
                        event_received_at(event).map(|instant| {
                            processing_started_at
                                .saturating_duration_since(instant)
                                .as_micros() as u64
                        })
                    })
                    .collect::<Vec<_>>();

                let mut persistence_tasks = Vec::new();
                let mut gate_execution_tasks = Vec::new();
                let mut lighter_execution_tasks = Vec::new();
                {
                    let mut app = state.write().await;
                    for event in events {
                        let effects = app.apply_event(event);
                        persistence_tasks.extend(
                            effects
                                .new_signals
                                .iter()
                                .map(|signal| PersistenceTask::InsertSignalAndOrders {
                                    signal: signal.clone(),
                                    orders: effects
                                        .new_orders
                                        .iter()
                                        .filter(|order| {
                                            order.exchange == signal.exchange
                                                && order.instrument == signal.instrument
                                                && order.opened_at_ms == signal.created_at_ms
                                        })
                                        .cloned()
                                        .collect::<Vec<_>>(),
                                })
                                .chain(
                                    effects
                                        .updated_orders
                                        .iter()
                                        .cloned()
                                        .map(|order| PersistenceTask::UpdateOrder { order }),
                                ),
                        );
                        let gate_live_trading_enabled =
                            app.live_trading_enabled && app.gate_live_trading_configured;
                        if gate_live_trading_enabled {
                            gate_execution_tasks
                                .extend(build_gate_execution_tasks(&app, &effects));
                        }
                        let lighter_live_trading_enabled =
                            app.live_trading_enabled && app.lighter_live_trading_configured;
                        if lighter_live_trading_enabled {
                            lighter_execution_tasks
                                .extend(build_lighter_execution_tasks(&app, &effects));
                        }
                        app.commit_effects(effects);
                    }
                }

                for task in persistence_tasks {
                    if let Err(error) = persistence_tx.send(task) {
                        warn!(?error, "failed to enqueue persistence task");
                    }
                }
                if let Some(gate_execution_tx) = &gate_execution_tx {
                    for task in gate_execution_tasks {
                        if let Err(error) = gate_execution_tx.send(task) {
                            warn!(?error, "failed to enqueue execution task");
                        }
                    }
                }
                if let Some(lighter_execution_tx) = &lighter_execution_tx {
                    for task in lighter_execution_tasks {
                        if let Err(error) = lighter_execution_tx.send(task) {
                            warn!(?error, "failed to enqueue Lighter execution task");
                        }
                    }
                }
                let processing_finished_at = std::time::Instant::now();
                let processing_us_per_event = processing_finished_at
                    .saturating_duration_since(processing_started_at)
                    .as_micros() as u64
                    / queue_delay_us_values.len().max(1) as u64;
                let end_to_end_us_values = queue_delay_us_values
                    .iter()
                    .map(|queue_delay_us| queue_delay_us + processing_us_per_event)
                    .collect::<Vec<_>>();
                update_metrics(&metrics, |latency_metrics| {
                    for value in &queue_delay_us_values {
                        latency_metrics.queue_delay_us.record(*value);
                    }
                    latency_metrics.processing_us.record(processing_us_per_event);
                    for value in &end_to_end_us_values {
                        latency_metrics.end_to_end_us.record(*value);
                    }
                });
            }
        }
    }

    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct LiveOpenGroupKey {
    exchange: String,
    instrument: String,
    opened_at_ms: u64,
}

#[derive(Debug, Clone)]
struct LiveOpenGroup {
    key: LiveOpenGroupKey,
    symbol: String,
    quote_notional: f64,
    orders: Vec<SimulatedOrderRecord>,
}

fn build_gate_execution_tasks(app: &App, effects: &EventEffects) -> Vec<ExecutionTask> {
    let mut tasks = Vec::new();
    let mut pending_symbol_exposure = HashMap::<InstrumentKey, f64>::new();

    for group in collect_live_open_groups(&effects.new_orders, GATE_EXCHANGE) {
        if allow_live_open_group(app, &group, &mut pending_symbol_exposure) {
            tasks.extend(
                group
                    .orders
                    .into_iter()
                    .map(|order| ExecutionTask::Open { order }),
            );
        }
    }

    tasks.extend(
        effects
            .updated_orders
            .iter()
            .filter(|order| order.exchange == GATE_EXCHANGE)
            .filter(|order| order.status == OrderStatus::Closed)
            .cloned()
            .map(|order| ExecutionTask::Close { order }),
    );

    tasks
}

fn build_lighter_execution_tasks(app: &App, effects: &EventEffects) -> Vec<LighterExecutionTask> {
    let mut tasks = Vec::new();
    let mut pending_symbol_exposure = HashMap::<InstrumentKey, f64>::new();

    for group in collect_live_open_groups(&effects.new_orders, LIGHTER_EXCHANGE) {
        if allow_live_open_group(app, &group, &mut pending_symbol_exposure) {
            for order in group.orders {
                if let Some(market) = build_lighter_execution_market(app, &order) {
                    tasks.push(LighterExecutionTask::Open { order, market });
                }
            }
        }
    }
    for order in effects
        .updated_orders
        .iter()
        .filter(|order| order.exchange == LIGHTER_EXCHANGE)
        .filter(|order| order.status == OrderStatus::Closed)
    {
        if let Some(market) = build_lighter_execution_market(app, order) {
            tasks.push(LighterExecutionTask::Close {
                order: order.clone(),
                market,
            });
        }
    }

    tasks
}

fn collect_live_open_groups(orders: &[SimulatedOrderRecord], exchange: &str) -> Vec<LiveOpenGroup> {
    let mut groups = Vec::<LiveOpenGroup>::new();
    let mut group_indices = HashMap::<LiveOpenGroupKey, usize>::new();

    for order in orders.iter().filter(|order| order.exchange == exchange) {
        let key = LiveOpenGroupKey {
            exchange: order.exchange.clone(),
            instrument: order.instrument.clone(),
            opened_at_ms: order.opened_at_ms,
        };
        if let Some(index) = group_indices.get(&key).copied() {
            groups[index].quote_notional += order.quote_notional;
            groups[index].orders.push(order.clone());
            continue;
        }

        let index = groups.len();
        group_indices.insert(key.clone(), index);
        groups.push(LiveOpenGroup {
            key,
            symbol: order.symbol.clone(),
            quote_notional: order.quote_notional,
            orders: vec![order.clone()],
        });
    }

    groups
}

fn allow_live_open_group(
    app: &App,
    group: &LiveOpenGroup,
    pending_symbol_exposure: &mut HashMap<InstrumentKey, f64>,
) -> bool {
    let instrument_key = InstrumentKey {
        exchange: group.key.exchange.clone(),
        instrument: group.key.instrument.clone(),
    };

    if !app.live_trading_allow_opens {
        warn!(
            exchange = group.key.exchange,
            instrument = group.key.instrument,
            symbol = group.symbol,
            "skipping live open because open-side trading is disabled"
        );
        return false;
    }

    if !app.stream_control.get(&group.key.exchange) {
        warn!(
            exchange = group.key.exchange,
            instrument = group.key.instrument,
            symbol = group.symbol,
            "skipping live open because the exchange stream is stopped"
        );
        return false;
    }

    if app.live_max_order_quote_notional > 0.0
        && group.quote_notional > app.live_max_order_quote_notional + 1e-9
    {
        warn!(
            exchange = group.key.exchange,
            instrument = group.key.instrument,
            symbol = group.symbol,
            quote_notional = group.quote_notional,
            limit = app.live_max_order_quote_notional,
            "skipping live open because group quote notional exceeds the configured limit"
        );
        return false;
    }

    if app.live_open_cooldown_seconds > 0.0 {
        let cooldown_ms = (app.live_open_cooldown_seconds * 1000.0).round() as u64;
        if let Some(last_opened_at_ms) = app
            .orders
            .iter()
            .filter(|order| order.exchange == group.key.exchange)
            .filter(|order| order.instrument == group.key.instrument)
            .filter(|order| order.opened_at_ms < group.key.opened_at_ms)
            .map(|order| order.opened_at_ms)
            .max()
        {
            if group.key.opened_at_ms.saturating_sub(last_opened_at_ms) < cooldown_ms {
                warn!(
                    exchange = group.key.exchange,
                    instrument = group.key.instrument,
                    symbol = group.symbol,
                    opened_at_ms = group.key.opened_at_ms,
                    last_opened_at_ms,
                    cooldown_ms,
                    "skipping live open because cooldown is still active"
                );
                return false;
            }
        }
    }

    let existing_symbol_exposure = app
        .orders
        .iter()
        .filter(|order| order.status == OrderStatus::Open)
        .filter(|order| order.exchange == group.key.exchange)
        .filter(|order| order.instrument == group.key.instrument)
        .map(|order| order.quote_notional)
        .sum::<f64>();
    let pending_symbol_exposure_value = pending_symbol_exposure
        .get(&instrument_key)
        .copied()
        .unwrap_or_default();
    let next_symbol_exposure =
        existing_symbol_exposure + pending_symbol_exposure_value + group.quote_notional;
    if app.live_max_symbol_exposure_quote_notional > 0.0
        && next_symbol_exposure > app.live_max_symbol_exposure_quote_notional + 1e-9
    {
        warn!(
            exchange = group.key.exchange,
            instrument = group.key.instrument,
            symbol = group.symbol,
            next_symbol_exposure,
            limit = app.live_max_symbol_exposure_quote_notional,
            "skipping live open because symbol exposure would exceed the configured limit"
        );
        return false;
    }

    *pending_symbol_exposure.entry(instrument_key).or_default() += group.quote_notional;
    true
}

fn build_lighter_execution_market(
    app: &App,
    order: &SimulatedOrderRecord,
) -> Option<LighterExecutionMarket> {
    let key = InstrumentKey {
        exchange: LIGHTER_EXCHANGE.to_string(),
        instrument: order.instrument.clone(),
    };
    let row = match app.markets.get(&key) {
        Some(row) => row,
        None => {
            warn!(
                instrument = order.instrument,
                client_order_id = order.client_order_id,
                "missing Lighter market row for live execution task"
            );
            return None;
        }
    };
    let market_id = match order.instrument.parse::<u64>() {
        Ok(value) => value,
        Err(error) => {
            warn!(
                instrument = order.instrument,
                client_order_id = order.client_order_id,
                ?error,
                "invalid Lighter market id for live execution task"
            );
            return None;
        }
    };

    Some(LighterExecutionMarket {
        market_id,
        price_decimals: row.price_decimals,
        size_decimals: row.size_decimals,
        min_base_amount: row.min_base_amount.clone(),
        best_bid_price: row.best_bid_price.clone(),
        best_ask_price: row.best_ask_price.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        app::App,
        models::{PersistedConfig, SignalDirection, StreamControlState},
    };

    fn gate_order(
        instrument: &str,
        symbol: &str,
        opened_at_ms: u64,
        quote_notional: f64,
        status: OrderStatus,
    ) -> SimulatedOrderRecord {
        SimulatedOrderRecord {
            id: None,
            client_order_id: format!("{instrument}-{opened_at_ms}-{quote_notional}"),
            opened_at_ms,
            closed_at_ms: None,
            exchange: GATE_EXCHANGE.to_string(),
            instrument: instrument.to_string(),
            symbol: symbol.to_string(),
            direction: SignalDirection::Bullish,
            status,
            entry_price: "100".to_string(),
            quantity_base: "1".to_string(),
            quote_notional,
            take_profit_price: "--".to_string(),
            stop_loss_price: "--".to_string(),
            exit_price: None,
            exit_reason: None,
        }
    }

    fn app_with_gate_stream() -> App {
        let mut app = App::new(PersistedConfig::default(), Vec::new(), Vec::new());
        app.set_stream_control(StreamControlState {
            lighter: false,
            gate: true,
        });
        app.set_live_trading_enabled(true);
        app.set_gate_live_trading_configured(true);
        app
    }

    #[test]
    fn blocks_live_opens_when_open_side_switch_is_disabled() {
        let mut app = app_with_gate_stream();
        app.set_live_risk_controls(false, 0.0, 0.0, 0.0);

        let effects = EventEffects {
            new_orders: vec![gate_order(
                "BTC_USDT",
                "BTC_USDT",
                10_000,
                100.0,
                OrderStatus::Open,
            )],
            ..EventEffects::default()
        };

        let tasks = build_gate_execution_tasks(&app, &effects);
        assert!(tasks.is_empty());
    }

    #[test]
    fn allows_live_closes_when_open_side_switch_is_disabled() {
        let mut app = app_with_gate_stream();
        app.set_live_risk_controls(false, 0.0, 0.0, 0.0);

        let effects = EventEffects {
            updated_orders: vec![gate_order(
                "BTC_USDT",
                "BTC_USDT",
                10_000,
                100.0,
                OrderStatus::Closed,
            )],
            ..EventEffects::default()
        };

        let tasks = build_gate_execution_tasks(&app, &effects);
        assert_eq!(tasks.len(), 1);
        assert!(matches!(tasks[0], ExecutionTask::Close { .. }));
    }

    #[test]
    fn blocks_grouped_live_open_when_group_notional_exceeds_limit() {
        let mut app = app_with_gate_stream();
        app.set_live_risk_controls(true, 0.0, 90.0, 0.0);

        let effects = EventEffects {
            new_orders: vec![
                gate_order("BTC_USDT", "BTC_USDT", 10_000, 50.0, OrderStatus::Open),
                gate_order("BTC_USDT", "BTC_USDT", 10_000, 50.0, OrderStatus::Open),
            ],
            ..EventEffects::default()
        };

        let tasks = build_gate_execution_tasks(&app, &effects);
        assert!(tasks.is_empty());
    }

    #[test]
    fn blocks_live_open_when_symbol_exposure_limit_would_be_exceeded() {
        let mut app = app_with_gate_stream();
        app.set_live_risk_controls(true, 0.0, 0.0, 120.0);
        app.orders.push_front(gate_order(
            "BTC_USDT",
            "BTC_USDT",
            5_000,
            80.0,
            OrderStatus::Open,
        ));

        let effects = EventEffects {
            new_orders: vec![gate_order(
                "BTC_USDT",
                "BTC_USDT",
                10_000,
                50.0,
                OrderStatus::Open,
            )],
            ..EventEffects::default()
        };

        let tasks = build_gate_execution_tasks(&app, &effects);
        assert!(tasks.is_empty());
    }

    #[test]
    fn blocks_live_open_when_cooldown_is_active() {
        let mut app = app_with_gate_stream();
        app.set_live_risk_controls(true, 60.0, 0.0, 0.0);
        app.orders.push_front(gate_order(
            "BTC_USDT",
            "BTC_USDT",
            10_000,
            80.0,
            OrderStatus::Closed,
        ));

        let effects = EventEffects {
            new_orders: vec![gate_order(
                "BTC_USDT",
                "BTC_USDT",
                50_000,
                50.0,
                OrderStatus::Open,
            )],
            ..EventEffects::default()
        };

        let tasks = build_gate_execution_tasks(&app, &effects);
        assert!(tasks.is_empty());
    }
}

pub async fn run_persistence_worker(
    metrics: SharedMetrics,
    db: Database,
    mut persistence_rx: mpsc::UnboundedReceiver<PersistenceTask>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<()> {
    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => break,
            maybe_task = persistence_rx.recv() => {
                let Some(task) = maybe_task else {
                    break;
                };

                let mut batch = vec![task];
                while batch.len() < PERSISTENCE_BATCH_SIZE {
                    match persistence_rx.try_recv() {
                        Ok(task) => batch.push(task),
                        Err(_) => break,
                    }
                }

                let queue_depth = persistence_rx.len();
                update_metrics(&metrics, |latency_metrics| {
                    latency_metrics.persistence_queue_depth = queue_depth;
                    latency_metrics
                        .persistence_batch_size
                        .record(batch.len() as u64);
                });

                let persist_started_at = std::time::Instant::now();
                for task in &batch {
                    let result = match task {
                        PersistenceTask::InsertSignalAndOrders { signal, orders } => {
                            db.insert_signal_and_orders(signal, orders)
                        }
                        PersistenceTask::UpdateOrder { order } => db.update_order(order),
                    };
                    if let Err(error) = result {
                        match task {
                            PersistenceTask::InsertSignalAndOrders { signal, .. } => {
                                warn!(
                                    exchange = signal.exchange,
                                    instrument = signal.instrument,
                                    symbol = signal.symbol,
                                    ?error,
                                    "failed to persist signal and simulated order"
                                );
                            }
                            PersistenceTask::UpdateOrder { order } => {
                                warn!(
                                    exchange = order.exchange,
                                    instrument = order.instrument,
                                    symbol = order.symbol,
                                    client_order_id = order.client_order_id,
                                    ?error,
                                    "failed to update simulated order"
                                );
                            }
                        }
                    }
                }
                let persist_us = persist_started_at.elapsed().as_micros() as u64;
                let avg_persist_us = persist_us / batch.len().max(1) as u64;
                update_metrics(&metrics, |latency_metrics| {
                    latency_metrics.persist_us.record(avg_persist_us);
                    latency_metrics.persistence_queue_depth = persistence_rx.len();
                });
            }
        }
    }

    Ok(())
}

pub async fn run_read_model_worker(
    state: SharedState,
    metrics: SharedMetrics,
    read_model: SharedReadModel,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<()> {
    let mut interval = time::interval(READ_MODEL_REFRESH_INTERVAL);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => break,
            _ = interval.tick() => {
                sync_read_model(&state, &metrics, &read_model).await;
            }
        }
    }

    Ok(())
}
