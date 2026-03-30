use crate::{
    constants::{
        DEPTH_SAMPLE_WINDOW, GATE_EXCHANGE, LIGHTER_EXCHANGE, MAX_ORDER_HISTORY, MAX_SIGNAL_HISTORY,
    },
    models::{
        BboUpdate, ConnectionSnapshot, ConnectionStatus, FeedTiming, InstrumentKey, MarketSnapshot,
        MarketStatsBatch, MetadataBatch, OrderStatus, PersistedConfig, RuntimeLatencySnapshot,
        ServiceEvent, SignalDirection, SignalRecord, SimulatedOrderRecord, SnapshotResponse,
        StatusSnapshot, StreamControlState,
    },
    util::{floor_units, format_decimal, max_units_strict_below, unix_now_ms, unix_now_ns},
    window::{SlidingWindow, WindowPoint, weighted_average_recent},
};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    time::Instant,
};

#[derive(Debug, Default)]
pub(crate) struct EventEffects {
    pub(crate) new_signals: Vec<SignalRecord>,
    pub(crate) new_orders: Vec<SimulatedOrderRecord>,
    pub(crate) updated_orders: Vec<SimulatedOrderRecord>,
}

#[derive(Debug, Clone)]
pub struct MarketRow {
    pub exchange: String,
    pub instrument: String,
    pub symbol: String,
    pub prev_last_trade_price: String,
    pub last_trade_price: String,
    pub open_interest: String,
    pub window_low_price: String,
    pub window_high_price: String,
    pub best_bid_price: String,
    pub best_bid_size: String,
    pub best_bid_size_base: String,
    pub best_bid_notional_samples: VecDeque<f64>,
    pub best_ask_price: String,
    pub best_ask_size: String,
    pub best_ask_size_base: String,
    pub best_ask_notional_samples: VecDeque<f64>,
    pub current_funding_rate: String,
    pub precision: String,
    pub price_decimals: u32,
    pub size_decimals: u32,
    pub min_base_amount: String,
    pub min_quote_amount: String,
    pub order_quote_limit: String,
    pub quantity_multiplier: f64,
}

impl MarketRow {
    fn new(key: &InstrumentKey, symbol: String) -> Self {
        Self {
            exchange: key.exchange.clone(),
            instrument: key.instrument.clone(),
            symbol,
            prev_last_trade_price: "--".to_string(),
            last_trade_price: "--".to_string(),
            open_interest: "--".to_string(),
            window_low_price: "--".to_string(),
            window_high_price: "--".to_string(),
            best_bid_price: "--".to_string(),
            best_bid_size: "--".to_string(),
            best_bid_size_base: "--".to_string(),
            best_bid_notional_samples: VecDeque::new(),
            best_ask_price: "--".to_string(),
            best_ask_size: "--".to_string(),
            best_ask_size_base: "--".to_string(),
            best_ask_notional_samples: VecDeque::new(),
            current_funding_rate: "--".to_string(),
            precision: "--".to_string(),
            price_decimals: 0,
            size_decimals: 0,
            min_base_amount: "--".to_string(),
            min_quote_amount: "--".to_string(),
            order_quote_limit: "--".to_string(),
            quantity_multiplier: 1.0,
        }
    }
}

pub struct App {
    pub stream_control: StreamControlState,
    pub window_hours: f64,
    pub price_jump_threshold_pct: f64,
    pub open_interest_threshold: f64,
    pub spread_threshold_pct: f64,
    pub max_hold_minutes: f64,
    pub sim_order_quote_size: f64,
    pub sim_take_profit_pct: f64,
    pub sim_take_profit_ratio_pct: f64,
    pub sim_take_profit_pct_2: f64,
    pub sim_take_profit_ratio_pct_2: f64,
    pub sim_stop_loss_pct: f64,
    pub live_trading_enabled: bool,
    pub live_trading_allow_opens: bool,
    pub live_open_cooldown_seconds: f64,
    pub live_max_order_quote_notional: f64,
    pub live_max_symbol_exposure_quote_notional: f64,
    pub gate_live_trading_configured: bool,
    pub lighter_live_trading_configured: bool,
    pub markets: HashMap<InstrumentKey, MarketRow>,
    pub windows: HashMap<InstrumentKey, SlidingWindow>,
    pub active_market_keys: HashSet<InstrumentKey>,
    pub market_order: Vec<InstrumentKey>,
    pub connection_states: HashMap<String, ConnectionStatus>,
    pub signals: VecDeque<SignalRecord>,
    pub orders: VecDeque<SimulatedOrderRecord>,
    pub last_message_at: Option<Instant>,
    pub last_exchange_timestamp_ms: Option<u64>,
    pub last_exchange_latency_ms: Option<u64>,
    pub latency_samples: u64,
    pub latency_sum_ms: u128,
    pub max_exchange_latency_ms: u64,
    pub last_rest_sync_at: Option<Instant>,
    pub last_rest_sync_count: usize,
    pub last_rest_sync_status: String,
}

impl App {
    pub fn new(
        config: PersistedConfig,
        persisted_signals: Vec<SignalRecord>,
        persisted_orders: Vec<SimulatedOrderRecord>,
    ) -> Self {
        let mut signals = VecDeque::new();
        for signal in persisted_signals {
            signals.push_back(signal);
        }
        let mut orders = VecDeque::new();
        for order in persisted_orders {
            orders.push_back(order);
        }

        Self {
            stream_control: StreamControlState::default(),
            window_hours: config.window_hours,
            price_jump_threshold_pct: config.price_jump_threshold_pct,
            open_interest_threshold: config.open_interest_threshold,
            spread_threshold_pct: config.spread_threshold_pct,
            max_hold_minutes: config.max_hold_minutes,
            sim_order_quote_size: config.sim_order_quote_size,
            sim_take_profit_pct: config.sim_take_profit_pct,
            sim_take_profit_ratio_pct: config.sim_take_profit_ratio_pct,
            sim_take_profit_pct_2: config.sim_take_profit_pct_2,
            sim_take_profit_ratio_pct_2: config.sim_take_profit_ratio_pct_2,
            sim_stop_loss_pct: config.sim_stop_loss_pct,
            live_trading_enabled: config.live_trading_enabled,
            live_trading_allow_opens: config.live_trading_allow_opens,
            live_open_cooldown_seconds: config.live_open_cooldown_seconds,
            live_max_order_quote_notional: config.live_max_order_quote_notional,
            live_max_symbol_exposure_quote_notional: config.live_max_symbol_exposure_quote_notional,
            gate_live_trading_configured: false,
            lighter_live_trading_configured: false,
            markets: HashMap::new(),
            windows: HashMap::new(),
            active_market_keys: HashSet::new(),
            market_order: Vec::new(),
            connection_states: HashMap::new(),
            signals,
            orders,
            last_message_at: None,
            last_exchange_timestamp_ms: None,
            last_exchange_latency_ms: None,
            latency_samples: 0,
            latency_sum_ms: 0,
            max_exchange_latency_ms: 0,
            last_rest_sync_at: None,
            last_rest_sync_count: 0,
            last_rest_sync_status: "waiting".to_string(),
        }
    }

    pub(crate) fn apply_event(&mut self, event: ServiceEvent) -> EventEffects {
        match event {
            ServiceEvent::StreamControl(control) => {
                self.set_stream_control(control);
                EventEffects::default()
            }
            ServiceEvent::MarketStats(batch) => self.apply_market_stats(batch),
            ServiceEvent::Bbo(update) => self.apply_bbo(update),
            ServiceEvent::Metadata(batch) => {
                self.apply_metadata(batch);
                EventEffects::default()
            }
            ServiceEvent::Connection(update) => {
                self.connection_states.insert(update.name, update.status);
                EventEffects::default()
            }
            ServiceEvent::RestError {
                message,
                occurred_at,
            } => {
                self.last_rest_sync_at = Some(occurred_at);
                self.last_rest_sync_status = format!("error: {message}");
                EventEffects::default()
            }
        }
    }

    fn apply_market_stats(&mut self, batch: MarketStatsBatch) -> EventEffects {
        let event_timestamp_ms = batch
            .timing
            .exchange_timestamp_ms
            .unwrap_or_else(unix_now_ms);
        let mut effects = EventEffects::default();

        for update in batch.updates {
            let key = update.key;
            let symbol = update.symbol;
            let current_funding_rate = update.current_funding_rate;
            let last_trade_price = update.last_trade_price;
            let open_interest = update.open_interest;

            {
                let row = self.ensure_market(&key, symbol);
                row.current_funding_rate = current_funding_rate;
                if row.last_trade_price != "--" && row.last_trade_price != last_trade_price {
                    row.prev_last_trade_price = row.last_trade_price.clone();
                }
                row.last_trade_price = last_trade_price.clone();
                row.open_interest = open_interest;
            }

            self.update_window(&key, &last_trade_price, event_timestamp_ms);
            effects
                .updated_orders
                .extend(self.maybe_close_active_orders(&key, event_timestamp_ms));
            if let Some((signal, orders)) = self.evaluate_signal_and_order(&key, event_timestamp_ms)
            {
                effects.new_signals.push(signal);
                effects.new_orders.extend(orders);
            }
        }
        self.apply_timing(batch.timing);
        effects
    }

    fn apply_bbo(&mut self, update: BboUpdate) -> EventEffects {
        let row = self.ensure_market(&update.key, update.symbol);
        row.best_bid_price = update.best_bid_price;
        row.best_bid_size = update.best_bid_size;
        row.best_bid_size_base = update.best_bid_size_base;
        row.best_bid_notional_samples
            .push_back(update.best_bid_notional);
        while row.best_bid_notional_samples.len() > DEPTH_SAMPLE_WINDOW {
            row.best_bid_notional_samples.pop_front();
        }
        row.best_ask_price = update.best_ask_price;
        row.best_ask_size = update.best_ask_size;
        row.best_ask_size_base = update.best_ask_size_base;
        row.best_ask_notional_samples
            .push_back(update.best_ask_notional);
        while row.best_ask_notional_samples.len() > DEPTH_SAMPLE_WINDOW {
            row.best_ask_notional_samples.pop_front();
        }
        self.apply_timing(update.timing);
        let mut effects = EventEffects::default();
        effects.updated_orders.extend(
            self.maybe_close_active_orders(
                &update.key,
                update
                    .timing
                    .exchange_timestamp_ms
                    .unwrap_or_else(unix_now_ms),
            ),
        );
        effects
    }

    fn apply_metadata(&mut self, batch: MetadataBatch) {
        let update_count = batch.updates.len();
        self.active_market_keys
            .retain(|key| key.exchange != batch.exchange);
        self.active_market_keys.extend(batch.active_market_keys);

        for update in batch.updates {
            let fallback_symbol = update.key.instrument.clone();
            let row = self.ensure_market(&update.key, fallback_symbol);
            row.price_decimals = update.price_decimals;
            row.size_decimals = update.size_decimals;
            row.precision = format!("{}/{}", update.price_decimals, update.size_decimals);
            row.min_base_amount = update.min_base_amount;
            row.min_quote_amount = update.min_quote_amount;
            row.order_quote_limit = update.order_quote_limit;
            row.quantity_multiplier = update.quantity_multiplier;
        }

        self.rebuild_market_order();
        self.last_rest_sync_at = Some(batch.fetched_at);
        self.last_rest_sync_count = update_count;
        self.last_rest_sync_status = format!("ok: {}", batch.reason);
    }

    fn apply_timing(&mut self, timing: FeedTiming) {
        self.last_message_at = Some(timing.received_at);
        self.last_exchange_timestamp_ms = timing.exchange_timestamp_ms;

        if let Some(latency_ms) = timing.exchange_latency_ms {
            self.last_exchange_latency_ms = Some(latency_ms);
            self.latency_samples += 1;
            self.latency_sum_ms += u128::from(latency_ms);
            self.max_exchange_latency_ms = self.max_exchange_latency_ms.max(latency_ms);
        }
    }

    pub(crate) fn commit_effects(&mut self, effects: EventEffects) {
        for signal in effects.new_signals {
            self.signals.push_front(signal);
        }
        while self.signals.len() > MAX_SIGNAL_HISTORY {
            self.signals.pop_back();
        }

        for order in effects.new_orders {
            self.orders.push_front(order);
        }
        while self.orders.len() > MAX_ORDER_HISTORY {
            self.orders.pop_back();
        }

        for updated in effects.updated_orders {
            if let Some(order) = self
                .orders
                .iter_mut()
                .find(|order| order.client_order_id == updated.client_order_id)
            {
                *order = updated;
            }
        }
    }

    fn ensure_market(&mut self, key: &InstrumentKey, symbol: String) -> &mut MarketRow {
        let mut needs_sort = false;

        match self.markets.get_mut(key) {
            Some(row) => {
                if row.symbol != symbol {
                    row.symbol = symbol;
                    needs_sort = true;
                }
            }
            None => {
                self.markets
                    .insert(key.clone(), MarketRow::new(key, symbol));
                self.market_order.push(key.clone());
                needs_sort = true;
            }
        }

        if needs_sort {
            self.rebuild_market_order();
        }

        self.markets
            .get_mut(key)
            .expect("market must exist after ensure")
    }

    fn rebuild_market_order(&mut self) {
        self.market_order = self
            .active_market_keys
            .iter()
            .cloned()
            .filter(|key| self.markets.contains_key(key))
            .collect();

        self.market_order.sort_by(|left, right| {
            let left_symbol = self
                .markets
                .get(left)
                .map(|row| row.symbol.to_ascii_uppercase())
                .unwrap_or_default();
            let right_symbol = self
                .markets
                .get(right)
                .map(|row| row.symbol.to_ascii_uppercase())
                .unwrap_or_default();
            left_symbol.cmp(&right_symbol)
        });
    }

    fn average_exchange_latency_ms(&self) -> Option<u64> {
        (self.latency_samples > 0)
            .then(|| (self.latency_sum_ms / u128::from(self.latency_samples)) as u64)
    }

    fn update_window(&mut self, key: &InstrumentKey, price_text: &str, timestamp_ms: u64) {
        let Ok(price) = price_text.parse::<f64>() else {
            return;
        };

        let cutoff_ms = timestamp_ms.saturating_sub(self.window_duration_ms());
        let window = self.windows.entry(key.clone()).or_default();
        window.push(
            WindowPoint {
                timestamp_ms,
                price,
                display: price_text.to_string(),
            },
            cutoff_ms,
        );

        if let Some(row) = self.markets.get_mut(key) {
            row.window_low_price = window.low().unwrap_or("--").to_string();
            row.window_high_price = window.high().unwrap_or("--").to_string();
        }
    }

    fn window_duration_ms(&self) -> u64 {
        (self.window_hours * 60.0 * 60.0 * 1000.0).round() as u64
    }

    fn reset_runtime_market_data(&mut self) {
        self.last_message_at = None;
        self.last_exchange_timestamp_ms = None;
        self.last_exchange_latency_ms = None;
        self.latency_samples = 0;
        self.latency_sum_ms = 0;
        self.max_exchange_latency_ms = 0;

        for row in self.markets.values_mut() {
            row.prev_last_trade_price = "--".to_string();
            row.last_trade_price = "--".to_string();
            row.open_interest = "--".to_string();
            row.window_low_price = "--".to_string();
            row.window_high_price = "--".to_string();
            row.best_bid_price = "--".to_string();
            row.best_bid_size = "--".to_string();
            row.best_bid_size_base = "--".to_string();
            row.best_bid_notional_samples.clear();
            row.best_ask_price = "--".to_string();
            row.best_ask_size = "--".to_string();
            row.best_ask_size_base = "--".to_string();
            row.best_ask_notional_samples.clear();
            row.current_funding_rate = "--".to_string();
        }

        self.reset_windows_only();
    }

    fn reset_windows_only(&mut self) {
        self.windows.clear();
        for row in self.markets.values_mut() {
            row.window_low_price = "--".to_string();
            row.window_high_price = "--".to_string();
        }
    }

    pub fn set_stream_control(&mut self, control: StreamControlState) {
        let was_enabled = self.stream_control.any_enabled();
        let is_enabled = control.any_enabled();
        if is_enabled && !was_enabled {
            self.reset_runtime_market_data();
        }
        for exchange in [LIGHTER_EXCHANGE, GATE_EXCHANGE] {
            if self.stream_control.get(exchange) && !control.get(exchange) {
                self.clear_exchange_runtime_data(exchange);
            }
        }
        self.stream_control = control;
    }

    fn clear_exchange_runtime_data(&mut self, exchange: &str) {
        self.markets.retain(|key, _| key.exchange != exchange);
        self.windows.retain(|key, _| key.exchange != exchange);
        self.active_market_keys
            .retain(|key| key.exchange != exchange);
        self.market_order.retain(|key| key.exchange != exchange);
    }

    pub fn set_window_hours(&mut self, window_hours: f64) {
        self.window_hours = window_hours;
        self.reset_windows_only();
    }

    pub fn set_signal_thresholds(
        &mut self,
        price_jump_threshold_pct: f64,
        open_interest_threshold: f64,
        spread_threshold_pct: f64,
        max_hold_minutes: f64,
        sim_order_quote_size: f64,
        sim_take_profit_pct: f64,
        sim_take_profit_ratio_pct: f64,
        sim_take_profit_pct_2: f64,
        sim_take_profit_ratio_pct_2: f64,
        sim_stop_loss_pct: f64,
    ) {
        self.price_jump_threshold_pct = price_jump_threshold_pct;
        self.open_interest_threshold = open_interest_threshold;
        self.spread_threshold_pct = spread_threshold_pct;
        self.max_hold_minutes = max_hold_minutes;
        self.sim_order_quote_size = sim_order_quote_size;
        self.sim_take_profit_pct = sim_take_profit_pct;
        self.sim_take_profit_ratio_pct = sim_take_profit_ratio_pct;
        self.sim_take_profit_pct_2 = sim_take_profit_pct_2;
        self.sim_take_profit_ratio_pct_2 = sim_take_profit_ratio_pct_2;
        self.sim_stop_loss_pct = sim_stop_loss_pct;
    }

    pub fn set_live_trading_enabled(&mut self, enabled: bool) {
        self.live_trading_enabled = enabled;
    }

    pub fn set_live_risk_controls(
        &mut self,
        live_trading_allow_opens: bool,
        live_open_cooldown_seconds: f64,
        live_max_order_quote_notional: f64,
        live_max_symbol_exposure_quote_notional: f64,
    ) {
        self.live_trading_allow_opens = live_trading_allow_opens;
        self.live_open_cooldown_seconds = live_open_cooldown_seconds;
        self.live_max_order_quote_notional = live_max_order_quote_notional;
        self.live_max_symbol_exposure_quote_notional = live_max_symbol_exposure_quote_notional;
    }

    pub fn set_gate_live_trading_configured(&mut self, configured: bool) {
        self.gate_live_trading_configured = configured;
    }

    pub fn set_lighter_live_trading_configured(&mut self, configured: bool) {
        self.lighter_live_trading_configured = configured;
    }

    fn max_hold_duration_ms(&self) -> Option<u64> {
        (self.max_hold_minutes > 0.0)
            .then(|| (self.max_hold_minutes * 60.0 * 1000.0).round() as u64)
    }

    pub fn snapshot(&self, latency_metrics: RuntimeLatencySnapshot) -> SnapshotResponse {
        let markets = self
            .market_order
            .iter()
            .filter_map(|key| self.markets.get(key))
            .map(MarketSnapshot::from)
            .collect::<Vec<_>>();
        let signals = self.signals.iter().cloned().collect::<Vec<_>>();
        let orders = self.orders.iter().cloned().collect::<Vec<_>>();

        let mut connections = self
            .connection_states
            .iter()
            .map(|(name, status)| ConnectionSnapshot {
                name: name.clone(),
                status: status.label(),
            })
            .collect::<Vec<_>>();
        connections.sort_by(|left, right| left.name.cmp(&right.name));

        SnapshotResponse {
            generated_at_ms: unix_now_ms(),
            status: StatusSnapshot {
                stream_enabled: self.stream_control.any_enabled(),
                stream_control: self.stream_control.clone(),
                window_hours: self.window_hours,
                price_jump_threshold_pct: self.price_jump_threshold_pct,
                open_interest_threshold: self.open_interest_threshold,
                spread_threshold_pct: self.spread_threshold_pct,
                max_hold_minutes: self.max_hold_minutes,
                sim_order_quote_size: self.sim_order_quote_size,
                sim_take_profit_pct: self.sim_take_profit_pct,
                sim_take_profit_ratio_pct: self.sim_take_profit_ratio_pct,
                sim_take_profit_pct_2: self.sim_take_profit_pct_2,
                sim_take_profit_ratio_pct_2: self.sim_take_profit_ratio_pct_2,
                sim_stop_loss_pct: self.sim_stop_loss_pct,
                live_trading_enabled: self.live_trading_enabled,
                live_trading_allow_opens: self.live_trading_allow_opens,
                live_open_cooldown_seconds: self.live_open_cooldown_seconds,
                live_max_order_quote_notional: self.live_max_order_quote_notional,
                live_max_symbol_exposure_quote_notional: self
                    .live_max_symbol_exposure_quote_notional,
                gate_live_trading_configured: self.gate_live_trading_configured,
                lighter_live_trading_configured: self.lighter_live_trading_configured,
                active_orders: self
                    .orders
                    .iter()
                    .filter(|order| order.status == OrderStatus::Open)
                    .count(),
                latency_metrics,
                live_connections: self
                    .connection_states
                    .values()
                    .filter(|status| matches!(status, ConnectionStatus::Live { .. }))
                    .count(),
                total_connections: self.connection_states.len(),
                market_count: self.market_order.len(),
                last_message_age_ms: self
                    .last_message_at
                    .map(|instant| instant.elapsed().as_millis() as u64),
                last_exchange_timestamp_ms: self.last_exchange_timestamp_ms,
                last_exchange_latency_ms: self.last_exchange_latency_ms,
                avg_exchange_latency_ms: self.average_exchange_latency_ms(),
                max_exchange_latency_ms: (self.latency_samples > 0)
                    .then_some(self.max_exchange_latency_ms),
                last_rest_sync_age_ms: self
                    .last_rest_sync_at
                    .map(|instant| instant.elapsed().as_millis() as u64),
                last_rest_sync_count: self.last_rest_sync_count,
                last_rest_sync_status: self.last_rest_sync_status.clone(),
                connections,
            },
            markets,
            signals,
            orders,
        }
    }

    fn has_open_order(&self, key: &InstrumentKey) -> bool {
        self.orders.iter().any(|order| {
            order.status == OrderStatus::Open
                && order.exchange == key.exchange
                && order.instrument == key.instrument
        })
    }

    fn active_order_indices(&self, key: &InstrumentKey) -> Vec<usize> {
        self.orders
            .iter()
            .enumerate()
            .filter_map(|(index, order)| {
                (order.status == OrderStatus::Open
                    && order.exchange == key.exchange
                    && order.instrument == key.instrument)
                    .then_some(index)
            })
            .collect()
    }

    fn maybe_close_active_orders(
        &mut self,
        key: &InstrumentKey,
        event_timestamp_ms: u64,
    ) -> Vec<SimulatedOrderRecord> {
        let row = match self.markets.get(key) {
            Some(row) => row,
            None => return Vec::new(),
        };
        let (Ok(best_bid), Ok(best_ask)) = (
            row.best_bid_price.parse::<f64>(),
            row.best_ask_price.parse::<f64>(),
        ) else {
            return Vec::new();
        };
        let max_hold_duration_ms = self.max_hold_duration_ms();
        let mut closed_orders = Vec::new();

        for order_index in self.active_order_indices(key) {
            let Some(order) = self.orders.get(order_index).cloned() else {
                continue;
            };
            let take_profit_price = (order.take_profit_price.as_str() != "--")
                .then(|| order.take_profit_price.parse::<f64>().ok())
                .flatten();
            let stop_loss_price = (order.stop_loss_price.as_str() != "--")
                .then(|| order.stop_loss_price.parse::<f64>().ok())
                .flatten();
            let max_hold_reached = max_hold_duration_ms.is_some_and(|threshold_ms| {
                event_timestamp_ms.saturating_sub(order.opened_at_ms) >= threshold_ms
            });

            if max_hold_reached {
                let mut closed = order.clone();
                closed.status = OrderStatus::Closed;
                closed.closed_at_ms = Some(event_timestamp_ms);
                closed.exit_price = Some(match closed.direction {
                    SignalDirection::Bullish => format_decimal(best_bid),
                    SignalDirection::Bearish => format_decimal(best_ask),
                });
                closed.exit_reason = Some("max_hold_time".to_string());
                closed_orders.push(closed);
                continue;
            }

            if let Some(stop_loss_price) = stop_loss_price {
                let stop_loss_hit = match order.direction {
                    SignalDirection::Bullish => best_bid <= stop_loss_price,
                    SignalDirection::Bearish => best_ask >= stop_loss_price,
                };

                if stop_loss_hit {
                    let mut closed = order.clone();
                    closed.status = OrderStatus::Closed;
                    closed.closed_at_ms = Some(event_timestamp_ms);
                    closed.exit_price = Some(closed.stop_loss_price.clone());
                    closed.exit_reason = Some("stop_loss".to_string());
                    closed_orders.push(closed);
                    continue;
                }
            }

            if let Some(take_profit_price) = take_profit_price {
                let take_profit_hit = match order.direction {
                    SignalDirection::Bullish => best_bid >= take_profit_price,
                    SignalDirection::Bearish => best_ask <= take_profit_price,
                };

                if take_profit_hit {
                    let mut closed = order.clone();
                    closed.status = OrderStatus::Closed;
                    closed.closed_at_ms = Some(event_timestamp_ms);
                    closed.exit_price = Some(closed.take_profit_price.clone());
                    closed.exit_reason = Some("take_profit".to_string());
                    closed_orders.push(closed);
                }
            }
        }

        closed_orders
    }

    fn evaluate_signal_candidate(
        &self,
        key: &InstrumentKey,
        event_timestamp_ms: u64,
    ) -> Option<SignalRecord> {
        if !self.active_market_keys.contains(key) {
            return None;
        }

        let row = self.markets.get(key)?;
        let window = self.windows.get(key)?;
        if [
            &row.prev_last_trade_price,
            &row.last_trade_price,
            &row.window_low_price,
            &row.window_high_price,
            &row.open_interest,
            &row.best_bid_price,
            &row.best_ask_price,
        ]
        .iter()
        .any(|value| value.as_str() == "--")
        {
            return None;
        }

        let prev_last = row.prev_last_trade_price.parse::<f64>().ok()?;
        let last = row.last_trade_price.parse::<f64>().ok()?;
        let window_low = row.window_low_price.parse::<f64>().ok()?;
        let window_high = row.window_high_price.parse::<f64>().ok()?;
        let open_interest = row.open_interest.parse::<f64>().ok()?;
        let best_bid = row.best_bid_price.parse::<f64>().ok()?;
        let best_ask = row.best_ask_price.parse::<f64>().ok()?;

        if window_low <= 0.0 || window_high <= 0.0 || best_bid <= 0.0 || best_ask <= 0.0 {
            return None;
        }

        let mid_price = (best_bid + best_ask) / 2.0;
        if mid_price <= 0.0 {
            return None;
        }

        let spread_pct = ((best_ask - best_bid) / mid_price) * 100.0;

        let last_jump_pct = ((last - window_low) / window_low) * 100.0;
        let prev_jump_pct = ((prev_last - window_low) / window_low) * 100.0;
        let last_drop_pct = ((window_high - last) / window_high) * 100.0;
        let prev_drop_pct = ((window_high - prev_last) / window_high) * 100.0;
        let last_is_window_high = row.last_trade_price == row.window_high_price;
        let last_is_window_low = row.last_trade_price == row.window_low_price;
        let oi_ok = open_interest > self.open_interest_threshold;
        let spread_ok = spread_pct <= self.spread_threshold_pct;

        let (direction, move_pct, reference_timestamp_ms) = if last_jump_pct
            >= self.price_jump_threshold_pct
            && prev_jump_pct < self.price_jump_threshold_pct
            && last_is_window_high
            && oi_ok
            && spread_ok
        {
            (
                SignalDirection::Bullish,
                last_jump_pct,
                window.low_point()?.timestamp_ms,
            )
        } else if last_drop_pct >= self.price_jump_threshold_pct
            && prev_drop_pct < self.price_jump_threshold_pct
            && last_is_window_low
            && oi_ok
            && spread_ok
        {
            (
                SignalDirection::Bearish,
                -last_drop_pct,
                window.high_point()?.timestamp_ms,
            )
        } else {
            return None;
        };

        Some(SignalRecord {
            id: None,
            created_at_ms: event_timestamp_ms,
            time_to_threshold_ms: Some(event_timestamp_ms.saturating_sub(reference_timestamp_ms)),
            exchange: row.exchange.clone(),
            instrument: row.instrument.clone(),
            symbol: row.symbol.clone(),
            direction: direction.clone(),
            prev_last_trade_price: row.prev_last_trade_price.clone(),
            last_trade_price: row.last_trade_price.clone(),
            window_low_price: row.window_low_price.clone(),
            window_high_price: row.window_high_price.clone(),
            move_pct,
            open_interest: row.open_interest.clone(),
            price_jump_threshold_pct: self.price_jump_threshold_pct,
            open_interest_threshold: self.open_interest_threshold,
            spread_threshold_pct: self.spread_threshold_pct,
        })
    }

    fn build_orders_from_signal(&self, signal: &SignalRecord) -> Option<Vec<SimulatedOrderRecord>> {
        let key = InstrumentKey {
            exchange: signal.exchange.clone(),
            instrument: signal.instrument.clone(),
        };
        let row = self.markets.get(&key)?;
        let min_base = row.min_base_amount.parse::<f64>().ok()?;
        if min_base <= 0.0 {
            return None;
        }

        let (entry_price, effective_half_book_notional) = match signal.direction {
            SignalDirection::Bullish => (
                row.best_ask_price.parse::<f64>().ok()?,
                weighted_average_recent(&row.best_ask_notional_samples)? / 2.0,
            ),
            SignalDirection::Bearish => (
                row.best_bid_price.parse::<f64>().ok()?,
                weighted_average_recent(&row.best_bid_notional_samples)? / 2.0,
            ),
        };

        if !(entry_price > 0.0 && effective_half_book_notional > 0.0) {
            return None;
        }

        if effective_half_book_notional + 1e-9 < self.sim_order_quote_size {
            return None;
        }

        let effective_half_book_size = effective_half_book_notional / entry_price;
        let target_units = floor_units(self.sim_order_quote_size / entry_price, min_base)?;
        let max_book_units = max_units_strict_below(effective_half_book_size, min_base)?;
        let quantity_units = target_units.min(max_book_units);
        if quantity_units < 1.0 {
            return None;
        }

        let quantity_base = quantity_units * min_base;
        if !(quantity_base > 0.0 && quantity_base < effective_half_book_size) {
            return None;
        }

        let quote_notional = quantity_base * entry_price;
        if !(quote_notional > 0.0 && quote_notional <= self.sim_order_quote_size + 1e-9) {
            return None;
        }

        let stop_loss_price = if self.sim_stop_loss_pct > 0.0 {
            match signal.direction {
                SignalDirection::Bullish => {
                    format_decimal(entry_price * (1.0 - self.sim_stop_loss_pct / 100.0))
                }
                SignalDirection::Bearish => {
                    format_decimal(entry_price * (1.0 + self.sim_stop_loss_pct / 100.0))
                }
            }
        } else {
            "--".to_string()
        };

        let total_units = quantity_units.round() as u64;
        let tp1_units = if self.sim_take_profit_pct > 0.0 && self.sim_take_profit_ratio_pct > 0.0 {
            ((total_units as f64) * self.sim_take_profit_ratio_pct / 100.0).floor() as u64
        } else {
            0
        };
        let remaining_after_tp1 = total_units.saturating_sub(tp1_units);
        let tp2_units =
            if self.sim_take_profit_pct_2 > 0.0 && self.sim_take_profit_ratio_pct_2 > 0.0 {
                ((total_units as f64) * self.sim_take_profit_ratio_pct_2 / 100.0)
                    .floor()
                    .min(remaining_after_tp1 as f64) as u64
            } else {
                0
            };
        let runner_units = total_units.saturating_sub(tp1_units + tp2_units);

        let mut orders = Vec::with_capacity(3);
        let build_leg = |leg_suffix: &str, leg_units: u64, take_profit_pct: Option<f64>| {
            (leg_units > 0).then(|| {
                let take_profit_price = take_profit_pct
                    .filter(|pct| *pct > 0.0)
                    .map(|pct| match signal.direction {
                        SignalDirection::Bullish => {
                            format_decimal(entry_price * (1.0 + pct / 100.0))
                        }
                        SignalDirection::Bearish => {
                            format_decimal(entry_price * (1.0 - pct / 100.0))
                        }
                    })
                    .unwrap_or_else(|| "--".to_string());
                let leg_quantity_base = leg_units as f64 * min_base;
                SimulatedOrderRecord {
                    id: None,
                    client_order_id: format!(
                        "sim-{}-{}-{}-{}-{}",
                        signal.created_at_ms,
                        signal.instrument,
                        signal.direction.as_str(),
                        leg_suffix,
                        unix_now_ns()
                    ),
                    opened_at_ms: signal.created_at_ms,
                    closed_at_ms: None,
                    exchange: signal.exchange.clone(),
                    instrument: signal.instrument.clone(),
                    symbol: signal.symbol.clone(),
                    direction: signal.direction.clone(),
                    status: OrderStatus::Open,
                    entry_price: format_decimal(entry_price),
                    quantity_base: format_decimal(leg_quantity_base),
                    quote_notional: leg_quantity_base * entry_price,
                    take_profit_price,
                    stop_loss_price: stop_loss_price.clone(),
                    exit_price: None,
                    exit_reason: None,
                }
            })
        };

        if let Some(order) = build_leg("tp1", tp1_units, Some(self.sim_take_profit_pct)) {
            orders.push(order);
        }
        if let Some(order) = build_leg("tp2", tp2_units, Some(self.sim_take_profit_pct_2)) {
            orders.push(order);
        }
        if let Some(order) = build_leg("runner", runner_units, None) {
            orders.push(order);
        }

        (!orders.is_empty()).then_some(orders)
    }

    fn evaluate_signal_and_order(
        &mut self,
        key: &InstrumentKey,
        event_timestamp_ms: u64,
    ) -> Option<(SignalRecord, Vec<SimulatedOrderRecord>)> {
        if self.has_open_order(key) {
            return None;
        }

        let signal = self.evaluate_signal_candidate(key, event_timestamp_ms)?;
        let orders = self.build_orders_from_signal(&signal)?;

        Some((signal, orders))
    }
}

impl From<&MarketRow> for MarketSnapshot {
    fn from(row: &MarketRow) -> Self {
        Self {
            exchange: row.exchange.clone(),
            instrument: row.instrument.clone(),
            symbol: row.symbol.clone(),
            prev_last_trade_price: row.prev_last_trade_price.clone(),
            last_trade_price: row.last_trade_price.clone(),
            open_interest: row.open_interest.clone(),
            window_low_price: row.window_low_price.clone(),
            window_high_price: row.window_high_price.clone(),
            best_bid_price: row.best_bid_price.clone(),
            best_bid_size: row.best_bid_size.clone(),
            best_ask_price: row.best_ask_price.clone(),
            best_ask_size: row.best_ask_size.clone(),
            current_funding_rate: row.current_funding_rate.clone(),
            precision: row.precision.clone(),
            min_base_amount: row.min_base_amount.clone(),
            min_quote_amount: row.min_quote_amount.clone(),
            order_quote_limit: row.order_quote_limit.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{MarketStatsUpdate, MetadataUpdate};
    use std::{collections::HashSet, time::Instant};

    fn test_key() -> InstrumentKey {
        InstrumentKey::lighter(42)
    }

    fn test_config() -> PersistedConfig {
        PersistedConfig {
            window_hours: 1.0,
            price_jump_threshold_pct: 2.0,
            open_interest_threshold: 500.0,
            spread_threshold_pct: 0.2,
            max_hold_minutes: 0.0,
            sim_order_quote_size: 100.0,
            sim_take_profit_pct: 0.5,
            sim_take_profit_ratio_pct: 100.0,
            sim_take_profit_pct_2: 0.0,
            sim_take_profit_ratio_pct_2: 0.0,
            sim_stop_loss_pct: 0.0,
            live_trading_enabled: false,
            live_trading_allow_opens: true,
            live_open_cooldown_seconds: 0.0,
            live_max_order_quote_notional: 0.0,
            live_max_symbol_exposure_quote_notional: 0.0,
        }
    }

    fn test_timing(timestamp_ms: u64) -> FeedTiming {
        FeedTiming {
            received_at: Instant::now(),
            exchange_timestamp_ms: Some(timestamp_ms),
            exchange_latency_ms: Some(0),
        }
    }

    fn metadata_event(key: &InstrumentKey) -> ServiceEvent {
        ServiceEvent::Metadata(MetadataBatch {
            exchange: key.exchange.clone(),
            updates: vec![MetadataUpdate {
                key: key.clone(),
                price_decimals: 2,
                size_decimals: 1,
                min_base_amount: "0.1".to_string(),
                min_quote_amount: "10".to_string(),
                order_quote_limit: "1000".to_string(),
                quantity_multiplier: 1.0,
            }],
            active_market_keys: HashSet::from([key.clone()]),
            fetched_at: Instant::now(),
            reason: "test".to_string(),
        })
    }

    fn bbo_event(key: &InstrumentKey, bid: f64, ask: f64, timestamp_ms: u64) -> ServiceEvent {
        ServiceEvent::Bbo(BboUpdate {
            key: key.clone(),
            symbol: "TEST-PERP".to_string(),
            best_bid_price: format_decimal(bid),
            best_bid_size: "10".to_string(),
            best_bid_size_base: "10".to_string(),
            best_bid_notional: 1_000.0,
            best_ask_price: format_decimal(ask),
            best_ask_size: "10".to_string(),
            best_ask_size_base: "10".to_string(),
            best_ask_notional: 1_000.0,
            timing: test_timing(timestamp_ms),
        })
    }

    fn stats_event(
        key: &InstrumentKey,
        price: f64,
        open_interest: f64,
        timestamp_ms: u64,
    ) -> ServiceEvent {
        ServiceEvent::MarketStats(MarketStatsBatch {
            updates: vec![MarketStatsUpdate {
                key: key.clone(),
                symbol: "TEST-PERP".to_string(),
                current_funding_rate: "0.01".to_string(),
                last_trade_price: format_decimal(price),
                open_interest: format_decimal(open_interest),
            }],
            timing: test_timing(timestamp_ms),
        })
    }

    fn apply_and_commit(app: &mut App, event: ServiceEvent) {
        let effects = app.apply_event(event);
        app.commit_effects(effects);
    }

    fn bullish_signal_effects(app: &mut App, key: &InstrumentKey) -> EventEffects {
        apply_and_commit(app, metadata_event(key));
        apply_and_commit(app, bbo_event(key, 102.4, 102.5, 1_000));
        apply_and_commit(app, stats_event(key, 100.0, 1_000.0, 1_000));
        apply_and_commit(app, stats_event(key, 101.0, 1_000.0, 2_000));

        app.apply_event(stats_event(key, 102.5, 1_000.0, 3_000))
    }

    fn open_bullish_order(app: &mut App, key: &InstrumentKey) {
        let effects = bullish_signal_effects(app, key);
        assert_eq!(effects.new_signals.len(), 1);
        assert_eq!(effects.new_orders.len(), 1);
        app.commit_effects(effects);
    }

    #[test]
    fn creates_signal_and_order_when_bullish_threshold_is_crossed() {
        let key = test_key();
        let mut app = App::new(test_config(), Vec::new(), Vec::new());

        open_bullish_order(&mut app, &key);

        assert_eq!(app.signals.len(), 1);
        assert_eq!(app.orders.len(), 1);

        let signal = app.signals.front().expect("signal should exist");
        assert_eq!(signal.direction, SignalDirection::Bullish);
        assert_eq!(signal.window_low_price, "100");
        assert_eq!(signal.window_high_price, "102.5");
        assert_eq!(signal.time_to_threshold_ms, Some(2_000));

        let order = app.orders.front().expect("order should exist");
        assert_eq!(order.status, OrderStatus::Open);
        assert_eq!(order.direction, SignalDirection::Bullish);
        assert_eq!(order.entry_price, "102.5");
        assert_eq!(order.take_profit_price, "103.0125");
        assert_eq!(order.stop_loss_price, "--");
    }

    #[test]
    fn closes_open_order_when_take_profit_is_hit() {
        let key = test_key();
        let mut app = App::new(test_config(), Vec::new(), Vec::new());
        open_bullish_order(&mut app, &key);

        let effects = app.apply_event(bbo_event(&key, 103.1, 103.2, 4_000));
        assert_eq!(effects.updated_orders.len(), 1);
        assert!(effects.new_signals.is_empty());
        assert!(effects.new_orders.is_empty());
        let updated_order = effects.updated_orders[0].clone();
        app.commit_effects(effects);

        assert_eq!(updated_order.status, OrderStatus::Closed);
        assert_eq!(updated_order.exit_reason.as_deref(), Some("take_profit"));
        assert_eq!(updated_order.exit_price.as_deref(), Some("103.0125"));

        let stored_order = app.orders.front().expect("stored order should exist");
        assert_eq!(stored_order.status, OrderStatus::Closed);
        assert_eq!(stored_order.exit_reason.as_deref(), Some("take_profit"));
    }

    #[test]
    fn closes_open_order_when_max_hold_time_is_reached() {
        let key = test_key();
        let mut config = test_config();
        config.max_hold_minutes = 0.01;
        config.sim_take_profit_pct = 0.0;
        let mut app = App::new(config, Vec::new(), Vec::new());
        open_bullish_order(&mut app, &key);

        let effects = app.apply_event(bbo_event(&key, 102.3, 102.4, 4_000));
        assert_eq!(effects.updated_orders.len(), 1);
        let updated_order = effects.updated_orders[0].clone();
        app.commit_effects(effects);

        assert_eq!(updated_order.status, OrderStatus::Closed);
        assert_eq!(updated_order.exit_reason.as_deref(), Some("max_hold_time"));
        assert_eq!(updated_order.exit_price.as_deref(), Some("102.3"));
    }

    #[test]
    fn splits_order_into_take_profit_legs_and_runner() {
        let key = test_key();
        let mut config = test_config();
        config.sim_take_profit_ratio_pct = 50.0;
        config.sim_take_profit_pct_2 = 1.0;
        config.sim_take_profit_ratio_pct_2 = 25.0;
        let mut app = App::new(config, Vec::new(), Vec::new());

        let effects = bullish_signal_effects(&mut app, &key);
        assert_eq!(effects.new_signals.len(), 1);
        assert_eq!(effects.new_orders.len(), 3);

        let take_profit_prices = effects
            .new_orders
            .iter()
            .map(|order| order.take_profit_price.clone())
            .collect::<Vec<_>>();
        assert_eq!(take_profit_prices, vec!["103.0125", "103.525", "--"]);

        let quantities = effects
            .new_orders
            .iter()
            .map(|order| order.quantity_base.clone())
            .collect::<Vec<_>>();
        assert_eq!(quantities, vec!["0.4", "0.2", "0.3"]);
    }

    #[test]
    fn closes_only_first_take_profit_leg_when_first_target_is_hit() {
        let key = test_key();
        let mut config = test_config();
        config.sim_take_profit_ratio_pct = 50.0;
        config.sim_take_profit_pct_2 = 1.0;
        config.sim_take_profit_ratio_pct_2 = 25.0;
        let mut app = App::new(config, Vec::new(), Vec::new());

        let effects = bullish_signal_effects(&mut app, &key);
        app.commit_effects(effects);

        let effects = app.apply_event(bbo_event(&key, 103.1, 103.2, 4_000));
        assert_eq!(effects.updated_orders.len(), 1);
        assert!(effects.new_signals.is_empty());
        assert!(effects.new_orders.is_empty());
        app.commit_effects(effects);

        let closed_orders = app
            .orders
            .iter()
            .filter(|order| order.status == OrderStatus::Closed)
            .collect::<Vec<_>>();
        assert_eq!(closed_orders.len(), 1);
        assert_eq!(closed_orders[0].take_profit_price, "103.0125");
        assert_eq!(closed_orders[0].exit_reason.as_deref(), Some("take_profit"));

        let open_orders = app
            .orders
            .iter()
            .filter(|order| order.status == OrderStatus::Open)
            .map(|order| order.take_profit_price.clone())
            .collect::<Vec<_>>();
        let mut open_orders_sorted = open_orders;
        open_orders_sorted.sort();
        assert_eq!(open_orders_sorted, vec!["--", "103.525"]);
    }

    #[test]
    fn snapshot_exposes_gate_live_trading_configuration_status() {
        let mut app = App::new(test_config(), Vec::new(), Vec::new());
        app.set_gate_live_trading_configured(true);
        app.set_lighter_live_trading_configured(true);
        app.set_live_trading_enabled(true);

        let snapshot = app.snapshot(RuntimeLatencySnapshot::default());

        assert!(snapshot.status.gate_live_trading_configured);
        assert!(snapshot.status.lighter_live_trading_configured);
        assert!(snapshot.status.live_trading_enabled);
        assert!(snapshot.status.live_trading_allow_opens);
    }

    #[test]
    #[ignore = "manual performance smoke test"]
    fn perf_smoke_event_processing_hot_path() {
        let key = test_key();
        let mut config = test_config();
        config.price_jump_threshold_pct = 99.0;
        config.open_interest_threshold = 0.0;
        config.spread_threshold_pct = 5.0;
        config.sim_take_profit_pct = 0.0;
        config.sim_stop_loss_pct = 0.0;
        let mut app = App::new(config, Vec::new(), Vec::new());

        apply_and_commit(&mut app, metadata_event(&key));
        apply_and_commit(&mut app, bbo_event(&key, 100.0, 100.1, 1_000));
        apply_and_commit(&mut app, stats_event(&key, 100.0, 1_000.0, 1_000));

        let iterations = 20_000_u64;
        let started_at = Instant::now();

        for index in 0..iterations {
            let price = 100.0 + (index % 25) as f64 * 0.02;
            let stats_ts = 2_000 + index * 2;
            let bbo_ts = stats_ts + 1;

            apply_and_commit(&mut app, stats_event(&key, price, 1_000.0, stats_ts));
            apply_and_commit(
                &mut app,
                bbo_event(&key, price - 0.05, price + 0.05, bbo_ts),
            );
        }

        let elapsed = started_at.elapsed();
        let processed_events = iterations * 2;
        let ns_per_event = elapsed.as_nanos() / u128::from(processed_events);
        eprintln!(
            "processed {processed_events} events in {:?} ({ns_per_event} ns/event)",
            elapsed
        );

        let max_allowed = if cfg!(debug_assertions) {
            std::time::Duration::from_secs(10)
        } else {
            std::time::Duration::from_secs(2)
        };
        assert!(
            elapsed < max_allowed,
            "event hot path regression: {:?} for {} events",
            elapsed,
            processed_events
        );
    }
}
