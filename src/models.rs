use crate::constants::{
    DEFAULT_LIVE_MAX_ORDER_QUOTE_NOTIONAL, DEFAULT_LIVE_MAX_SYMBOL_EXPOSURE_QUOTE_NOTIONAL,
    DEFAULT_LIVE_OPEN_COOLDOWN_SECONDS, DEFAULT_LIVE_TRADING_ALLOW_OPENS, DEFAULT_MAX_HOLD_MINUTES,
    DEFAULT_OPEN_INTEREST_THRESHOLD, DEFAULT_PRICE_JUMP_THRESHOLD_PCT,
    DEFAULT_SIM_ORDER_QUOTE_SIZE, DEFAULT_SIM_STOP_LOSS_PCT, DEFAULT_SIM_TAKE_PROFIT_PCT,
    DEFAULT_SIM_TAKE_PROFIT_PCT_2, DEFAULT_SIM_TAKE_PROFIT_RATIO_PCT,
    DEFAULT_SIM_TAKE_PROFIT_RATIO_PCT_2, DEFAULT_SPREAD_THRESHOLD_PCT, DEFAULT_WINDOW_HOURS,
    GATE_EXCHANGE, LATENCY_SAMPLE_WINDOW, LIGHTER_EXCHANGE,
};
use serde::{Deserialize, Deserializer, Serialize};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    time::Instant,
};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StreamControlState {
    pub lighter: bool,
    pub gate: bool,
}

impl StreamControlState {
    pub fn any_enabled(&self) -> bool {
        self.lighter || self.gate
    }

    pub fn get(&self, exchange: &str) -> bool {
        match exchange {
            LIGHTER_EXCHANGE => self.lighter,
            GATE_EXCHANGE => self.gate,
            _ => false,
        }
    }

    pub fn set(&mut self, exchange: &str, enabled: bool) {
        match exchange {
            LIGHTER_EXCHANGE => self.lighter = enabled,
            GATE_EXCHANGE => self.gate = enabled,
            _ => {}
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct InstrumentKey {
    pub exchange: String,
    pub instrument: String,
}

impl InstrumentKey {
    pub fn lighter(market_id: u64) -> Self {
        Self {
            exchange: LIGHTER_EXCHANGE.to_string(),
            instrument: market_id.to_string(),
        }
    }

    pub fn gate(contract: impl Into<String>) -> Self {
        Self {
            exchange: GATE_EXCHANGE.to_string(),
            instrument: contract.into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PersistedConfig {
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
}

impl Default for PersistedConfig {
    fn default() -> Self {
        Self {
            window_hours: DEFAULT_WINDOW_HOURS,
            price_jump_threshold_pct: DEFAULT_PRICE_JUMP_THRESHOLD_PCT,
            open_interest_threshold: DEFAULT_OPEN_INTEREST_THRESHOLD,
            spread_threshold_pct: DEFAULT_SPREAD_THRESHOLD_PCT,
            max_hold_minutes: DEFAULT_MAX_HOLD_MINUTES,
            sim_order_quote_size: DEFAULT_SIM_ORDER_QUOTE_SIZE,
            sim_take_profit_pct: DEFAULT_SIM_TAKE_PROFIT_PCT,
            sim_take_profit_ratio_pct: DEFAULT_SIM_TAKE_PROFIT_RATIO_PCT,
            sim_take_profit_pct_2: DEFAULT_SIM_TAKE_PROFIT_PCT_2,
            sim_take_profit_ratio_pct_2: DEFAULT_SIM_TAKE_PROFIT_RATIO_PCT_2,
            sim_stop_loss_pct: DEFAULT_SIM_STOP_LOSS_PCT,
            live_trading_enabled: false,
            live_trading_allow_opens: DEFAULT_LIVE_TRADING_ALLOW_OPENS,
            live_open_cooldown_seconds: DEFAULT_LIVE_OPEN_COOLDOWN_SECONDS,
            live_max_order_quote_notional: DEFAULT_LIVE_MAX_ORDER_QUOTE_NOTIONAL,
            live_max_symbol_exposure_quote_notional:
                DEFAULT_LIVE_MAX_SYMBOL_EXPOSURE_QUOTE_NOTIONAL,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SignalDirection {
    Bullish,
    Bearish,
}

impl SignalDirection {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Bullish => "bullish",
            Self::Bearish => "bearish",
        }
    }

    pub fn from_db(value: &str) -> Option<Self> {
        match value {
            "bullish" => Some(Self::Bullish),
            "bearish" => Some(Self::Bearish),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SignalRecord {
    pub id: Option<i64>,
    pub created_at_ms: u64,
    pub time_to_threshold_ms: Option<u64>,
    pub exchange: String,
    pub instrument: String,
    pub symbol: String,
    pub direction: SignalDirection,
    pub prev_last_trade_price: String,
    pub last_trade_price: String,
    pub window_low_price: String,
    pub window_high_price: String,
    pub move_pct: f64,
    pub open_interest: String,
    pub price_jump_threshold_pct: f64,
    pub open_interest_threshold: f64,
    pub spread_threshold_pct: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum OrderStatus {
    Open,
    Closed,
}

impl OrderStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Closed => "closed",
        }
    }

    pub fn from_db(value: &str) -> Option<Self> {
        match value {
            "open" => Some(Self::Open),
            "closed" => Some(Self::Closed),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SimulatedOrderRecord {
    pub id: Option<i64>,
    pub client_order_id: String,
    pub opened_at_ms: u64,
    pub closed_at_ms: Option<u64>,
    pub exchange: String,
    pub instrument: String,
    pub symbol: String,
    pub direction: SignalDirection,
    pub status: OrderStatus,
    pub entry_price: String,
    pub quantity_base: String,
    pub quote_notional: f64,
    pub take_profit_price: String,
    pub stop_loss_price: String,
    pub exit_price: Option<String>,
    pub exit_reason: Option<String>,
}

#[derive(Debug, Default, Clone)]
pub struct RollingLatency {
    pub samples: VecDeque<u64>,
}

impl RollingLatency {
    pub fn record(&mut self, value_us: u64) {
        self.samples.push_back(value_us);
        while self.samples.len() > LATENCY_SAMPLE_WINDOW {
            self.samples.pop_front();
        }
    }

    pub fn snapshot(&self) -> LatencySummary {
        if self.samples.is_empty() {
            return LatencySummary::default();
        }

        let mut values = self.samples.iter().copied().collect::<Vec<_>>();
        values.sort_unstable();

        let percentile = |ratio: f64| -> u64 {
            let index = ((values.len() - 1) as f64 * ratio).round() as usize;
            values[index]
        };

        let sum = values.iter().copied().map(u128::from).sum::<u128>();
        LatencySummary {
            samples: values.len(),
            latest_us: self.samples.back().copied(),
            avg_us: Some((sum / values.len() as u128) as u64),
            p50_us: Some(percentile(0.50)),
            p95_us: Some(percentile(0.95)),
            p99_us: Some(percentile(0.99)),
            max_us: values.last().copied(),
        }
    }
}

#[derive(Debug, Default, Clone, Serialize)]
pub struct LatencySummary {
    pub samples: usize,
    pub latest_us: Option<u64>,
    pub avg_us: Option<u64>,
    pub p50_us: Option<u64>,
    pub p95_us: Option<u64>,
    pub p99_us: Option<u64>,
    pub max_us: Option<u64>,
}

#[derive(Debug, Default, Clone)]
pub struct RuntimeLatencyMetrics {
    pub queue_delay_us: RollingLatency,
    pub processing_us: RollingLatency,
    pub persist_us: RollingLatency,
    pub end_to_end_us: RollingLatency,
    pub persistence_batch_size: RollingLatency,
    pub persistence_queue_depth: usize,
}

#[derive(Debug, Default, Clone, Serialize)]
pub struct RuntimeLatencySnapshot {
    pub queue_delay_us: LatencySummary,
    pub processing_us: LatencySummary,
    pub persist_us: LatencySummary,
    pub end_to_end_us: LatencySummary,
    pub persistence_batch_size: LatencySummary,
    pub persistence_queue_depth: usize,
}

impl RuntimeLatencyMetrics {
    pub fn snapshot(&self) -> RuntimeLatencySnapshot {
        RuntimeLatencySnapshot {
            queue_delay_us: self.queue_delay_us.snapshot(),
            processing_us: self.processing_us.snapshot(),
            persist_us: self.persist_us.snapshot(),
            end_to_end_us: self.end_to_end_us.snapshot(),
            persistence_batch_size: self.persistence_batch_size.snapshot(),
            persistence_queue_depth: self.persistence_queue_depth,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct OrderBookDetailsResponse {
    pub code: u16,
    pub order_book_details: Vec<RestMarketMetadata>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RestMarketMetadata {
    pub market_id: u64,
    pub market_type: String,
    pub status: String,
    pub price_decimals: u32,
    pub size_decimals: u32,
    pub min_base_amount: String,
    pub min_quote_amount: String,
    pub order_quote_limit: String,
}

#[derive(Debug, Serialize)]
pub struct SubscriptionRequest {
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub channel: String,
}

#[derive(Debug, Deserialize)]
pub struct MarketStatsEnvelope {
    pub market_stats: Option<HashMap<String, MarketStatsPayload>>,
    pub timestamp: Option<u64>,
    #[serde(rename = "type")]
    pub kind: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MarketStatsPayload {
    pub symbol: String,
    pub market_id: u64,
    pub current_funding_rate: String,
    pub last_trade_price: String,
    pub open_interest: String,
}

#[derive(Debug, Deserialize)]
pub struct TickerEnvelope {
    pub channel: String,
    pub ticker: Option<TickerPayload>,
    pub timestamp: Option<u64>,
    #[serde(rename = "type")]
    pub kind: String,
}

#[derive(Debug, Deserialize)]
pub struct TickerPayload {
    pub a: Level,
    pub b: Level,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GateContractMetadata {
    pub name: String,
    pub in_delisting: bool,
    pub order_price_round: String,
    #[serde(deserialize_with = "de_gate_f64")]
    pub order_size_min: f64,
    #[serde(default, deserialize_with = "de_gate_opt_f64")]
    pub order_size_max: Option<f64>,
    #[serde(default, deserialize_with = "de_gate_opt_f64")]
    pub market_order_size_max: Option<f64>,
    #[serde(default, deserialize_with = "de_gate_opt_f64")]
    pub market_order_slip_ratio: Option<f64>,
    #[serde(default)]
    pub enable_decimal: bool,
    pub quanto_multiplier: String,
}

fn de_gate_f64<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    match value {
        serde_json::Value::String(rendered) => {
            rendered.parse::<f64>().map_err(serde::de::Error::custom)
        }
        serde_json::Value::Number(number) => number
            .as_f64()
            .ok_or_else(|| serde::de::Error::custom("invalid numeric Gate field")),
        other => Err(serde::de::Error::custom(format!(
            "unexpected Gate numeric field type: {other}"
        ))),
    }
}

fn de_gate_opt_f64<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    value
        .map(|value| match value {
            serde_json::Value::String(rendered) => {
                rendered.parse::<f64>().map_err(serde::de::Error::custom)
            }
            serde_json::Value::Number(number) => number
                .as_f64()
                .ok_or_else(|| serde::de::Error::custom("invalid numeric Gate field")),
            other => Err(serde::de::Error::custom(format!(
                "unexpected Gate numeric field type: {other}"
            ))),
        })
        .transpose()
}

#[derive(Debug, Deserialize)]
pub struct GateTickersEnvelope {
    pub channel: String,
    pub event: String,
    pub result: Option<Vec<GateTickerPayload>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GateTickerPayload {
    pub contract: String,
    pub last: String,
    pub mark_price: String,
    pub funding_rate: String,
    pub total_size: String,
    pub t: u64,
}

#[derive(Debug, Deserialize)]
pub struct GateBookTickerEnvelope {
    pub channel: String,
    pub event: String,
    pub result: Option<GateBookTickerPayload>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GateBookTickerPayload {
    pub t: u64,
    pub s: String,
    pub b: String,
    #[serde(rename = "B")]
    pub bid_size: serde_json::Number,
    pub a: String,
    #[serde(rename = "A")]
    pub ask_size: serde_json::Number,
}

#[derive(Debug, Serialize)]
pub struct GateSubscriptionRequest<'a> {
    pub time: u64,
    pub channel: &'a str,
    pub event: &'a str,
    pub payload: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Level {
    pub price: String,
    pub size: String,
}

#[derive(Debug, Clone, Copy)]
pub struct FeedTiming {
    pub received_at: Instant,
    pub exchange_timestamp_ms: Option<u64>,
    pub exchange_latency_ms: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct MarketStatsUpdate {
    pub key: InstrumentKey,
    pub symbol: String,
    pub current_funding_rate: String,
    pub last_trade_price: String,
    pub open_interest: String,
}

#[derive(Debug, Clone)]
pub struct MarketStatsBatch {
    pub updates: Vec<MarketStatsUpdate>,
    pub timing: FeedTiming,
}

#[derive(Debug, Clone)]
pub struct BboUpdate {
    pub key: InstrumentKey,
    pub symbol: String,
    pub best_bid_price: String,
    pub best_bid_size: String,
    pub best_bid_size_base: String,
    pub best_bid_notional: f64,
    pub best_ask_price: String,
    pub best_ask_size: String,
    pub best_ask_size_base: String,
    pub best_ask_notional: f64,
    pub timing: FeedTiming,
}

#[derive(Debug, Clone)]
pub struct MetadataUpdate {
    pub key: InstrumentKey,
    pub price_decimals: u32,
    pub size_decimals: u32,
    pub min_base_amount: String,
    pub min_quote_amount: String,
    pub order_quote_limit: String,
    pub quantity_multiplier: f64,
}

#[derive(Debug, Clone)]
pub struct MetadataBatch {
    pub exchange: String,
    pub updates: Vec<MetadataUpdate>,
    pub active_market_keys: HashSet<InstrumentKey>,
    pub fetched_at: Instant,
    pub reason: String,
}

#[derive(Debug, Clone)]
pub enum ServiceEvent {
    StreamControl(StreamControlState),
    MarketStats(MarketStatsBatch),
    Bbo(BboUpdate),
    Metadata(MetadataBatch),
    Connection(ConnectionUpdate),
    RestError {
        message: String,
        occurred_at: Instant,
    },
}

#[derive(Debug, Clone)]
pub struct ConnectionUpdate {
    pub name: String,
    pub status: ConnectionStatus,
}

#[derive(Debug, Clone)]
pub enum ConnectionStatus {
    Idle,
    Connecting,
    Live { subscriptions: usize },
    Closed { reason: String },
}

impl ConnectionStatus {
    pub fn label(&self) -> String {
        match self {
            Self::Idle => "stopped".to_string(),
            Self::Connecting => "connecting".to_string(),
            Self::Live { subscriptions } => format!("live ({subscriptions})"),
            Self::Closed { reason } => format!("closed: {reason}"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum TickerCommand {
    Discover { market_id: u64, symbol: String },
    SetActiveMarkets { active_market_ids: HashSet<u64> },
}

#[derive(Debug, Clone)]
pub enum MetadataCommand {
    SyncAll { reason: String },
}

#[derive(Debug, Clone)]
pub enum ConnectionCommand {
    Subscribe { market_id: u64, symbol: String },
    Unsubscribe { market_id: u64 },
}

#[derive(Debug, Clone, Serialize)]
pub struct SnapshotResponse {
    pub generated_at_ms: u64,
    pub status: StatusSnapshot,
    pub markets: Vec<MarketSnapshot>,
    pub signals: Vec<SignalRecord>,
    pub orders: Vec<SimulatedOrderRecord>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StatusSnapshot {
    pub stream_enabled: bool,
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
    pub active_orders: usize,
    pub latency_metrics: RuntimeLatencySnapshot,
    pub live_connections: usize,
    pub total_connections: usize,
    pub market_count: usize,
    pub last_message_age_ms: Option<u64>,
    pub last_exchange_timestamp_ms: Option<u64>,
    pub last_exchange_latency_ms: Option<u64>,
    pub avg_exchange_latency_ms: Option<u64>,
    pub max_exchange_latency_ms: Option<u64>,
    pub last_rest_sync_age_ms: Option<u64>,
    pub last_rest_sync_count: usize,
    pub last_rest_sync_status: String,
    pub connections: Vec<ConnectionSnapshot>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConnectionSnapshot {
    pub name: String,
    pub status: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct MarketSnapshot {
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
    pub best_ask_price: String,
    pub best_ask_size: String,
    pub current_funding_rate: String,
    pub precision: String,
    pub min_base_amount: String,
    pub min_quote_amount: String,
    pub order_quote_limit: String,
}
