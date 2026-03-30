use crate::{
    app::MarketRow,
    constants::{GATE_EXCHANGE, LIGHTER_EXCHANGE, WEB_BIND_ADDR},
    database::{Database, PersistenceTask},
    gate::GateContractRuntime,
    gate_execution::{
        GateExecutionContract, GateMarketOrderAction, GateOrderClient, GatePlaceOrderRequest,
        base_quantity_to_contract_size, quote_notional_to_contract_size,
    },
    models::*,
    support::{SharedMetrics, SharedReadModel, SharedState, sync_read_model},
    util::{unix_now_ms, unix_now_ns},
};
use anyhow::{Context, Result};
use axum::{
    Json, Router,
    extract::{Query, State, rejection::JsonRejection},
    http::{StatusCode, header},
    response::{Html, IntoResponse},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::{
    net::TcpListener,
    sync::{mpsc, watch},
};
use tracing::info;

#[derive(Clone)]
pub(crate) struct WebState {
    pub(crate) state: SharedState,
    pub(crate) metrics: SharedMetrics,
    pub(crate) read_model: SharedReadModel,
    pub(crate) db: Database,
    pub(crate) event_tx: mpsc::Sender<ServiceEvent>,
    pub(crate) persistence_tx: mpsc::UnboundedSender<PersistenceTask>,
    pub(crate) metadata_cmd_tx: mpsc::Sender<MetadataCommand>,
    pub(crate) stream_control_tx: watch::Sender<StreamControlState>,
    pub(crate) gate_contracts_rx: watch::Receiver<Vec<GateContractRuntime>>,
    pub(crate) gate_order_client: Option<Arc<GateOrderClient>>,
}

pub(crate) async fn run_http_server(
    web_state: WebState,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<()> {
    #[cfg(debug_assertions)]
    let web_state_clone = web_state.clone();

    let router = Router::new()
        .route("/", get(index_handler))
        .route("/redesign", get(redesign_handler))
        .route("/api/snapshot", get(snapshot_handler))
        .route("/api/pnl", get(pnl_handler))
        .route("/api/stream", post(stream_control_handler))
        .route("/api/config", post(config_handler))
        .with_state(web_state);

    #[cfg(debug_assertions)]
    let router = router
        .route(
            "/api/dev/gate/market-order",
            post(gate_market_order_handler),
        )
        .route(
            "/api/dev/stress-persistence",
            post(stress_persistence_handler),
        )
        .with_state(web_state_clone);

    let listener = TcpListener::bind(WEB_BIND_ADDR)
        .await
        .with_context(|| format!("failed to bind {WEB_BIND_ADDR}"))?;

    let local_addr = listener
        .local_addr()
        .context("failed to read bound web address")?;
    info!(url = %format!("http://{local_addr}"), "web ui ready");

    axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            let _ = shutdown_rx.changed().await;
        })
        .await
        .context("web server error")?;

    Ok(())
}

#[cfg(debug_assertions)]
fn build_benchmark_persistence_pair(
    base_ts_ms: u64,
    sequence: u64,
) -> (SignalRecord, SimulatedOrderRecord) {
    let created_at_ms = base_ts_ms.saturating_add(sequence);
    let symbol = "__BENCH__/USDC".to_string();
    let client_order_id = format!("bench-{base_ts_ms}-{sequence}-{}", unix_now_ns());

    let signal = SignalRecord {
        id: None,
        created_at_ms,
        time_to_threshold_ms: Some(250),
        exchange: LIGHTER_EXCHANGE.to_string(),
        instrument: "9999999".to_string(),
        symbol: symbol.clone(),
        direction: SignalDirection::Bullish,
        prev_last_trade_price: "100.00".to_string(),
        last_trade_price: "100.25".to_string(),
        window_low_price: "100.00".to_string(),
        window_high_price: "100.25".to_string(),
        move_pct: 0.25,
        open_interest: "1000000".to_string(),
        price_jump_threshold_pct: 0.2,
        open_interest_threshold: 500000.0,
        spread_threshold_pct: 0.05,
    };

    let order = SimulatedOrderRecord {
        id: None,
        client_order_id,
        opened_at_ms: created_at_ms,
        closed_at_ms: None,
        exchange: signal.exchange.clone(),
        instrument: signal.instrument.clone(),
        symbol,
        direction: SignalDirection::Bullish,
        status: OrderStatus::Open,
        entry_price: "100.10".to_string(),
        quantity_base: "0.998".to_string(),
        quote_notional: 99.8998,
        take_profit_price: "100.20".to_string(),
        stop_loss_price: "--".to_string(),
        exit_price: None,
        exit_reason: None,
    };

    (signal, order)
}

fn control_room_html_response() -> impl IntoResponse {
    (
        [(header::CACHE_CONTROL, "no-store, max-age=0")],
        Html(include_str!("../static/redesign.html")),
    )
}

async fn index_handler() -> impl IntoResponse {
    control_room_html_response()
}

async fn redesign_handler() -> impl IntoResponse {
    control_room_html_response()
}

async fn snapshot_handler(State(web_state): State<WebState>) -> Json<SnapshotResponse> {
    let snapshot = web_state.read_model.read().await.clone();
    Json(snapshot)
}

#[derive(Debug, Deserialize)]
struct StreamControlRequest {
    exchange: String,
    enabled: bool,
}

#[derive(Debug, Serialize)]
struct StreamControlResponse {
    exchange: String,
    enabled: bool,
    control: StreamControlState,
}

async fn stream_control_handler(
    State(web_state): State<WebState>,
    request: Result<Json<StreamControlRequest>, JsonRejection>,
) -> impl IntoResponse {
    let Json(request) = match request {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApiError { error: e.body_text() }),
            )
                .into_response();
        }
    };
    let normalized_exchange = request.exchange.to_ascii_lowercase();
    if normalized_exchange != LIGHTER_EXCHANGE && normalized_exchange != GATE_EXCHANGE {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiError {
                error: format!("exchange must be one of: {LIGHTER_EXCHANGE}, {GATE_EXCHANGE}"),
            }),
        )
            .into_response();
    }

    let next_control = {
        let mut app = web_state.state.write().await;
        let mut control = app.stream_control.clone();
        control.set(&normalized_exchange, request.enabled);
        app.set_stream_control(control.clone());
        control
    };
    sync_read_model(&web_state.state, &web_state.metrics, &web_state.read_model).await;

    let _ = web_state.stream_control_tx.send(next_control.clone());
    let _ = web_state
        .event_tx
        .send(ServiceEvent::StreamControl(next_control.clone()))
        .await;

    if normalized_exchange == LIGHTER_EXCHANGE && request.enabled {
        let _ = web_state
            .metadata_cmd_tx
            .send(MetadataCommand::SyncAll {
                reason: "manual start".to_string(),
            })
            .await;
    }

    Json(StreamControlResponse {
        exchange: normalized_exchange,
        enabled: request.enabled,
        control: next_control,
    })
    .into_response()
}

#[derive(Debug, Deserialize)]
struct ConfigUpdateRequest {
    window_hours: Option<f64>,
    price_jump_threshold_pct: Option<f64>,
    open_interest_threshold: Option<f64>,
    spread_threshold_pct: Option<f64>,
    max_hold_minutes: Option<f64>,
    sim_order_quote_size: Option<f64>,
    sim_take_profit_pct: Option<f64>,
    sim_take_profit_ratio_pct: Option<f64>,
    sim_take_profit_pct_2: Option<f64>,
    sim_take_profit_ratio_pct_2: Option<f64>,
    sim_stop_loss_pct: Option<f64>,
    live_trading_enabled: Option<bool>,
    live_trading_allow_opens: Option<bool>,
    live_open_cooldown_seconds: Option<f64>,
    live_max_order_quote_notional: Option<f64>,
    live_max_symbol_exposure_quote_notional: Option<f64>,
}

#[derive(Debug, Serialize)]
struct ConfigUpdateResponse {
    window_hours: f64,
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
    live_trading_enabled: bool,
    live_trading_allow_opens: bool,
    live_open_cooldown_seconds: f64,
    live_max_order_quote_notional: f64,
    live_max_symbol_exposure_quote_notional: f64,
}

#[cfg(debug_assertions)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum GateMarketOrderActionRequest {
    OpenLong,
    OpenShort,
    CloseLong,
    CloseShort,
}

#[cfg(debug_assertions)]
impl From<GateMarketOrderActionRequest> for GateMarketOrderAction {
    fn from(value: GateMarketOrderActionRequest) -> Self {
        match value {
            GateMarketOrderActionRequest::OpenLong => GateMarketOrderAction::OpenLong,
            GateMarketOrderActionRequest::OpenShort => GateMarketOrderAction::OpenShort,
            GateMarketOrderActionRequest::CloseLong => GateMarketOrderAction::CloseLong,
            GateMarketOrderActionRequest::CloseShort => GateMarketOrderAction::CloseShort,
        }
    }
}

#[cfg(debug_assertions)]
#[derive(Debug, Deserialize)]
struct GateMarketOrderApiRequest {
    contract: String,
    action: GateMarketOrderActionRequest,
    quote_notional: Option<f64>,
    base_quantity: Option<f64>,
    market_order_slip_ratio: Option<f64>,
    text: Option<String>,
    execute: Option<bool>,
}

#[cfg(debug_assertions)]
#[derive(Debug, Serialize)]
struct GateMarketOrderApiResponse {
    contract: String,
    execute: bool,
    absolute_size: String,
    request_body: serde_json::Value,
    gate_response: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
struct ApiError {
    error: String,
}

#[derive(Debug, Deserialize)]
struct PnlQuery {
    range: Option<String>,
    exchange: Option<String>,
    direction: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct PnlResponse {
    range: String,
    exchange_filter: Option<String>,
    direction_filter: Option<String>,
    summary: PnlSummary,
    unrealized: UnrealizedSummary,
    equity_curve: Vec<PnlPoint>,
    daily_pnl: Vec<DailyPnlPoint>,
    exchange_breakdown: Vec<PnlBreakdown>,
    symbol_breakdown: Vec<PnlBreakdown>,
    recent_orders: Vec<PnlOrderRow>,
    open_orders: Vec<OpenPnlRow>,
}

#[derive(Debug, Clone, Serialize)]
struct PnlSummary {
    net_pnl: f64,
    gross_profit: f64,
    gross_loss: f64,
    closed_trades: usize,
    wins: usize,
    losses: usize,
    win_rate_pct: f64,
    avg_pnl: f64,
    avg_return_pct: f64,
    avg_hold_minutes: f64,
    max_drawdown: f64,
    best_trade: f64,
    worst_trade: f64,
    max_win_streak: usize,
    max_loss_streak: usize,
}

#[derive(Debug, Clone, Serialize)]
struct PnlPoint {
    timestamp_ms: u64,
    equity: f64,
}

#[derive(Debug, Clone, Serialize)]
struct DailyPnlPoint {
    day_start_ms: u64,
    pnl: f64,
    trades: usize,
}

#[derive(Debug, Clone, Serialize)]
struct PnlBreakdown {
    key: String,
    trades: usize,
    wins: usize,
    losses: usize,
    win_rate_pct: f64,
    net_pnl: f64,
    avg_pnl: f64,
}

#[derive(Debug, Clone, Serialize)]
struct PnlOrderRow {
    closed_at_ms: u64,
    exchange: String,
    symbol: String,
    direction: SignalDirection,
    quote_notional: f64,
    pnl: f64,
    return_pct: f64,
    hold_minutes: f64,
    exit_reason: String,
}

#[derive(Debug, Clone, Serialize)]
struct UnrealizedSummary {
    open_orders: usize,
    net_pnl: f64,
    avg_return_pct: f64,
}

#[derive(Debug, Clone, Serialize)]
struct OpenPnlRow {
    opened_at_ms: u64,
    exchange: String,
    symbol: String,
    direction: SignalDirection,
    quote_notional: f64,
    mark_price: f64,
    pnl: f64,
    return_pct: f64,
}

#[derive(Debug, Default, Clone)]
struct BreakdownAccumulator {
    trades: usize,
    wins: usize,
    losses: usize,
    net_pnl: f64,
}

fn pnl_range_since_ms(range: &str, now_ms: u64) -> Option<u64> {
    match range {
        "24h" => Some(now_ms.saturating_sub(24 * 60 * 60 * 1000)),
        "7d" => Some(now_ms.saturating_sub(7 * 24 * 60 * 60 * 1000)),
        _ => None,
    }
}

fn parse_order_pnl(order: &SimulatedOrderRecord) -> Option<(f64, f64, f64)> {
    let entry_price = order.entry_price.parse::<f64>().ok()?;
    let exit_price = order.exit_price.as_ref()?.parse::<f64>().ok()?;
    let quantity_base = order.quantity_base.parse::<f64>().ok()?;
    if !(entry_price > 0.0 && exit_price > 0.0 && quantity_base > 0.0 && order.quote_notional > 0.0)
    {
        return None;
    }

    let pnl = match order.direction {
        SignalDirection::Bullish => (exit_price - entry_price) * quantity_base,
        SignalDirection::Bearish => (entry_price - exit_price) * quantity_base,
    };
    let return_pct = (pnl / order.quote_notional) * 100.0;
    let hold_minutes = order
        .closed_at_ms
        .map(|closed_at_ms| closed_at_ms.saturating_sub(order.opened_at_ms) as f64 / 60_000.0)
        .unwrap_or(0.0);

    Some((pnl, return_pct, hold_minutes))
}

fn normalize_filter_value(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty() && *value != "all")
        .map(|value| value.to_ascii_lowercase())
}

fn order_matches_filters(
    order: &SimulatedOrderRecord,
    exchange_filter: Option<&str>,
    direction_filter: Option<&str>,
) -> bool {
    if let Some(exchange) = exchange_filter {
        if !order.exchange.eq_ignore_ascii_case(exchange) {
            return false;
        }
    }
    if let Some(direction) = direction_filter {
        if order.direction.as_str() != direction {
            return false;
        }
    }
    true
}

fn parse_open_order_pnl(order: &SimulatedOrderRecord, row: &MarketRow) -> Option<(f64, f64, f64)> {
    let entry_price = order.entry_price.parse::<f64>().ok()?;
    let quantity_base = order.quantity_base.parse::<f64>().ok()?;
    if !(entry_price > 0.0 && quantity_base > 0.0 && order.quote_notional > 0.0) {
        return None;
    }

    let mark_price = match order.direction {
        SignalDirection::Bullish => row.best_bid_price.parse::<f64>().ok()?,
        SignalDirection::Bearish => row.best_ask_price.parse::<f64>().ok()?,
    };
    if mark_price <= 0.0 {
        return None;
    }

    let pnl = match order.direction {
        SignalDirection::Bullish => (mark_price - entry_price) * quantity_base,
        SignalDirection::Bearish => (entry_price - mark_price) * quantity_base,
    };
    let return_pct = (pnl / order.quote_notional) * 100.0;
    Some((mark_price, pnl, return_pct))
}

fn build_pnl_response(
    range: &str,
    exchange_filter: Option<&str>,
    direction_filter: Option<&str>,
    orders: &[SimulatedOrderRecord],
    open_orders: &[OpenPnlRow],
) -> PnlResponse {
    let mut equity_curve = Vec::with_capacity(orders.len());
    let mut daily_buckets = HashMap::<u64, (f64, usize)>::new();
    let mut exchange_buckets = HashMap::<String, BreakdownAccumulator>::new();
    let mut symbol_buckets = HashMap::<String, BreakdownAccumulator>::new();
    let mut recent_orders = Vec::new();

    let mut equity = 0.0_f64;
    let mut gross_profit = 0.0_f64;
    let mut gross_loss = 0.0_f64;
    let mut wins = 0usize;
    let mut losses = 0usize;
    let mut trade_count = 0usize;
    let mut hold_minutes_sum = 0.0_f64;
    let mut return_pct_sum = 0.0_f64;
    let mut best_trade = f64::NEG_INFINITY;
    let mut worst_trade = f64::INFINITY;
    let mut equity_peak = 0.0_f64;
    let mut max_drawdown = 0.0_f64;
    let mut win_streak = 0usize;
    let mut loss_streak = 0usize;
    let mut max_win_streak = 0usize;
    let mut max_loss_streak = 0usize;

    for order in orders {
        let Some(closed_at_ms) = order.closed_at_ms else {
            continue;
        };
        let Some((pnl, return_pct, hold_minutes)) = parse_order_pnl(order) else {
            continue;
        };

        trade_count += 1;
        equity += pnl;
        equity_curve.push(PnlPoint {
            timestamp_ms: closed_at_ms,
            equity,
        });

        let day_start_ms = (closed_at_ms / 86_400_000) * 86_400_000;
        let daily_entry = daily_buckets.entry(day_start_ms).or_insert((0.0, 0));
        daily_entry.0 += pnl;
        daily_entry.1 += 1;

        let exchange_entry = exchange_buckets.entry(order.exchange.clone()).or_default();
        exchange_entry.trades += 1;
        exchange_entry.net_pnl += pnl;

        let symbol_entry = symbol_buckets.entry(order.symbol.clone()).or_default();
        symbol_entry.trades += 1;
        symbol_entry.net_pnl += pnl;

        if pnl > 0.0 {
            wins += 1;
            gross_profit += pnl;
            exchange_entry.wins += 1;
            symbol_entry.wins += 1;
            win_streak += 1;
            loss_streak = 0;
            max_win_streak = max_win_streak.max(win_streak);
        } else if pnl < 0.0 {
            losses += 1;
            gross_loss += pnl;
            exchange_entry.losses += 1;
            symbol_entry.losses += 1;
            loss_streak += 1;
            win_streak = 0;
            max_loss_streak = max_loss_streak.max(loss_streak);
        } else {
            win_streak = 0;
            loss_streak = 0;
        }

        hold_minutes_sum += hold_minutes;
        return_pct_sum += return_pct;
        best_trade = best_trade.max(pnl);
        worst_trade = worst_trade.min(pnl);
        equity_peak = equity_peak.max(equity);
        max_drawdown = max_drawdown.max(equity_peak - equity);

        recent_orders.push(PnlOrderRow {
            closed_at_ms,
            exchange: order.exchange.clone(),
            symbol: order.symbol.clone(),
            direction: order.direction.clone(),
            quote_notional: order.quote_notional,
            pnl,
            return_pct,
            hold_minutes,
            exit_reason: order
                .exit_reason
                .clone()
                .unwrap_or_else(|| "--".to_string()),
        });
    }

    recent_orders.sort_by(|left, right| right.closed_at_ms.cmp(&left.closed_at_ms));
    recent_orders.truncate(20);

    let win_rate_pct = if trade_count > 0 {
        (wins as f64 / trade_count as f64) * 100.0
    } else {
        0.0
    };

    let mut daily_pnl = daily_buckets
        .into_iter()
        .map(|(day_start_ms, (pnl, trades))| DailyPnlPoint {
            day_start_ms,
            pnl,
            trades,
        })
        .collect::<Vec<_>>();
    daily_pnl.sort_by(|left, right| left.day_start_ms.cmp(&right.day_start_ms));

    let mut exchange_breakdown = exchange_buckets
        .into_iter()
        .map(|(key, value)| PnlBreakdown {
            key,
            trades: value.trades,
            wins: value.wins,
            losses: value.losses,
            win_rate_pct: if value.trades > 0 {
                value.wins as f64 * 100.0 / value.trades as f64
            } else {
                0.0
            },
            net_pnl: value.net_pnl,
            avg_pnl: if value.trades > 0 {
                value.net_pnl / value.trades as f64
            } else {
                0.0
            },
        })
        .collect::<Vec<_>>();
    exchange_breakdown.sort_by(|left, right| {
        right
            .net_pnl
            .partial_cmp(&left.net_pnl)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let mut symbol_breakdown = symbol_buckets
        .into_iter()
        .map(|(key, value)| PnlBreakdown {
            key,
            trades: value.trades,
            wins: value.wins,
            losses: value.losses,
            win_rate_pct: if value.trades > 0 {
                value.wins as f64 * 100.0 / value.trades as f64
            } else {
                0.0
            },
            net_pnl: value.net_pnl,
            avg_pnl: if value.trades > 0 {
                value.net_pnl / value.trades as f64
            } else {
                0.0
            },
        })
        .collect::<Vec<_>>();
    symbol_breakdown.sort_by(|left, right| {
        right
            .net_pnl
            .partial_cmp(&left.net_pnl)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    symbol_breakdown.truncate(20);

    let unrealized_open_count = open_orders.len();
    let unrealized_net_pnl = open_orders.iter().map(|order| order.pnl).sum::<f64>();
    let unrealized_avg_return_pct = if unrealized_open_count > 0 {
        open_orders
            .iter()
            .map(|order| order.return_pct)
            .sum::<f64>()
            / unrealized_open_count as f64
    } else {
        0.0
    };

    PnlResponse {
        range: range.to_string(),
        exchange_filter: exchange_filter.map(str::to_string),
        direction_filter: direction_filter.map(str::to_string),
        summary: PnlSummary {
            net_pnl: equity,
            gross_profit,
            gross_loss,
            closed_trades: trade_count,
            wins,
            losses,
            win_rate_pct,
            avg_pnl: if trade_count > 0 {
                equity / trade_count as f64
            } else {
                0.0
            },
            avg_return_pct: if trade_count > 0 {
                return_pct_sum / trade_count as f64
            } else {
                0.0
            },
            avg_hold_minutes: if trade_count > 0 {
                hold_minutes_sum / trade_count as f64
            } else {
                0.0
            },
            max_drawdown,
            best_trade: if trade_count > 0 { best_trade } else { 0.0 },
            worst_trade: if trade_count > 0 { worst_trade } else { 0.0 },
            max_win_streak,
            max_loss_streak,
        },
        unrealized: UnrealizedSummary {
            open_orders: unrealized_open_count,
            net_pnl: unrealized_net_pnl,
            avg_return_pct: unrealized_avg_return_pct,
        },
        equity_curve,
        daily_pnl,
        exchange_breakdown,
        symbol_breakdown,
        recent_orders,
        open_orders: open_orders.to_vec(),
    }
}

#[cfg(debug_assertions)]
#[derive(Debug, Deserialize)]
struct StressPersistenceRequest {
    count: usize,
}

#[cfg(debug_assertions)]
#[derive(Debug, Serialize)]
struct StressPersistenceResponse {
    enqueued: usize,
}

fn check_nonneg(name: &str, value: f64) -> Result<f64, (StatusCode, ApiError)> {
    if !(value.is_finite() && value >= 0.0) {
        return Err((
            StatusCode::BAD_REQUEST,
            ApiError { error: format!("{name} must be a non-negative number") },
        ));
    }
    Ok((value * 100.0).round() / 100.0)
}

fn check_positive(name: &str, value: f64) -> Result<f64, (StatusCode, ApiError)> {
    if !(value.is_finite() && value > 0.0) {
        return Err((
            StatusCode::BAD_REQUEST,
            ApiError { error: format!("{name} must be a positive number") },
        ));
    }
    Ok((value * 100.0).round() / 100.0)
}

fn check_pct(name: &str, value: f64) -> Result<f64, (StatusCode, ApiError)> {
    if !(value.is_finite() && (0.0..=100.0).contains(&value)) {
        return Err((
            StatusCode::BAD_REQUEST,
            ApiError { error: format!("{name} must be between 0 and 100") },
        ));
    }
    Ok((value * 100.0).round() / 100.0)
}

async fn config_handler(
    State(web_state): State<WebState>,
    Json(request): Json<ConfigUpdateRequest>,
) -> impl IntoResponse {
    macro_rules! validate {
        ($result:expr) => {
            match $result {
                Ok(v) => v,
                Err((status, error)) => return (status, Json(error)).into_response(),
            }
        };
    }
    let (
        stream_enabled,
        current_window_hours,
        current_price_jump_threshold_pct,
        current_open_interest_threshold,
        current_spread_threshold_pct,
        current_max_hold_minutes,
        current_sim_order_quote_size,
        current_sim_take_profit_pct,
        current_sim_take_profit_ratio_pct,
        current_sim_take_profit_pct_2,
        current_sim_take_profit_ratio_pct_2,
        current_sim_stop_loss_pct,
        current_live_trading_enabled,
        current_live_trading_allow_opens,
        current_live_open_cooldown_seconds,
        current_live_max_order_quote_notional,
        current_live_max_symbol_exposure_quote_notional,
        gate_live_trading_configured,
        lighter_live_trading_configured,
    ) = {
        let app = web_state.state.read().await;
        (
            app.stream_control.any_enabled(),
            app.window_hours,
            app.price_jump_threshold_pct,
            app.open_interest_threshold,
            app.spread_threshold_pct,
            app.max_hold_minutes,
            app.sim_order_quote_size,
            app.sim_take_profit_pct,
            app.sim_take_profit_ratio_pct,
            app.sim_take_profit_pct_2,
            app.sim_take_profit_ratio_pct_2,
            app.sim_stop_loss_pct,
            app.live_trading_enabled,
            app.live_trading_allow_opens,
            app.live_open_cooldown_seconds,
            app.live_max_order_quote_notional,
            app.live_max_symbol_exposure_quote_notional,
            app.gate_live_trading_configured,
            app.lighter_live_trading_configured,
        )
    };

    if request.window_hours.is_none()
        && request.price_jump_threshold_pct.is_none()
        && request.open_interest_threshold.is_none()
        && request.spread_threshold_pct.is_none()
        && request.max_hold_minutes.is_none()
        && request.sim_order_quote_size.is_none()
        && request.sim_take_profit_pct.is_none()
        && request.sim_take_profit_ratio_pct.is_none()
        && request.sim_take_profit_pct_2.is_none()
        && request.sim_take_profit_ratio_pct_2.is_none()
        && request.sim_stop_loss_pct.is_none()
        && request.live_trading_enabled.is_none()
        && request.live_trading_allow_opens.is_none()
        && request.live_open_cooldown_seconds.is_none()
        && request.live_max_order_quote_notional.is_none()
        && request.live_max_symbol_exposure_quote_notional.is_none()
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiError {
                error: "at least one config field must be provided".to_string(),
            }),
        )
            .into_response();
    }

    let mut next_window_hours = current_window_hours;
    if let Some(v) = request.window_hours {
        next_window_hours = validate!(check_positive("window_hours", v));
        if stream_enabled {
            return (
                StatusCode::CONFLICT,
                Json(ApiError {
                    error: "window_hours can only be changed while the stream is stopped"
                        .to_string(),
                }),
            )
                .into_response();
        }
    }

    let mut next_price_jump_threshold_pct = current_price_jump_threshold_pct;
    if let Some(v) = request.price_jump_threshold_pct {
        next_price_jump_threshold_pct = validate!(check_nonneg("price_jump_threshold_pct", v));
    }

    let mut next_open_interest_threshold = current_open_interest_threshold;
    if let Some(v) = request.open_interest_threshold {
        next_open_interest_threshold = validate!(check_nonneg("open_interest_threshold", v));
    }

    let mut next_spread_threshold_pct = current_spread_threshold_pct;
    if let Some(v) = request.spread_threshold_pct {
        next_spread_threshold_pct = validate!(check_nonneg("spread_threshold_pct", v));
    }

    let mut next_max_hold_minutes = current_max_hold_minutes;
    if let Some(v) = request.max_hold_minutes {
        next_max_hold_minutes = validate!(check_nonneg("max_hold_minutes", v));
    }

    let mut next_sim_order_quote_size = current_sim_order_quote_size;
    if let Some(v) = request.sim_order_quote_size {
        next_sim_order_quote_size = validate!(check_positive("sim_order_quote_size", v));
    }

    let mut next_sim_take_profit_pct = current_sim_take_profit_pct;
    if let Some(v) = request.sim_take_profit_pct {
        next_sim_take_profit_pct = validate!(check_nonneg("sim_take_profit_pct", v));
    }

    let mut next_sim_take_profit_ratio_pct = current_sim_take_profit_ratio_pct;
    if let Some(v) = request.sim_take_profit_ratio_pct {
        next_sim_take_profit_ratio_pct = validate!(check_pct("sim_take_profit_ratio_pct", v));
    }

    let mut next_sim_take_profit_pct_2 = current_sim_take_profit_pct_2;
    if let Some(v) = request.sim_take_profit_pct_2 {
        next_sim_take_profit_pct_2 = validate!(check_nonneg("sim_take_profit_pct_2", v));
    }

    let mut next_sim_take_profit_ratio_pct_2 = current_sim_take_profit_ratio_pct_2;
    if let Some(v) = request.sim_take_profit_ratio_pct_2 {
        next_sim_take_profit_ratio_pct_2 = validate!(check_pct("sim_take_profit_ratio_pct_2", v));
    }

    let mut next_sim_stop_loss_pct = current_sim_stop_loss_pct;
    if let Some(v) = request.sim_stop_loss_pct {
        next_sim_stop_loss_pct = validate!(check_nonneg("sim_stop_loss_pct", v));
    }

    let mut next_live_trading_enabled = current_live_trading_enabled;
    if let Some(live_trading_enabled) = request.live_trading_enabled {
        if live_trading_enabled && !gate_live_trading_configured && !lighter_live_trading_configured
        {
            return (
                StatusCode::CONFLICT,
                Json(ApiError {
                    error: "No live trading backend is configured in the environment. Set Gate and/or Lighter credentials first.".to_string(),
                }),
            )
                .into_response();
        }
        next_live_trading_enabled = live_trading_enabled;
    }

    let mut next_live_trading_allow_opens = current_live_trading_allow_opens;
    if let Some(live_trading_allow_opens) = request.live_trading_allow_opens {
        next_live_trading_allow_opens = live_trading_allow_opens;
    }

    let mut next_live_open_cooldown_seconds = current_live_open_cooldown_seconds;
    if let Some(v) = request.live_open_cooldown_seconds {
        next_live_open_cooldown_seconds = validate!(check_nonneg("live_open_cooldown_seconds", v));
    }

    let mut next_live_max_order_quote_notional = current_live_max_order_quote_notional;
    if let Some(v) = request.live_max_order_quote_notional {
        next_live_max_order_quote_notional = validate!(check_nonneg("live_max_order_quote_notional", v));
    }

    let mut next_live_max_symbol_exposure_quote_notional =
        current_live_max_symbol_exposure_quote_notional;
    if let Some(v) = request.live_max_symbol_exposure_quote_notional {
        next_live_max_symbol_exposure_quote_notional =
            validate!(check_nonneg("live_max_symbol_exposure_quote_notional", v));
    }

    let effective_tp1_ratio = if next_sim_take_profit_pct > 0.0 {
        next_sim_take_profit_ratio_pct
    } else {
        0.0
    };
    let effective_tp2_ratio = if next_sim_take_profit_pct_2 > 0.0 {
        next_sim_take_profit_ratio_pct_2
    } else {
        0.0
    };
    if effective_tp1_ratio + effective_tp2_ratio > 100.0 + 1e-9 {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiError {
                error: "take profit ratios must sum to 100 or less".to_string(),
            }),
        )
            .into_response();
    }

    if let Some(window_hours) = request.window_hours {
        let _ = window_hours;
        if let Err(error) = web_state.db.save_window_hours(next_window_hours) {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError {
                    error: format!("failed to persist window_hours: {error}"),
                }),
            )
                .into_response();
        }
    }

    let thresholds_changed = request.price_jump_threshold_pct.is_some()
        || request.open_interest_threshold.is_some()
        || request.spread_threshold_pct.is_some()
        || request.max_hold_minutes.is_some()
        || request.sim_order_quote_size.is_some()
        || request.sim_take_profit_pct.is_some()
        || request.sim_take_profit_ratio_pct.is_some()
        || request.sim_take_profit_pct_2.is_some()
        || request.sim_take_profit_ratio_pct_2.is_some()
        || request.sim_stop_loss_pct.is_some()
        || request.live_trading_enabled.is_some()
        || request.live_trading_allow_opens.is_some()
        || request.live_open_cooldown_seconds.is_some()
        || request.live_max_order_quote_notional.is_some()
        || request.live_max_symbol_exposure_quote_notional.is_some();
    if thresholds_changed {
        if let Err(error) = web_state.db.save_signal_thresholds(
            next_price_jump_threshold_pct,
            next_open_interest_threshold,
            next_spread_threshold_pct,
            next_max_hold_minutes,
            next_sim_order_quote_size,
            next_sim_take_profit_pct,
            next_sim_take_profit_ratio_pct,
            next_sim_take_profit_pct_2,
            next_sim_take_profit_ratio_pct_2,
            next_sim_stop_loss_pct,
            next_live_trading_enabled,
            next_live_trading_allow_opens,
            next_live_open_cooldown_seconds,
            next_live_max_order_quote_notional,
            next_live_max_symbol_exposure_quote_notional,
        ) {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError {
                    error: format!("failed to persist thresholds: {error}"),
                }),
            )
                .into_response();
        }
    }

    {
        let mut app = web_state.state.write().await;
        if request.window_hours.is_some() {
            app.set_window_hours(next_window_hours);
        }
        if thresholds_changed {
            app.set_signal_thresholds(
                next_price_jump_threshold_pct,
                next_open_interest_threshold,
                next_spread_threshold_pct,
                next_max_hold_minutes,
                next_sim_order_quote_size,
                next_sim_take_profit_pct,
                next_sim_take_profit_ratio_pct,
                next_sim_take_profit_pct_2,
                next_sim_take_profit_ratio_pct_2,
                next_sim_stop_loss_pct,
            );
            app.set_live_trading_enabled(next_live_trading_enabled);
            app.set_live_risk_controls(
                next_live_trading_allow_opens,
                next_live_open_cooldown_seconds,
                next_live_max_order_quote_notional,
                next_live_max_symbol_exposure_quote_notional,
            );
        }
    }
    sync_read_model(&web_state.state, &web_state.metrics, &web_state.read_model).await;

    Json(ConfigUpdateResponse {
        window_hours: next_window_hours,
        price_jump_threshold_pct: next_price_jump_threshold_pct,
        open_interest_threshold: next_open_interest_threshold,
        spread_threshold_pct: next_spread_threshold_pct,
        max_hold_minutes: next_max_hold_minutes,
        sim_order_quote_size: next_sim_order_quote_size,
        sim_take_profit_pct: next_sim_take_profit_pct,
        sim_take_profit_ratio_pct: next_sim_take_profit_ratio_pct,
        sim_take_profit_pct_2: next_sim_take_profit_pct_2,
        sim_take_profit_ratio_pct_2: next_sim_take_profit_ratio_pct_2,
        sim_stop_loss_pct: next_sim_stop_loss_pct,
        live_trading_enabled: next_live_trading_enabled,
        live_trading_allow_opens: next_live_trading_allow_opens,
        live_open_cooldown_seconds: next_live_open_cooldown_seconds,
        live_max_order_quote_notional: next_live_max_order_quote_notional,
        live_max_symbol_exposure_quote_notional: next_live_max_symbol_exposure_quote_notional,
    })
    .into_response()
}

#[cfg(debug_assertions)]
async fn gate_market_order_handler(
    State(web_state): State<WebState>,
    Json(request): Json<GateMarketOrderApiRequest>,
) -> impl IntoResponse {
    let Some(client) = web_state.gate_order_client.clone() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ApiError {
                error: "Gate live trading is not configured. Set GATE_API_KEY and GATE_API_SECRET first.".to_string(),
            }),
        )
            .into_response();
    };

    let contract_runtime = match web_state
        .gate_contracts_rx
        .borrow()
        .iter()
        .find(|contract| contract.contract == request.contract)
        .cloned()
    {
        Some(contract) => contract,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(ApiError {
                    error: format!("unknown Gate contract: {}", request.contract),
                }),
            )
                .into_response();
        }
    };
    let execution_contract = GateExecutionContract::from(&contract_runtime);
    let action: GateMarketOrderAction = request.action.into();

    let absolute_size = match (request.base_quantity, request.quote_notional) {
        (Some(base_quantity), None) => {
            match base_quantity_to_contract_size(base_quantity, &execution_contract) {
                Ok(size) => size,
                Err(error) => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(ApiError {
                            error: error.to_string(),
                        }),
                    )
                        .into_response();
                }
            }
        }
        (None, Some(quote_notional)) => {
            let reference_price = {
                let app = web_state.state.read().await;
                let key = InstrumentKey::gate(request.contract.clone());
                let Some(row) = app.markets.get(&key) else {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(ApiError {
                            error: format!(
                                "missing live market row for Gate contract {}",
                                request.contract
                            ),
                        }),
                    )
                        .into_response();
                };
                let price_str = match action {
                    GateMarketOrderAction::OpenLong | GateMarketOrderAction::CloseShort => {
                        &row.best_ask_price
                    }
                    GateMarketOrderAction::OpenShort | GateMarketOrderAction::CloseLong => {
                        &row.best_bid_price
                    }
                };
                match price_str.parse::<f64>() {
                    Ok(price) if price > 0.0 => price,
                    _ => {
                        return (
                            StatusCode::BAD_REQUEST,
                            Json(ApiError {
                                error: format!(
                                    "missing live BBO price for Gate contract {}",
                                    request.contract
                                ),
                            }),
                        )
                            .into_response();
                    }
                }
            };

            match quote_notional_to_contract_size(
                quote_notional,
                reference_price,
                &execution_contract,
            ) {
                Ok(size) => size,
                Err(error) => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(ApiError {
                            error: error.to_string(),
                        }),
                    )
                        .into_response();
                }
            }
        }
        (Some(_), Some(_)) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApiError {
                    error: "provide either base_quantity or quote_notional, not both".to_string(),
                }),
            )
                .into_response();
        }
        (None, None) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApiError {
                    error: "provide base_quantity or quote_notional".to_string(),
                }),
            )
                .into_response();
        }
    };

    let place_request = GatePlaceOrderRequest {
        contract: request.contract.clone(),
        action,
        absolute_size: absolute_size.clone(),
        market_order_slip_ratio: request
            .market_order_slip_ratio
            .or(execution_contract.market_order_slip_ratio),
        text: request
            .text
            .unwrap_or_else(|| format!("t-manual-{}", unix_now_ns())),
    };
    let preview = match client.preview_market_order(place_request.clone()) {
        Ok(preview) => preview,
        Err(error) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApiError {
                    error: error.to_string(),
                }),
            )
                .into_response();
        }
    };
    let request_body = match serde_json::from_str::<serde_json::Value>(&preview.body_json) {
        Ok(body) => body,
        Err(error) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError {
                    error: format!("failed to decode Gate order preview: {error}"),
                }),
            )
                .into_response();
        }
    };

    let execute = request.execute.unwrap_or(false);
    let gate_response = if execute {
        match client.place_market_order(place_request).await {
            Ok(response) => Some(response),
            Err(error) => {
                return (
                    StatusCode::BAD_GATEWAY,
                    Json(ApiError {
                        error: error.to_string(),
                    }),
                )
                    .into_response();
            }
        }
    } else {
        None
    };

    Json(GateMarketOrderApiResponse {
        contract: request.contract,
        execute,
        absolute_size,
        request_body,
        gate_response,
    })
    .into_response()
}

async fn pnl_handler(
    State(web_state): State<WebState>,
    Query(query): Query<PnlQuery>,
) -> impl IntoResponse {
    let range = query.range.as_deref().unwrap_or("all");
    let normalized_range = match range {
        "24h" | "7d" | "all" => range,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApiError {
                    error: "range must be one of: 24h, 7d, all".to_string(),
                }),
            )
                .into_response();
        }
    };

    let exchange_filter = normalize_filter_value(query.exchange.as_deref());
    let direction_filter = normalize_filter_value(query.direction.as_deref());
    if let Some(exchange) = exchange_filter.as_deref() {
        if exchange != LIGHTER_EXCHANGE && exchange != GATE_EXCHANGE {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApiError {
                    error: "exchange must be one of: lighter, gate, all".to_string(),
                }),
            )
                .into_response();
        }
    }
    if let Some(direction) = direction_filter.as_deref() {
        if direction != "bullish" && direction != "bearish" {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApiError {
                    error: "direction must be one of: bullish, bearish, all".to_string(),
                }),
            )
                .into_response();
        }
    }

    let since_ms = pnl_range_since_ms(normalized_range, unix_now_ms());
    let orders = match web_state.db.load_closed_orders_since(
        since_ms,
        exchange_filter.as_deref(),
        direction_filter.as_deref(),
    ) {
        Ok(orders) => orders,
        Err(error) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError {
                    error: format!("failed to load pnl data: {error}"),
                }),
            )
                .into_response();
        }
    };

    let open_orders = {
        let app = web_state.state.read().await;
        let mut rows = app
            .orders
            .iter()
            .filter(|order| order.status == OrderStatus::Open)
            .filter(|order| {
                order_matches_filters(
                    order,
                    exchange_filter.as_deref(),
                    direction_filter.as_deref(),
                )
            })
            .filter_map(|order| {
                let key = InstrumentKey {
                    exchange: order.exchange.clone(),
                    instrument: order.instrument.clone(),
                };
                let market = app.markets.get(&key)?;
                let (mark_price, pnl, return_pct) = parse_open_order_pnl(order, market)?;
                Some(OpenPnlRow {
                    opened_at_ms: order.opened_at_ms,
                    exchange: order.exchange.clone(),
                    symbol: order.symbol.clone(),
                    direction: order.direction.clone(),
                    quote_notional: order.quote_notional,
                    mark_price,
                    pnl,
                    return_pct,
                })
            })
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| right.opened_at_ms.cmp(&left.opened_at_ms));
        rows
    };

    Json(build_pnl_response(
        normalized_range,
        exchange_filter.as_deref(),
        direction_filter.as_deref(),
        &orders,
        &open_orders,
    ))
    .into_response()
}

#[cfg(debug_assertions)]
async fn stress_persistence_handler(
    State(web_state): State<WebState>,
    Json(request): Json<StressPersistenceRequest>,
) -> impl IntoResponse {
    if request.count == 0 {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiError {
                error: "count must be greater than 0".to_string(),
            }),
        )
            .into_response();
    }

    let base_ts = unix_now_ms();
    let mut enqueued = 0usize;
    for index in 0..request.count {
        let (signal, order) = build_benchmark_persistence_pair(base_ts, index as u64);
        if web_state
            .persistence_tx
            .send(PersistenceTask::InsertSignalAndOrders {
                signal,
                orders: vec![order],
            })
            .is_err()
        {
            break;
        }
        enqueued += 1;
    }

    Json(StressPersistenceResponse { enqueued }).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;

    #[tokio::test]
    async fn index_handler_serves_control_room_ui() {
        let response = index_handler().await.into_response();
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("index body should be readable");
        let html = String::from_utf8(body.to_vec()).expect("index body should be valid utf-8");

        assert!(
            html.contains("Live Operating Frame"),
            "expected homepage to serve redesigned control room UI"
        );
    }

    #[tokio::test]
    async fn redesign_handler_matches_homepage_ui() {
        let index_response = index_handler().await.into_response();
        let redesign_response = redesign_handler().await.into_response();

        let index_body = to_bytes(index_response.into_body(), usize::MAX)
            .await
            .expect("index body should be readable");
        let redesign_body = to_bytes(redesign_response.into_body(), usize::MAX)
            .await
            .expect("redesign body should be readable");

        assert_eq!(index_body, redesign_body);
    }

    #[tokio::test]
    async fn redesign_ui_exposes_mode_toggle_control() {
        let response = redesign_handler().await.into_response();
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("redesign body should be readable");
        let html = String::from_utf8(body.to_vec()).expect("redesign body should be valid utf-8");

        assert!(
            html.contains("id=\"mode-toggle-link\""),
            "expected redesign ui to expose a mode toggle control"
        );
    }
}
