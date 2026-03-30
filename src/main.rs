mod app;
mod constants;
mod database;
mod gate;
mod gate_execution;
mod http;
mod lighter;
mod lighter_execution;
mod models;
mod runtime;
mod support;
mod util;
mod window;

use anyhow::{Context, Result};
use app::App;
use database::Database;
use gate::{run_gate_book_ticker_feed, run_gate_market_stats_feed, run_gate_metadata_sync};
use gate_execution::{GateOrderClient, run_gate_execution_worker};
use http::{WebState, run_http_server};
use lighter::{run_market_stats_feed, run_metadata_sync, run_ticker_manager};
use lighter_execution::{LighterExecutionConfig, run_lighter_execution_worker};
use models::*;
use reqwest::Client;
use runtime::{run_persistence_worker, run_read_model_worker, run_state_manager};
use std::sync::{Arc, Mutex};
use tokio::{
    signal,
    sync::{RwLock, mpsc, watch},
};
use tracing::info;
use tracing_subscriber::EnvFilter;

use constants::{DATABASE_PATH, MAX_ORDER_HISTORY, MAX_SIGNAL_HISTORY};

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let client = Client::builder()
        .user_agent("lighter-rust-bbo/0.1.0")
        .build()
        .context("failed to build reqwest client")?;
    let gate_order_client = GateOrderClient::from_env(client.clone())?.map(Arc::new);
    let lighter_execution_config = LighterExecutionConfig::from_env()?;

    let db = Database::open(DATABASE_PATH)?;
    let persisted_config = db.load_config()?;
    let persisted_signals = db.load_recent_signals(MAX_SIGNAL_HISTORY)?;
    let persisted_orders = db.load_recent_orders(MAX_ORDER_HISTORY)?;

    let mut app = App::new(persisted_config, persisted_signals, persisted_orders);
    app.set_live_trading_enabled(false);
    app.set_gate_live_trading_configured(gate_order_client.is_some());
    app.set_lighter_live_trading_configured(lighter_execution_config.is_some());
    let state = Arc::new(RwLock::new(app));
    let metrics = Arc::new(Mutex::new(RuntimeLatencyMetrics::default()));
    let initial_latency_snapshot = metrics
        .lock()
        .map(|guard| guard.snapshot())
        .unwrap_or_default();
    let initial_snapshot = state.read().await.snapshot(initial_latency_snapshot);
    let read_model = Arc::new(RwLock::new(initial_snapshot));
    let (event_tx, event_rx) = mpsc::channel(16_384);
    let (persistence_tx, persistence_rx) = mpsc::unbounded_channel();
    let (gate_execution_tx, gate_execution_rx) = mpsc::unbounded_channel();
    let (lighter_execution_tx, lighter_execution_rx) = mpsc::unbounded_channel();
    let event_tx_for_web = event_tx.clone();
    let (ticker_cmd_tx, ticker_cmd_rx) = mpsc::channel(4_096);
    let (metadata_cmd_tx, metadata_cmd_rx) = mpsc::channel(64);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (stream_control_tx, stream_control_rx) = watch::channel(StreamControlState::default());
    let (gate_contracts_tx, gate_contracts_rx) = watch::channel(Vec::new());

    let mut tasks = Vec::new();
    tasks.push(tokio::spawn(run_state_manager(
        state.clone(),
        metrics.clone(),
        event_rx,
        persistence_tx.clone(),
        gate_order_client
            .as_ref()
            .map(|_| gate_execution_tx.clone()),
        lighter_execution_config
            .as_ref()
            .map(|_| lighter_execution_tx.clone()),
        shutdown_rx.clone(),
    )));
    tasks.push(tokio::spawn(run_persistence_worker(
        metrics.clone(),
        db.clone(),
        persistence_rx,
        shutdown_rx.clone(),
    )));
    tasks.push(tokio::spawn(run_read_model_worker(
        state.clone(),
        metrics.clone(),
        read_model.clone(),
        shutdown_rx.clone(),
    )));
    tasks.push(tokio::spawn(run_market_stats_feed(
        event_tx.clone(),
        ticker_cmd_tx.clone(),
        metadata_cmd_tx.clone(),
        stream_control_rx.clone(),
        shutdown_rx.clone(),
    )));
    tasks.push(tokio::spawn(run_gate_metadata_sync(
        client.clone(),
        event_tx.clone(),
        gate_contracts_tx,
        stream_control_rx.clone(),
        shutdown_rx.clone(),
    )));
    tasks.push(tokio::spawn(run_gate_market_stats_feed(
        client.clone(),
        event_tx.clone(),
        gate_contracts_rx.clone(),
        stream_control_rx.clone(),
        shutdown_rx.clone(),
    )));
    tasks.push(tokio::spawn(run_ticker_manager(
        event_tx.clone(),
        ticker_cmd_rx,
        stream_control_rx.clone(),
        shutdown_rx.clone(),
    )));
    tasks.push(tokio::spawn(run_gate_book_ticker_feed(
        client.clone(),
        event_tx.clone(),
        gate_contracts_rx.clone(),
        stream_control_rx.clone(),
        shutdown_rx.clone(),
    )));
    if let Some(gate_order_client) = gate_order_client.clone() {
        tasks.push(tokio::spawn(run_gate_execution_worker(
            (*gate_order_client).clone(),
            gate_execution_rx,
            gate_contracts_rx.clone(),
            shutdown_rx.clone(),
        )));
    }
    if let Some(lighter_execution_config) = lighter_execution_config.clone() {
        tasks.push(tokio::spawn(run_lighter_execution_worker(
            lighter_execution_config,
            lighter_execution_rx,
            shutdown_rx.clone(),
        )));
    }
    tasks.push(tokio::spawn(run_metadata_sync(
        client,
        event_tx,
        ticker_cmd_tx,
        metadata_cmd_rx,
        stream_control_rx.clone(),
        shutdown_rx.clone(),
    )));

    let web_state = WebState {
        state,
        metrics,
        read_model,
        db,
        event_tx: event_tx_for_web,
        persistence_tx,
        metadata_cmd_tx: metadata_cmd_tx.clone(),
        stream_control_tx,
        gate_contracts_rx: gate_contracts_rx.clone(),
        gate_order_client,
    };
    let mut server_task = tokio::spawn(run_http_server(web_state, shutdown_rx.clone()));

    let server_stopped_early = tokio::select! {
        ctrl_c = signal::ctrl_c() => {
            ctrl_c.context("failed to listen for ctrl-c")?;
            info!("received ctrl-c, shutting down");
            false
        }
        server_result = &mut server_task => {
            server_result.context("web server task join error")??;
            info!("web server exited");
            true
        }
    };

    let _ = shutdown_tx.send(true);

    for task in tasks {
        task.await.context("background task join error")??;
    }

    if !server_stopped_early {
        server_task.await.context("web server task join error")??;
    }

    Ok(())
}

fn init_tracing() {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .compact()
        .init();
}
