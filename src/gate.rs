use crate::{
    constants::{GATE_EXCHANGE, GATE_REST_BASE, GATE_WS_BASE, SNAPSHOT_REFRESH_INTERVAL},
    models::*,
    support::{build_timing, stream_enabled_for, wait_or_shutdown},
    util::{format_decimal, unix_now_ms},
};
use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};
use tokio::{
    sync::{mpsc, watch},
    time::{self, MissedTickBehavior},
};
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct GateContractRuntime {
    pub(crate) contract: String,
    pub(crate) quantity_multiplier: f64,
    pub(crate) min_base_amount: String,
    pub(crate) price_decimals: u32,
    pub(crate) size_decimals: u32,
    pub(crate) order_size_min: f64,
    pub(crate) order_size_max: Option<f64>,
    pub(crate) market_order_size_max: Option<f64>,
    pub(crate) market_order_slip_ratio: Option<f64>,
    pub(crate) enable_decimal: bool,
}

async fn fetch_gate_contracts(client: &Client) -> Result<Vec<GateContractRuntime>> {
    let mut contracts = client
        .get(format!("{GATE_REST_BASE}/futures/usdt/contracts"))
        .send()
        .await
        .context("failed to request Gate futures contracts")?
        .error_for_status()
        .context("Gate futures contracts returned non-success status")?
        .json::<Vec<GateContractMetadata>>()
        .await
        .context("failed to deserialize Gate futures contracts response")?
        .into_iter()
        .filter(|contract| !contract.in_delisting)
        .filter_map(|contract| {
            let quantity_multiplier = contract.quanto_multiplier.parse::<f64>().ok()?;
            let min_base_amount =
                format_decimal(contract.order_size_min as f64 * quantity_multiplier);
            Some(GateContractRuntime {
                contract: contract.name,
                quantity_multiplier,
                min_base_amount,
                price_decimals: decimal_places(&contract.order_price_round),
                size_decimals: decimal_places(&format_decimal(contract.order_size_min)),
                order_size_min: contract.order_size_min,
                order_size_max: contract.order_size_max,
                market_order_size_max: contract.market_order_size_max,
                market_order_slip_ratio: contract.market_order_slip_ratio,
                enable_decimal: contract.enable_decimal,
            })
        })
        .collect::<Vec<_>>();

    contracts.sort_unstable_by(|left, right| left.contract.cmp(&right.contract));

    Ok(contracts)
}

fn build_gate_metadata_batch(contracts: &[GateContractRuntime]) -> MetadataBatch {
    let active_market_keys = contracts
        .iter()
        .map(|contract| InstrumentKey::gate(contract.contract.clone()))
        .collect::<HashSet<_>>();
    let updates = contracts
        .iter()
        .map(|contract| MetadataUpdate {
            key: InstrumentKey::gate(contract.contract.clone()),
            price_decimals: contract.price_decimals,
            size_decimals: contract.size_decimals,
            min_base_amount: contract.min_base_amount.clone(),
            min_quote_amount: "--".to_string(),
            order_quote_limit: "--".to_string(),
            quantity_multiplier: contract.quantity_multiplier,
        })
        .collect::<Vec<_>>();

    MetadataBatch {
        exchange: GATE_EXCHANGE.to_string(),
        updates,
        active_market_keys,
        fetched_at: Instant::now(),
        reason: "gate refresh".to_string(),
    }
}

async fn sync_gate_metadata(
    client: &Client,
    event_tx: &mpsc::Sender<ServiceEvent>,
    contracts_tx: &watch::Sender<Vec<GateContractRuntime>>,
    last_contracts: &mut Vec<GateContractRuntime>,
) {
    match fetch_gate_contracts(client).await {
        Ok(contracts) => {
            let changed = last_contracts != &contracts;
            if changed {
                let _ = event_tx
                    .send(ServiceEvent::Metadata(build_gate_metadata_batch(
                        &contracts,
                    )))
                    .await;
                let _ = contracts_tx.send(contracts.clone());
                *last_contracts = contracts;
            }
        }
        Err(error) => {
            let _ = event_tx
                .send(ServiceEvent::RestError {
                    message: format!("gate metadata sync: {error}"),
                    occurred_at: Instant::now(),
                })
                .await;
        }
    }
}

pub(crate) async fn run_gate_metadata_sync(
    client: Client,
    event_tx: mpsc::Sender<ServiceEvent>,
    contracts_tx: watch::Sender<Vec<GateContractRuntime>>,
    mut stream_control_rx: watch::Receiver<StreamControlState>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<()> {
    let mut interval = time::interval(SNAPSHOT_REFRESH_INTERVAL);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    interval.tick().await;
    let mut last_contracts = Vec::<GateContractRuntime>::new();

    if stream_enabled_for(&stream_control_rx.borrow(), GATE_EXCHANGE) {
        sync_gate_metadata(&client, &event_tx, &contracts_tx, &mut last_contracts).await;
    }

    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => break,
            _ = stream_control_rx.changed() => {
                if !stream_enabled_for(&stream_control_rx.borrow(), GATE_EXCHANGE) {
                    continue;
                }
                sync_gate_metadata(&client, &event_tx, &contracts_tx, &mut last_contracts).await;
            }
            _ = interval.tick() => {
                if !stream_enabled_for(&stream_control_rx.borrow(), GATE_EXCHANGE) {
                    continue;
                }
                sync_gate_metadata(&client, &event_tx, &contracts_tx, &mut last_contracts).await;
            }
        }
    }

    Ok(())
}

pub(crate) async fn run_gate_market_stats_feed(
    client: Client,
    event_tx: mpsc::Sender<ServiceEvent>,
    mut contracts_rx: watch::Receiver<Vec<GateContractRuntime>>,
    mut stream_control_rx: watch::Receiver<StreamControlState>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<()> {
    let connection_name = "gate-stats".to_string();

    loop {
        if !stream_enabled_for(&stream_control_rx.borrow(), GATE_EXCHANGE) {
            let _ = event_tx
                .send(ServiceEvent::Connection(ConnectionUpdate {
                    name: connection_name.clone(),
                    status: ConnectionStatus::Idle,
                }))
                .await;

            tokio::select! {
                _ = shutdown_rx.changed() => break,
                _ = stream_control_rx.changed() => continue,
            }
        }

        let contracts = if contracts_rx.borrow().is_empty() {
            match fetch_gate_contracts(&client).await {
                Ok(contracts) => contracts,
                Err(error) => {
                    let _ = event_tx
                        .send(ServiceEvent::RestError {
                            message: format!("gate metadata sync: {error}"),
                            occurred_at: Instant::now(),
                        })
                        .await;
                    if wait_or_shutdown(&mut shutdown_rx, Duration::from_secs(1)).await {
                        break;
                    }
                    continue;
                }
            }
        } else {
            contracts_rx.borrow().clone()
        };

        let quantity_multipliers = contracts
            .iter()
            .map(|contract| (contract.contract.clone(), contract.quantity_multiplier))
            .collect::<HashMap<_, _>>();

        let payload = contracts
            .iter()
            .map(|contract| contract.contract.clone())
            .collect::<Vec<_>>();
        if payload.is_empty() {
            if wait_or_shutdown(&mut shutdown_rx, Duration::from_secs(1)).await {
                break;
            }
            continue;
        }

        let _ = event_tx
            .send(ServiceEvent::Connection(ConnectionUpdate {
                name: connection_name.clone(),
                status: ConnectionStatus::Connecting,
            }))
            .await;

        let connect_result = connect_async(GATE_WS_BASE).await;
        let (ws_stream, _) = match connect_result {
            Ok(pair) => pair,
            Err(error) => {
                let _ = event_tx
                    .send(ServiceEvent::Connection(ConnectionUpdate {
                        name: connection_name.clone(),
                        status: ConnectionStatus::Closed {
                            reason: format!("connect error: {error}"),
                        },
                    }))
                    .await;
                if wait_or_shutdown(&mut shutdown_rx, Duration::from_secs(1)).await {
                    break;
                }
                continue;
            }
        };

        let (mut write, mut read) = ws_stream.split();
        subscribe_gate_channel(&mut write, "futures.tickers", &payload, &connection_name).await?;

        let _ = event_tx
            .send(ServiceEvent::Connection(ConnectionUpdate {
                name: connection_name.clone(),
                status: ConnectionStatus::Live {
                    subscriptions: payload.len(),
                },
            }))
            .await;

        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => return Ok(()),
                _ = contracts_rx.changed() => {
                    let latest_contracts = contracts_rx.borrow().clone();
                    let latest_payload = latest_contracts
                        .iter()
                        .map(|contract| contract.contract.clone())
                        .collect::<Vec<_>>();
                    if latest_payload != payload {
                        let _ = event_tx.send(ServiceEvent::Connection(ConnectionUpdate {
                            name: connection_name.clone(),
                            status: ConnectionStatus::Closed {
                                reason: "contract set changed".to_string(),
                            },
                        })).await;
                        break;
                    }
                }
                _ = stream_control_rx.changed() => {
                    if !stream_enabled_for(&stream_control_rx.borrow(), GATE_EXCHANGE) {
                        let _ = event_tx.send(ServiceEvent::Connection(ConnectionUpdate {
                            name: connection_name.clone(),
                            status: ConnectionStatus::Idle,
                        })).await;
                        break;
                    }
                }
                maybe_message = read.next() => {
                    let Some(message) = maybe_message else {
                        let _ = event_tx.send(ServiceEvent::Connection(ConnectionUpdate {
                            name: connection_name.clone(),
                            status: ConnectionStatus::Closed {
                                reason: "stream ended".to_string(),
                            },
                        })).await;
                        break;
                    };

                    let message = match message {
                        Ok(message) => message,
                        Err(error) => {
                            let _ = event_tx.send(ServiceEvent::Connection(ConnectionUpdate {
                                name: connection_name.clone(),
                                status: ConnectionStatus::Closed {
                                    reason: format!("read error: {error}"),
                                },
                            })).await;
                            break;
                        }
                    };

                    match message {
                        Message::Text(text) => {
                            if let Some(batch) = parse_gate_market_stats_message(&text, &quantity_multipliers)? {
                                let _ = event_tx.send(ServiceEvent::MarketStats(batch)).await;
                            }
                        }
                        Message::Binary(bytes) => {
                            if let Ok(text) = String::from_utf8(bytes.to_vec()) {
                                if let Some(batch) = parse_gate_market_stats_message(&text, &quantity_multipliers)? {
                                    let _ = event_tx.send(ServiceEvent::MarketStats(batch)).await;
                                }
                            }
                        }
                        Message::Ping(payload) => {
                            let _ = write.send(Message::Pong(payload)).await;
                        }
                        Message::Close(frame) => {
                            let reason = frame
                                .as_ref()
                                .map(|close| close.reason.to_string())
                                .filter(|reason| !reason.is_empty())
                                .unwrap_or_else(|| "remote close".to_string());
                            let _ = event_tx.send(ServiceEvent::Connection(ConnectionUpdate {
                                name: connection_name.clone(),
                                status: ConnectionStatus::Closed { reason },
                            })).await;
                            break;
                        }
                        _ => {}
                    }
                }
            }
        }

        if wait_or_shutdown(&mut shutdown_rx, Duration::from_secs(1)).await {
            break;
        }
    }

    Ok(())
}

pub(crate) async fn run_gate_book_ticker_feed(
    client: Client,
    event_tx: mpsc::Sender<ServiceEvent>,
    mut contracts_rx: watch::Receiver<Vec<GateContractRuntime>>,
    mut stream_control_rx: watch::Receiver<StreamControlState>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<()> {
    let connection_name = "gate-book".to_string();

    loop {
        if !stream_enabled_for(&stream_control_rx.borrow(), GATE_EXCHANGE) {
            let _ = event_tx
                .send(ServiceEvent::Connection(ConnectionUpdate {
                    name: connection_name.clone(),
                    status: ConnectionStatus::Idle,
                }))
                .await;

            tokio::select! {
                _ = shutdown_rx.changed() => break,
                _ = stream_control_rx.changed() => continue,
            }
        }

        let contracts = if contracts_rx.borrow().is_empty() {
            match fetch_gate_contracts(&client).await {
                Ok(contracts) => contracts,
                Err(error) => {
                    let _ = event_tx
                        .send(ServiceEvent::RestError {
                            message: format!("gate metadata sync: {error}"),
                            occurred_at: Instant::now(),
                        })
                        .await;
                    if wait_or_shutdown(&mut shutdown_rx, Duration::from_secs(1)).await {
                        break;
                    }
                    continue;
                }
            }
        } else {
            contracts_rx.borrow().clone()
        };

        let quantity_multipliers = contracts
            .iter()
            .map(|contract| (contract.contract.clone(), contract.quantity_multiplier))
            .collect::<HashMap<_, _>>();

        let payload = contracts
            .iter()
            .map(|contract| contract.contract.clone())
            .collect::<Vec<_>>();
        if payload.is_empty() {
            if wait_or_shutdown(&mut shutdown_rx, Duration::from_secs(1)).await {
                break;
            }
            continue;
        }

        let _ = event_tx
            .send(ServiceEvent::Connection(ConnectionUpdate {
                name: connection_name.clone(),
                status: ConnectionStatus::Connecting,
            }))
            .await;

        let connect_result = connect_async(GATE_WS_BASE).await;
        let (ws_stream, _) = match connect_result {
            Ok(pair) => pair,
            Err(error) => {
                let _ = event_tx
                    .send(ServiceEvent::Connection(ConnectionUpdate {
                        name: connection_name.clone(),
                        status: ConnectionStatus::Closed {
                            reason: format!("connect error: {error}"),
                        },
                    }))
                    .await;
                if wait_or_shutdown(&mut shutdown_rx, Duration::from_secs(1)).await {
                    break;
                }
                continue;
            }
        };

        let (mut write, mut read) = ws_stream.split();
        subscribe_gate_channel(
            &mut write,
            "futures.book_ticker",
            &payload,
            &connection_name,
        )
        .await?;

        let _ = event_tx
            .send(ServiceEvent::Connection(ConnectionUpdate {
                name: connection_name.clone(),
                status: ConnectionStatus::Live {
                    subscriptions: payload.len(),
                },
            }))
            .await;

        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => return Ok(()),
                _ = contracts_rx.changed() => {
                    let latest_contracts = contracts_rx.borrow().clone();
                    let latest_payload = latest_contracts
                        .iter()
                        .map(|contract| contract.contract.clone())
                        .collect::<Vec<_>>();
                    if latest_payload != payload {
                        let _ = event_tx.send(ServiceEvent::Connection(ConnectionUpdate {
                            name: connection_name.clone(),
                            status: ConnectionStatus::Closed {
                                reason: "contract set changed".to_string(),
                            },
                        })).await;
                        break;
                    }
                }
                _ = stream_control_rx.changed() => {
                    if !stream_enabled_for(&stream_control_rx.borrow(), GATE_EXCHANGE) {
                        let _ = event_tx.send(ServiceEvent::Connection(ConnectionUpdate {
                            name: connection_name.clone(),
                            status: ConnectionStatus::Idle,
                        })).await;
                        break;
                    }
                }
                maybe_message = read.next() => {
                    let Some(message) = maybe_message else {
                        let _ = event_tx.send(ServiceEvent::Connection(ConnectionUpdate {
                            name: connection_name.clone(),
                            status: ConnectionStatus::Closed {
                                reason: "stream ended".to_string(),
                            },
                        })).await;
                        break;
                    };

                    let message = match message {
                        Ok(message) => message,
                        Err(error) => {
                            let _ = event_tx.send(ServiceEvent::Connection(ConnectionUpdate {
                                name: connection_name.clone(),
                                status: ConnectionStatus::Closed {
                                    reason: format!("read error: {error}"),
                                },
                            })).await;
                            break;
                        }
                    };

                    match message {
                        Message::Text(text) => {
                            if let Some(update) = parse_gate_bbo_update(&text, &quantity_multipliers)? {
                                let _ = event_tx.send(ServiceEvent::Bbo(update)).await;
                            }
                        }
                        Message::Binary(bytes) => {
                            if let Ok(text) = String::from_utf8(bytes.to_vec()) {
                                if let Some(update) = parse_gate_bbo_update(&text, &quantity_multipliers)? {
                                    let _ = event_tx.send(ServiceEvent::Bbo(update)).await;
                                }
                            }
                        }
                        Message::Ping(payload) => {
                            let _ = write.send(Message::Pong(payload)).await;
                        }
                        Message::Close(frame) => {
                            let reason = frame
                                .as_ref()
                                .map(|close| close.reason.to_string())
                                .filter(|reason| !reason.is_empty())
                                .unwrap_or_else(|| "remote close".to_string());
                            let _ = event_tx.send(ServiceEvent::Connection(ConnectionUpdate {
                                name: connection_name.clone(),
                                status: ConnectionStatus::Closed { reason },
                            })).await;
                            break;
                        }
                        _ => {}
                    }
                }
            }
        }

        if wait_or_shutdown(&mut shutdown_rx, Duration::from_secs(1)).await {
            break;
        }
    }

    Ok(())
}

async fn subscribe_gate_channel<S>(
    write: &mut S,
    channel: &'static str,
    payload: &[String],
    connection_name: &str,
) -> Result<()>
where
    S: SinkExt<Message> + Unpin,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    let message = GateSubscriptionRequest {
        time: unix_now_ms() / 1000,
        channel,
        event: "subscribe",
        payload: payload.to_vec(),
    };
    let encoded =
        serde_json::to_string(&message).context("failed to serialize Gate subscription")?;
    write
        .send(Message::Text(encoded.into()))
        .await
        .with_context(|| format!("failed to subscribe {channel} on {connection_name}"))?;
    Ok(())
}

fn parse_gate_market_stats_message(
    text: &str,
    quantity_multipliers: &HashMap<String, f64>,
) -> Result<Option<MarketStatsBatch>> {
    let envelope = match serde_json::from_str::<GateTickersEnvelope>(text) {
        Ok(message) => message,
        Err(_) => return Ok(None),
    };

    if envelope.channel != "futures.tickers" || envelope.event != "update" {
        return Ok(None);
    }

    let Some(result) = envelope.result else {
        return Ok(None);
    };

    let mut timestamp_ms = None;
    let updates = result
        .into_iter()
        .filter_map(|market| {
            let quantity_multiplier = *quantity_multipliers.get(&market.contract)?;
            let total_size = market.total_size.parse::<f64>().ok()?;
            let mark_price = market.mark_price.parse::<f64>().ok()?;
            let open_interest_quote = total_size * quantity_multiplier * mark_price;
            timestamp_ms = Some(market.t);

            Some(MarketStatsUpdate {
                key: InstrumentKey::gate(market.contract.clone()),
                symbol: market.contract.clone(),
                current_funding_rate: market.funding_rate,
                last_trade_price: market.last,
                open_interest: format_decimal(open_interest_quote),
            })
        })
        .collect::<Vec<_>>();

    if updates.is_empty() {
        return Ok(None);
    }

    let timing = build_timing(timestamp_ms);
    Ok(Some(MarketStatsBatch { updates, timing }))
}

fn parse_gate_bbo_update(
    text: &str,
    quantity_multipliers: &HashMap<String, f64>,
) -> Result<Option<BboUpdate>> {
    let envelope = match serde_json::from_str::<GateBookTickerEnvelope>(text) {
        Ok(message) => message,
        Err(_) => return Ok(None),
    };

    if envelope.channel != "futures.book_ticker" || envelope.event != "update" {
        return Ok(None);
    }

    let Some(result) = envelope.result else {
        return Ok(None);
    };

    let quantity_multiplier = quantity_multipliers
        .get(&result.s)
        .copied()
        .context("missing Gate quantity multiplier")?;
    let bid_size = result
        .bid_size
        .as_f64()
        .context("invalid Gate best bid size")?;
    let ask_size = result
        .ask_size
        .as_f64()
        .context("invalid Gate best ask size")?;
    let bid_size_base = bid_size * quantity_multiplier;
    let ask_size_base = ask_size * quantity_multiplier;
    let bid_price = result
        .b
        .parse::<f64>()
        .context("invalid Gate best bid price")?;
    let ask_price = result
        .a
        .parse::<f64>()
        .context("invalid Gate best ask price")?;

    Ok(Some(BboUpdate {
        key: InstrumentKey::gate(result.s.clone()),
        symbol: result.s,
        best_bid_price: result.b,
        best_bid_size: format_decimal(bid_size_base),
        best_bid_size_base: format_decimal(bid_size_base),
        best_bid_notional: bid_size_base * bid_price,
        best_ask_price: result.a,
        best_ask_size: format_decimal(ask_size_base),
        best_ask_size_base: format_decimal(ask_size_base),
        best_ask_notional: ask_size_base * ask_price,
        timing: build_timing(Some(result.t)),
    }))
}

fn decimal_places(step: &str) -> u32 {
    step.split('.')
        .nth(1)
        .map(|fraction| fraction.trim_end_matches('0').len() as u32)
        .unwrap_or(0)
}
