use crate::{
    constants::{
        LIGHTER_EXCHANGE, MARKET_STATS_CHANNEL, REST_BASE, SNAPSHOT_REFRESH_INTERVAL,
        TARGET_SUBSCRIPTIONS_PER_CONNECTION, WS_BASE,
    },
    models::*,
    support::{build_timing, stream_enabled_for, wait_or_shutdown},
    util::format_decimal,
};
use anyhow::{Context, Result};
use axum::http::Request;
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
    time::{self, MissedTickBehavior},
};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::warn;

pub(crate) async fn run_market_stats_feed(
    event_tx: mpsc::Sender<ServiceEvent>,
    ticker_cmd_tx: mpsc::Sender<TickerCommand>,
    metadata_cmd_tx: mpsc::Sender<MetadataCommand>,
    mut stream_control_rx: watch::Receiver<StreamControlState>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<()> {
    let mut discovered_markets = HashSet::new();
    let connection_name = "stats".to_string();

    loop {
        if !stream_enabled_for(&stream_control_rx.borrow(), LIGHTER_EXCHANGE) {
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

        let _ = event_tx
            .send(ServiceEvent::Connection(ConnectionUpdate {
                name: connection_name.clone(),
                status: ConnectionStatus::Connecting,
            }))
            .await;

        let request = build_lighter_ws_request()?;
        let connect_result = connect_async(request).await;
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
        let subscribe = SubscriptionRequest {
            kind: "subscribe",
            channel: MARKET_STATS_CHANNEL.to_string(),
        };
        let payload = serde_json::to_string(&subscribe)
            .context("failed to serialize market stats subscription")?;

        if let Err(error) = write.send(Message::Text(payload.into())).await {
            let _ = event_tx
                .send(ServiceEvent::Connection(ConnectionUpdate {
                    name: connection_name.clone(),
                    status: ConnectionStatus::Closed {
                        reason: format!("subscribe error: {error}"),
                    },
                }))
                .await;
            if wait_or_shutdown(&mut shutdown_rx, Duration::from_secs(1)).await {
                break;
            }
            continue;
        }

        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => return Ok(()),
                _ = stream_control_rx.changed() => {
                    if !stream_enabled_for(&stream_control_rx.borrow(), LIGHTER_EXCHANGE) {
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
                            if let Some(parsed) = parse_market_stats_message(&text)? {
                                let was_empty = discovered_markets.is_empty();
                                let mut newly_discovered = Vec::new();

                                for update in &parsed.updates {
                                    let Ok(market_id) = update.key.instrument.parse::<u64>() else {
                                        continue;
                                    };
                                    if discovered_markets.insert(market_id) {
                                        newly_discovered.push((market_id, update.symbol.clone()));
                                    }
                                }

                                let _ = event_tx.send(ServiceEvent::Connection(ConnectionUpdate {
                                    name: connection_name.clone(),
                                    status: ConnectionStatus::Live {
                                        subscriptions: discovered_markets.len(),
                                    },
                                })).await;

                                if !parsed.updates.is_empty() {
                                    let _ = event_tx.send(ServiceEvent::MarketStats(parsed.clone())).await;
                                }

                                let had_new_market = !newly_discovered.is_empty();
                                for (market_id, symbol) in newly_discovered {
                                    let _ = ticker_cmd_tx.send(TickerCommand::Discover { market_id, symbol }).await;
                                }

                                if had_new_market {
                                    let reason = if was_empty {
                                        "initial snapshot".to_string()
                                    } else {
                                        "new market detected".to_string()
                                    };
                                    let _ = metadata_cmd_tx.send(MetadataCommand::SyncAll { reason }).await;
                                }
                            }
                        }
                        Message::Binary(bytes) => {
                            if let Ok(text) = String::from_utf8(bytes.to_vec()) {
                                if let Some(parsed) = parse_market_stats_message(&text)? {
                                    let was_empty = discovered_markets.is_empty();
                                    let mut newly_discovered = Vec::new();

                                    for update in &parsed.updates {
                                        let Ok(market_id) = update.key.instrument.parse::<u64>() else {
                                            continue;
                                        };
                                        if discovered_markets.insert(market_id) {
                                            newly_discovered.push((market_id, update.symbol.clone()));
                                        }
                                    }

                                    let _ = event_tx.send(ServiceEvent::Connection(ConnectionUpdate {
                                        name: connection_name.clone(),
                                        status: ConnectionStatus::Live {
                                            subscriptions: discovered_markets.len(),
                                        },
                                    })).await;

                                    if !parsed.updates.is_empty() {
                                        let _ = event_tx.send(ServiceEvent::MarketStats(parsed.clone())).await;
                                    }

                                    let had_new_market = !newly_discovered.is_empty();
                                    for (market_id, symbol) in newly_discovered {
                                        let _ = ticker_cmd_tx.send(TickerCommand::Discover { market_id, symbol }).await;
                                    }

                                    if had_new_market {
                                        let reason = if was_empty {
                                            "initial snapshot".to_string()
                                        } else {
                                            "new market detected".to_string()
                                        };
                                        let _ = metadata_cmd_tx.send(MetadataCommand::SyncAll { reason }).await;
                                    }
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

pub(crate) async fn run_ticker_manager(
    event_tx: mpsc::Sender<ServiceEvent>,
    mut ticker_cmd_rx: mpsc::Receiver<TickerCommand>,
    stream_control_rx: watch::Receiver<StreamControlState>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<()> {
    let mut discovered_market_symbols = HashMap::new();
    let mut active_market_ids = HashSet::new();
    let mut subscribed_market_ids = HashSet::new();
    let mut market_connection_index = HashMap::<u64, usize>::new();
    let mut connections = Vec::new();

    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => break,
            maybe_cmd = ticker_cmd_rx.recv() => {
                let Some(cmd) = maybe_cmd else {
                    break;
                };

                match cmd {
                    TickerCommand::Discover { market_id, symbol } => {
                        discovered_market_symbols.insert(market_id, symbol.clone());
                        if active_market_ids.contains(&market_id) && !subscribed_market_ids.contains(&market_id) {
                            subscribe_market(
                                market_id,
                                symbol,
                                &event_tx,
                                &stream_control_rx,
                                &shutdown_rx,
                                &mut connections,
                                &mut subscribed_market_ids,
                                &mut market_connection_index,
                            ).await?;
                        }
                    }
                    TickerCommand::SetActiveMarkets { active_market_ids: next_active_market_ids } => {
                        let to_unsubscribe = subscribed_market_ids
                            .iter()
                            .copied()
                            .filter(|market_id| !next_active_market_ids.contains(market_id))
                            .collect::<Vec<_>>();

                        for market_id in to_unsubscribe {
                            if let Some(connection_index) = market_connection_index.remove(&market_id) {
                                if let Some(connection) = connections.get_mut(connection_index) {
                                    connection.load = connection.load.saturating_sub(1);
                                    connection
                                        .tx
                                        .send(ConnectionCommand::Unsubscribe { market_id })
                                        .await
                                        .context("failed to dispatch ticker unsubscription command")?;
                                }
                            }
                            subscribed_market_ids.remove(&market_id);
                        }

                        active_market_ids = next_active_market_ids;

                        for (market_id, symbol) in &discovered_market_symbols {
                            if active_market_ids.contains(market_id) && !subscribed_market_ids.contains(market_id) {
                                subscribe_market(
                                    *market_id,
                                    symbol.clone(),
                                    &event_tx,
                                    &stream_control_rx,
                                    &shutdown_rx,
                                    &mut connections,
                                    &mut subscribed_market_ids,
                                    &mut market_connection_index,
                                ).await?;
                            }
                        }
                    }
                }
            }
        }
    }

    for connection in connections {
        connection
            .join_handle
            .await
            .context("ticker connection join error")??;
    }

    Ok(())
}

struct TickerConnectionHandle {
    load: usize,
    tx: mpsc::Sender<ConnectionCommand>,
    join_handle: JoinHandle<Result<()>>,
}

async fn subscribe_market(
    market_id: u64,
    symbol: String,
    event_tx: &mpsc::Sender<ServiceEvent>,
    stream_control_rx: &watch::Receiver<StreamControlState>,
    shutdown_rx: &watch::Receiver<bool>,
    connections: &mut Vec<TickerConnectionHandle>,
    subscribed_market_ids: &mut HashSet<u64>,
    market_connection_index: &mut HashMap<u64, usize>,
) -> Result<()> {
    if !subscribed_market_ids.insert(market_id) {
        return Ok(());
    }

    let target_index = match connections
        .iter()
        .position(|connection| connection.load < TARGET_SUBSCRIPTIONS_PER_CONNECTION)
    {
        Some(index) => index,
        None => {
            let name = format!("ticker-{}", connections.len());
            let (tx, rx) = mpsc::channel(1_024);
            let join_handle = tokio::spawn(run_ticker_connection(
                name,
                event_tx.clone(),
                rx,
                stream_control_rx.clone(),
                shutdown_rx.clone(),
            ));

            connections.push(TickerConnectionHandle {
                load: 0,
                tx,
                join_handle,
            });
            connections.len() - 1
        }
    };

    let connection = &mut connections[target_index];
    connection.load += 1;
    connection
        .tx
        .send(ConnectionCommand::Subscribe { market_id, symbol })
        .await
        .context("failed to dispatch ticker subscription command")?;
    market_connection_index.insert(market_id, target_index);
    Ok(())
}

async fn run_ticker_connection(
    connection_name: String,
    event_tx: mpsc::Sender<ServiceEvent>,
    mut cmd_rx: mpsc::Receiver<ConnectionCommand>,
    mut stream_control_rx: watch::Receiver<StreamControlState>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<()> {
    let mut market_symbols = HashMap::<u64, String>::new();

    loop {
        while let Ok(command) = cmd_rx.try_recv() {
            match command {
                ConnectionCommand::Subscribe { market_id, symbol } => {
                    market_symbols.entry(market_id).or_insert(symbol);
                }
                ConnectionCommand::Unsubscribe { market_id } => {
                    market_symbols.remove(&market_id);
                }
            }
        }

        if market_symbols.is_empty()
            || !stream_enabled_for(&stream_control_rx.borrow(), LIGHTER_EXCHANGE)
        {
            let _ = event_tx
                .send(ServiceEvent::Connection(ConnectionUpdate {
                    name: connection_name.clone(),
                    status: ConnectionStatus::Idle,
                }))
                .await;

            tokio::select! {
                _ = shutdown_rx.changed() => break,
                _ = stream_control_rx.changed() => {},
                maybe_cmd = cmd_rx.recv() => {
                    let Some(command) = maybe_cmd else {
                        break;
                    };

                    match command {
                        ConnectionCommand::Subscribe { market_id, symbol } => {
                            market_symbols.entry(market_id).or_insert(symbol);
                        }
                        ConnectionCommand::Unsubscribe { .. } => {}
                    }
                }
            }
            continue;
        }

        let _ = event_tx
            .send(ServiceEvent::Connection(ConnectionUpdate {
                name: connection_name.clone(),
                status: ConnectionStatus::Connecting,
            }))
            .await;

        let request = build_lighter_ws_request()?;
        let connect_result = connect_async(request).await;
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
        for market_id in market_symbols.keys().copied().collect::<Vec<_>>() {
            subscribe_ticker(&mut write, market_id, &connection_name).await?;
        }

        let _ = event_tx
            .send(ServiceEvent::Connection(ConnectionUpdate {
                name: connection_name.clone(),
                status: ConnectionStatus::Live {
                    subscriptions: market_symbols.len(),
                },
            }))
            .await;

        let should_reconnect = loop {
            tokio::select! {
                _ = shutdown_rx.changed() => return Ok(()),
                _ = stream_control_rx.changed() => {
                    if !stream_enabled_for(&stream_control_rx.borrow(), LIGHTER_EXCHANGE) {
                        let _ = event_tx.send(ServiceEvent::Connection(ConnectionUpdate {
                            name: connection_name.clone(),
                            status: ConnectionStatus::Idle,
                        })).await;
                        break true;
                    }
                }
                maybe_cmd = cmd_rx.recv() => {
                    match maybe_cmd {
                        Some(ConnectionCommand::Subscribe { market_id, symbol }) => {
                            let is_new = market_symbols.insert(market_id, symbol).is_none();
                            if is_new {
                                subscribe_ticker(&mut write, market_id, &connection_name).await?;
                                let _ = event_tx.send(ServiceEvent::Connection(ConnectionUpdate {
                                    name: connection_name.clone(),
                                    status: ConnectionStatus::Live {
                                        subscriptions: market_symbols.len(),
                                    },
                                })).await;
                            }
                        }
                        Some(ConnectionCommand::Unsubscribe { market_id }) => {
                            if market_symbols.remove(&market_id).is_some() {
                                unsubscribe_ticker(&mut write, market_id, &connection_name).await?;
                                let _ = event_tx.send(ServiceEvent::Connection(ConnectionUpdate {
                                    name: connection_name.clone(),
                                    status: ConnectionStatus::Live {
                                        subscriptions: market_symbols.len(),
                                    },
                                })).await;
                            }
                        }
                        None => return Ok(()),
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
                        break true;
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
                            break true;
                        }
                    };

                    match message {
                        Message::Text(text) => {
                            if let Some(update) = parse_bbo_update(&text, &market_symbols)? {
                                let _ = event_tx.send(ServiceEvent::Bbo(update)).await;
                            }
                        }
                        Message::Binary(bytes) => {
                            if let Ok(text) = String::from_utf8(bytes.to_vec()) {
                                if let Some(update) = parse_bbo_update(&text, &market_symbols)? {
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
                            break true;
                        }
                        _ => {}
                    }
                }
            }
        };

        if !should_reconnect {
            break;
        }

        if wait_or_shutdown(&mut shutdown_rx, Duration::from_secs(1)).await {
            break;
        }
    }

    Ok(())
}

async fn subscribe_ticker<S>(write: &mut S, market_id: u64, connection_name: &str) -> Result<()>
where
    S: SinkExt<Message> + Unpin,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    let message = SubscriptionRequest {
        kind: "subscribe",
        channel: format!("ticker/{market_id}"),
    };
    let payload =
        serde_json::to_string(&message).context("failed to serialize ticker subscription")?;
    write
        .send(Message::Text(payload.into()))
        .await
        .with_context(|| format!("failed to subscribe market {market_id} on {connection_name}"))?;
    Ok(())
}

async fn unsubscribe_ticker<S>(write: &mut S, market_id: u64, connection_name: &str) -> Result<()>
where
    S: SinkExt<Message> + Unpin,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    let message = SubscriptionRequest {
        kind: "unsubscribe",
        channel: format!("ticker/{market_id}"),
    };
    let payload =
        serde_json::to_string(&message).context("failed to serialize ticker unsubscription")?;
    write
        .send(Message::Text(payload.into()))
        .await
        .with_context(|| {
            format!("failed to unsubscribe market {market_id} on {connection_name}")
        })?;
    Ok(())
}

pub(crate) async fn run_metadata_sync(
    client: Client,
    event_tx: mpsc::Sender<ServiceEvent>,
    ticker_cmd_tx: mpsc::Sender<TickerCommand>,
    mut metadata_cmd_rx: mpsc::Receiver<MetadataCommand>,
    mut stream_control_rx: watch::Receiver<StreamControlState>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<()> {
    let mut interval = time::interval(SNAPSHOT_REFRESH_INTERVAL);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    interval.tick().await;

    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => break,
            _ = stream_control_rx.changed(), if !stream_enabled_for(&stream_control_rx.borrow(), LIGHTER_EXCHANGE) => {
                continue;
            }
            _ = interval.tick() => {
                if stream_enabled_for(&stream_control_rx.borrow(), LIGHTER_EXCHANGE) {
                    sync_metadata(&client, &event_tx, &ticker_cmd_tx, "daily refresh".to_string()).await;
                }
            }
            maybe_cmd = metadata_cmd_rx.recv() => {
                let Some(MetadataCommand::SyncAll { reason }) = maybe_cmd else {
                    break;
                };
                if stream_enabled_for(&stream_control_rx.borrow(), LIGHTER_EXCHANGE) {
                    sync_metadata(&client, &event_tx, &ticker_cmd_tx, reason).await;
                }
            }
        }
    }

    Ok(())
}

async fn sync_metadata(
    client: &Client,
    event_tx: &mpsc::Sender<ServiceEvent>,
    ticker_cmd_tx: &mpsc::Sender<TickerCommand>,
    reason: String,
) {
    match fetch_perp_metadata(client).await {
        Ok((updates, active_market_keys)) => {
            let active_market_ids = active_market_keys
                .iter()
                .filter_map(|key| key.instrument.parse::<u64>().ok())
                .collect::<HashSet<_>>();
            let _ = ticker_cmd_tx
                .send(TickerCommand::SetActiveMarkets {
                    active_market_ids: active_market_ids.clone(),
                })
                .await;
            let _ = event_tx
                .send(ServiceEvent::Metadata(MetadataBatch {
                    exchange: LIGHTER_EXCHANGE.to_string(),
                    updates,
                    active_market_keys,
                    fetched_at: Instant::now(),
                    reason,
                }))
                .await;
        }
        Err(error) => {
            let _ = event_tx
                .send(ServiceEvent::RestError {
                    message: format!("{reason}: {error}"),
                    occurred_at: Instant::now(),
                })
                .await;
        }
    }
}

async fn fetch_perp_metadata(
    client: &Client,
) -> Result<(Vec<MetadataUpdate>, HashSet<InstrumentKey>)> {
    let response = client
        .get(format!("{REST_BASE}/orderBookDetails"))
        .send()
        .await
        .context("failed to request orderBookDetails")?
        .error_for_status()
        .context("orderBookDetails returned non-success status")?
        .json::<OrderBookDetailsResponse>()
        .await
        .context("failed to deserialize orderBookDetails response")?;

    if response.code != 200 {
        warn!(code = response.code, "lighter returned unexpected code");
    }

    let active_markets = response
        .order_book_details
        .into_iter()
        .filter(|market| market.market_type == "perp" && market.status == "active")
        .collect::<Vec<_>>();

    let active_market_keys = active_markets
        .iter()
        .map(|market| InstrumentKey::lighter(market.market_id))
        .collect::<HashSet<_>>();

    let updates = active_markets
        .into_iter()
        .map(|market| MetadataUpdate {
            key: InstrumentKey::lighter(market.market_id),
            price_decimals: market.price_decimals,
            size_decimals: market.size_decimals,
            min_base_amount: market.min_base_amount,
            min_quote_amount: market.min_quote_amount,
            order_quote_limit: market.order_quote_limit,
            quantity_multiplier: 1.0,
        })
        .collect::<Vec<_>>();

    Ok((updates, active_market_keys))
}

fn parse_market_stats_message(text: &str) -> Result<Option<MarketStatsBatch>> {
    let envelope = match serde_json::from_str::<MarketStatsEnvelope>(text) {
        Ok(message) => message,
        Err(_) => return Ok(None),
    };

    if envelope.kind != "subscribed/market_stats" && envelope.kind != "update/market_stats" {
        return Ok(None);
    }

    let Some(market_stats) = envelope.market_stats else {
        return Ok(None);
    };

    let timing = build_timing(envelope.timestamp);
    let updates = market_stats
        .into_values()
        .map(|market| MarketStatsUpdate {
            key: InstrumentKey::lighter(market.market_id),
            symbol: market.symbol,
            current_funding_rate: market.current_funding_rate,
            last_trade_price: market.last_trade_price,
            open_interest: market.open_interest,
        })
        .collect::<Vec<_>>();

    Ok(Some(MarketStatsBatch { updates, timing }))
}

fn parse_bbo_update(
    text: &str,
    market_symbols: &HashMap<u64, String>,
) -> Result<Option<BboUpdate>> {
    let envelope = match serde_json::from_str::<TickerEnvelope>(text) {
        Ok(message) => message,
        Err(_) => return Ok(None),
    };

    if envelope.kind != "update/ticker" {
        return Ok(None);
    }

    let Some(ticker) = envelope.ticker else {
        return Ok(None);
    };

    let market_id = envelope
        .channel
        .split(':')
        .nth(1)
        .context("missing market id in ticker channel")?
        .parse::<u64>()
        .context("invalid market id in ticker channel")?;

    let symbol = market_symbols
        .get(&market_id)
        .cloned()
        .unwrap_or_else(|| format!("#{market_id}"));
    let bid_size_base = ticker
        .b
        .size
        .parse::<f64>()
        .context("invalid Lighter best bid size")?;
    let ask_size_base = ticker
        .a
        .size
        .parse::<f64>()
        .context("invalid Lighter best ask size")?;
    let bid_price = ticker
        .b
        .price
        .parse::<f64>()
        .context("invalid Lighter best bid price")?;
    let ask_price = ticker
        .a
        .price
        .parse::<f64>()
        .context("invalid Lighter best ask price")?;

    Ok(Some(BboUpdate {
        key: InstrumentKey::lighter(market_id),
        symbol,
        best_bid_price: ticker.b.price,
        best_bid_size: format_decimal(bid_size_base),
        best_bid_size_base: format_decimal(bid_size_base),
        best_bid_notional: bid_size_base * bid_price,
        best_ask_price: ticker.a.price,
        best_ask_size: format_decimal(ask_size_base),
        best_ask_size_base: format_decimal(ask_size_base),
        best_ask_notional: ask_size_base * ask_price,
        timing: build_timing(envelope.timestamp),
    }))
}

fn build_lighter_ws_request() -> Result<Request<()>> {
    Request::builder()
        .uri(WS_BASE)
        .header("Host", "mainnet.zklighter.elliot.ai")
        .header("Connection", "Upgrade")
        .header("Upgrade", "websocket")
        .header("Sec-WebSocket-Version", "13")
        .header("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
        .body(())
        .context("failed to build Lighter websocket request")
}
