use crate::{
    constants::{
        DEFAULT_LIVE_MAX_ORDER_QUOTE_NOTIONAL, DEFAULT_LIVE_MAX_SYMBOL_EXPOSURE_QUOTE_NOTIONAL,
        DEFAULT_LIVE_OPEN_COOLDOWN_SECONDS, DEFAULT_LIVE_TRADING_ALLOW_OPENS,
        DEFAULT_MAX_HOLD_MINUTES, DEFAULT_SIM_ORDER_QUOTE_SIZE, DEFAULT_SIM_STOP_LOSS_PCT,
        DEFAULT_SIM_TAKE_PROFIT_PCT, DEFAULT_SIM_TAKE_PROFIT_PCT_2,
        DEFAULT_SIM_TAKE_PROFIT_RATIO_PCT, DEFAULT_SIM_TAKE_PROFIT_RATIO_PCT_2,
        DEFAULT_SPREAD_THRESHOLD_PCT,
    },
    models::{OrderStatus, PersistedConfig, SignalDirection, SignalRecord, SimulatedOrderRecord},
    util::unix_now_ms,
};
use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, params};
use std::{
    fs,
    path::Path,
    sync::{Arc, Mutex},
};

#[derive(Clone)]
pub(crate) struct Database {
    conn: Arc<Mutex<Connection>>,
}

#[derive(Debug, Clone)]
pub(crate) enum PersistenceTask {
    InsertSignalAndOrders {
        signal: SignalRecord,
        orders: Vec<SimulatedOrderRecord>,
    },
    UpdateOrder {
        order: SimulatedOrderRecord,
    },
}

impl Database {
    pub(crate) fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create data dir {}", parent.display()))?;
        }

        let conn = Connection::open(&path)
            .with_context(|| format!("failed to open database {}", path.display()))?;

        let db = Self {
            conn: Arc::new(Mutex::new(conn)),
        };
        db.init_schema()?;
        Ok(db)
    }

    fn init_schema(&self) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| anyhow::anyhow!("database mutex poisoned"))?;

        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS app_config (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                window_hours REAL NOT NULL,
                price_jump_threshold_pct REAL NOT NULL,
                open_interest_threshold REAL NOT NULL,
                spread_threshold_pct REAL NOT NULL,
                max_hold_minutes REAL NOT NULL,
                sim_order_quote_size REAL NOT NULL,
                sim_take_profit_pct REAL NOT NULL,
                sim_take_profit_ratio_pct REAL NOT NULL,
                sim_take_profit_pct_2 REAL NOT NULL,
                sim_take_profit_ratio_pct_2 REAL NOT NULL,
                sim_stop_loss_pct REAL NOT NULL,
                live_trading_enabled INTEGER NOT NULL DEFAULT 0,
                live_trading_allow_opens INTEGER NOT NULL DEFAULT 1,
                live_open_cooldown_seconds REAL NOT NULL DEFAULT 0,
                live_max_order_quote_notional REAL NOT NULL DEFAULT 0,
                live_max_symbol_exposure_quote_notional REAL NOT NULL DEFAULT 0,
                updated_at_ms INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS signal_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at_ms INTEGER NOT NULL,
                time_to_threshold_ms INTEGER,
                exchange TEXT NOT NULL,
                instrument TEXT NOT NULL,
                market_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                prev_last_trade_price TEXT NOT NULL,
                last_trade_price TEXT NOT NULL,
                window_low_price TEXT NOT NULL,
                window_high_price TEXT NOT NULL,
                move_pct REAL NOT NULL,
                open_interest TEXT NOT NULL,
                price_jump_threshold_pct REAL NOT NULL,
                open_interest_threshold REAL NOT NULL,
                spread_threshold_pct REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS simulated_orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_order_id TEXT NOT NULL UNIQUE,
                opened_at_ms INTEGER NOT NULL,
                closed_at_ms INTEGER,
                exchange TEXT NOT NULL,
                instrument TEXT NOT NULL,
                market_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                direction TEXT NOT NULL,
                status TEXT NOT NULL,
                entry_price TEXT NOT NULL,
                quantity_base TEXT NOT NULL,
                quote_notional REAL NOT NULL,
                take_profit_price TEXT NOT NULL,
                stop_loss_price TEXT NOT NULL,
                exit_price TEXT,
                exit_reason TEXT
            );
            "#,
        )
        .context("failed to initialize database schema")?;

        let mut app_config_columns = conn
            .prepare("PRAGMA table_info(app_config)")
            .context("failed to inspect app_config schema")?;
        let app_config_column_names = app_config_columns
            .query_map([], |row| row.get::<_, String>(1))
            .context("failed to query app_config columns")?
            .collect::<std::result::Result<Vec<_>, _>>()
            .context("failed to decode app_config columns")?;
        let has_spread_threshold_in_config = app_config_column_names
            .iter()
            .any(|column| column == "spread_threshold_pct");
        let has_max_hold_minutes_in_config = app_config_column_names
            .iter()
            .any(|column| column == "max_hold_minutes");
        let has_sim_order_quote_size_in_config = app_config_column_names
            .iter()
            .any(|column| column == "sim_order_quote_size");
        let has_sim_take_profit_pct_in_config = app_config_column_names
            .iter()
            .any(|column| column == "sim_take_profit_pct");
        let has_sim_take_profit_ratio_pct_in_config = app_config_column_names
            .iter()
            .any(|column| column == "sim_take_profit_ratio_pct");
        let has_sim_take_profit_pct_2_in_config = app_config_column_names
            .iter()
            .any(|column| column == "sim_take_profit_pct_2");
        let has_sim_take_profit_ratio_pct_2_in_config = app_config_column_names
            .iter()
            .any(|column| column == "sim_take_profit_ratio_pct_2");
        let has_sim_stop_loss_pct_in_config = app_config_column_names
            .iter()
            .any(|column| column == "sim_stop_loss_pct");
        let has_live_trading_enabled_in_config = app_config_column_names
            .iter()
            .any(|column| column == "live_trading_enabled");
        let has_live_trading_allow_opens_in_config = app_config_column_names
            .iter()
            .any(|column| column == "live_trading_allow_opens");
        let has_live_open_cooldown_seconds_in_config = app_config_column_names
            .iter()
            .any(|column| column == "live_open_cooldown_seconds");
        let has_live_max_order_quote_notional_in_config = app_config_column_names
            .iter()
            .any(|column| column == "live_max_order_quote_notional");
        let has_live_max_symbol_exposure_quote_notional_in_config = app_config_column_names
            .iter()
            .any(|column| column == "live_max_symbol_exposure_quote_notional");

        if !has_spread_threshold_in_config {
            conn.execute(
                &format!(
                    "ALTER TABLE app_config ADD COLUMN spread_threshold_pct REAL NOT NULL DEFAULT {}",
                    DEFAULT_SPREAD_THRESHOLD_PCT
                ),
                [],
            )
            .context("failed to add app_config.spread_threshold_pct column")?;
        }

        if !has_max_hold_minutes_in_config {
            conn.execute(
                &format!(
                    "ALTER TABLE app_config ADD COLUMN max_hold_minutes REAL NOT NULL DEFAULT {}",
                    DEFAULT_MAX_HOLD_MINUTES
                ),
                [],
            )
            .context("failed to add app_config.max_hold_minutes column")?;
        }

        if !has_sim_order_quote_size_in_config {
            conn.execute(
                &format!(
                    "ALTER TABLE app_config ADD COLUMN sim_order_quote_size REAL NOT NULL DEFAULT {}",
                    DEFAULT_SIM_ORDER_QUOTE_SIZE
                ),
                [],
            )
            .context("failed to add app_config.sim_order_quote_size column")?;
        }

        if !has_sim_take_profit_pct_in_config {
            conn.execute(
                &format!(
                    "ALTER TABLE app_config ADD COLUMN sim_take_profit_pct REAL NOT NULL DEFAULT {}",
                    DEFAULT_SIM_TAKE_PROFIT_PCT
                ),
                [],
            )
            .context("failed to add app_config.sim_take_profit_pct column")?;
        }

        if !has_sim_take_profit_ratio_pct_in_config {
            conn.execute(
                &format!(
                    "ALTER TABLE app_config ADD COLUMN sim_take_profit_ratio_pct REAL NOT NULL DEFAULT {}",
                    DEFAULT_SIM_TAKE_PROFIT_RATIO_PCT
                ),
                [],
            )
            .context("failed to add app_config.sim_take_profit_ratio_pct column")?;
        }

        if !has_sim_take_profit_pct_2_in_config {
            conn.execute(
                &format!(
                    "ALTER TABLE app_config ADD COLUMN sim_take_profit_pct_2 REAL NOT NULL DEFAULT {}",
                    DEFAULT_SIM_TAKE_PROFIT_PCT_2
                ),
                [],
            )
            .context("failed to add app_config.sim_take_profit_pct_2 column")?;
        }

        if !has_sim_take_profit_ratio_pct_2_in_config {
            conn.execute(
                &format!(
                    "ALTER TABLE app_config ADD COLUMN sim_take_profit_ratio_pct_2 REAL NOT NULL DEFAULT {}",
                    DEFAULT_SIM_TAKE_PROFIT_RATIO_PCT_2
                ),
                [],
            )
            .context("failed to add app_config.sim_take_profit_ratio_pct_2 column")?;
        }

        if !has_sim_stop_loss_pct_in_config {
            conn.execute(
                &format!(
                    "ALTER TABLE app_config ADD COLUMN sim_stop_loss_pct REAL NOT NULL DEFAULT {}",
                    DEFAULT_SIM_STOP_LOSS_PCT
                ),
                [],
            )
            .context("failed to add app_config.sim_stop_loss_pct column")?;
        }

        if !has_live_trading_enabled_in_config {
            conn.execute(
                "ALTER TABLE app_config ADD COLUMN live_trading_enabled INTEGER NOT NULL DEFAULT 0",
                [],
            )
            .context("failed to add app_config.live_trading_enabled column")?;
        }

        if !has_live_trading_allow_opens_in_config {
            conn.execute(
                &format!(
                    "ALTER TABLE app_config ADD COLUMN live_trading_allow_opens INTEGER NOT NULL DEFAULT {}",
                    if DEFAULT_LIVE_TRADING_ALLOW_OPENS { 1 } else { 0 }
                ),
                [],
            )
            .context("failed to add app_config.live_trading_allow_opens column")?;
        }

        if !has_live_open_cooldown_seconds_in_config {
            conn.execute(
                &format!(
                    "ALTER TABLE app_config ADD COLUMN live_open_cooldown_seconds REAL NOT NULL DEFAULT {}",
                    DEFAULT_LIVE_OPEN_COOLDOWN_SECONDS
                ),
                [],
            )
            .context("failed to add app_config.live_open_cooldown_seconds column")?;
        }

        if !has_live_max_order_quote_notional_in_config {
            conn.execute(
                &format!(
                    "ALTER TABLE app_config ADD COLUMN live_max_order_quote_notional REAL NOT NULL DEFAULT {}",
                    DEFAULT_LIVE_MAX_ORDER_QUOTE_NOTIONAL
                ),
                [],
            )
            .context("failed to add app_config.live_max_order_quote_notional column")?;
        }

        if !has_live_max_symbol_exposure_quote_notional_in_config {
            conn.execute(
                &format!(
                    "ALTER TABLE app_config ADD COLUMN live_max_symbol_exposure_quote_notional REAL NOT NULL DEFAULT {}",
                    DEFAULT_LIVE_MAX_SYMBOL_EXPOSURE_QUOTE_NOTIONAL
                ),
                [],
            )
            .context("failed to add app_config.live_max_symbol_exposure_quote_notional column")?;
        }

        let mut signal_columns = conn
            .prepare("PRAGMA table_info(signal_events)")
            .context("failed to inspect signal_events schema")?;
        let signal_column_meta = signal_columns
            .query_map([], |row| {
                Ok((row.get::<_, String>(1)?, row.get::<_, i64>(3)? != 0))
            })
            .context("failed to query signal_events columns")?
            .collect::<std::result::Result<Vec<_>, _>>()
            .context("failed to decode signal_events columns")?;
        let has_direction = signal_column_meta
            .iter()
            .any(|(column, _)| column == "direction");
        let has_exchange_in_signals = signal_column_meta
            .iter()
            .any(|(column, _)| column == "exchange");
        let has_instrument_in_signals = signal_column_meta
            .iter()
            .any(|(column, _)| column == "instrument");
        let has_spread_threshold_in_signals = signal_column_meta
            .iter()
            .any(|(column, _)| column == "spread_threshold_pct");
        let has_time_to_threshold_in_signals = signal_column_meta
            .iter()
            .any(|(column, _)| column == "time_to_threshold_ms");
        let time_to_threshold_not_null = signal_column_meta
            .iter()
            .find(|(column, _)| column == "time_to_threshold_ms")
            .is_some_and(|(_, not_null)| *not_null);

        if !has_direction {
            conn.execute(
                "ALTER TABLE signal_events ADD COLUMN direction TEXT NOT NULL DEFAULT 'bullish'",
                [],
            )
            .context("failed to add signal_events.direction column")?;
        }

        if !has_exchange_in_signals {
            conn.execute(
                "ALTER TABLE signal_events ADD COLUMN exchange TEXT NOT NULL DEFAULT 'lighter'",
                [],
            )
            .context("failed to add signal_events.exchange column")?;
        }

        if !has_instrument_in_signals {
            conn.execute(
                "ALTER TABLE signal_events ADD COLUMN instrument TEXT NOT NULL DEFAULT ''",
                [],
            )
            .context("failed to add signal_events.instrument column")?;
            conn.execute(
                "UPDATE signal_events SET instrument = CAST(market_id AS TEXT) WHERE instrument = ''",
                [],
            )
            .context("failed to backfill signal_events.instrument column")?;
        }

        if !has_spread_threshold_in_signals {
            conn.execute(
                &format!(
                    "ALTER TABLE signal_events ADD COLUMN spread_threshold_pct REAL NOT NULL DEFAULT {}",
                    DEFAULT_SPREAD_THRESHOLD_PCT
                ),
                [],
            )
            .context("failed to add signal_events.spread_threshold_pct column")?;
        }

        if !has_time_to_threshold_in_signals {
            conn.execute(
                "ALTER TABLE signal_events ADD COLUMN time_to_threshold_ms INTEGER",
                [],
            )
            .context("failed to add signal_events.time_to_threshold_ms column")?;
        }

        if time_to_threshold_not_null {
            conn.execute_batch(
                r#"
                ALTER TABLE signal_events RENAME TO signal_events_old;

                CREATE TABLE signal_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at_ms INTEGER NOT NULL,
                    time_to_threshold_ms INTEGER,
                    exchange TEXT NOT NULL,
                    instrument TEXT NOT NULL,
                    market_id INTEGER NOT NULL,
                    symbol TEXT NOT NULL,
                    prev_last_trade_price TEXT NOT NULL,
                    last_trade_price TEXT NOT NULL,
                    window_low_price TEXT NOT NULL,
                    window_high_price TEXT NOT NULL,
                    move_pct REAL NOT NULL,
                    open_interest TEXT NOT NULL,
                    price_jump_threshold_pct REAL NOT NULL,
                    open_interest_threshold REAL NOT NULL,
                    spread_threshold_pct REAL NOT NULL,
                    direction TEXT NOT NULL DEFAULT 'bullish'
                );

                INSERT INTO signal_events (
                    id,
                    created_at_ms,
                    time_to_threshold_ms,
                    exchange,
                    instrument,
                    market_id,
                    symbol,
                    prev_last_trade_price,
                    last_trade_price,
                    window_low_price,
                    window_high_price,
                    move_pct,
                    open_interest,
                    price_jump_threshold_pct,
                    open_interest_threshold,
                    spread_threshold_pct,
                    direction
                )
                SELECT
                    id,
                    created_at_ms,
                    NULLIF(time_to_threshold_ms, 0),
                    COALESCE(exchange, 'lighter'),
                    CASE
                        WHEN COALESCE(instrument, '') = '' THEN CAST(market_id AS TEXT)
                        ELSE instrument
                    END,
                    market_id,
                    symbol,
                    prev_last_trade_price,
                    last_trade_price,
                    window_low_price,
                    window_high_price,
                    move_pct,
                    open_interest,
                    price_jump_threshold_pct,
                    open_interest_threshold,
                    spread_threshold_pct,
                    COALESCE(direction, 'bullish')
                FROM signal_events_old;

                DROP TABLE signal_events_old;
                "#,
            )
            .context("failed to migrate signal_events.time_to_threshold_ms to nullable")?;
        }

        let exists = conn
            .query_row("SELECT 1 FROM app_config WHERE id = 1", [], |row| {
                row.get::<_, i64>(0)
            })
            .optional()
            .context("failed to check app_config row")?;

        if exists.is_none() {
            let defaults = PersistedConfig::default();
            conn.execute(
                "INSERT INTO app_config (id, window_hours, price_jump_threshold_pct, open_interest_threshold, spread_threshold_pct, max_hold_minutes, sim_order_quote_size, sim_take_profit_pct, sim_take_profit_ratio_pct, sim_take_profit_pct_2, sim_take_profit_ratio_pct_2, sim_stop_loss_pct, live_trading_enabled, live_trading_allow_opens, live_open_cooldown_seconds, live_max_order_quote_notional, live_max_symbol_exposure_quote_notional, updated_at_ms) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                params![
                    defaults.window_hours,
                    defaults.price_jump_threshold_pct,
                    defaults.open_interest_threshold,
                    defaults.spread_threshold_pct,
                    defaults.max_hold_minutes,
                    defaults.sim_order_quote_size,
                    defaults.sim_take_profit_pct,
                    defaults.sim_take_profit_ratio_pct,
                    defaults.sim_take_profit_pct_2,
                    defaults.sim_take_profit_ratio_pct_2,
                    defaults.sim_stop_loss_pct,
                    defaults.live_trading_enabled,
                    defaults.live_trading_allow_opens,
                    defaults.live_open_cooldown_seconds,
                    defaults.live_max_order_quote_notional,
                    defaults.live_max_symbol_exposure_quote_notional,
                    unix_now_ms() as i64,
                ],
            )
            .context("failed to seed app_config")?;
        }

        let mut order_columns = conn
            .prepare("PRAGMA table_info(simulated_orders)")
            .context("failed to inspect simulated_orders schema")?;
        let order_column_names = order_columns
            .query_map([], |row| row.get::<_, String>(1))
            .context("failed to query simulated_orders columns")?
            .collect::<std::result::Result<Vec<_>, _>>()
            .context("failed to decode simulated_orders columns")?;
        let has_exchange_in_orders = order_column_names.iter().any(|column| column == "exchange");
        let has_instrument_in_orders = order_column_names
            .iter()
            .any(|column| column == "instrument");
        let has_stop_loss_price_in_orders = order_column_names
            .iter()
            .any(|column| column == "stop_loss_price");

        if !has_exchange_in_orders {
            conn.execute(
                "ALTER TABLE simulated_orders ADD COLUMN exchange TEXT NOT NULL DEFAULT 'lighter'",
                [],
            )
            .context("failed to add simulated_orders.exchange column")?;
        }

        if !has_instrument_in_orders {
            conn.execute(
                "ALTER TABLE simulated_orders ADD COLUMN instrument TEXT NOT NULL DEFAULT ''",
                [],
            )
            .context("failed to add simulated_orders.instrument column")?;
            conn.execute(
                "UPDATE simulated_orders SET instrument = CAST(market_id AS TEXT) WHERE instrument = ''",
                [],
            )
            .context("failed to backfill simulated_orders.instrument column")?;
        }

        if !has_stop_loss_price_in_orders {
            conn.execute(
                "ALTER TABLE simulated_orders ADD COLUMN stop_loss_price TEXT NOT NULL DEFAULT '--'",
                [],
            )
            .context("failed to add simulated_orders.stop_loss_price column")?;
        }

        Ok(())
    }

    pub(crate) fn load_config(&self) -> Result<PersistedConfig> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| anyhow::anyhow!("database mutex poisoned"))?;

        let config = conn
            .query_row(
                "SELECT window_hours, price_jump_threshold_pct, open_interest_threshold, spread_threshold_pct, max_hold_minutes, sim_order_quote_size, sim_take_profit_pct, sim_take_profit_ratio_pct, sim_take_profit_pct_2, sim_take_profit_ratio_pct_2, sim_stop_loss_pct, live_trading_enabled, live_trading_allow_opens, live_open_cooldown_seconds, live_max_order_quote_notional, live_max_symbol_exposure_quote_notional FROM app_config WHERE id = 1",
                [],
                |row| {
                    Ok(PersistedConfig {
                        window_hours: row.get(0)?,
                        price_jump_threshold_pct: row.get(1)?,
                        open_interest_threshold: row.get(2)?,
                        spread_threshold_pct: row.get(3)?,
                        max_hold_minutes: row.get(4)?,
                        sim_order_quote_size: row.get(5)?,
                        sim_take_profit_pct: row.get(6)?,
                        sim_take_profit_ratio_pct: row.get(7)?,
                        sim_take_profit_pct_2: row.get(8)?,
                        sim_take_profit_ratio_pct_2: row.get(9)?,
                        sim_stop_loss_pct: row.get(10)?,
                        live_trading_enabled: row.get(11)?,
                        live_trading_allow_opens: row.get(12)?,
                        live_open_cooldown_seconds: row.get(13)?,
                        live_max_order_quote_notional: row.get(14)?,
                        live_max_symbol_exposure_quote_notional: row.get(15)?,
                    })
                },
            )
            .context("failed to load config from database")?;

        Ok(config)
    }

    pub(crate) fn save_window_hours(&self, window_hours: f64) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| anyhow::anyhow!("database mutex poisoned"))?;

        conn.execute(
            "UPDATE app_config SET window_hours = ?, updated_at_ms = ? WHERE id = 1",
            params![window_hours, unix_now_ms() as i64],
        )
        .context("failed to save window_hours")?;

        Ok(())
    }

    pub(crate) fn save_signal_thresholds(
        &self,
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
    ) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| anyhow::anyhow!("database mutex poisoned"))?;

        conn.execute(
            "UPDATE app_config SET price_jump_threshold_pct = ?, open_interest_threshold = ?, spread_threshold_pct = ?, max_hold_minutes = ?, sim_order_quote_size = ?, sim_take_profit_pct = ?, sim_take_profit_ratio_pct = ?, sim_take_profit_pct_2 = ?, sim_take_profit_ratio_pct_2 = ?, sim_stop_loss_pct = ?, live_trading_enabled = ?, live_trading_allow_opens = ?, live_open_cooldown_seconds = ?, live_max_order_quote_notional = ?, live_max_symbol_exposure_quote_notional = ?, updated_at_ms = ? WHERE id = 1",
            params![
                price_jump_threshold_pct,
                open_interest_threshold,
                spread_threshold_pct,
                max_hold_minutes,
                sim_order_quote_size,
                sim_take_profit_pct,
                sim_take_profit_ratio_pct,
                sim_take_profit_pct_2,
                sim_take_profit_ratio_pct_2,
                sim_stop_loss_pct,
                live_trading_enabled,
                live_trading_allow_opens,
                live_open_cooldown_seconds,
                live_max_order_quote_notional,
                live_max_symbol_exposure_quote_notional,
                unix_now_ms() as i64
            ],
        )
        .context("failed to save signal thresholds")?;

        Ok(())
    }

    pub(crate) fn insert_signal_and_orders(
        &self,
        signal: &SignalRecord,
        orders: &[SimulatedOrderRecord],
    ) -> Result<()> {
        let mut conn = self
            .conn
            .lock()
            .map_err(|_| anyhow::anyhow!("database mutex poisoned"))?;
        let tx = conn
            .transaction()
            .context("failed to start signal/order transaction")?;

        tx.execute(
            "INSERT INTO signal_events (created_at_ms, time_to_threshold_ms, exchange, instrument, market_id, symbol, direction, prev_last_trade_price, last_trade_price, window_low_price, window_high_price, move_pct, open_interest, price_jump_threshold_pct, open_interest_threshold, spread_threshold_pct)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            params![
                signal.created_at_ms as i64,
                signal.time_to_threshold_ms.map(|value| value as i64),
                signal.exchange,
                signal.instrument,
                signal.instrument.parse::<i64>().unwrap_or_default(),
                signal.symbol,
                signal.direction.as_str(),
                signal.prev_last_trade_price,
                signal.last_trade_price,
                signal.window_low_price,
                signal.window_high_price,
                signal.move_pct,
                signal.open_interest,
                signal.price_jump_threshold_pct,
                signal.open_interest_threshold,
                signal.spread_threshold_pct,
            ],
        )
        .context("failed to insert signal event")?;

        for order in orders {
            tx.execute(
                "INSERT INTO simulated_orders (client_order_id, opened_at_ms, closed_at_ms, exchange, instrument, market_id, symbol, direction, status, entry_price, quantity_base, quote_notional, take_profit_price, stop_loss_price, exit_price, exit_reason)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                params![
                    order.client_order_id,
                    order.opened_at_ms as i64,
                    order.closed_at_ms.map(|value| value as i64),
                    order.exchange,
                    order.instrument,
                    order.instrument.parse::<i64>().unwrap_or_default(),
                    order.symbol,
                    order.direction.as_str(),
                    order.status.as_str(),
                    order.entry_price,
                    order.quantity_base,
                    order.quote_notional,
                    order.take_profit_price,
                    order.stop_loss_price,
                    order.exit_price,
                    order.exit_reason,
                ],
            )
            .context("failed to insert simulated order")?;
        }

        tx.commit()
            .context("failed to commit signal/order transaction")?;
        Ok(())
    }

    pub(crate) fn load_recent_signals(&self, limit: usize) -> Result<Vec<SignalRecord>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| anyhow::anyhow!("database mutex poisoned"))?;

        let mut stmt = conn
            .prepare(
                "SELECT id, created_at_ms, time_to_threshold_ms, exchange, instrument, market_id, symbol, direction, prev_last_trade_price, last_trade_price, window_low_price, window_high_price, move_pct, open_interest, price_jump_threshold_pct, open_interest_threshold, spread_threshold_pct
                 FROM signal_events
                 ORDER BY created_at_ms DESC
                 LIMIT ?",
            )
            .context("failed to prepare signal query")?;

        let rows = stmt
            .query_map(params![limit as i64], |row| {
                let direction = row.get::<_, String>(7)?;
                Ok(SignalRecord {
                    id: row.get(0)?,
                    created_at_ms: row.get::<_, i64>(1)? as u64,
                    time_to_threshold_ms: row
                        .get::<_, Option<i64>>(2)?
                        .and_then(|value| (value > 0).then_some(value as u64)),
                    exchange: row.get(3)?,
                    instrument: row.get(4)?,
                    symbol: row.get(6)?,
                    direction: SignalDirection::from_db(&direction).ok_or_else(|| {
                        rusqlite::Error::FromSqlConversionFailure(
                            7,
                            rusqlite::types::Type::Text,
                            Box::new(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                format!("unknown signal direction: {direction}"),
                            )),
                        )
                    })?,
                    prev_last_trade_price: row.get(8)?,
                    last_trade_price: row.get(9)?,
                    window_low_price: row.get(10)?,
                    window_high_price: row.get(11)?,
                    move_pct: row.get(12)?,
                    open_interest: row.get(13)?,
                    price_jump_threshold_pct: row.get(14)?,
                    open_interest_threshold: row.get(15)?,
                    spread_threshold_pct: row.get(16)?,
                })
            })
            .context("failed to query recent signals")?;

        let mut signals = Vec::new();
        for row in rows {
            signals.push(row.context("failed to decode signal row")?);
        }

        Ok(signals)
    }

    pub(crate) fn update_order(&self, order: &SimulatedOrderRecord) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| anyhow::anyhow!("database mutex poisoned"))?;

        conn.execute(
            "UPDATE simulated_orders
             SET closed_at_ms = ?, status = ?, exit_price = ?, exit_reason = ?
             WHERE client_order_id = ?",
            params![
                order.closed_at_ms.map(|value| value as i64),
                order.status.as_str(),
                order.exit_price,
                order.exit_reason,
                order.client_order_id,
            ],
        )
        .context("failed to update simulated order")?;

        Ok(())
    }

    pub(crate) fn load_recent_orders(&self, limit: usize) -> Result<Vec<SimulatedOrderRecord>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| anyhow::anyhow!("database mutex poisoned"))?;

        let mut stmt = conn
            .prepare(
                "SELECT id, client_order_id, opened_at_ms, closed_at_ms, exchange, instrument, market_id, symbol, direction, status, entry_price, quantity_base, quote_notional, take_profit_price, stop_loss_price, exit_price, exit_reason
                 FROM simulated_orders
                 ORDER BY opened_at_ms DESC
                 LIMIT ?",
            )
            .context("failed to prepare simulated order query")?;

        let rows = stmt
            .query_map(params![limit as i64], |row| {
                let direction = row.get::<_, String>(8)?;
                let status = row.get::<_, String>(9)?;
                Ok(SimulatedOrderRecord {
                    id: row.get(0)?,
                    client_order_id: row.get(1)?,
                    opened_at_ms: row.get::<_, i64>(2)? as u64,
                    closed_at_ms: row.get::<_, Option<i64>>(3)?.map(|value| value as u64),
                    exchange: row.get(4)?,
                    instrument: row.get(5)?,
                    symbol: row.get(7)?,
                    direction: SignalDirection::from_db(&direction).ok_or_else(|| {
                        rusqlite::Error::FromSqlConversionFailure(
                            8,
                            rusqlite::types::Type::Text,
                            Box::new(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                format!("unknown order direction: {direction}"),
                            )),
                        )
                    })?,
                    status: OrderStatus::from_db(&status).ok_or_else(|| {
                        rusqlite::Error::FromSqlConversionFailure(
                            9,
                            rusqlite::types::Type::Text,
                            Box::new(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                format!("unknown order status: {status}"),
                            )),
                        )
                    })?,
                    entry_price: row.get(10)?,
                    quantity_base: row.get(11)?,
                    quote_notional: row.get(12)?,
                    take_profit_price: row.get(13)?,
                    stop_loss_price: row.get(14)?,
                    exit_price: row.get(15)?,
                    exit_reason: row.get(16)?,
                })
            })
            .context("failed to query simulated orders")?;

        let mut orders = Vec::new();
        for row in rows {
            orders.push(row.context("failed to decode simulated order row")?);
        }

        Ok(orders)
    }

    pub(crate) fn load_closed_orders_since(
        &self,
        since_ms: Option<u64>,
        exchange_filter: Option<&str>,
        direction_filter: Option<&str>,
    ) -> Result<Vec<SimulatedOrderRecord>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| anyhow::anyhow!("database mutex poisoned"))?;

        let mut sql = String::from(
            "SELECT id, client_order_id, opened_at_ms, closed_at_ms, exchange, instrument, market_id, symbol, direction, status, entry_price, quantity_base, quote_notional, take_profit_price, stop_loss_price, exit_price, exit_reason
             FROM simulated_orders
             WHERE status = 'closed' AND closed_at_ms IS NOT NULL",
        );
        if since_ms.is_some() {
            sql.push_str(" AND closed_at_ms >= ?1");
        }
        if exchange_filter.is_some() {
            let param_idx = if since_ms.is_some() { 2 } else { 1 };
            sql.push_str(&format!(" AND LOWER(exchange) = ?{param_idx}"));
        }
        if direction_filter.is_some() {
            let param_idx = match (since_ms.is_some(), exchange_filter.is_some()) {
                (true, true) => 3,
                (true, false) | (false, true) => 2,
                (false, false) => 1,
            };
            sql.push_str(&format!(" AND direction = ?{param_idx}"));
        }
        sql.push_str(" ORDER BY closed_at_ms ASC, opened_at_ms ASC");

        let mut stmt = conn
            .prepare(&sql)
            .context("failed to prepare closed orders query")?;

        let decode_row = |row: &rusqlite::Row<'_>| {
            let direction = row.get::<_, String>(8)?;
            let status = row.get::<_, String>(9)?;
            Ok(SimulatedOrderRecord {
                id: row.get(0)?,
                client_order_id: row.get(1)?,
                opened_at_ms: row.get::<_, i64>(2)? as u64,
                closed_at_ms: row.get::<_, Option<i64>>(3)?.map(|value| value as u64),
                exchange: row.get(4)?,
                instrument: row.get(5)?,
                symbol: row.get(7)?,
                direction: SignalDirection::from_db(&direction).ok_or_else(|| {
                    rusqlite::Error::FromSqlConversionFailure(
                        8,
                        rusqlite::types::Type::Text,
                        Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("unknown order direction: {direction}"),
                        )),
                    )
                })?,
                status: OrderStatus::from_db(&status).ok_or_else(|| {
                    rusqlite::Error::FromSqlConversionFailure(
                        9,
                        rusqlite::types::Type::Text,
                        Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("unknown order status: {status}"),
                        )),
                    )
                })?,
                entry_price: row.get(10)?,
                quantity_base: row.get(11)?,
                quote_notional: row.get(12)?,
                take_profit_price: row.get(13)?,
                stop_loss_price: row.get(14)?,
                exit_price: row.get(15)?,
                exit_reason: row.get(16)?,
            })
        };

        let rows = match (since_ms, exchange_filter, direction_filter) {
            (Some(since), Some(ex), Some(dir)) => {
                stmt.query_map(params![since as i64, ex, dir], decode_row)
            }
            (Some(since), Some(ex), None) => {
                stmt.query_map(params![since as i64, ex], decode_row)
            }
            (Some(since), None, Some(dir)) => {
                stmt.query_map(params![since as i64, dir], decode_row)
            }
            (Some(since), None, None) => stmt.query_map(params![since as i64], decode_row),
            (None, Some(ex), Some(dir)) => stmt.query_map(params![ex, dir], decode_row),
            (None, Some(ex), None) => stmt.query_map(params![ex], decode_row),
            (None, None, Some(dir)) => stmt.query_map(params![dir], decode_row),
            (None, None, None) => stmt.query_map([], decode_row),
        }
        .context("failed to query closed orders")?;

        let mut orders = Vec::new();
        for row in rows {
            orders.push(row.context("failed to decode closed order row")?);
        }

        Ok(orders)
    }
}
