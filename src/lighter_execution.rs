use crate::{
    constants::REST_BASE,
    models::{SignalDirection, SimulatedOrderRecord},
    support::wait_or_shutdown,
};
use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::HashMap, env, path::PathBuf, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines},
    process::{Child, ChildStderr, ChildStdin, ChildStdout, Command},
    sync::mpsc,
    sync::watch,
};
use tracing::{info, warn};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LighterExecutionMarket {
    pub(crate) market_id: u64,
    pub(crate) price_decimals: u32,
    pub(crate) size_decimals: u32,
    pub(crate) min_base_amount: String,
    pub(crate) best_bid_price: String,
    pub(crate) best_ask_price: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LighterMarketOrderAction {
    OpenLong,
    OpenShort,
    CloseLong,
    CloseShort,
}

#[derive(Debug, Clone)]
pub(crate) enum LighterExecutionTask {
    Open {
        order: SimulatedOrderRecord,
        market: LighterExecutionMarket,
    },
    Close {
        order: SimulatedOrderRecord,
        market: LighterExecutionMarket,
    },
}

pub(crate) fn base_quantity_to_base_amount(
    base_quantity: f64,
    market: &LighterExecutionMarket,
) -> Result<u64> {
    if !(base_quantity.is_finite() && base_quantity > 0.0) {
        bail!("base quantity must be positive");
    }

    let scale = 10f64.powi(market.size_decimals as i32);
    if !(scale.is_finite() && scale > 0.0) {
        bail!("invalid Lighter size scale for {}", market.market_id);
    }

    let min_base_amount = market.min_base_amount.parse::<f64>().with_context(|| {
        format!(
            "invalid Lighter minimum base amount for {}",
            market.market_id
        )
    })?;
    let scaled = (base_quantity * scale).floor();
    if scaled < 1.0 {
        bail!("calculated Lighter base amount must be positive");
    }

    let normalized_base_quantity = scaled / scale;
    if normalized_base_quantity + 1e-12 < min_base_amount {
        bail!(
            "calculated Lighter base quantity {} is below Lighter minimum {}",
            normalized_base_quantity,
            min_base_amount
        );
    }

    Ok(scaled as u64)
}

pub(crate) fn price_limit_for_market_order(
    action: LighterMarketOrderAction,
    market: &LighterExecutionMarket,
    slippage_ratio: f64,
) -> Result<u32> {
    if !(slippage_ratio.is_finite() && slippage_ratio >= 0.0) {
        bail!("slippage ratio must be a non-negative number");
    }

    let reference_price = match action {
        LighterMarketOrderAction::OpenLong | LighterMarketOrderAction::CloseShort => market
            .best_ask_price
            .parse::<f64>()
            .with_context(|| format!("invalid Lighter best ask price for {}", market.market_id))?,
        LighterMarketOrderAction::OpenShort | LighterMarketOrderAction::CloseLong => market
            .best_bid_price
            .parse::<f64>()
            .with_context(|| format!("invalid Lighter best bid price for {}", market.market_id))?,
    };
    if !(reference_price.is_finite() && reference_price > 0.0) {
        bail!("reference price must be positive");
    }

    let adjusted_price = match action {
        LighterMarketOrderAction::OpenLong | LighterMarketOrderAction::CloseShort => {
            reference_price * (1.0 + slippage_ratio)
        }
        LighterMarketOrderAction::OpenShort | LighterMarketOrderAction::CloseLong => {
            reference_price * (1.0 - slippage_ratio)
        }
    };
    if !(adjusted_price.is_finite() && adjusted_price > 0.0) {
        bail!("calculated price limit must be positive");
    }

    let scale = 10f64.powi(market.price_decimals as i32);
    let scaled = match action {
        LighterMarketOrderAction::OpenLong | LighterMarketOrderAction::CloseShort => {
            (adjusted_price * scale).ceil()
        }
        LighterMarketOrderAction::OpenShort | LighterMarketOrderAction::CloseLong => {
            (adjusted_price * scale).floor()
        }
    };
    if !(scaled.is_finite() && scaled > 0.0 && scaled <= u32::MAX as f64) {
        bail!("calculated price limit is out of range");
    }

    Ok(scaled as u32)
}

#[derive(Debug, Clone)]
pub(crate) struct LighterExecutionConfig {
    python_bin: String,
    script_path: PathBuf,
    base_url: String,
    account_index: u64,
    api_key_index: u32,
    api_private_key: String,
    market_order_slippage_ratio: f64,
}

impl LighterExecutionConfig {
    pub(crate) fn from_env() -> Result<Option<Self>> {
        let api_private_key = match env::var("LIGHTER_API_PRIVATE_KEY") {
            Ok(value) if !value.trim().is_empty() => value,
            Ok(_) | Err(env::VarError::NotPresent) => return Ok(None),
            Err(error) => return Err(error.into()),
        };

        let account_index = env::var("LIGHTER_ACCOUNT_INDEX")
            .map_err(|error| anyhow!("missing LIGHTER_ACCOUNT_INDEX: {error}"))?
            .parse::<u64>()
            .context("LIGHTER_ACCOUNT_INDEX must be an integer")?;
        let api_key_index = env::var("LIGHTER_API_KEY_INDEX")
            .map_err(|error| anyhow!("missing LIGHTER_API_KEY_INDEX: {error}"))?
            .parse::<u32>()
            .context("LIGHTER_API_KEY_INDEX must be an integer")?;
        if api_key_index <= 3 {
            bail!(
                "LIGHTER_API_KEY_INDEX must be greater than 3 because 0-3 are reserved by Lighter"
            );
        }

        let market_order_slippage_ratio = env::var("LIGHTER_MARKET_ORDER_MAX_SLIPPAGE_RATIO")
            .ok()
            .and_then(|value| value.parse::<f64>().ok())
            .unwrap_or(0.01);
        if !(market_order_slippage_ratio.is_finite() && market_order_slippage_ratio >= 0.0) {
            bail!("LIGHTER_MARKET_ORDER_MAX_SLIPPAGE_RATIO must be a non-negative number");
        }

        let script_path = std::env::current_dir()
            .context("failed to read current working directory")?
            .join("scripts")
            .join("lighter_signer_bridge.py");
        let base_url = env::var("LIGHTER_API_BASE_URL")
            .unwrap_or_else(|_| REST_BASE.trim_end_matches("/api/v1").to_string());
        let python_bin = env::var("LIGHTER_PYTHON_BIN").unwrap_or_else(|_| "python3".to_string());
        let probe = std::process::Command::new(&python_bin)
            .arg("-c")
            .arg("import lighter")
            .output()
            .with_context(|| format!("failed to execute {python_bin}"))?;
        if !probe.status.success() {
            bail!(
                "failed to import the official Lighter Python SDK with {}: {}",
                python_bin,
                String::from_utf8_lossy(&probe.stderr).trim()
            );
        }

        Ok(Some(Self {
            python_bin,
            script_path,
            base_url,
            account_index,
            api_key_index,
            api_private_key,
            market_order_slippage_ratio,
        }))
    }
}

#[derive(Debug, Serialize)]
struct LighterBridgeRequest {
    market_index: u64,
    client_order_index: u64,
    base_amount: u64,
    price: u32,
    is_ask: bool,
    reduce_only: bool,
}

#[derive(Debug, Deserialize)]
struct LighterBridgeReady {
    ready: bool,
    error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct LighterBridgeResponse {
    ok: bool,
    error: Option<String>,
    response: Option<Value>,
}

struct LighterBridge {
    child: Child,
    stdin: ChildStdin,
    stdout: Lines<BufReader<ChildStdout>>,
}

impl LighterBridge {
    async fn spawn(config: &LighterExecutionConfig) -> Result<Self> {
        let mut command = Command::new(&config.python_bin);
        command
            .arg("-u")
            .arg(&config.script_path)
            .env("LIGHTER_API_BASE_URL", &config.base_url)
            .env("LIGHTER_ACCOUNT_INDEX", config.account_index.to_string())
            .env("LIGHTER_API_KEY_INDEX", config.api_key_index.to_string())
            .env("LIGHTER_API_PRIVATE_KEY", &config.api_private_key)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let mut child = command
            .spawn()
            .with_context(|| format!("failed to spawn {}", config.script_path.display()))?;
        let stdin = child.stdin.take().context("missing Lighter bridge stdin")?;
        let stdout = child
            .stdout
            .take()
            .context("missing Lighter bridge stdout")?;
        if let Some(stderr) = child.stderr.take() {
            spawn_bridge_stderr_logger(stderr);
        }
        let mut stdout = BufReader::new(stdout).lines();
        let ready_line = stdout
            .next_line()
            .await
            .context("failed to read Lighter bridge startup message")?
            .context("Lighter bridge exited before startup completed")?;
        let ready: LighterBridgeReady = serde_json::from_str(&ready_line)
            .context("failed to decode Lighter bridge startup message")?;
        if !ready.ready {
            bail!(
                "Lighter bridge failed to initialize: {}",
                ready.error.unwrap_or_else(|| "unknown error".to_string())
            );
        }

        Ok(Self {
            child,
            stdin,
            stdout,
        })
    }

    async fn request(&mut self, request: &LighterBridgeRequest) -> Result<Value> {
        let payload =
            serde_json::to_vec(request).context("failed to encode Lighter bridge request")?;
        self.stdin
            .write_all(&payload)
            .await
            .context("failed to write Lighter bridge request")?;
        self.stdin
            .write_all(b"\n")
            .await
            .context("failed to finalize Lighter bridge request")?;
        self.stdin
            .flush()
            .await
            .context("failed to flush Lighter bridge stdin")?;

        let line = self
            .stdout
            .next_line()
            .await
            .context("failed to read Lighter bridge response")?
            .context("Lighter bridge exited while waiting for response")?;
        let response: LighterBridgeResponse =
            serde_json::from_str(&line).context("failed to decode Lighter bridge response")?;
        if !response.ok {
            bail!(
                "Lighter bridge rejected request: {}",
                response
                    .error
                    .unwrap_or_else(|| "unknown error".to_string())
            );
        }
        response
            .response
            .context("missing Lighter bridge response payload")
    }

    async fn shutdown(mut self) {
        let _ = self.child.kill().await;
        let _ = self.child.wait().await;
    }
}

fn spawn_bridge_stderr_logger(stderr: ChildStderr) {
    tokio::spawn(async move {
        let mut lines = BufReader::new(stderr).lines();
        loop {
            match lines.next_line().await {
                Ok(Some(line)) => warn!(line, "Lighter bridge stderr"),
                Ok(None) => break,
                Err(error) => {
                    warn!(?error, "failed to read Lighter bridge stderr");
                    break;
                }
            }
        }
    });
}

pub(crate) async fn run_lighter_execution_worker(
    config: LighterExecutionConfig,
    mut execution_rx: mpsc::UnboundedReceiver<LighterExecutionTask>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<()> {
    let mut opened_amounts = HashMap::<String, u64>::new();
    let mut bridge = None::<LighterBridge>;
    let mut order_counter = 0u64;

    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => break,
            maybe_task = execution_rx.recv() => {
                let Some(task) = maybe_task else {
                    break;
                };
                if let Err(error) = process_execution_task(&config, &mut bridge, &mut opened_amounts, &mut order_counter, task).await {
                    warn!(?error, "Lighter live execution task failed");
                    if let Some(active_bridge) = bridge.take() {
                        active_bridge.shutdown().await;
                    }
                    if wait_or_shutdown(&mut shutdown_rx, Duration::from_millis(50)).await {
                        break;
                    }
                }
            }
        }
    }

    if let Some(active_bridge) = bridge.take() {
        active_bridge.shutdown().await;
    }

    Ok(())
}

async fn process_execution_task(
    config: &LighterExecutionConfig,
    bridge: &mut Option<LighterBridge>,
    opened_amounts: &mut HashMap<String, u64>,
    order_counter: &mut u64,
    task: LighterExecutionTask,
) -> Result<()> {
    if opened_amounts.len() > 500 {
        warn!(
            count = opened_amounts.len(),
            "opened_amounts map is large; possible orphaned entries from missed close tasks"
        );
    }
    let (order, market, phase) = match task {
        LighterExecutionTask::Open { order, market } => (order, market, "open"),
        LighterExecutionTask::Close { order, market } => (order, market, "close"),
    };
    let action = match (phase, &order.direction) {
        ("open", SignalDirection::Bullish) => LighterMarketOrderAction::OpenLong,
        ("open", SignalDirection::Bearish) => LighterMarketOrderAction::OpenShort,
        ("close", SignalDirection::Bullish) => LighterMarketOrderAction::CloseLong,
        ("close", SignalDirection::Bearish) => LighterMarketOrderAction::CloseShort,
        _ => bail!("unsupported Lighter execution phase"),
    };
    let base_amount = match phase {
        "open" => {
            let base_quantity = order.quantity_base.parse::<f64>().with_context(|| {
                format!("invalid order.quantity_base for {}", order.client_order_id)
            })?;
            base_quantity_to_base_amount(base_quantity, &market)?
        }
        "close" => match opened_amounts.get(&order.client_order_id) {
            Some(base_amount) => *base_amount,
            None => {
                warn!(
                    client_order_id = order.client_order_id,
                    instrument = order.instrument,
                    "skipping Lighter close because matching open execution was not confirmed"
                );
                return Ok(());
            }
        },
        _ => unreachable!(),
    };
    let price = price_limit_for_market_order(action, &market, config.market_order_slippage_ratio)?;
    let request = LighterBridgeRequest {
        market_index: market.market_id,
        client_order_index: { *order_counter = order_counter.wrapping_add(1); *order_counter },
        base_amount,
        price,
        is_ask: action.is_ask(),
        reduce_only: action.reduce_only(),
    };

    if bridge.is_none() {
        *bridge = Some(LighterBridge::spawn(config).await?);
    }
    let response = bridge
        .as_mut()
        .context("Lighter bridge was not initialized")?
        .request(&request)
        .await?;

    match phase {
        "open" => {
            opened_amounts.insert(order.client_order_id.clone(), base_amount);
        }
        "close" => {
            opened_amounts.remove(&order.client_order_id);
        }
        _ => {}
    }

    info!(
        phase,
        instrument = order.instrument,
        symbol = order.symbol,
        client_order_id = order.client_order_id,
        lighter_base_amount = base_amount,
        lighter_price_limit = price,
        lighter_response = response.to_string(),
        "Lighter live market order submitted"
    );
    Ok(())
}

impl LighterMarketOrderAction {
    pub(crate) fn is_ask(self) -> bool {
        match self {
            Self::OpenLong | Self::CloseShort => false,
            Self::OpenShort | Self::CloseLong => true,
        }
    }

    pub(crate) fn reduce_only(self) -> bool {
        matches!(self, Self::CloseLong | Self::CloseShort)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_market() -> LighterExecutionMarket {
        LighterExecutionMarket {
            market_id: 42,
            price_decimals: 2,
            size_decimals: 3,
            min_base_amount: "0.01".to_string(),
            best_bid_price: "100.12".to_string(),
            best_ask_price: "100.34".to_string(),
        }
    }

    #[test]
    fn converts_base_quantity_to_scaled_base_amount() {
        let base_amount = base_quantity_to_base_amount(0.1239, &test_market())
            .expect("base amount should convert");

        assert_eq!(base_amount, 123);
    }

    #[test]
    fn rejects_base_quantity_below_market_minimum() {
        let error = base_quantity_to_base_amount(0.009, &test_market())
            .expect_err("size below minimum should fail");

        assert!(error.to_string().contains("below Lighter minimum"));
    }

    #[test]
    fn computes_buy_side_market_order_limit_with_ceiling() {
        let limit =
            price_limit_for_market_order(LighterMarketOrderAction::OpenLong, &test_market(), 0.01)
                .expect("limit should be computed");

        assert_eq!(limit, 10135);
    }

    #[test]
    fn computes_sell_side_market_order_limit_with_floor() {
        let limit =
            price_limit_for_market_order(LighterMarketOrderAction::CloseLong, &test_market(), 0.01)
                .expect("limit should be computed");

        assert_eq!(limit, 9911);
    }

    #[test]
    fn rejects_invalid_slippage_ratio() {
        let error =
            price_limit_for_market_order(LighterMarketOrderAction::OpenShort, &test_market(), -0.1)
                .expect_err("negative slippage should fail");

        assert!(error.to_string().contains("slippage ratio"));
    }

    #[test]
    fn maps_close_actions_to_reduce_only_asks_and_bids() {
        assert!(LighterMarketOrderAction::CloseLong.is_ask());
        assert!(LighterMarketOrderAction::CloseLong.reduce_only());
        assert!(!LighterMarketOrderAction::CloseShort.is_ask());
        assert!(LighterMarketOrderAction::CloseShort.reduce_only());
    }
}

