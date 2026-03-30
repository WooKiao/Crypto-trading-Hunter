use crate::{
    gate::GateContractRuntime,
    models::{SignalDirection, SimulatedOrderRecord},
    support::wait_or_shutdown,
    util::{format_decimal, unix_now_ms},
};
use anyhow::{Result, anyhow, bail};
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha512};
use std::{collections::HashMap, env, time::Duration};
use tokio::sync::{mpsc, watch};
use tracing::{info, warn};

type HmacSha512 = Hmac<Sha512>;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct GateExecutionContract {
    pub(crate) contract: String,
    pub(crate) quantity_multiplier: f64,
    pub(crate) order_size_min: f64,
    pub(crate) order_size_max: Option<f64>,
    pub(crate) market_order_size_max: Option<f64>,
    pub(crate) market_order_slip_ratio: Option<f64>,
    pub(crate) price_decimals: u32,
    pub(crate) size_decimals: u32,
    pub(crate) enable_decimal: bool,
}

impl GateExecutionContract {
    fn size_step(&self) -> f64 {
        if self.enable_decimal {
            10f64.powi(-(self.size_decimals as i32))
        } else {
            1.0
        }
    }

    fn max_market_size(&self) -> Option<f64> {
        self.market_order_size_max.or(self.order_size_max)
    }
}

impl From<&GateContractRuntime> for GateExecutionContract {
    fn from(value: &GateContractRuntime) -> Self {
        Self {
            contract: value.contract.clone(),
            quantity_multiplier: value.quantity_multiplier,
            order_size_min: value.order_size_min,
            order_size_max: value.order_size_max,
            market_order_size_max: value.market_order_size_max,
            market_order_slip_ratio: value.market_order_slip_ratio,
            price_decimals: value.price_decimals,
            size_decimals: value.size_decimals,
            enable_decimal: value.enable_decimal,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GateMarketOrderAction {
    OpenLong,
    OpenShort,
    CloseLong,
    CloseShort,
}

impl GateMarketOrderAction {
    fn signed_size(self, absolute_size: &str) -> String {
        match self {
            Self::OpenLong | Self::CloseShort => absolute_size.to_string(),
            Self::OpenShort | Self::CloseLong => format!("-{absolute_size}"),
        }
    }

    fn reduce_only(self) -> bool {
        matches!(self, Self::CloseLong | Self::CloseShort)
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct GateMarketOrderRequest {
    pub(crate) contract: String,
    pub(crate) size: String,
    pub(crate) price: String,
    pub(crate) tif: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) reduce_only: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) market_order_slip_ratio: Option<String>,
    pub(crate) text: String,
}

impl GateMarketOrderRequest {
    pub(crate) fn new(
        contract: &str,
        action: GateMarketOrderAction,
        absolute_size: String,
        market_order_slip_ratio: Option<f64>,
        text: String,
    ) -> Self {
        Self {
            contract: contract.to_string(),
            size: action.signed_size(&absolute_size),
            price: "0".to_string(),
            tif: "ioc".to_string(),
            reduce_only: action.reduce_only().then_some(true),
            market_order_slip_ratio: market_order_slip_ratio.map(format_decimal),
            text,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct GatePlaceOrderRequest {
    pub(crate) contract: String,
    pub(crate) action: GateMarketOrderAction,
    pub(crate) absolute_size: String,
    pub(crate) market_order_slip_ratio: Option<f64>,
    pub(crate) text: String,
}

#[derive(Debug, Clone)]
pub(crate) enum ExecutionTask {
    Open { order: SimulatedOrderRecord },
    Close { order: SimulatedOrderRecord },
}

#[derive(Debug, Clone)]
pub(crate) struct GateOrderPreview {
    pub(crate) path: String,
    pub(crate) body_json: String,
    pub(crate) timestamp: String,
    pub(crate) signature: String,
    pub(crate) exptime_ms: String,
}

#[derive(Debug, Clone)]
pub(crate) struct GateOrderClient {
    client: Client,
    api_key: String,
    api_secret: String,
    base_url: String,
    default_market_order_slip_ratio: Option<f64>,
    exptime_buffer_ms: u64,
}

impl GateOrderClient {
    pub(crate) fn from_env(client: Client) -> Result<Option<Self>> {
        let api_key = match env::var("GATE_API_KEY") {
            Ok(value) if !value.trim().is_empty() => value,
            Ok(_) | Err(env::VarError::NotPresent) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let api_secret = env::var("GATE_API_SECRET")
            .map_err(|error| anyhow!("missing GATE_API_SECRET: {error}"))?;
        let base_url = env::var("GATE_FUTURES_REST_BASE")
            .unwrap_or_else(|_| "https://fx-api.gateio.ws/api/v4".to_string());
        let default_market_order_slip_ratio = env::var("GATE_MARKET_ORDER_SLIP_RATIO")
            .ok()
            .and_then(|value| value.parse::<f64>().ok());
        let exptime_buffer_ms = env::var("GATE_ORDER_EXPTIME_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(5_000);

        Ok(Some(Self {
            client,
            api_key,
            api_secret,
            base_url,
            default_market_order_slip_ratio,
            exptime_buffer_ms,
        }))
    }

    pub(crate) fn preview_market_order(
        &self,
        request: GatePlaceOrderRequest,
    ) -> Result<GateOrderPreview> {
        if !request.text.starts_with("t-") {
            bail!("Gate order text must start with `t-`");
        }

        let path = "/futures/usdt/orders".to_string();
        let payload = GateMarketOrderRequest::new(
            &request.contract,
            request.action,
            request.absolute_size,
            request
                .market_order_slip_ratio
                .or(self.default_market_order_slip_ratio),
            request.text,
        );
        let body_json = serde_json::to_string(&payload)?;
        let timestamp = (unix_now_ms() / 1_000).to_string();
        let signature = sign_gate_request(
            &self.api_secret,
            "POST",
            &format!("/api/v4{path}"),
            "",
            &body_json,
            &timestamp,
        );
        let exptime_ms = unix_now_ms()
            .saturating_add(self.exptime_buffer_ms)
            .to_string();

        Ok(GateOrderPreview {
            path,
            body_json,
            timestamp,
            signature,
            exptime_ms,
        })
    }

    pub(crate) async fn place_market_order(&self, request: GatePlaceOrderRequest) -> Result<Value> {
        let preview = self.preview_market_order(request)?;
        let url = format!("{}{}", self.base_url.trim_end_matches('/'), preview.path);
        let response = self
            .client
            .post(url)
            .header("KEY", &self.api_key)
            .header("Timestamp", &preview.timestamp)
            .header("SIGN", &preview.signature)
            .header("x-gate-exptime", &preview.exptime_ms)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(preview.body_json)
            .send()
            .await?;
        let status = response.status();
        let response_text = response.text().await?;
        if !status.is_success() {
            bail!("Gate market order rejected with {status}: {response_text}");
        }

        Ok(serde_json::from_str(&response_text)?)
    }
}

pub(crate) fn base_quantity_to_contract_size(
    base_quantity: f64,
    contract: &GateExecutionContract,
) -> Result<String> {
    if !(base_quantity.is_finite() && base_quantity > 0.0) {
        bail!("base quantity must be positive");
    }
    if !(contract.quantity_multiplier.is_finite() && contract.quantity_multiplier > 0.0) {
        bail!("invalid Gate quantity multiplier for {}", contract.contract);
    }

    let raw_size = base_quantity / contract.quantity_multiplier;
    normalize_contract_size(raw_size, contract)
}

pub(crate) fn quote_notional_to_contract_size(
    quote_notional: f64,
    reference_price: f64,
    contract: &GateExecutionContract,
) -> Result<String> {
    if !(quote_notional.is_finite() && quote_notional > 0.0) {
        bail!("quote notional must be positive");
    }
    if !(reference_price.is_finite() && reference_price > 0.0) {
        bail!("reference price must be positive");
    }

    base_quantity_to_contract_size(quote_notional / reference_price, contract)
}

fn normalize_contract_size(raw_size: f64, contract: &GateExecutionContract) -> Result<String> {
    if !(raw_size.is_finite() && raw_size > 0.0) {
        bail!("calculated Gate contract size must be positive");
    }

    let step = contract.size_step();
    if !(step.is_finite() && step > 0.0) {
        bail!("invalid Gate size step for {}", contract.contract);
    }

    let normalized_units = (raw_size / step).floor();
    if normalized_units < 1.0 {
        bail!(
            "calculated Gate contract size {:.8} is below Gate minimum {}",
            raw_size,
            format_decimal(contract.order_size_min)
        );
    }

    let normalized_size = normalized_units * step;
    if normalized_size + 1e-9 < contract.order_size_min {
        bail!(
            "calculated Gate contract size {} is below Gate minimum {}",
            format_decimal(normalized_size),
            format_decimal(contract.order_size_min)
        );
    }

    if let Some(max_size) = contract.max_market_size() {
        if normalized_size > max_size + 1e-9 {
            bail!(
                "calculated Gate contract size {} exceeds Gate market max {}",
                format_decimal(normalized_size),
                format_decimal(max_size)
            );
        }
    }

    Ok(format_size(normalized_size, contract.size_decimals))
}

fn format_size(value: f64, size_decimals: u32) -> String {
    let decimals = size_decimals as usize;
    if decimals == 0 {
        return format!("{:.0}", value.floor());
    }

    let mut rendered = format!("{value:.decimals$}");
    while rendered.contains('.') && rendered.ends_with('0') {
        rendered.pop();
    }
    if rendered.ends_with('.') {
        rendered.pop();
    }
    rendered
}

pub(crate) fn sign_gate_request(
    secret: &str,
    method: &str,
    path: &str,
    query: &str,
    body: &str,
    timestamp: &str,
) -> String {
    let body_hash = Sha512::digest(body.as_bytes());
    let sign_payload = format!(
        "{}\n{}\n{}\n{}\n{}",
        method,
        path,
        query,
        hex::encode(body_hash),
        timestamp
    );
    let mut mac =
        HmacSha512::new_from_slice(secret.as_bytes()).expect("HMAC accepts arbitrary key sizes");
    mac.update(sign_payload.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

pub(crate) async fn run_gate_execution_worker(
    client: GateOrderClient,
    mut execution_rx: mpsc::UnboundedReceiver<ExecutionTask>,
    contracts_rx: watch::Receiver<Vec<GateContractRuntime>>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<()> {
    let mut opened_sizes = HashMap::<String, String>::new();

    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => break,
            maybe_task = execution_rx.recv() => {
                let Some(task) = maybe_task else {
                    break;
                };
                if let Err(error) = process_execution_task(&client, &mut opened_sizes, &contracts_rx, task).await {
                    warn!(?error, "Gate live execution task failed");
                    if wait_or_shutdown(&mut shutdown_rx, Duration::from_millis(50)).await {
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}

async fn process_execution_task(
    client: &GateOrderClient,
    opened_sizes: &mut HashMap<String, String>,
    contracts_rx: &watch::Receiver<Vec<GateContractRuntime>>,
    task: ExecutionTask,
) -> Result<()> {
    let (order, phase) = match task {
        ExecutionTask::Open { order } => (order, "open"),
        ExecutionTask::Close { order } => (order, "close"),
    };
    let execution_contract = contracts_rx
        .borrow()
        .iter()
        .find(|contract| contract.contract == order.instrument)
        .map(GateExecutionContract::from)
        .ok_or_else(|| anyhow!("missing Gate execution metadata for {}", order.instrument))?;

    let action = match (phase, &order.direction) {
        ("open", SignalDirection::Bullish) => GateMarketOrderAction::OpenLong,
        ("open", SignalDirection::Bearish) => GateMarketOrderAction::OpenShort,
        ("close", SignalDirection::Bullish) => GateMarketOrderAction::CloseLong,
        ("close", SignalDirection::Bearish) => GateMarketOrderAction::CloseShort,
        _ => bail!("unsupported Gate execution phase"),
    };

    let absolute_size = match phase {
        "open" => {
            let base_quantity = order.quantity_base.parse::<f64>().map_err(|error| {
                anyhow!(
                    "invalid order.quantity_base for {}: {error}",
                    order.client_order_id
                )
            })?;
            base_quantity_to_contract_size(base_quantity, &execution_contract)?
        }
        "close" => match opened_sizes.get(&order.client_order_id) {
            Some(size) => size.clone(),
            None => {
                warn!(
                    client_order_id = order.client_order_id,
                    instrument = order.instrument,
                    "skipping Gate close because matching open execution was not confirmed"
                );
                return Ok(());
            }
        },
        _ => unreachable!(),
    };

    let request = GatePlaceOrderRequest {
        contract: order.instrument.clone(),
        action,
        absolute_size: absolute_size.clone(),
        market_order_slip_ratio: execution_contract.market_order_slip_ratio,
        text: build_gate_execution_text(phase),
    };
    let response = client.place_market_order(request).await?;

    match phase {
        "open" => {
            opened_sizes.insert(order.client_order_id.clone(), absolute_size.clone());
        }
        "close" => {
            opened_sizes.remove(&order.client_order_id);
        }
        _ => {}
    }

    info!(
        phase,
        instrument = order.instrument,
        symbol = order.symbol,
        client_order_id = order.client_order_id,
        gate_size = absolute_size,
        gate_response = response.to_string(),
        "Gate live market order submitted"
    );
    Ok(())
}

fn build_gate_execution_text(phase: &str) -> String {
    format!("t-{phase}-{}", unix_now_ms())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn integer_contract() -> GateExecutionContract {
        GateExecutionContract {
            contract: "BTC_USDT".to_string(),
            quantity_multiplier: 0.001,
            order_size_min: 1.0,
            order_size_max: Some(10_000.0),
            market_order_size_max: Some(500.0),
            market_order_slip_ratio: Some(0.01),
            price_decimals: 1,
            size_decimals: 0,
            enable_decimal: false,
        }
    }

    fn decimal_contract() -> GateExecutionContract {
        GateExecutionContract {
            contract: "XRP_USDT".to_string(),
            quantity_multiplier: 10.0,
            order_size_min: 0.1,
            order_size_max: Some(10_000.0),
            market_order_size_max: Some(1000.0),
            market_order_slip_ratio: Some(0.02),
            price_decimals: 4,
            size_decimals: 1,
            enable_decimal: true,
        }
    }

    #[test]
    fn converts_base_quantity_to_integer_contract_size() {
        let size = base_quantity_to_contract_size(0.0239, &integer_contract())
            .expect("size should be converted");

        assert_eq!(size, "23");
    }

    #[test]
    fn converts_quote_notional_to_decimal_contract_size() {
        let size = quote_notional_to_contract_size(256.0, 2.5, &decimal_contract())
            .expect("size should be converted");

        assert_eq!(size, "10.2");
    }

    #[test]
    fn rejects_size_below_minimum_contracts() {
        let error = base_quantity_to_contract_size(0.0005, &integer_contract())
            .expect_err("size below minimum should fail");

        assert!(error.to_string().contains("below Gate minimum"));
    }

    #[test]
    fn builds_reduce_only_market_order_payload_for_close() {
        let payload = GateMarketOrderRequest::new(
            "BTC_USDT",
            GateMarketOrderAction::CloseLong,
            "23".to_string(),
            Some(0.015),
            "t-test-close".to_string(),
        );

        let body = serde_json::to_value(&payload).expect("payload should serialize");
        assert_eq!(body["contract"], "BTC_USDT");
        assert_eq!(body["size"], "-23");
        assert_eq!(body["price"], "0");
        assert_eq!(body["tif"], "ioc");
        assert_eq!(body["reduce_only"], true);
        assert_eq!(body["market_order_slip_ratio"], "0.015");
        assert_eq!(body["text"], "t-test-close");
    }

    #[test]
    fn signs_gate_request_deterministically() {
        let signature = sign_gate_request(
            "secret",
            "POST",
            "/api/v4/futures/usdt/orders",
            "",
            r#"{"contract":"BTC_USDT","size":"23","price":"0","tif":"ioc"}"#,
            "1700000000",
        );

        assert_eq!(
            signature,
            "9fde2168b333d0b14d40ce802c05b2c232ba8ccf3e4f90bd537e2e5246ce9d00cea61481e7c59d0bdb9adf56a7ad5708f1e545701f18103cc39ba636e67e417d"
        );
    }
}
