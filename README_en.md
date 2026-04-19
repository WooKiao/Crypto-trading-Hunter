# Crypto-trading-Hunter

[中文](README.md) | English

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
![Rust](https://img.shields.io/badge/Rust-2024_edition-orange.svg)
![Docker](https://img.shields.io/badge/Docker-ready-blue.svg)

> A low-latency perpetual trading bot in Rust, supporting Lighter on-chain DEX and Gate.io.
> Core strategy **「Momentum Hunter / 启动币抓手」**: monitors all markets in real time and catches tokens breaking out fast — because every token that pumps 30–40% in a day starts from that first breakout moment.

---

## Screenshots

### Dashboard

<p align="center">
  <img src="docs/screenshot-overview.jpg" width="49%" alt="Overview" />
  <img src="docs/screenshot-orders.jpg" width="49%" alt="Orders" />
</p>
<p align="center">
  <img src="docs/screenshot-pnl.jpg" width="49%" alt="Performance & P&L" />
  <img src="docs/screenshot-market.jpg" width="49%" alt="Market View" />
</p>

### Historical Trade Results

> ⚠️ **Important**: The results below are from real trades executed during the **early stage of the last bull market**, when altcoin season momentum was strong and the strategy had a high hit rate.
>
> **Do not run live trading with the same parameters in a bear market or choppy sideways market.** Momentum breakout strategies perform best in sustained one-directional rallies. In bear markets, false breakouts are frequent and stop-loss rates will increase significantly.
>
> It is recommended to run this bot for live trading only when **clear bull market signals appear or an altcoin season begins**. **Currently for learning and technical discussion purposes only.**
>
> Past performance does not guarantee future results.

<p align="center">
  <img src="docs/profit-history-1.jpg" width="100%" alt="Historical profits 1" />
</p>
<p align="center">
  <img src="docs/profit-history-2.jpg" width="100%" alt="Historical profits 2" />
</p>
<p align="center">
  <img src="docs/profit-history-3.jpg" width="100%" alt="Historical profits 3" />
</p>

---

## Overview

**Crypto-trading-Hunter** subscribes to real-time market data from [Lighter](https://lighter.xyz) perpetual DEX and Gate.io via WebSocket. It detects price breakout signals using a configurable sliding time window, supports both paper trading and live trading, and serves a local web dashboard at `http://127.0.0.1:3000` for full monitoring.

The core design goal is **low latency**: the signal hot path is strictly decoupled from persistence and UI snapshots, keeping `process p99` consistently in the tens-of-microseconds range.

---

## Features

- **Dual exchange support**: Lighter on-chain perpetual DEX + Gate.io, both via WebSocket
- **Real-time BBO tracking**: Continuously tracks best bid/ask price and depth for all active perpetual markets
- **Breakout signal detection**: Sliding window-based bullish/bearish signal detection with all parameters tunable live from the dashboard
- **Paper trading**: Dual take-profit levels + stop-loss + max hold time, with automatic open/close and persistent results
- **Live trading**: Real order execution on Gate.io and Lighter with built-in multi-layer risk controls
- **Web dashboard**: Real-time market data, signal history, order status, connection health, and latency metrics
- **SQLite persistence**: Signals and order history stored locally, preserved across restarts
- **Docker deployment**: Dockerfile and Docker Compose included, ready to run

---

## Strategy Logic: Momentum Hunter / 启动币抓手

### Core Observation

Every day, some tokens pump 30–40%. But those big moves never happen out of thin air — they always pass through +5%, +10%, +15% first.

**The edge is in entering early, when momentum is just igniting, rather than chasing a move that's already obvious.**

### Signal Trigger

When a perpetual contract **rapidly rises from its window low by a configured threshold** (e.g. 15%) within a short time window, the bot treats this as a pump signal firing:

- A large move in a short window indicates strong buying pressure entering the market
- Price reaching a new window high confirms the direction of momentum
- Open interest and spread filters remove low-liquidity or abnormal markets

A **bullish signal** fires when all of the following are true:

1. Current price has risen ≥ `price_jump_threshold_pct` from the window low (first crossing — previous tick was still below)
2. Current price equals the window high (momentum direction confirmed)
3. Open interest > `open_interest_threshold` (filters thin markets)
4. Bid-ask spread ≤ `spread_threshold_pct` (ensures fills at reasonable prices)

**Bearish signal** is the mirror: a fast drop from the window high, treated as a distribution/dump beginning — the bot goes short to follow the momentum.

### Exit Strategy

On signal, the bot automatically opens a position. Two exit approaches are supported:

**Option A — Dual take-profit**
- TP1 (`sim_take_profit_pct`): quickly capture a small gain, close a partial position (`sim_take_profit_ratio_pct`)
- TP2 (`sim_take_profit_pct_2`): let the remaining position ride for a larger move

**Option B — Cost recovery exit (recommended)**
- Set TP1 ratio to 100% to close fully and lock in profits immediately; or set TP1 ratio to exactly recover entry cost, leaving the rest as a zero-cost position — no matter what the market does after, your principal is already safe.

### Stop-Loss Strategy

Two stop-loss modes, usable independently or together:

- **Fixed stop-loss** (`sim_stop_loss_pct`): closes immediately when price moves against the position by the set percentage
- **Time stop-loss** (`max_hold_minutes`): if TP is not hit within the max hold time, momentum has faded — the bot exits automatically

---

## Architecture

```
WebSocket feed (Lighter / Gate.io)
        │
        ▼
  Event queue (Tokio channel)
        │
        ▼
  State machine hot path (app.rs)
  ├── Sliding window update
  ├── Signal evaluation
  └── Simulated / live order decisions
        │
   ┌────┴────┐
   ▼         ▼
Persist queue  Read model worker (periodic sample)
 (SQLite)              │
                       ▼
              /api/snapshot ◄── Dashboard polling
```

The hot path never blocks on SQLite writes. UI snapshots are generated by a side-channel worker on a timer, eliminating lock contention on the signal path.

---

## Quick Start

### Prerequisites

- [Rust](https://rustup.rs/) — for local native development
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) — for container deployment
- Python 3 + [lighter-python](https://github.com/elliottech/lighter-python) — only required for Lighter live trading (pre-installed in the Docker image)

### Run locally

```bash
cargo run
```

Open `http://127.0.0.1:3000` in your browser.

### Docker

```bash
# Build
docker build -t lighter-rust-bbo:local .

# Run
docker run --rm --env-file .env -p 3000:3000 lighter-rust-bbo:local
```

### Docker Compose (recommended)

```bash
cp .env.example .env
# Edit .env and fill in your exchange credentials
docker compose up --build
```

---

## Environment Variables

Copy `.env.example` to `.env`. Exchange credentials are only required for live trading — market monitoring and paper trading work without any keys.

| Variable | Description | Required for |
|----------|-------------|--------------|
| `GATE_API_KEY` | Gate.io API Key | Gate live trading |
| `GATE_API_SECRET` | Gate.io API Secret | Gate live trading |
| `LIGHTER_ACCOUNT_INDEX` | Lighter account index | Lighter live trading |
| `LIGHTER_API_KEY_INDEX` | Lighter API key index (must be > 3) | Lighter live trading |
| `LIGHTER_API_PRIVATE_KEY` | Lighter API private key | Lighter live trading |

> **Security note**: `.env` is excluded by `.gitignore`. Never commit real credentials to the repository.

---

## Tech Stack

| Layer | Technology |
|-------|------------|
| Language | Rust 2024 edition |
| Async runtime | Tokio |
| Web framework | Axum |
| WebSocket | tokio-tungstenite |
| HTTP client | reqwest (rustls-tls) |
| Database | SQLite (rusqlite bundled) |
| Serialization | serde / serde_json |
| Logging | tracing / tracing-subscriber |
| Lighter signing bridge | Python 3 + lighter-python |
| Deployment | Docker / Docker Compose |

---

## Important Notes

1. **API key security**: `.env` is excluded by `.gitignore` — never commit real credentials to any public repository
2. **Paper trade first**: This project has a built-in paper trading mode — no API keys required to experience the full flow of signal detection, order open/close, and P&L tracking. **It is strongly recommended to run in paper mode before going live, so you can fully understand the bot's behavior and strategy characteristics and avoid unnecessary losses from unfamiliarity with the system.**
3. **Lighter API Key Index**: `LIGHTER_API_KEY_INDEX` must be greater than 3, or the service will fail validation at startup
4. **Python dependency**: lighter-python is only needed for Lighter live trading; the Docker image includes it automatically
5. **Network stability**: WebSocket subscriptions require a stable connection — reconnection is automatic, but a reliable network environment is recommended
6. **Position sizing**: Start with small amounts when first going live; scale up after confirming system behavior

---

## FAQ

**Q: Do I need to know Rust to use this?**
A: No. Docker Compose deployment requires no programming knowledge — just follow the Quick Start steps.

**Q: What's the difference between paper trading and live trading?**
A: Paper trading only records signals and simulated order outcomes — no real orders are placed and no API keys are needed. Live trading requires exchange credentials and must be explicitly enabled in the dashboard.

**Q: What is Lighter?**
A: [Lighter](https://lighter.xyz) is an on-chain perpetual DEX built on Ethereum Layer 2. Assets are held in smart contracts — it is non-custodial.

**Q: A signal fired but no real order was placed — is that normal?**
A: Yes. Signal detection and live order execution are two separate switches. By default the bot runs in paper mode only; you need to enable "Live Trading" and allow new opens in the dashboard for real orders to be sent.

**Q: Do parameter changes take effect immediately?**
A: Yes. All threshold parameters (window duration, jump threshold, take-profit, stop-loss, etc.) take effect immediately for new signals after saving in the dashboard, and are persisted to the local database so they survive restarts.

---

## Disclaimer

This project is intended for educational and research purposes only. Cryptocurrency trading carries substantial risk of loss, including total loss of principal. Nothing in this project constitutes financial or investment advice. Any profits or losses resulting from live trading with this software are solely the responsibility of the user. The author accepts no legal or financial liability.

---

## Author

- **X / Twitter**: https://x.com/WooKiao

Looking for a crypto exchange with competitive rebates?

- [Gate.io](https://www.gateport.business/share/avuwuwte)
- [Bitget](https://partner.hdmune.cn/bg/mcqta56y)

---

## License

[MIT](LICENSE) © 2026 WooKiao
