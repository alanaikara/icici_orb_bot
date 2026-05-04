# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Setup
source venv/bin/activate

# Fib-MACD live trading (always dry-run first)
python run_live_trader.py --dry-run
python run_live_trader.py --dry-run --stocks ADAENT JSWSTE TATMOT
python run_live_trader.py --risk 500

# Fib-MACD backtest grid search
python run_fib_backtest.py --quick --stocks RELIND    # Fast single-stock validation
python run_fib_backtest.py --workers 4                # Full parallel sweep
python run_fib_backtest.py --resume                   # Resume interrupted run
python run_fib_backtest.py --report                   # Reports from last run
python run_fib_backtest.py --fib-levels 0.618 --targets 2.0 2.5  # Pin params

# OHLC data downloader
python download_ohlc.py --status         # Check per-stock progress
python download_ohlc.py                  # Download / resume
python download_ohlc.py --reset RELIND   # Re-download one stock
python download_ohlc.py --reset-errors   # Retry errored stocks
```

**Environment** — set before running live trader:
```
ICICI_API_KEY, ICICI_SECRET_KEY, ICICI_API_SESSION   # ICICI_API_SESSION expires daily
```

---

# ICICI ORB Bot — Codebase Guide

## ⚠️ Active Strategy: Fib-MACD Pullback (NOT Simple ORB)

There are **two strategies** in this repo. The **Fib-MACD pullback strategy is the current, better strategy** and should be the default focus for any analysis, debugging, or extension work.

---

## Strategy Overview

### ✅ CURRENT: Fib-MACD Pullback Strategy
**Logic:** ORB sets direction → breakout fires → wait for post-breakout swing high/low → enter at 61.8% Fibonacci retracement on candle-close bounce → MACD (5-min) confirms direction → stop-loss just below 78.6% level → 1.5R target

**Key files:**
- `run_fib_backtest.py` — backtest entry point
- `run_live_trader.py` — live trading entry point
- `src/backtest/fib_macd_engine.py` — per-stock simulator (state machine + Fibonacci logic)
- `src/backtest/fib_macd_runner.py` — parallel backtest runner
- `src/live/stock_state.py` — live per-stock state machine (WAITING_OR → IN_TRADE → DONE)
- `src/live/live_trader.py` — live trading orchestrator
- `src/live/strategy_config.py` — FIB_MACD_PORTFOLIO: 34-stock best portfolio
- `src/live/breeze_broker.py` — ICICI Breeze broker adapter
- `src/live/broker_base.py` — broker-agnostic abstract interface

**Backtest results:** `Data/backtest_results.db` — Runs 1–7 are Fib-MACD runs
- Best portfolio: 34 stocks with Sharpe > 2.5 (Run 7, realistic Zerodha charges + 0.05% slippage per side)
- Parameters: 30-min OR, 61.8% fib entry, MACD cross on 5-min bars, 1.5R target, exit 15:14

### ❌ LEGACY: Simple ORB (do not use for new work)
The original simple ORB strategy (enter on breakout, fixed stop, time exit) is still in the repo but is **not actively used**. Sharpe ~1.63 vs Fib-MACD's ~2.0+ portfolio-wide.

**Legacy files (do not confuse with active strategy):**
- `src/backtest/backtest_engine.py` — simple ORB per-stock simulator
- `src/backtest/runner.py` — simple ORB runner
- `run_backtest.py` — simple ORB backtest entry point
- `orb_bot_with_tracking.py` — original live bot (simple ORB, superseded)

---

## Project Layout

```
icici_orb_bot/
├── run_fib_backtest.py        ← Fib-MACD backtest (USE THIS)
├── run_live_trader.py         ← Fib-MACD live trader (USE THIS)
├── run_backtest.py            ← Simple ORB backtest (LEGACY)
├── download_ohlc.py           ← Historical data downloader
├── src/
│   ├── api/icici_api.py       ← Breeze Connect API client
│   ├── backtest/
│   │   ├── fib_macd_engine.py ← Fib-MACD simulator (ACTIVE)
│   │   ├── fib_macd_runner.py ← Fib-MACD runner (ACTIVE)
│   │   ├── data_loader.py     ← Loads OHLC + 5-min MACD
│   │   ├── backtest_engine.py ← Simple ORB simulator (LEGACY)
│   │   └── runner.py          ← Simple ORB runner (LEGACY)
│   └── live/
│       ├── broker_base.py     ← Abstract broker interface
│       ├── breeze_broker.py   ← ICICI Breeze implementation
│       ├── strategy_config.py ← FIB_MACD_PORTFOLIO config
│       ├── stock_state.py     ← Per-stock state machine
│       └── live_trader.py     ← Live trading orchestrator
├── config/config.json         ← Stock list (50 Nifty stocks, ISEC codes)
└── Data/
    ├── backtest.db            ← 5-year 1-min OHLC (50 stocks, ~23M rows)
    └── backtest_results.db    ← Fib-MACD backtest results (Runs 1–7)
```

---

## Key Facts for Development

- **Stock codes**: ISEC codes (not NSE symbols) — e.g. RELIANCE → RELIND, SBIN → STABAN
- **API**: `get_historical_data_v2()` for 1-min data; v1 returns "No Data Found"
- **Session token**: `ICICI_API_SESSION` expires daily, must be refreshed each morning
- **Charges**: Zerodha intraday ~₹82/trade (brokerage + STT + exchange + SEBI + stamp + GST)
- **Slippage**: 0.05% per side (0.1% round-trip) for realistic backtest
- **Live trading**: Always start with `--dry-run` to verify signal detection before going live
- **Data**: 50 Nifty stocks, 5-year span (2021-02-09 to 2026-02-09), all downloads complete
