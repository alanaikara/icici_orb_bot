# ORB Backtesting Framework - Implementation Plan

## Goal
Build a comprehensive backtesting engine that tests ALL combinations of ORB strategy parameters across ALL 50 Nifty stocks, finds the best strategy + stock combinations, and generates ranked reports.

## Data Available
- 1-minute OHLC data in `Data/backtest.db` (~13M+ records, 50 stocks, 5 years)
- ~460K records per stock, ~375 candles per trading day, ~1,200 trading days

---

## Parameter Grid to Sweep

| Parameter | Values | Count |
|---|---|---|
| Opening Range Duration | 5, 10, 15, 20, 30, 45, 60 min | 7 |
| Target Multiplier (R:R) | 0 (time-exit only), 1x, 1.5x, 2x, 2.5x, 3x | 6 |
| Stop Loss Type | Fixed (OR opposite), Trailing %, ATR-based | 3 |
| Max OR Filter | 0.5%, 1%, 1.5%, 2%, none | 5 |
| Trade Direction | Long only, Short only, Both | 3 |
| Exit Time | 14:30, 14:45, 15:00, 15:14, 15:25 | 5 |
| Entry Confirmation | Immediate breakout, Candle close, Volume confirm | 3 |

**Total: 7 x 6 x 3 x 5 x 3 x 5 x 3 = 28,350 combos per stock x 50 stocks = 1,417,500 total**

---

## New Files to Create (8 files)

### 1. `src/backtest/parameter_grid.py` — Parameter definitions & grid generation
- `StopLossType`, `TradeDirection`, `EntryConfirmation` enums
- `StrategyParams` frozen dataclass (hashable, with `param_id()` method)
- `ParameterGrid` class — generates all combos, supports filtering/subsets

### 2. `src/backtest/data_loader.py` — Efficient data loading & precomputation
- Loads OHLC data from SQLite into pandas DataFrames (one per stock)
- **Key optimization**: Precomputes opening ranges per (stock, OR duration) — only 7 variants per stock, reused across all other parameter combos
- Computes daily ATR and average volume for ATR/volume-based strategies
- Caches everything in memory (~26 MB per stock)

### 3. `src/backtest/backtest_engine.py` — Core ORB simulation engine
- `Trade` dataclass — represents one simulated trade
- `ORBSimulator` class:
  - `run(stock_code, params)` → list of Trades
  - `_simulate_day()` — processes one trading day candle by candle
  - `_check_entry()` — checks entry conditions (immediate/candle close/volume)
  - `_calculate_exit()` — checks SL/target/trailing/time exit
  - `_calculate_position_size()` — risk-based sizing (₹1,000 risk per trade)
  - `_calculate_costs()` — brokerage (0.01%) + STT (0.025% sell side)
- Faithfully replicates the logic from `src/core/bot.py`
- One trade per day per stock (first valid signal only)

### 4. `src/backtest/metrics.py` — Performance metric computation
- `PerformanceMetrics.compute(trades)` → dict of all metrics:
  - Total/net P&L, win rate, profit factor
  - Max drawdown (absolute & %), Sharpe ratio, Sortino ratio
  - Expectancy, avg win/loss, best/worst trade
  - Calmar ratio, avg holding time
- `composite_score()` — weighted ranking score:
  - 25% net P&L + 20% Sharpe + 15% profit factor + 15% win rate + 15% (1 - drawdown) + 10% expectancy

### 5. `src/backtest/results_db.py` — Results database manager
- Follows `BacktestDatabase` pattern (context manager, WAL mode)
- Stores in `Data/backtest_results.db` (separate from OHLC data)
- Tables:
  - `backtest_runs` — run metadata (config snapshot, timestamps)
  - `backtest_trades` — individual simulated trades (optional, togglable)
  - `backtest_metrics` — aggregated metrics per (param_id, stock) combo (denormalized params for easy SQL queries)
  - `backtest_params` — param_id → full JSON lookup
  - `backtest_progress` — resume support for interrupted runs

### 6. `src/backtest/ranking.py` — Strategy & stock ranking
- `StrategyRanker` class:
  - `rank_strategies()` — top N parameter combos by any metric
  - `rank_stocks()` — best stocks for ORB (overall or for specific strategy)
  - `find_best_pairs()` — best (stock, strategy) combinations
  - `parameter_sensitivity()` — which params matter most
  - `stability_analysis()` — rolling window consistency check

### 7. `src/backtest/report_generator.py` — Reports & visualizations
- Text summary (top strategies, top stocks, top pairs)
- Heatmaps (any 2 params vs any metric, e.g., OR duration × target → net P&L)
- Equity curves for top N strategies
- Stock performance grid (bar chart)
- Parameter sensitivity charts
- Monthly P&L breakdown
- CSV export for external analysis
- All charts saved to `Reports/backtest/`

### 8. `run_backtest.py` — CLI entry point
```
python run_backtest.py                          # Full grid search
python run_backtest.py --quick                  # Reduced grid (fast test)
python run_backtest.py --stocks RELIND INFTEC   # Specific stocks
python run_backtest.py --or-minutes 15 30       # Specific OR durations
python run_backtest.py --status                 # Progress of current run
python run_backtest.py --report                 # Generate reports from last run
python run_backtest.py --resume                 # Resume interrupted run
python run_backtest.py --workers 4              # Parallel workers
python run_backtest.py --no-trades              # Skip storing individual trades (saves disk)
python run_backtest.py --dates 2023-01-01 2025-12-31  # Custom date range
```

## Files to Modify (2 files)

### 9. `config/config.json` — Add `backtest_sweep` section
```json
"backtest_sweep": {
    "results_db_path": "Data/backtest_results.db",
    "or_minutes": [5, 10, 15, 20, 30, 45, 60],
    "target_multipliers": [0, 1.0, 1.5, 2.0, 2.5, 3.0],
    "stop_loss_types": ["fixed", "trailing", "atr_based"],
    "max_or_filter_pct": [0.5, 1.0, 1.5, 2.0, 0],
    "trade_directions": ["long_only", "short_only", "both"],
    "exit_times": ["14:30", "14:45", "15:00", "15:14", "15:25"],
    "entry_confirmations": ["immediate", "candle_close", "volume"],
    "trailing_stop_pct": 0.5,
    "atr_multiplier": 1.5,
    "atr_period": 14,
    "capital": 100000,
    "max_risk_per_trade": 1000,
    "brokerage_rate": 0.0001,
    "stt_rate": 0.00025
}
```

### 10. `Data/backtest_results_schema.sql` — New schema file

---

## Parallelization Strategy

**Stock-level parallelism** using `multiprocessing.Pool`:
1. **Preload phase** (serial): Load all stock data + precompute opening ranges
2. **Simulation phase** (parallel): Each worker processes ONE stock × ALL param combos
3. **Storage phase** (serial): Collect results, bulk-insert into SQLite

Each worker:
- Loads its stock's data (~26 MB) independently
- Precomputes 7 opening range variants
- Runs all 28,350 param combos
- Returns metrics (not written to DB by worker)

**Performance optimization — vectorized inner loop:**
- For simple combos (IMMEDIATE entry + FIXED SL): use numpy `argmax` on boolean arrays instead of Python candle-by-candle loops
- For complex combos (TRAILING/ATR/VOLUME): fall back to candle iteration
- Estimated runtime: ~4-6 hours with 8 workers for full grid on 50 stocks

---

## Implementation Order

| Phase | Files | Dependencies |
|---|---|---|
| 1. Foundation | `parameter_grid.py`, `results_db.py` + schema, `metrics.py` | None |
| 2. Data & Engine | `data_loader.py`, `backtest_engine.py` | Phase 1 |
| 3. Analysis | `ranking.py`, `report_generator.py` | Phase 2 |
| 4. Integration | `run_backtest.py`, config updates, `__init__.py` | Phase 3 |

---

## Output Example

After a full run, you'll get:
```
==================================================
  ORB Backtest Grid Search — COMPLETE
==================================================
  Total combos tested: 1,417,500
  Runtime: 5h 23m
  Results stored in: Data/backtest_results.db

  TOP 5 STRATEGIES (across all stocks):
  ┌────┬──────────┬────────┬───────┬────────┬──────────┬──────────┬───────────┐
  │ #  │ OR (min) │ Target │ SL    │ Dir    │ Exit     │ Entry    │ Net P&L   │
  ├────┼──────────┼────────┼───────┼────────┼──────────┼──────────┼───────────┤
  │ 1  │ 15       │ 2.0x   │ Fixed │ Both   │ 15:14    │ Candle   │ ₹4,23,500 │
  │ 2  │ 15       │ 1.5x   │ Trail │ Both   │ 15:00    │ Immed    │ ₹3,98,200 │
  │ 3  │ 30       │ 2.0x   │ Fixed │ Long   │ 15:14    │ Candle   │ ₹3,85,100 │
  │ ...│          │        │       │        │          │          │           │
  └────┴──────────┴────────┴───────┴────────┴──────────┴──────────┴───────────┘

  TOP 5 STOCKS FOR ORB:
  1. RELIND  — Win Rate: 58%, Sharpe: 1.82, Net P&L: ₹1,25,000
  2. INFTEC  — Win Rate: 55%, Sharpe: 1.64, Net P&L: ₹1,12,000
  ...

  PARAMETER SENSITIVITY (most impactful → least):
  1. OR Duration (explains 32% of variance)
  2. Target Multiplier (24%)
  3. Entry Confirmation (15%)
  ...
```

Plus heatmaps, equity curves, and CSV exports in `Reports/backtest/`.
