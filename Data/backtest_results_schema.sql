-- ============================================================
-- Backtest Results Schema
-- Stores results of ORB parameter grid search backtesting.
-- Separate from backtest.db (OHLC source data) to avoid
-- locking contention. OHLC DB is read-only during backtesting.
-- ============================================================

-- Run metadata — one row per grid search execution
CREATE TABLE IF NOT EXISTS backtest_runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    completed_at TEXT,
    status TEXT NOT NULL DEFAULT 'running',  -- running, completed, interrupted
    config_snapshot TEXT NOT NULL,            -- Full JSON of config at time of run
    total_stocks INTEGER NOT NULL,
    total_param_combos INTEGER NOT NULL,
    total_simulations INTEGER NOT NULL,      -- stocks * combos
    combos_completed INTEGER DEFAULT 0,
    stocks_completed INTEGER DEFAULT 0,
    elapsed_seconds REAL DEFAULT 0,
    workers INTEGER DEFAULT 1,
    store_trades INTEGER DEFAULT 0,          -- 1 if individual trades stored
    start_date TEXT,                          -- Backtest date range start
    end_date TEXT,                            -- Backtest date range end
    notes TEXT
);

-- Parameter lookup table (param_id -> full definition)
CREATE TABLE IF NOT EXISTS backtest_params (
    param_id TEXT PRIMARY KEY,               -- MD5 hash of frozen StrategyParams
    param_json TEXT NOT NULL,                -- Full serialized params JSON
    or_minutes INTEGER NOT NULL,
    target_multiplier REAL NOT NULL,
    stop_loss_type TEXT NOT NULL,
    trade_direction TEXT NOT NULL,
    exit_time TEXT NOT NULL,
    max_or_filter_pct REAL NOT NULL,
    entry_confirmation TEXT NOT NULL
);

-- Aggregated metrics per (run, stock, params) combination
-- Denormalized: params are duplicated here for fast SQL filtering without joins
CREATE TABLE IF NOT EXISTS backtest_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    param_id TEXT NOT NULL,
    stock_code TEXT NOT NULL,

    -- Denormalized params (for direct SQL queries like WHERE or_minutes = 15)
    or_minutes INTEGER NOT NULL,
    target_multiplier REAL NOT NULL,
    stop_loss_type TEXT NOT NULL,
    trade_direction TEXT NOT NULL,
    exit_time TEXT NOT NULL,
    max_or_filter_pct REAL NOT NULL,
    entry_confirmation TEXT NOT NULL,

    -- Core metrics
    total_trades INTEGER NOT NULL,
    winning_trades INTEGER NOT NULL,
    losing_trades INTEGER NOT NULL,
    win_rate REAL NOT NULL,
    total_pnl REAL NOT NULL,
    net_pnl REAL NOT NULL,
    avg_pnl_per_trade REAL NOT NULL,
    avg_winner REAL NOT NULL,
    avg_loser REAL NOT NULL,
    profit_factor REAL NOT NULL,
    max_drawdown REAL NOT NULL,
    max_drawdown_pct REAL NOT NULL,
    max_consecutive_losses INTEGER NOT NULL,
    sharpe_ratio REAL NOT NULL,
    sortino_ratio REAL NOT NULL,
    expectancy REAL NOT NULL,
    avg_r_multiple REAL NOT NULL,
    calmar_ratio REAL NOT NULL,
    best_trade REAL NOT NULL,
    worst_trade REAL NOT NULL,
    avg_holding_minutes REAL NOT NULL,
    composite_score REAL NOT NULL,

    UNIQUE(run_id, param_id, stock_code),
    FOREIGN KEY (run_id) REFERENCES backtest_runs(run_id),
    FOREIGN KEY (param_id) REFERENCES backtest_params(param_id)
);

-- Individual trade records (optional, only populated with --trades flag)
CREATE TABLE IF NOT EXISTS backtest_trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    param_id TEXT NOT NULL,
    stock_code TEXT NOT NULL,
    date TEXT NOT NULL,
    direction TEXT NOT NULL,
    entry_time TEXT NOT NULL,
    entry_price REAL NOT NULL,
    exit_time TEXT NOT NULL,
    exit_price REAL NOT NULL,
    quantity INTEGER NOT NULL,
    stop_loss_initial REAL NOT NULL,
    stop_loss_final REAL NOT NULL,
    target_price REAL NOT NULL,
    or_high REAL NOT NULL,
    or_low REAL NOT NULL,
    exit_reason TEXT NOT NULL,             -- 'target', 'stop_loss', 'time_exit'
    gross_pnl REAL NOT NULL,
    costs REAL NOT NULL,
    net_pnl REAL NOT NULL,
    risk_amount REAL NOT NULL,
    r_multiple REAL NOT NULL,
    FOREIGN KEY (run_id) REFERENCES backtest_runs(run_id)
);

-- Progress tracking for resume capability
CREATE TABLE IF NOT EXISTS backtest_progress (
    run_id INTEGER NOT NULL,
    stock_code TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',  -- pending, in_progress, completed
    combos_tested INTEGER DEFAULT 0,
    total_trades_found INTEGER DEFAULT 0,
    elapsed_seconds REAL DEFAULT 0,
    completed_at TEXT,
    PRIMARY KEY (run_id, stock_code),
    FOREIGN KEY (run_id) REFERENCES backtest_runs(run_id)
);

-- ============================================================
-- Indexes for fast querying
-- ============================================================

-- Metrics indexes
CREATE INDEX IF NOT EXISTS idx_metrics_run ON backtest_metrics(run_id);
CREATE INDEX IF NOT EXISTS idx_metrics_stock ON backtest_metrics(stock_code);
CREATE INDEX IF NOT EXISTS idx_metrics_param ON backtest_metrics(param_id);
CREATE INDEX IF NOT EXISTS idx_metrics_run_stock ON backtest_metrics(run_id, stock_code);
CREATE INDEX IF NOT EXISTS idx_metrics_composite ON backtest_metrics(run_id, composite_score DESC);
CREATE INDEX IF NOT EXISTS idx_metrics_net_pnl ON backtest_metrics(run_id, net_pnl DESC);
CREATE INDEX IF NOT EXISTS idx_metrics_or_minutes ON backtest_metrics(run_id, or_minutes);
CREATE INDEX IF NOT EXISTS idx_metrics_sharpe ON backtest_metrics(run_id, sharpe_ratio DESC);

-- Trades indexes (only useful if --trades flag was used)
CREATE INDEX IF NOT EXISTS idx_trades_run ON backtest_trades(run_id);
CREATE INDEX IF NOT EXISTS idx_trades_run_stock ON backtest_trades(run_id, stock_code);
CREATE INDEX IF NOT EXISTS idx_trades_run_param ON backtest_trades(run_id, param_id);

-- Progress index
CREATE INDEX IF NOT EXISTS idx_progress_run ON backtest_progress(run_id);
