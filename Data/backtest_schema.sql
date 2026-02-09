-- Backtest OHLC Data Schema
-- Stores historical 1-minute OHLC data for backtesting

-- OHLC minute-level data
CREATE TABLE IF NOT EXISTS ohlc_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stock_code TEXT NOT NULL,
    datetime TEXT NOT NULL,
    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    volume INTEGER NOT NULL,
    UNIQUE(stock_code, datetime)
);

-- Download progress tracking for resume capability
CREATE TABLE IF NOT EXISTS download_progress (
    stock_code TEXT PRIMARY KEY,
    last_downloaded_date TEXT,
    first_target_date TEXT NOT NULL,
    last_target_date TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    total_records INTEGER DEFAULT 0,
    total_api_calls INTEGER DEFAULT 0,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- Daily API usage tracking for rate limit persistence
CREATE TABLE IF NOT EXISTS api_usage (
    date TEXT PRIMARY KEY,
    calls_made INTEGER DEFAULT 0,
    last_call_time TEXT
);

-- Indexes for fast backtesting queries
CREATE INDEX IF NOT EXISTS idx_ohlc_stock_datetime ON ohlc_data(stock_code, datetime);
CREATE INDEX IF NOT EXISTS idx_ohlc_datetime ON ohlc_data(datetime);
CREATE INDEX IF NOT EXISTS idx_ohlc_stock ON ohlc_data(stock_code);
CREATE INDEX IF NOT EXISTS idx_download_status ON download_progress(status);
