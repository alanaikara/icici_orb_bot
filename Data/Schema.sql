-- Trades table to store individual trade information
CREATE TABLE trades (
    trade_id INTEGER PRIMARY KEY AUTOINCREMENT,
    stock_code TEXT NOT NULL,
    exchange_code TEXT NOT NULL,
    action TEXT NOT NULL,  -- 'buy' or 'sell'
    entry_time TIMESTAMP NOT NULL,
    exit_time TIMESTAMP,
    entry_price REAL NOT NULL,
    exit_price REAL,
    quantity INTEGER NOT NULL,
    position_type TEXT NOT NULL,  -- 'LONG' or 'SHORT'
    product_type TEXT NOT NULL,  -- 'cash', 'margin', etc.
    order_id TEXT,
    stop_loss REAL,
    target REAL,
    status TEXT NOT NULL,  -- 'open', 'closed', 'cancelled'
    strategy TEXT NOT NULL,  -- 'ORB', 'manual', etc.
    brokerage REAL,
    other_charges REAL,
    pnl REAL,
    notes TEXT
);

-- Daily summary table for overall P&L tracking
CREATE TABLE daily_summary (
    summary_id INTEGER PRIMARY KEY AUTOINCREMENT,
    date DATE UNIQUE NOT NULL,
    gross_pnl REAL NOT NULL,
    net_pnl REAL NOT NULL,
    total_trades INTEGER NOT NULL,
    winning_trades INTEGER NOT NULL,
    losing_trades INTEGER NOT NULL,
    brokerage_total REAL NOT NULL,
    other_charges_total REAL NOT NULL,
    max_profit_trade REAL,
    max_loss_trade REAL,
    capital_used REAL,
    notes TEXT
);

-- Portfolio table to track current holdings
CREATE TABLE portfolio (
    portfolio_id INTEGER PRIMARY KEY AUTOINCREMENT,
    stock_code TEXT NOT NULL,
    exchange_code TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    average_price REAL NOT NULL,
    current_price REAL,
    current_value REAL,
    unrealized_pnl REAL,
    realized_pnl REAL,
    last_updated TIMESTAMP NOT NULL,
    product_type TEXT NOT NULL  -- 'cash', 'margin', etc.
);

-- Capital history table to track capital changes
CREATE TABLE capital_history (
    capital_id INTEGER PRIMARY KEY AUTOINCREMENT,
    date DATE NOT NULL,
    amount REAL NOT NULL,
    transaction_type TEXT NOT NULL,  -- 'deposit', 'withdrawal'
    notes TEXT,
    balance_after REAL NOT NULL
);

-- Performance metrics table for strategy evaluation
CREATE TABLE performance_metrics (
    metric_id INTEGER PRIMARY KEY AUTOINCREMENT,
    date DATE NOT NULL,
    strategy TEXT NOT NULL,
    win_rate REAL,
    profit_factor REAL,
    avg_profit_per_trade REAL,
    max_drawdown REAL,
    sharpe_ratio REAL,
    sortino_ratio REAL,
    total_trades INTEGER,
    period TEXT NOT NULL  -- 'daily', 'weekly', 'monthly', 'yearly'
);

-- Indexes for performance
CREATE INDEX idx_trades_date ON trades(entry_time);
CREATE INDEX idx_trades_stock ON trades(stock_code);
CREATE INDEX idx_portfolio_stock ON portfolio(stock_code);
CREATE INDEX idx_daily_summary_date ON daily_summary(date);