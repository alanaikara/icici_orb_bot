# ICICI Direct Opening Range Breakout (ORB) Trading Bot

A Python-based automated trading bot that implements the Opening Range Breakout (ORB) strategy for stocks on ICICI Direct, with a bulk historical data downloader for backtesting.

## Features

- Automated trading based on ORB strategy
- Paper trading mode for strategy testing
- Risk management with position sizing and stop-loss orders
- Real-time market data integration via Breeze Connect API (v2)
- Configurable parameters for trading strategy
- Bulk OHLC data downloader for backtesting (1-minute candles, 5 years, Nifty 50)
- Rate-limited API usage (100 calls/min, 5000 calls/day)
- Resume-capable downloads across daily sessions
- SQLite-based trade tracking, portfolio management, and historical data storage
- Comprehensive logging and reporting

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/icici-orb-bot.git
cd icici-orb-bot
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows, use: venv\Scripts\activate
```

3. Install the required dependencies:
```bash
pip install -r requirements.txt
```

4. Set up environment variables by creating a `.env` file:
```
ICICI_APP_KEY="your_app_key_here"
ICICI_SECRET_KEY="your_secret_key_here"
ICICI_API_SESSION="your_api_session_here"
```

Note: The API session token must be refreshed daily from the ICICI Direct API portal.

## Configuration

The bot's configuration is stored in `config/config.json`.

### Trading Configuration

- `stocks`: List of Breeze ISEC stock codes to monitor for live trading
- `capital`: Total capital available for trading
- `max_risk_per_trade`: Maximum risk amount per trade (INR)
- `opening_range_minutes`: Duration of opening range in minutes (default: 30)
- `max_opening_range_percent`: Maximum opening range as percentage
- `trade_exit_time`: Time to exit all trades (default: "15:14:00")
- `market_open_time` / `market_close_time`: Trading hours
- `exchange_code`: Exchange code (default: "NSE")
- `paper_trading`: Enable/disable paper trading mode
- `max_daily_loss`: Maximum daily loss before circuit breaker triggers
- `max_open_positions`: Maximum simultaneous open positions
- `max_consecutive_losses`: Stop trading after N consecutive losses

### Backtest Data Configuration

- `nifty_50_stocks`: List of 50 Breeze ISEC stock codes for data download
- `backtest.db_path`: Path to backtest SQLite database (default: "Data/backtest.db")
- `backtest.start_date`: Start date for historical data (YYYY-MM-DD)
- `backtest.end_date`: End date for historical data (YYYY-MM-DD)
- `backtest.interval`: Candle interval (default: "1minute")
- `backtest.chunk_days`: Days per API call (default: 2, keeps under 1000-row API limit)
- `backtest.calls_per_minute`: Rate limit per minute (default: 95)
- `backtest.calls_per_day`: Rate limit per day (default: 4900)

### Stock Code Format

Breeze API uses ISEC stock codes, which differ from NSE symbols. Examples:

| NSE Symbol   | Breeze ISEC Code |
|--------------|------------------|
| RELIANCE     | RELIND           |
| HDFCBANK     | HDFBAN           |
| INFY         | INFTEC           |
| SBIN         | STABAN           |
| TCS          | TCS              |
| TATAMOTORS   | TATMOT           |

To look up a stock's ISEC code, use the Breeze API `get_names()` method:
```python
result = api.breeze.get_names(exchange_code='NSE', stock_code='RELIANCE')
print(result['isec_stock_code'])  # -> 'RELIND'
```

## Usage

### ORB Trading Bot

Start the bot in paper trading mode:
```bash
python -m src.main --paper
```

Start the bot in live trading mode:
```bash
python -m src.main --live
```

Use a custom configuration file:
```bash
python -m src.main --config path/to/your/config.json
```

### Bulk OHLC Data Downloader (for Backtesting)

The downloader fetches historical 1-minute OHLC data for all Nifty 50 stocks.
It respects API rate limits and can be run daily until all data is downloaded.

**Start downloading:**
```bash
python download_ohlc.py
```

**Check download progress:**
```bash
python download_ohlc.py --status
```

**Initialize tracking without downloading (dry run):**
```bash
python download_ohlc.py --dry-run
```

**Reset a specific stock (re-download from scratch):**
```bash
python download_ohlc.py --reset RELIND
```

**Reset all stocks that encountered errors:**
```bash
python download_ohlc.py --reset-errors
```

**Use a custom config file:**
```bash
python download_ohlc.py --config path/to/config.json
```

#### How the Downloader Works

1. Downloads data in **2-day chunks** per API call (~750 candles, under the 1000-row API limit)
2. Enforces **rate limits**: 95 calls/minute, 4900 calls/day (safety margins from 100/5000 limits)
3. **Skips weekends** automatically to save API calls
4. **Saves progress** after each chunk -- if interrupted or daily limit hit, resumes from where it left off
5. Uses **INSERT OR IGNORE** for idempotent re-runs (no duplicate data)
6. **Retries failed requests** with exponential backoff (3 attempts: 5s, 10s, 20s delays)
7. Stores data in a separate `Data/backtest.db` SQLite database

#### Daily Workflow

Since the API allows 5,000 calls/day and each stock needs ~469 calls for 5 years of data:

1. Update `ICICI_API_SESSION` in `.env` with a fresh session token
2. Run `python download_ohlc.py`
3. The script downloads ~10 stocks per day and stops when the daily limit is reached
4. Repeat the next day -- it automatically resumes from where it left off
5. After ~5 days, all 50 stocks will be downloaded (~23.4 million records)

#### Querying Downloaded Data

```python
from src.backtest.backtest_db import BacktestDatabase

db = BacktestDatabase("Data/backtest.db")

# Get all 1-minute data for a stock
data = db.get_ohlc_data("RELIND", start_date="2024-01-01", end_date="2024-12-31")

# Get record counts per stock
counts = db.get_records_per_stock()

# Get total records
total = db.get_total_records()
```

## Project Structure

```
icici_orb_bot/
├── config/
│   └── config.json              # Trading + backtest configuration
├── Data/
│   ├── Schema.sql               # Trading database schema
│   ├── backtest_schema.sql      # Backtest OHLC database schema
│   ├── portfolio.db             # Trading database (trades, portfolio, metrics)
│   └── backtest.db              # Historical OHLC data (created at runtime)
├── src/
│   ├── api/
│   │   └── icici_api.py         # Breeze Connect API client (v1 + v2)
│   ├── core/
│   │   ├── bot.py               # ORB Trading Bot implementation
│   │   └── risk_manager.py      # Risk management & position sizing
│   ├── backtest/
│   │   ├── __init__.py          # Backtest package
│   │   ├── backtest_db.py       # BacktestDatabase class
│   │   ├── rate_limiter.py      # API rate limiter (per-minute + per-day)
│   │   └── ohlc_downloader.py   # Bulk OHLC data downloader
│   └── utils/
│       └── logger.py            # Logging configuration
├── Tools/
│   └── reporting_tools.py       # Report generation & visualization
├── download_ohlc.py             # Entry point: OHLC data downloader
├── orb_bot_with_tracking.py     # Extended bot with portfolio tracking
├── database_manager.py          # Portfolio database manager
├── Portfolio_tracker.py         # Portfolio tracking integration
├── .env                         # API credentials (not tracked by git)
├── requirements.txt             # Python dependencies
├── logs/                        # Log files
└── Reports/                     # Generated trading reports
```

## Dependencies

- `breeze-connect==1.0.62` - ICICI Direct Breeze API client
- `pandas==2.2.3` - Data manipulation
- `numpy==2.2.4` - Numerical computing
- `python-dotenv==1.0.1` - Environment variable management
- `schedule==1.2.2` - Task scheduling
- `pytz==2025.1` - Timezone handling

## Disclaimer

This bot is provided for educational and research purposes only. Trading in financial markets involves risk. Use this bot at your own risk. The authors and contributors are not responsible for any financial losses incurred from using this software.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
