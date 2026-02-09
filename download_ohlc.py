#!/usr/bin/env python3
"""
Bulk OHLC Data Downloader for Backtesting
Downloads 5 years of 1-minute OHLC data for Nifty 50 stocks from Breeze API.
Respects rate limits (100 calls/min, 5000 calls/day) and resumes across runs.

Usage:
    python download_ohlc.py                  # Run download
    python download_ohlc.py --status         # Show download progress
    python download_ohlc.py --reset RELIANCE # Reset a stock's progress
    python download_ohlc.py --reset-errors   # Reset all errored stocks to pending
    python download_ohlc.py --dry-run        # Initialize tracking without downloading
"""

import os
import sys
import json
import argparse
from dotenv import load_dotenv

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.utils.logger import setup_logger
from src.api.icici_api import ICICIDirectAPI
from src.backtest.backtest_db import BacktestDatabase
from src.backtest.rate_limiter import RateLimiter
from src.backtest.ohlc_downloader import OHLCDownloader


def load_config(config_path="config/config.json"):
    """Load configuration from JSON file."""
    with open(config_path, 'r') as f:
        return json.load(f)


def show_status(db, config):
    """Display download progress summary."""
    downloader = OHLCDownloader(None, db, None, config)
    summary = downloader.get_download_summary()

    print(f"\n{'='*50}")
    print(f"  OHLC Download Progress")
    print(f"{'='*50}")
    print(f"  Completed:   {summary['completed']}/{summary['total_stocks']} stocks")
    print(f"  In Progress: {summary['in_progress']}")
    print(f"  Pending:     {summary['pending']}")
    print(f"  Errors:      {summary['errored']}")
    print(f"{'─'*50}")
    print(f"  Total Records:   {summary['total_records']:,}")
    print(f"  Total API Calls: {summary['total_api_calls']:,}")
    print(f"{'='*50}")

    if summary['completed_stocks']:
        print(f"\n  Completed stocks:")
        for stock in summary['completed_stocks']:
            count = db.get_stock_record_count(stock)
            print(f"    {stock}: {count:,} records")

    if summary['in_progress_stocks']:
        print(f"\n  In-progress stocks:")
        for stock in summary['in_progress_stocks']:
            progress = db.get_download_progress(stock)
            print(f"    {stock}: last downloaded {progress['last_downloaded_date'] or 'N/A'}")

    if summary['errored_stocks']:
        print(f"\n  Errored stocks:")
        for stock, error in summary['errored_stocks']:
            print(f"    {stock}: {error}")

    print()


def main():
    parser = argparse.ArgumentParser(
        description='Download OHLC data for backtesting from Breeze API'
    )
    parser.add_argument(
        '--config', default='config/config.json',
        help='Path to configuration file (default: config/config.json)'
    )
    parser.add_argument(
        '--status', action='store_true',
        help='Show download progress without downloading'
    )
    parser.add_argument(
        '--reset', metavar='STOCK_CODE',
        help='Reset download progress for a specific stock'
    )
    parser.add_argument(
        '--reset-errors', action='store_true',
        help='Reset all stocks with error status back to pending'
    )
    parser.add_argument(
        '--dry-run', action='store_true',
        help='Initialize progress tracking without starting download'
    )
    args = parser.parse_args()

    # Setup logger
    logger = setup_logger(
        name="ICICI_ORB_Bot",
        log_file="logs/ohlc_download.log"
    )

    # Load environment and config
    load_dotenv()
    config = load_config(args.config)

    # Validate config has required sections
    if 'nifty_50_stocks' not in config:
        logger.error("Config missing 'nifty_50_stocks' list")
        print("Error: config.json must contain a 'nifty_50_stocks' list")
        return 1

    if 'backtest' not in config:
        logger.error("Config missing 'backtest' section")
        print("Error: config.json must contain a 'backtest' section")
        return 1

    # Initialize database
    db_path = config["backtest"].get("db_path", "Data/backtest.db")
    db = BacktestDatabase(db_path)

    # Handle --status
    if args.status:
        show_status(db, config)
        return 0

    # Handle --reset
    if args.reset:
        db.reset_stock_progress(args.reset)
        print(f"Reset progress for {args.reset}")
        logger.info(f"Reset progress for {args.reset}")
        return 0

    # Handle --reset-errors
    if args.reset_errors:
        db.reset_errored_stocks()
        print("Reset all errored stocks to pending")
        logger.info("Reset all errored stocks to pending")
        return 0

    # Handle --dry-run
    if args.dry_run:
        downloader = OHLCDownloader(None, db, None, config)
        downloader.initialize_all_stocks()
        summary = downloader.get_download_summary()
        print(f"Initialized {summary['total_stocks']} stocks for download")
        print(f"  Date range: {config['backtest']['start_date']} to {config['backtest']['end_date']}")
        print(f"  Interval: {config['backtest'].get('interval', '1minute')}")
        print(f"\nRun without --dry-run to start downloading.")
        return 0

    # --- Full download mode: authenticate and run ---

    app_key = os.environ.get('ICICI_APP_KEY')
    secret_key = os.environ.get('ICICI_SECRET_KEY')
    api_session = os.environ.get('ICICI_API_SESSION')

    if not all([app_key, secret_key, api_session]):
        logger.error("Missing API credentials in .env file")
        print("Error: Set ICICI_APP_KEY, ICICI_SECRET_KEY, ICICI_API_SESSION in .env")
        return 1

    # Authenticate with Breeze API
    print("Authenticating with Breeze API...")
    api = ICICIDirectAPI(app_key, secret_key)
    customer_details = api.get_customer_details(api_session, app_key)

    if not api.is_connected:
        logger.error(f"Failed to authenticate: {customer_details.get('Error')}")
        print(f"Error: Authentication failed - {customer_details.get('Error')}")
        return 1

    print("Authenticated successfully!")
    logger.info("Authenticated with Breeze API for OHLC download")

    # Create rate limiter
    rate_limiter = RateLimiter(
        calls_per_minute=config["backtest"].get("calls_per_minute", 95),
        calls_per_day=config["backtest"].get("calls_per_day", 4900),
        db=db
    )

    remaining = rate_limiter.get_remaining_daily()
    print(f"API calls remaining today: {remaining}")

    if remaining <= 0:
        print("Daily API limit already reached. Try again tomorrow.")
        return 0

    # Create downloader and run
    downloader = OHLCDownloader(api, db, rate_limiter, config)

    print(f"\nStarting OHLC data download...")
    print(f"  Stocks: {len(config['nifty_50_stocks'])}")
    print(f"  Date range: {config['backtest']['start_date']} to {config['backtest']['end_date']}")
    print(f"  Interval: {config['backtest'].get('interval', '1minute')}")
    print(f"  Chunk size: {config['backtest'].get('chunk_days', 2)} days per API call")
    print()

    logger.info("Starting OHLC data download")
    result = downloader.run()

    # Print session summary
    summary = downloader.get_download_summary()
    print(f"\n{'='*50}")
    print(f"  Download Session Complete")
    print(f"{'='*50}")
    print(f"  Status: {result['status']}")
    print(f"  Records this session: {result.get('records_today', 0):,}")
    print(f"  API calls this session: {result.get('calls_today', 0):,}")
    print(f"  Stocks completed today: {result.get('stocks_completed_today', 0)}")
    print(f"{'─'*50}")
    print(f"  Overall: {summary['completed']}/{summary['total_stocks']} stocks completed")
    print(f"  Total records: {summary['total_records']:,}")
    print(f"  Total API calls: {summary['total_api_calls']:,}")
    print(f"{'='*50}")

    if result['status'] == 'daily_limit':
        print(f"\nDaily API limit reached. Run again tomorrow to continue.")
        print(f"Stocks remaining: {result.get('stocks_remaining', '?')}")

    if result['status'] == 'complete':
        print(f"\nAll stocks downloaded successfully!")

    return 0


if __name__ == "__main__":
    sys.exit(main())
