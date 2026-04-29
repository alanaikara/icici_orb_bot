#!/usr/bin/env python3
"""
Live trader entry point for Fib-MACD strategy.

Usage:
    # Dry run (logs orders, doesn't place them) — always start here
    python run_live_trader.py --dry-run

    # Live trading
    python run_live_trader.py

    # Trade specific stocks only
    python run_live_trader.py --stocks ADAENT JSWSTE TATMOT --dry-run

    # Custom risk per trade
    python run_live_trader.py --risk 500 --dry-run

Set environment variables before running:
    export ICICI_API_KEY=your_api_key
    export ICICI_SECRET_KEY=your_secret_key
    export ICICI_API_SESSION=your_session_token   # refreshed daily
"""

import os
import sys
import argparse
import logging

project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(project_root, 'src'))

from live.breeze_broker import BreezeBroker
from live.live_trader import LiveTrader
from live.strategy_config import FIB_MACD_PORTFOLIO, StrategyConfig, StockConfig


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("logs/live_trader.log", mode='a'),
        ]
    )


def main():
    parser = argparse.ArgumentParser(
        description="Fib-MACD Live Trader",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Strategy: 30min OR → 61.8% fib pullback → MACD cross → 1.5R target → exit 15:14

Always start with --dry-run to verify signal detection before going live.
        """
    )

    parser.add_argument("--dry-run", action="store_true", default=True,
                        help="Log orders without placing them (default: True)")
    parser.add_argument("--live", action="store_true",
                        help="Place real orders (overrides --dry-run)")
    parser.add_argument("--stocks", nargs="+", default=None,
                        help="Trade only these stocks (default: full 34-stock portfolio)")
    parser.add_argument("--risk", type=float, default=1000.0,
                        help="Max risk per trade in ₹ (default: 1000)")
    parser.add_argument("--capital", type=float, default=100_000.0,
                        help="Capital per trade for position sizing (default: 100000)")

    args = parser.parse_args()
    dry_run = not args.live

    os.makedirs("logs", exist_ok=True)
    setup_logging()
    logger = logging.getLogger("ICICI_ORB_Bot")

    # ── Credentials ───────────────────────────────────────────────────────────
    app_key       = os.environ.get("ICICI_API_KEY")
    secret_key    = os.environ.get("ICICI_SECRET_KEY")
    session_token = os.environ.get("ICICI_API_SESSION")

    if not all([app_key, secret_key, session_token]):
        print("Error: Set ICICI_API_KEY, ICICI_SECRET_KEY, ICICI_API_SESSION env vars")
        print("Example:")
        print("  export ICICI_API_KEY=xxxx")
        print("  export ICICI_SECRET_KEY=xxxx")
        print("  export ICICI_API_SESSION=xxxx   # from ICICI login, refreshed daily")
        sys.exit(1)

    # ── Strategy config ───────────────────────────────────────────────────────
    config = FIB_MACD_PORTFOLIO
    config.max_risk_per_trade = args.risk
    config.capital = args.capital

    # Filter to specific stocks if requested
    if args.stocks:
        stock_map = {sc.stock_code: sc for sc in config.stocks}
        config.stocks = [
            stock_map.get(s, StockConfig(s, "both"))
            for s in args.stocks
        ]

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"Fib-MACD Live Trader")
    print(f"{'='*60}")
    print(f"Mode:        {'DRY RUN ⚠️' if dry_run else '🔴 LIVE TRADING'}")
    print(f"Stocks:      {len(config.stocks)}")
    print(f"OR duration: {config.or_minutes} min")
    print(f"Fib entry:   {config.fib_entry_pct*100:.1f}%")
    print(f"MACD:        {config.macd_condition}")
    print(f"Exit time:   {config.exit_time}")
    print(f"Max risk:    ₹{config.max_risk_per_trade:,.0f}/trade")
    print(f"{'='*60}\n")

    if not dry_run:
        confirm = input("⚠️  LIVE MODE — type 'YES' to confirm: ")
        if confirm != "YES":
            print("Aborted.")
            sys.exit(0)

    # ── Run ───────────────────────────────────────────────────────────────────
    broker = BreezeBroker(app_key, secret_key, session_token)
    trader = LiveTrader(broker, config, dry_run=dry_run)
    trader.run()


if __name__ == "__main__":
    main()
