#!/usr/bin/env python3
"""
CLI entry point for Fib-MACD pullback strategy grid search.

Strategy: ORB sets direction → wait for post-breakout swing → enter at
Fibonacci retracement (38.2%, 50%, or 61.8%) on a candle-close bounce
confirmed by MACD → stop just below 78.6%.

Usage:
    python run_fib_backtest.py                          # Full grid, 1 worker
    python run_fib_backtest.py --quick                  # Fast validation (1 stock or --stocks)
    python run_fib_backtest.py --workers 4              # Parallel
    python run_fib_backtest.py --stocks RELIND INFTEC   # Specific stocks
    python run_fib_backtest.py --or-minutes 15          # Pin OR duration
    python run_fib_backtest.py --fib-levels 0.618       # Pin fib entry level
    python run_fib_backtest.py --macd histogram_rising  # Pin MACD condition
    python run_fib_backtest.py --targets 2.0 2.5        # Pin target R
    python run_fib_backtest.py --directions both        # Pin direction
    python run_fib_backtest.py --exit-times 15:14       # Pin exit time
    python run_fib_backtest.py --trades                 # Store individual trades
    python run_fib_backtest.py --dates 2023-01-01 2025-12-31
    python run_fib_backtest.py --status
    python run_fib_backtest.py --resume
    python run_fib_backtest.py --report
    python run_fib_backtest.py --report --run-id 5
"""

import os
import sys
import json
import argparse
import logging

project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(project_root, 'src'))

from backtest.fib_macd_runner import FibMACDRunner
from backtest.fib_macd_engine import generate_param_grid
from backtest.results_db import ResultsDatabase
from backtest.report_generator import ReportGenerator


def load_config(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )


def main():
    parser = argparse.ArgumentParser(
        description="Fib-MACD ORB Pullback Strategy — Grid Search",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Strategy overview:
  1. Opening Range (15 or 30 min) sets OR high/low.
  2. First breakout of OR high/low establishes trade direction.
  3. Post-breakout swing high/low is tracked; swing locked when price
     retraces 0.3% from the peak.
  4. Fibonacci drawn from OR low -> swing high (long) or OR high -> swing low (short).
  5. Entry: candle CLOSES above the fib level (38.2%, 50%, or 61.8%)
     after having wicked into it — the bounce confirmation.
  6. MACD on 5-min bars must confirm direction.
  7. Stop loss just below 78.6% fib level.
  8. Target = entry ± risk * target_R.

Examples:
  Quick test:   python run_fib_backtest.py --quick --stocks RELIND
  Full sweep:   python run_fib_backtest.py --workers 4
  Specific:     python run_fib_backtest.py --fib-levels 0.618 --macd histogram_rising
  Reports:      python run_fib_backtest.py --report
        """,
    )

    # Mode
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--status", action="store_true", help="Show progress of latest run")
    mode.add_argument("--report", action="store_true", help="Generate report from last completed run")

    # Run config
    parser.add_argument("--quick",   action="store_true", help="Use reduced grid for fast validation")
    parser.add_argument("--workers", type=int, default=1,  help="Parallel workers (default: 1)")
    parser.add_argument("--trades",  action="store_true", help="Store individual trade records")
    parser.add_argument("--resume",  action="store_true", help="Resume last interrupted run")
    parser.add_argument("--run-id",  type=int, default=None, help="Target a specific run ID")

    # Stock selection
    parser.add_argument("--stocks", nargs="+", type=str, default=None,
                        help="Stock codes to backtest (default: all 50 Nifty stocks)")

    # Parameter pinning
    parser.add_argument("--or-minutes",  nargs="+", type=int,   default=None,
                        help="OR durations in minutes (e.g. 15 30)")
    parser.add_argument("--fib-levels",  nargs="+", type=float, default=None,
                        help="Fib entry levels (e.g. 0.382 0.618)")
    parser.add_argument("--macd",        nargs="+", type=str,   default=None,
                        dest="macd_conditions",
                        help="MACD conditions: histogram_positive histogram_rising macd_cross none")
    parser.add_argument("--targets",     nargs="+", type=float, default=None,
                        help="Target R multiples (e.g. 2.0 2.5)")
    parser.add_argument("--directions",  nargs="+", type=str,   default=None,
                        help="Trade directions: long_only short_only both")
    parser.add_argument("--exit-times",  nargs="+", type=str,   default=None,
                        help="Force-exit times (e.g. 15:00 15:14)")

    # Date range
    parser.add_argument("--dates", nargs=2, type=str, default=None,
                        metavar=("START", "END"),
                        help="Date range YYYY-MM-DD YYYY-MM-DD")

    # Realistic cost model
    parser.add_argument("--slippage", type=float, default=0.0,
                        help="Slippage per side as decimal (e.g. 0.0005 = 0.05%%)")
    parser.add_argument("--zerodha-charges", action="store_true",
                        help="Use full Zerodha intraday charge structure")

    parser.add_argument("--config", type=str, default="config/config.json")

    args = parser.parse_args()
    setup_logging()

    # Resolve config — check project_root first, then cwd
    config_path = os.path.join(project_root, args.config)
    if not os.path.exists(config_path):
        config_path = os.path.join(os.getcwd(), args.config)
    if not os.path.exists(config_path):
        print(f"Error: config not found: {config_path}")
        sys.exit(1)
    config = load_config(config_path)

    # data_root = the directory that contains Data/, Reports/, config/, etc.
    # Config lives at <data_root>/config/config.json, so go one level up.
    data_root = os.path.dirname(os.path.abspath(config_path))
    if os.path.basename(data_root) == "config":
        data_root = os.path.dirname(data_root)
    sweep     = config.get("backtest_sweep", {})
    bt        = config.get("backtest", {})
    results_db_path = os.path.join(data_root, sweep.get("results_db_path", "Data/backtest_results.db"))
    ohlc_db_path    = os.path.join(data_root, bt.get("db_path", "Data/backtest.db"))

    # ── --status ──────────────────────────────────────────────────────────────
    if args.status:
        runner = FibMACDRunner(
            config=config, resume_run_id=args.run_id,
            _results_db_path=results_db_path, _ohlc_db_path=ohlc_db_path,
        )
        runner.show_status()
        return

    # ── --report ──────────────────────────────────────────────────────────────
    if args.report:
        db_path = results_db_path
        if not os.path.exists(db_path):
            print(f"No results database found at {db_path}")
            print("Run a backtest first: python run_fib_backtest.py --quick --stocks RELIND")
            sys.exit(1)

        results_db = ResultsDatabase(db_path)
        if args.run_id:
            run_id = args.run_id
        else:
            results_db.connect()
            run = results_db.get_latest_run()
            results_db.close()
            if not run:
                print("No completed runs found.")
                sys.exit(1)
            run_id = run["run_id"]

        output_dir = os.path.join(project_root, "Reports", "fib_macd")
        reporter = ReportGenerator(results_db, run_id, output_dir)
        reporter.generate_all()
        return

    # ── --resume: find last interrupted run ───────────────────────────────────
    resume_run_id = args.run_id
    if args.resume and resume_run_id is None:
        if os.path.exists(results_db_path):
            rdb = ResultsDatabase(results_db_path)
            rdb.connect()
            run = rdb.get_latest_run()
            rdb.close()
            if run and run["status"] != "completed":
                resume_run_id = run["run_id"]
                print(f"Resuming run #{resume_run_id}")
            else:
                print("No interrupted run found — starting fresh.")

    # ── Preview combo count ───────────────────────────────────────────────────
    if not args.quick:
        preview = generate_param_grid(
            or_minutes      = args.or_minutes,
            fib_entries     = args.fib_levels,
            macd_conditions = args.macd_conditions,
            targets         = args.targets,
            directions      = args.directions,
            exit_times      = args.exit_times,
        )
        n_stocks = len(args.stocks) if args.stocks else len(config.get("nifty_50_stocks", []))
        print(f"Grid: {len(preview):,} combos × {n_stocks} stocks = {len(preview)*n_stocks:,} simulations")

    # ── Run ───────────────────────────────────────────────────────────────────
    start_date = args.dates[0] if args.dates else None
    end_date   = args.dates[1] if args.dates else None

    runner = FibMACDRunner(
        config               = config,
        stocks               = args.stocks,
        workers              = args.workers,
        store_trades         = args.trades,
        resume_run_id        = resume_run_id,
        quick                = args.quick,
        or_minutes           = args.or_minutes,
        fib_entries          = args.fib_levels,
        macd_conditions      = args.macd_conditions,
        targets              = args.targets,
        directions           = args.directions,
        exit_times           = args.exit_times,
        start_date           = start_date,
        end_date             = end_date,
        _results_db_path     = results_db_path,
        _ohlc_db_path        = ohlc_db_path,
        slippage_pct         = args.slippage,
        use_zerodha_charges  = args.zerodha_charges,
    )

    result = runner.run()

    # Auto-report on completion
    if result.get("status") == "completed":
        print("\nAuto-generating report...")
        output_dir = os.path.join(data_root, "Reports", "fib_macd")
        reporter   = ReportGenerator(ResultsDatabase(results_db_path), result["run_id"], output_dir)
        reporter.generate_all()


if __name__ == "__main__":
    main()
