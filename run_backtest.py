#!/usr/bin/env python3
"""
CLI entry point for ORB backtest grid search.

Usage:
    python run_backtest.py                          # Full grid, 1 worker
    python run_backtest.py --quick                  # Reduced grid for validation
    python run_backtest.py --workers 4              # Parallel processing
    python run_backtest.py --stocks RELIND INFTEC   # Specific stocks
    python run_backtest.py --or-minutes 15 30       # Pin OR durations
    python run_backtest.py --targets 0 2.0          # Pin targets
    python run_backtest.py --sl-types fixed         # Pin SL type
    python run_backtest.py --directions both         # Pin direction
    python run_backtest.py --exit-times 15:14       # Pin exit time
    python run_backtest.py --trades                 # Store individual trades
    python run_backtest.py --dates 2023-01-01 2025-12-31  # Custom date range
    python run_backtest.py --status                 # Show progress
    python run_backtest.py --resume                 # Resume interrupted run
    python run_backtest.py --report                 # Generate reports from last run
    python run_backtest.py --report --run-id 3      # Reports for specific run
"""

import os
import sys
import json
import argparse
import logging

# Add project root to path (same pattern as download_ohlc.py)
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(project_root, 'src'))

from backtest.runner import BacktestRunner
from backtest.results_db import ResultsDatabase
from backtest.report_generator import ReportGenerator
from backtest.parameter_grid import ParameterGrid


def load_config(config_path: str) -> dict:
    """Load configuration from JSON file."""
    with open(config_path, 'r') as f:
        return json.load(f)


def setup_logging():
    """Set up basic logging."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )


def main():
    parser = argparse.ArgumentParser(
        description="ORB Backtest Grid Search — Find optimal parameters for the Opening Range Breakout strategy",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Quick validation:   python run_backtest.py --quick --stocks RELIND
  Full sweep:         python run_backtest.py --workers 4
  Specific params:    python run_backtest.py --or-minutes 15 30 --targets 0 2.0
  View results:       python run_backtest.py --report
  Resume:             python run_backtest.py --resume
        """,
    )

    # Mode flags
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--status", action="store_true",
                      help="Show progress of current/last run")
    mode.add_argument("--report", action="store_true",
                      help="Generate reports from completed run")

    # Run configuration
    parser.add_argument("--quick", action="store_true",
                        help="Use reduced parameter grid for quick validation")
    parser.add_argument("--workers", type=int, default=1,
                        help="Number of parallel workers (default: 1)")
    parser.add_argument("--trades", action="store_true",
                        help="Store individual trade records (uses more disk)")
    parser.add_argument("--resume", action="store_true",
                        help="Resume the last interrupted run")
    parser.add_argument("--run-id", type=int, default=None,
                        help="Specific run ID (for --status, --report, or --resume)")

    # Stock selection
    parser.add_argument("--stocks", nargs="+", type=str, default=None,
                        help="Specific stock codes to backtest (default: all 50)")

    # Parameter pinning
    parser.add_argument("--or-minutes", nargs="+", type=int, default=None,
                        help="Pin OR duration values (e.g., 15 30)")
    parser.add_argument("--targets", nargs="+", type=float, default=None,
                        help="Pin target multiplier values (e.g., 0 2.0)")
    parser.add_argument("--sl-types", nargs="+", type=str, default=None,
                        help="Pin SL types: fixed, trailing, atr_based")
    parser.add_argument("--directions", nargs="+", type=str, default=None,
                        help="Pin directions: long_only, short_only, both")
    parser.add_argument("--exit-times", nargs="+", type=str, default=None,
                        help="Pin exit times (e.g., 14:30 15:14)")

    # Date range
    parser.add_argument("--dates", nargs=2, type=str, default=None,
                        metavar=("START", "END"),
                        help="Custom date range: YYYY-MM-DD YYYY-MM-DD")

    # Config file
    parser.add_argument("--config", type=str, default="config/config.json",
                        help="Path to config file (default: config/config.json)")

    args = parser.parse_args()
    setup_logging()

    # Load config
    config_path = os.path.join(project_root, args.config)
    if not os.path.exists(config_path):
        print(f"Error: Config file not found: {config_path}")
        sys.exit(1)

    config = load_config(config_path)

    # Handle --status
    if args.status:
        runner = BacktestRunner(
            config=config,
            resume_run_id=args.run_id,
        )
        runner.show_status()
        return

    # Handle --report
    if args.report:
        sweep = config.get("backtest_sweep", {})
        results_db_path = sweep.get("results_db_path", "Data/backtest_results.db")
        results_db_path = os.path.join(project_root, results_db_path)

        if not os.path.exists(results_db_path):
            print(f"Error: Results database not found: {results_db_path}")
            print("Run a backtest first: python run_backtest.py --quick --stocks RELIND")
            sys.exit(1)

        results_db = ResultsDatabase(results_db_path)

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

        output_dir = os.path.join(project_root, "Reports", "backtest")
        reporter = ReportGenerator(results_db, run_id, output_dir)
        reporter.generate_all()
        return

    # Handle --resume
    resume_run_id = args.run_id
    if args.resume and resume_run_id is None:
        # Find latest non-completed run
        sweep = config.get("backtest_sweep", {})
        results_db_path = sweep.get("results_db_path", "Data/backtest_results.db")
        results_db_path = os.path.join(project_root, results_db_path)

        if os.path.exists(results_db_path):
            results_db = ResultsDatabase(results_db_path)
            results_db.connect()
            run = results_db.get_latest_run()
            results_db.close()
            if run and run["status"] != "completed":
                resume_run_id = run["run_id"]
                print(f"Found interrupted run #{resume_run_id}")
            else:
                print("No interrupted run found. Starting fresh.")
        else:
            print("No results database found. Starting fresh.")

    # Parse dates
    start_date = args.dates[0] if args.dates else None
    end_date = args.dates[1] if args.dates else None

    # Preview parameter count
    if not args.quick:
        grid = ParameterGrid(config)
        if any([args.or_minutes, args.targets, args.sl_types,
                args.directions, args.exit_times]):
            preview = grid.generate_filtered(
                or_minutes=args.or_minutes,
                targets=args.targets,
                sl_types=args.sl_types,
                directions=args.directions,
                exit_times=args.exit_times,
            )
            n_combos = len(preview)
        else:
            n_combos = grid.count()

        n_stocks = len(args.stocks) if args.stocks else len(config.get("nifty_50_stocks", []))
        total = n_combos * n_stocks
        print(f"Grid: {n_combos:,} combos × {n_stocks} stocks = {total:,} simulations")

    # Create and run
    runner = BacktestRunner(
        config=config,
        stocks=args.stocks,
        workers=args.workers,
        store_trades=args.trades,
        resume_run_id=resume_run_id,
        quick=args.quick,
        or_minutes=args.or_minutes,
        targets=args.targets,
        sl_types=args.sl_types,
        directions=args.directions,
        exit_times=args.exit_times,
        start_date=start_date,
        end_date=end_date,
    )

    result = runner.run()

    # Auto-generate report if completed
    if result.get("status") == "completed":
        print("\nAuto-generating report...")
        sweep = config.get("backtest_sweep", {})
        results_db_path = sweep.get("results_db_path", "Data/backtest_results.db")
        results_db_path = os.path.join(project_root, results_db_path)
        results_db = ResultsDatabase(results_db_path)
        output_dir = os.path.join(project_root, "Reports", "backtest")
        reporter = ReportGenerator(results_db, result["run_id"], output_dir)
        reporter.generate_all()


if __name__ == "__main__":
    main()
