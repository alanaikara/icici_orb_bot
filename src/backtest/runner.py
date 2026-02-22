"""
Backtest orchestrator for ORB grid search.

Manages the full workflow: load data, generate parameters, run simulations
(optionally in parallel), compute metrics, store results, track progress.
Supports resume of interrupted runs.
"""

import time
import json
import logging
import multiprocessing as mp
from datetime import datetime
from functools import partial

from backtest.parameter_grid import ParameterGrid, StrategyParams
from backtest.data_loader import DataLoader
from backtest.backtest_engine import ORBSimulator
from backtest.metrics import MetricsCalculator
from backtest.results_db import ResultsDatabase

logger = logging.getLogger("ICICI_ORB_Bot")


def _process_stock_worker(
    stock_code: str,
    params_list: list[StrategyParams],
    ohlc_db_path: str,
    start_date: str,
    end_date: str,
    capital: float,
    max_risk_per_trade: float,
    brokerage_rate: float,
    stt_rate: float,
    store_trades: bool,
) -> dict:
    """
    Process all parameter combos for a single stock.
    Standalone function (not method) for multiprocessing compatibility.

    OPTIMIZED: Groups params by (or_minutes, exit_time) to reuse
    precomputed DayCaches across params sharing the same values.

    Returns dict with results for the main process to insert into DB.
    """
    t0 = time.time()

    # Each worker creates its own instances (lightweight, stateless)
    loader = DataLoader(ohlc_db_path)
    simulator = ORBSimulator(
        capital=capital,
        max_risk_per_trade=max_risk_per_trade,
        brokerage_rate=brokerage_rate,
        stt_rate=stt_rate,
    )
    metrics_calc = MetricsCalculator(capital=capital)

    # Get unique OR durations needed
    or_minutes_list = sorted(set(p.or_minutes for p in params_list))

    # Load data once, precompute all OR durations
    stock_data = loader.load_stock(
        stock_code,
        start_date=start_date,
        end_date=end_date,
        or_minutes_list=or_minutes_list,
    )

    if not stock_data.trading_days:
        return {
            "stock_code": stock_code,
            "metrics_rows": [],
            "trade_rows": [],
            "total_trades": 0,
            "combos_tested": 0,
            "elapsed": time.time() - t0,
        }

    metrics_rows = []
    trade_rows = []
    total_trades = 0

    # Group params by (or_minutes, exit_time) to reuse DayCaches
    cache_groups = {}
    for params in params_list:
        key = (params.or_minutes, params.exit_time)
        cache_groups.setdefault(key, []).append(params)

    for (or_minutes, exit_time), group_params in cache_groups.items():
        # Build DayCaches once for this (or_minutes, exit_time) group
        or_data = stock_data.opening_ranges.get(or_minutes)
        if or_data is None:
            continue

        total_mins = 9 * 60 + 15 + or_minutes
        eh, em = divmod(total_mins, 60)
        or_end_str = f"{eh:02d}:{em:02d}:00"
        exit_str = f"{exit_time}:00"

        day_caches = simulator._build_day_caches(
            stock_data, or_data, or_end_str, exit_str,
        )

        # Run all params in this group using the shared caches
        for params in group_params:
            trades = simulator.run_with_caches(
                stock_data, params, or_data, day_caches,
            )
            total_trades += len(trades)

            result = metrics_calc.compute(trades)

            param_id = params.param_id()
            metrics_row = (
                param_id, stock_code,
                params.or_minutes, params.target_multiplier,
                params.stop_loss_type.value, params.trade_direction.value,
                params.exit_time, params.max_or_filter_pct,
                params.entry_confirmation.value,
                *result.to_metrics_tuple(),
            )
            metrics_rows.append(metrics_row)

            if store_trades and trades:
                for trade in trades:
                    trade_rows.append(trade.to_tuple(param_id))

    elapsed = time.time() - t0

    return {
        "stock_code": stock_code,
        "metrics_rows": metrics_rows,
        "trade_rows": trade_rows,
        "total_trades": total_trades,
        "combos_tested": len(params_list),
        "elapsed": elapsed,
    }


class BacktestRunner:
    """
    Orchestrates the full backtest grid search.

    Loads data, generates parameters, runs simulations (serial or parallel),
    computes metrics, and stores results with resume capability.
    """

    def __init__(
        self,
        config: dict,
        stocks: list[str] = None,
        workers: int = 1,
        store_trades: bool = False,
        resume_run_id: int = None,
        quick: bool = False,
        or_minutes: list[int] = None,
        targets: list[float] = None,
        sl_types: list[str] = None,
        directions: list[str] = None,
        exit_times: list[str] = None,
        start_date: str = None,
        end_date: str = None,
    ):
        self.config = config
        self.stocks = stocks or config.get("nifty_50_stocks", [])
        self.workers = workers
        self.store_trades = store_trades
        self.resume_run_id = resume_run_id

        # Config values
        sweep = config.get("backtest_sweep", {})
        bt = config.get("backtest", {})
        self.ohlc_db_path = bt.get("db_path", "Data/backtest.db")
        self.results_db_path = sweep.get("results_db_path", "Data/backtest_results.db")
        self.capital = sweep.get("capital", config.get("capital", 100000))
        self.max_risk_per_trade = sweep.get(
            "max_risk_per_trade", config.get("max_risk_per_trade", 1000)
        )
        self.brokerage_rate = sweep.get(
            "brokerage_rate", config.get("brokerage_rate", 0.0001)
        )
        self.stt_rate = sweep.get("stt_rate", config.get("stt_rate", 0.00025))
        self.start_date = start_date or bt.get("start_date")
        self.end_date = end_date or bt.get("end_date")

        # Generate parameter grid
        grid = ParameterGrid(config)
        if quick:
            self.params_list = grid.generate_quick()
        elif any([or_minutes, targets, sl_types, directions, exit_times]):
            self.params_list = grid.generate_filtered(
                or_minutes=or_minutes,
                targets=targets,
                sl_types=sl_types,
                directions=directions,
                exit_times=exit_times,
            )
        else:
            self.params_list = grid.generate_all()

        self.results_db = ResultsDatabase(self.results_db_path)

    def run(self) -> dict:
        """
        Execute the full grid search.

        Returns summary dict with run stats.
        """
        total_combos = len(self.params_list)
        total_stocks = len(self.stocks)
        total_sims = total_combos * total_stocks

        print(f"\n{'='*60}")
        print(f"ORB Backtest Grid Search")
        print(f"{'='*60}")
        print(f"Stocks:           {total_stocks}")
        print(f"Parameter combos: {total_combos:,}")
        print(f"Total simulations:{total_sims:,}")
        print(f"Workers:          {self.workers}")
        print(f"Date range:       {self.start_date or 'all'} to {self.end_date or 'all'}")
        print(f"Store trades:     {self.store_trades}")
        print(f"{'='*60}\n")

        # Create or resume run
        run_id = self._init_run(total_combos)
        print(f"Run ID: {run_id}\n")

        # Get stocks to process (skip completed if resuming)
        stocks_to_process = self._get_stocks_to_process(run_id)
        if not stocks_to_process:
            print("All stocks already completed! Use --report to view results.")
            return {"run_id": run_id, "status": "already_complete"}

        print(f"Stocks to process: {len(stocks_to_process)}/{total_stocks}")

        # Store all params in the params table
        self.results_db.connect()
        self.results_db.insert_params_batch(self.params_list)
        self.results_db.close()

        t0 = time.time()
        stocks_done = total_stocks - len(stocks_to_process)
        total_trades_all = 0

        try:
            if self.workers <= 1:
                # Serial processing
                for i, stock_code in enumerate(stocks_to_process):
                    result = self._process_stock_serial(stock_code, run_id, i, len(stocks_to_process))
                    stocks_done += 1
                    total_trades_all += result["total_trades"]

                    # Update run progress
                    elapsed = time.time() - t0
                    self.results_db.connect()
                    self.results_db.update_run_status(
                        run_id, "running",
                        combos_completed=stocks_done * total_combos,
                        stocks_completed=stocks_done,
                        elapsed_seconds=elapsed,
                    )
                    self.results_db.close()
            else:
                # Parallel processing
                self._process_stocks_parallel(
                    stocks_to_process, run_id, total_combos, t0
                )
                stocks_done = total_stocks
                elapsed = time.time() - t0

            # Mark run as complete
            elapsed = time.time() - t0
            self.results_db.connect()
            self.results_db.update_run_status(
                run_id, "completed",
                combos_completed=total_sims,
                stocks_completed=total_stocks,
                elapsed_seconds=elapsed,
            )
            self.results_db.close()

            print(f"\n{'='*60}")
            print(f"COMPLETED in {elapsed:.1f}s ({elapsed/60:.1f} min)")
            print(f"Total simulations: {total_sims:,}")
            print(f"Run: python run_backtest.py --report --run-id {run_id}")
            print(f"{'='*60}\n")

            return {
                "run_id": run_id,
                "status": "completed",
                "elapsed_seconds": elapsed,
                "total_simulations": total_sims,
            }

        except KeyboardInterrupt:
            elapsed = time.time() - t0
            print(f"\n\nInterrupted after {elapsed:.1f}s")
            print(f"Resume with: python run_backtest.py --resume")
            self.results_db.connect()
            self.results_db.update_run_status(
                run_id, "interrupted", elapsed_seconds=elapsed,
            )
            self.results_db.close()
            return {"run_id": run_id, "status": "interrupted"}

        except Exception as e:
            elapsed = time.time() - t0
            logger.error(f"Error during backtest: {e}")
            self.results_db.connect()
            self.results_db.update_run_status(
                run_id, "interrupted", elapsed_seconds=elapsed,
            )
            self.results_db.close()
            raise

    def _init_run(self, total_combos: int) -> int:
        """Create new run or resume existing one."""
        self.results_db.connect()

        if self.resume_run_id is not None:
            run = self.results_db.get_run(self.resume_run_id)
            if run:
                print(f"Resuming run {self.resume_run_id} "
                      f"({run['stocks_completed']}/{run['total_stocks']} stocks done)")
                self.results_db.close()
                return self.resume_run_id
            else:
                print(f"Run {self.resume_run_id} not found, creating new run")

        run_id = self.results_db.create_run(
            config_snapshot=self.config,
            total_combos=total_combos,
            stocks=self.stocks,
            workers=self.workers,
            store_trades=self.store_trades,
            start_date=self.start_date,
            end_date=self.end_date,
        )
        self.results_db.close()
        return run_id

    def _get_stocks_to_process(self, run_id: int) -> list[str]:
        """Get stocks that haven't been completed yet."""
        self.results_db.connect()
        completed = set(self.results_db.get_completed_stocks(run_id))
        self.results_db.close()
        return [s for s in self.stocks if s not in completed]

    def _process_stock_serial(
        self, stock_code: str, run_id: int, idx: int, total: int
    ) -> dict:
        """Process one stock serially with progress output."""
        print(f"[{idx + 1}/{total}] Processing {stock_code}...", end=" ", flush=True)

        self.results_db.connect()
        self.results_db.mark_stock_in_progress(run_id, stock_code)
        self.results_db.close()

        result = _process_stock_worker(
            stock_code=stock_code,
            params_list=self.params_list,
            ohlc_db_path=self.ohlc_db_path,
            start_date=self.start_date,
            end_date=self.end_date,
            capital=self.capital,
            max_risk_per_trade=self.max_risk_per_trade,
            brokerage_rate=self.brokerage_rate,
            stt_rate=self.stt_rate,
            store_trades=self.store_trades,
        )

        # Insert results into DB
        self.results_db.connect()
        self.results_db.insert_metrics_batch(run_id, result["metrics_rows"])
        if self.store_trades and result["trade_rows"]:
            self.results_db.insert_trades_batch(run_id, result["trade_rows"])
        self.results_db.mark_stock_complete(
            run_id, stock_code,
            combos_tested=result["combos_tested"],
            total_trades=result["total_trades"],
            elapsed=result["elapsed"],
        )
        self.results_db.close()

        print(
            f"done in {result['elapsed']:.1f}s "
            f"({result['combos_tested']} combos, {result['total_trades']} trades)"
        )

        return result

    def _process_stocks_parallel(
        self, stocks: list[str], run_id: int, total_combos: int, t0: float
    ):
        """Process stocks in parallel using multiprocessing."""
        worker_fn = partial(
            _process_stock_worker,
            params_list=self.params_list,
            ohlc_db_path=self.ohlc_db_path,
            start_date=self.start_date,
            end_date=self.end_date,
            capital=self.capital,
            max_risk_per_trade=self.max_risk_per_trade,
            brokerage_rate=self.brokerage_rate,
            stt_rate=self.stt_rate,
            store_trades=self.store_trades,
        )

        # Mark all as in_progress
        self.results_db.connect()
        for stock in stocks:
            self.results_db.mark_stock_in_progress(run_id, stock)
        self.results_db.close()

        completed = 0
        total = len(stocks)

        with mp.Pool(processes=self.workers) as pool:
            for result in pool.imap_unordered(worker_fn, stocks):
                completed += 1
                stock_code = result["stock_code"]

                # Insert results into DB (main process only)
                self.results_db.connect()
                self.results_db.insert_metrics_batch(run_id, result["metrics_rows"])
                if self.store_trades and result["trade_rows"]:
                    self.results_db.insert_trades_batch(run_id, result["trade_rows"])
                self.results_db.mark_stock_complete(
                    run_id, stock_code,
                    combos_tested=result["combos_tested"],
                    total_trades=result["total_trades"],
                    elapsed=result["elapsed"],
                )

                # Update run progress
                elapsed = time.time() - t0
                self.results_db.update_run_status(
                    run_id, "running",
                    combos_completed=completed * total_combos,
                    stocks_completed=completed,
                    elapsed_seconds=elapsed,
                )
                self.results_db.close()

                # ETA calculation
                per_stock = elapsed / completed
                remaining = (total - completed) * per_stock

                print(
                    f"[{completed}/{total}] {stock_code} done in "
                    f"{result['elapsed']:.1f}s "
                    f"({result['total_trades']} trades) | "
                    f"ETA: {remaining/60:.1f} min"
                )

    def show_status(self):
        """Display status of latest or specified run."""
        self.results_db.connect()

        if self.resume_run_id:
            run = self.results_db.get_run(self.resume_run_id)
        else:
            run = self.results_db.get_latest_run()

        if not run:
            print("No backtest runs found.")
            self.results_db.close()
            return

        print(f"\n{'='*60}")
        print(f"Backtest Run #{run['run_id']}")
        print(f"{'='*60}")
        print(f"Status:     {run['status']}")
        print(f"Created:    {run['created_at']}")
        if run['completed_at']:
            print(f"Completed:  {run['completed_at']}")
        print(f"Stocks:     {run['stocks_completed']}/{run['total_stocks']}")
        print(f"Combos:     {run['combos_completed']:,}/{run['total_simulations']:,}")
        print(f"Workers:    {run['workers']}")
        print(f"Elapsed:    {run['elapsed_seconds']:.1f}s ({run['elapsed_seconds']/60:.1f} min)")
        if run['start_date']:
            print(f"Date range: {run['start_date']} to {run['end_date']}")

        # Show per-stock progress
        progress = self.results_db.get_progress(run['run_id'])
        if progress:
            print(f"\nPer-stock progress:")
            for p in progress:
                status_icon = "✓" if p['status'] == 'completed' else "⋯" if p['status'] == 'in_progress' else "○"
                time_str = f" ({p['elapsed_seconds']:.1f}s)" if p['elapsed_seconds'] else ""
                trades_str = f" {p['total_trades_found']} trades" if p['total_trades_found'] else ""
                print(f"  {status_icon} {p['stock_code']}{time_str}{trades_str}")

        print(f"{'='*60}\n")
        self.results_db.close()
