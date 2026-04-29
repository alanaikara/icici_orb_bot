"""
Orchestrator for the Fib-MACD pullback strategy grid search.

Workflow:
  1. Load OHLC data + compute 5-min MACD for each stock.
  2. Run all FibMACDParams combos against every stock.
  3. Compute performance metrics per (stock, params) combo.
  4. Persist results to backtest_results.db (same schema as ORB runner).
  5. Support resume, parallel workers, and progress reporting.
"""

import time
import json
import logging
import multiprocessing as mp
from datetime import datetime
from functools import partial

from backtest.fib_macd_engine import FibMACDParams, FibMACDSimulator, generate_param_grid
from backtest.data_loader import DataLoader
from backtest.metrics import MetricsCalculator
from backtest.results_db import ResultsDatabase

logger = logging.getLogger("ICICI_ORB_Bot")


# ── Worker function (top-level so multiprocessing can pickle it) ──────────────

def _process_stock_worker(
    stock_code: str,
    params_list: list[FibMACDParams],
    ohlc_db_path: str,
    start_date: str | None,
    end_date: str | None,
    capital: float,
    max_risk_per_trade: float,
    brokerage_rate: float,
    stt_rate: float,
    store_trades: bool,
    slippage_pct: float = 0.0,
    use_zerodha_charges: bool = False,
) -> dict:
    """
    Process all FibMACDParams combinations for one stock.
    Returns a dict of results for the main process to insert into the DB.
    """
    t0 = time.time()

    loader    = DataLoader(ohlc_db_path)
    simulator = FibMACDSimulator(
        capital=capital,
        max_risk_per_trade=max_risk_per_trade,
        brokerage_rate=brokerage_rate,
        stt_rate=stt_rate,
        slippage_pct=slippage_pct,
        use_zerodha_charges=use_zerodha_charges,
    )
    metrics_calc = MetricsCalculator(capital=capital)

    or_minutes_needed = sorted(set(p.or_minutes for p in params_list))

    stock_data = loader.load_stock(
        stock_code,
        start_date=start_date,
        end_date=end_date,
        or_minutes_list=or_minutes_needed,
        compute_macd=True,
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
    trade_rows   = []
    total_trades = 0

    for params in params_list:
        trades = simulator.run(stock_data, params)
        total_trades += len(trades)

        result   = metrics_calc.compute(trades)
        param_id = params.param_id()

        metrics_row = (
            param_id, stock_code,
            params.or_minutes, params.fib_entry_pct,
            params.macd_condition, params.trade_direction,
            params.exit_time, 0.0,                   # max_or_filter_pct placeholder
            params.breakout_type,                     # entry_confirmation slot
            *result.to_metrics_tuple(),
        )
        metrics_rows.append(metrics_row)

        if store_trades and trades:
            for trade in trades:
                trade_rows.append(trade.to_tuple(param_id))

    return {
        "stock_code":    stock_code,
        "metrics_rows":  metrics_rows,
        "trade_rows":    trade_rows,
        "total_trades":  total_trades,
        "combos_tested": len(params_list),
        "elapsed":       time.time() - t0,
    }


# ── Runner class ──────────────────────────────────────────────────────────────

class FibMACDRunner:
    """
    Orchestrates the Fib-MACD grid search across all 50 Nifty stocks.
    Reuses ResultsDatabase, MetricsCalculator and ReportGenerator from the
    existing ORB backtesting infrastructure.
    """

    def __init__(
        self,
        config: dict,
        stocks: list[str] | None = None,
        workers: int = 1,
        store_trades: bool = False,
        resume_run_id: int | None = None,
        quick: bool = False,
        # Parameter pinning
        or_minutes: list[int] | None = None,
        fib_entries: list[float] | None = None,
        macd_conditions: list[str] | None = None,
        targets: list[float] | None = None,
        directions: list[str] | None = None,
        exit_times: list[str] | None = None,
        # Date range
        start_date: str | None = None,
        end_date: str | None = None,
        # Explicit absolute path overrides (set by CLI to ensure consistency)
        _results_db_path: str | None = None,
        _ohlc_db_path: str | None = None,
        # Realistic cost model
        slippage_pct: float = 0.0,
        use_zerodha_charges: bool = False,
    ):
        self.config       = config
        self.stocks       = stocks or config.get("nifty_50_stocks", [])
        self.workers      = workers
        self.store_trades = store_trades
        self.resume_run_id = resume_run_id

        sweep = config.get("backtest_sweep", {})
        bt    = config.get("backtest", {})

        self.ohlc_db_path       = _ohlc_db_path    or bt.get("db_path", "Data/backtest.db")
        self.results_db_path    = _results_db_path or sweep.get("results_db_path", "Data/backtest_results.db")
        self.capital            = sweep.get("capital", 100_000)
        self.max_risk           = sweep.get("max_risk_per_trade", 1_000)
        self.brokerage_rate     = sweep.get("brokerage_rate", 0.0001)
        self.stt_rate           = sweep.get("stt_rate", 0.00025)
        self.slippage_pct       = slippage_pct
        self.use_zerodha_charges = use_zerodha_charges
        self.start_date         = start_date or bt.get("start_date")
        self.end_date           = end_date   or bt.get("end_date")

        self.params_list = generate_param_grid(
            or_minutes      = or_minutes,
            fib_entries     = fib_entries,
            macd_conditions = macd_conditions,
            targets         = targets,
            directions      = directions,
            exit_times      = exit_times,
            quick           = quick,
        )

        self.results_db = ResultsDatabase(self.results_db_path)

    # ── Public run ────────────────────────────────────────────────────────────

    def run(self) -> dict:
        n_combos  = len(self.params_list)
        n_stocks  = len(self.stocks)
        n_total   = n_combos * n_stocks

        print(f"\n{'='*60}")
        print(f"Fib-MACD Backtest Grid Search")
        print(f"{'='*60}")
        print(f"Stocks:           {n_stocks}")
        print(f"Parameter combos: {n_combos:,}")
        print(f"Total simulations:{n_total:,}")
        print(f"Workers:          {self.workers}")
        print(f"Date range:       {self.start_date or 'all'} — {self.end_date or 'all'}")
        print(f"Store trades:     {self.store_trades}")
        print(f"{'='*60}\n")

        run_id = self._init_run(n_combos)
        print(f"Run ID: {run_id}\n")

        stocks_todo = self._get_stocks_to_process(run_id)
        if not stocks_todo:
            print("All stocks already completed. Use --report to view results.")
            return {"run_id": run_id, "status": "already_complete"}

        print(f"Stocks to process: {len(stocks_todo)}/{n_stocks}")

        # Store param definitions
        self.results_db.connect()
        self._insert_params(self.params_list)
        self.results_db.close()

        t0         = time.time()
        done_count = n_stocks - len(stocks_todo)

        try:
            if self.workers <= 1:
                for i, stock_code in enumerate(stocks_todo):
                    result = self._process_serial(stock_code, run_id, i, len(stocks_todo))
                    done_count += 1
                    elapsed = time.time() - t0
                    self.results_db.connect()
                    self.results_db.update_run_status(
                        run_id, "running",
                        combos_completed=done_count * n_combos,
                        stocks_completed=done_count,
                        elapsed_seconds=elapsed,
                    )
                    self.results_db.close()
            else:
                self._process_parallel(stocks_todo, run_id, n_combos, t0)
                done_count = n_stocks

            elapsed = time.time() - t0
            self.results_db.connect()
            self.results_db.update_run_status(
                run_id, "completed",
                combos_completed=n_total,
                stocks_completed=n_stocks,
                elapsed_seconds=elapsed,
            )
            self.results_db.close()

            print(f"\n{'='*60}")
            print(f"COMPLETED in {elapsed:.1f}s ({elapsed/60:.1f} min)")
            print(f"Total simulations: {n_total:,}")
            print(f"Run: python run_fib_backtest.py --report --run-id {run_id}")
            print(f"{'='*60}\n")

            return {"run_id": run_id, "status": "completed", "elapsed_seconds": elapsed}

        except KeyboardInterrupt:
            elapsed = time.time() - t0
            print(f"\nInterrupted after {elapsed:.1f}s")
            print("Resume with: python run_fib_backtest.py --resume")
            self.results_db.connect()
            self.results_db.update_run_status(run_id, "interrupted", elapsed_seconds=elapsed)
            self.results_db.close()
            return {"run_id": run_id, "status": "interrupted"}

        except Exception as e:
            elapsed = time.time() - t0
            logger.error(f"Backtest error: {e}")
            self.results_db.connect()
            self.results_db.update_run_status(run_id, "interrupted", elapsed_seconds=elapsed)
            self.results_db.close()
            raise

    def show_status(self):
        """Print status of the latest (or specified) run."""
        self.results_db.connect()
        run = (
            self.results_db.get_run(self.resume_run_id)
            if self.resume_run_id
            else self.results_db.get_latest_run()
        )
        if not run:
            print("No runs found.")
            self.results_db.close()
            return

        print(f"\n{'='*60}")
        print(f"Fib-MACD Run #{run['run_id']}")
        print(f"{'='*60}")
        print(f"Status:  {run['status']}")
        print(f"Stocks:  {run['stocks_completed']}/{run['total_stocks']}")
        print(f"Combos:  {run['combos_completed']:,}/{run['total_simulations']:,}")
        print(f"Elapsed: {run['elapsed_seconds']:.1f}s")

        progress = self.results_db.get_progress(run['run_id'])
        for p in progress:
            icon = "✓" if p['status'] == 'completed' else "⋯" if p['status'] == 'in_progress' else "○"
            t_str = f" ({p['elapsed_seconds']:.1f}s)" if p['elapsed_seconds'] else ""
            print(f"  {icon} {p['stock_code']}{t_str}")

        self.results_db.close()

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _init_run(self, n_combos: int) -> int:
        self.results_db.connect()
        if self.resume_run_id is not None:
            run = self.results_db.get_run(self.resume_run_id)
            if run:
                print(f"Resuming run #{self.resume_run_id} "
                      f"({run['stocks_completed']}/{run['total_stocks']} done)")
                self.results_db.close()
                return self.resume_run_id
            print(f"Run #{self.resume_run_id} not found — starting fresh")

        run_id = self.results_db.create_run(
            config_snapshot=self.config,
            total_combos=n_combos,
            stocks=self.stocks,
            workers=self.workers,
            store_trades=self.store_trades,
            start_date=self.start_date,
            end_date=self.end_date,
            notes="fib_macd_strategy",
        )
        self.results_db.close()
        return run_id

    def _get_stocks_to_process(self, run_id: int) -> list[str]:
        self.results_db.connect()
        done = set(self.results_db.get_completed_stocks(run_id))
        self.results_db.close()
        return [s for s in self.stocks if s not in done]

    def _insert_params(self, params_list: list[FibMACDParams]):
        """Store param definitions using the existing backtest_params table."""
        rows = []
        for p in params_list:
            rows.append((
                p.param_id(),
                json.dumps(p.to_dict(), sort_keys=True),
                p.or_minutes,
                p.fib_entry_pct,          # target_multiplier slot
                p.macd_condition,         # stop_loss_type slot
                p.trade_direction,
                p.exit_time,
                0.0,                      # max_or_filter_pct placeholder
                p.breakout_type,          # entry_confirmation slot
            ))
        self.results_db.executemany(
            """INSERT OR IGNORE INTO backtest_params
               (param_id, param_json, or_minutes, target_multiplier,
                stop_loss_type, trade_direction, exit_time,
                max_or_filter_pct, entry_confirmation)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )
        self.results_db.commit()

    def _process_serial(
        self, stock_code: str, run_id: int, idx: int, total: int
    ) -> dict:
        print(f"[{idx+1}/{total}] {stock_code}...", end=" ", flush=True)

        self.results_db.connect()
        self.results_db.mark_stock_in_progress(run_id, stock_code)
        self.results_db.close()

        result = _process_stock_worker(
            stock_code          = stock_code,
            params_list         = self.params_list,
            ohlc_db_path        = self.ohlc_db_path,
            start_date          = self.start_date,
            end_date            = self.end_date,
            capital             = self.capital,
            max_risk_per_trade  = self.max_risk,
            brokerage_rate      = self.brokerage_rate,
            stt_rate            = self.stt_rate,
            store_trades        = self.store_trades,
            slippage_pct        = self.slippage_pct,
            use_zerodha_charges = self.use_zerodha_charges,
        )

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
            f"done {result['elapsed']:.1f}s — "
            f"{result['combos_tested']} combos, {result['total_trades']} trades"
        )
        return result

    def _process_parallel(
        self, stocks: list[str], run_id: int, n_combos: int, t0: float
    ):
        worker_fn = partial(
            _process_stock_worker,
            params_list         = self.params_list,
            ohlc_db_path        = self.ohlc_db_path,
            start_date          = self.start_date,
            end_date            = self.end_date,
            capital             = self.capital,
            max_risk_per_trade  = self.max_risk,
            brokerage_rate      = self.brokerage_rate,
            stt_rate            = self.stt_rate,
            store_trades        = self.store_trades,
            slippage_pct        = self.slippage_pct,
            use_zerodha_charges = self.use_zerodha_charges,
        )

        self.results_db.connect()
        for s in stocks:
            self.results_db.mark_stock_in_progress(run_id, s)
        self.results_db.close()

        done  = 0
        total = len(stocks)

        with mp.Pool(processes=self.workers) as pool:
            for result in pool.imap_unordered(worker_fn, stocks):
                done += 1
                stock_code = result["stock_code"]
                elapsed    = time.time() - t0

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
                self.results_db.update_run_status(
                    run_id, "running",
                    combos_completed=done * n_combos,
                    stocks_completed=done,
                    elapsed_seconds=elapsed,
                )
                self.results_db.close()

                eta = ((total - done) * (elapsed / done)) / 60 if done else 0
                print(
                    f"[{done}/{total}] {stock_code} {result['elapsed']:.1f}s "
                    f"({result['total_trades']} trades) | ETA {eta:.1f}min"
                )
