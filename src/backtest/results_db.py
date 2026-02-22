"""
Results database manager for ORB backtesting.

Stores backtest run metadata, parameter definitions, aggregated metrics,
and optionally individual trades. Follows the BacktestDatabase pattern
(context manager, WAL mode, sqlite3.Row).

Separate from backtest.db (OHLC source data) to avoid locking contention.
"""

import sqlite3
import os
import json
import logging
from datetime import datetime

logger = logging.getLogger("ICICI_ORB_Bot")


class ResultsDatabase:
    """
    SQLite database manager for backtest results.
    Stored at Data/backtest_results.db.
    """

    def __init__(self, db_path="Data/backtest_results.db"):
        self.db_path = db_path
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        self.conn = None
        self.cur = None
        self.initialize_database()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def connect(self):
        """Open database connection."""
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path, timeout=30)
            self.conn.row_factory = sqlite3.Row
            self.cur = self.conn.cursor()
            self.cur.execute("PRAGMA journal_mode=WAL")
        return self.conn

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
            self.cur = None

    def execute(self, query, params=None):
        """Execute a single SQL query."""
        if not self.conn:
            self.connect()
        try:
            if params:
                self.cur.execute(query, params)
            else:
                self.cur.execute(query)
            return self.cur
        except sqlite3.Error as e:
            logger.error(f"Results DB error: {e} | Query: {query}")
            raise

    def executemany(self, query, params_list):
        """Batch execute for bulk inserts."""
        if not self.conn:
            self.connect()
        try:
            self.cur.executemany(query, params_list)
            return self.cur
        except sqlite3.Error as e:
            logger.error(f"Results DB batch error: {e}")
            raise

    def commit(self):
        """Commit current transaction."""
        if self.conn:
            self.conn.commit()

    def initialize_database(self):
        """Create tables from schema file or inline fallback."""
        try:
            self.connect()

            schema_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(
                    os.path.abspath(__file__)))),
                'Data', 'backtest_results_schema.sql'
            )

            if os.path.exists(schema_path):
                with open(schema_path, 'r') as f:
                    self.conn.executescript(f.read())
                logger.info(f"Results database initialized from {schema_path}")
            else:
                logger.warning(f"Schema file not found at {schema_path}, creating inline")
                self._create_tables_inline()

            self.commit()
        except sqlite3.Error as e:
            logger.error(f"Error initializing results database: {e}")
            raise
        finally:
            self.close()

    def _create_tables_inline(self):
        """Fallback inline table creation if schema file not found."""
        # Minimal version — full schema should come from .sql file
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS backtest_runs (
                run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                completed_at TEXT,
                status TEXT NOT NULL DEFAULT 'running',
                config_snapshot TEXT NOT NULL,
                total_stocks INTEGER NOT NULL,
                total_param_combos INTEGER NOT NULL,
                total_simulations INTEGER NOT NULL,
                combos_completed INTEGER DEFAULT 0,
                stocks_completed INTEGER DEFAULT 0,
                elapsed_seconds REAL DEFAULT 0,
                workers INTEGER DEFAULT 1,
                store_trades INTEGER DEFAULT 0,
                start_date TEXT,
                end_date TEXT,
                notes TEXT
            );

            CREATE TABLE IF NOT EXISTS backtest_params (
                param_id TEXT PRIMARY KEY,
                param_json TEXT NOT NULL,
                or_minutes INTEGER NOT NULL,
                target_multiplier REAL NOT NULL,
                stop_loss_type TEXT NOT NULL,
                trade_direction TEXT NOT NULL,
                exit_time TEXT NOT NULL,
                max_or_filter_pct REAL NOT NULL,
                entry_confirmation TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS backtest_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL,
                param_id TEXT NOT NULL,
                stock_code TEXT NOT NULL,
                or_minutes INTEGER NOT NULL,
                target_multiplier REAL NOT NULL,
                stop_loss_type TEXT NOT NULL,
                trade_direction TEXT NOT NULL,
                exit_time TEXT NOT NULL,
                max_or_filter_pct REAL NOT NULL,
                entry_confirmation TEXT NOT NULL,
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
                UNIQUE(run_id, param_id, stock_code)
            );

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
                exit_reason TEXT NOT NULL,
                gross_pnl REAL NOT NULL,
                costs REAL NOT NULL,
                net_pnl REAL NOT NULL,
                risk_amount REAL NOT NULL,
                r_multiple REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS backtest_progress (
                run_id INTEGER NOT NULL,
                stock_code TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                combos_tested INTEGER DEFAULT 0,
                total_trades_found INTEGER DEFAULT 0,
                elapsed_seconds REAL DEFAULT 0,
                completed_at TEXT,
                PRIMARY KEY (run_id, stock_code)
            );
        """)

    # ----------------------------------------------------------------
    # Run Management
    # ----------------------------------------------------------------

    def create_run(self, config_snapshot: dict, total_combos: int,
                   stocks: list, workers: int = 1, store_trades: bool = False,
                   start_date: str = None, end_date: str = None,
                   notes: str = None) -> int:
        """Create a new backtest run entry. Returns run_id."""
        self.connect()
        now = datetime.now().isoformat()
        total_stocks = len(stocks)
        total_simulations = total_stocks * total_combos

        self.execute(
            """INSERT INTO backtest_runs
               (created_at, status, config_snapshot, total_stocks,
                total_param_combos, total_simulations, workers,
                store_trades, start_date, end_date, notes)
               VALUES (?, 'running', ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (now, json.dumps(config_snapshot), total_stocks,
             total_combos, total_simulations, workers,
             1 if store_trades else 0, start_date, end_date, notes)
        )
        self.commit()

        # Initialize progress for each stock
        for stock in stocks:
            self.execute(
                """INSERT OR IGNORE INTO backtest_progress
                   (run_id, stock_code, status) VALUES (?, ?, 'pending')""",
                (self.cur.lastrowid, stock)
            )

        # Get the run_id (lastrowid might be wrong after the progress inserts)
        self.execute("SELECT MAX(run_id) FROM backtest_runs")
        run_id = self.cur.fetchone()[0]

        # Re-insert progress rows with correct run_id
        for stock in stocks:
            self.execute(
                """INSERT OR IGNORE INTO backtest_progress
                   (run_id, stock_code, status) VALUES (?, ?, 'pending')""",
                (run_id, stock)
            )

        self.commit()
        return run_id

    def update_run_status(self, run_id: int, status: str,
                          combos_completed: int = None,
                          stocks_completed: int = None,
                          elapsed_seconds: float = None):
        """Update run status and progress counters."""
        self.connect()
        updates = ["status = ?"]
        params = [status]

        if combos_completed is not None:
            updates.append("combos_completed = ?")
            params.append(combos_completed)
        if stocks_completed is not None:
            updates.append("stocks_completed = ?")
            params.append(stocks_completed)
        if elapsed_seconds is not None:
            updates.append("elapsed_seconds = ?")
            params.append(elapsed_seconds)
        if status == "completed":
            updates.append("completed_at = ?")
            params.append(datetime.now().isoformat())

        params.append(run_id)
        self.execute(
            f"UPDATE backtest_runs SET {', '.join(updates)} WHERE run_id = ?",
            params
        )
        self.commit()

    def get_run(self, run_id: int) -> dict:
        """Get run metadata."""
        self.connect()
        self.execute("SELECT * FROM backtest_runs WHERE run_id = ?", (run_id,))
        row = self.cur.fetchone()
        return dict(row) if row else None

    def get_latest_run(self) -> dict:
        """Get the most recent run."""
        self.connect()
        self.execute(
            "SELECT * FROM backtest_runs ORDER BY run_id DESC LIMIT 1"
        )
        row = self.cur.fetchone()
        return dict(row) if row else None

    # ----------------------------------------------------------------
    # Parameter Storage
    # ----------------------------------------------------------------

    def insert_params_batch(self, params_list):
        """
        Bulk insert parameter definitions. Idempotent (INSERT OR IGNORE).

        Args:
            params_list: list of StrategyParams objects
        """
        self.connect()
        rows = []
        for p in params_list:
            rows.append((
                p.param_id(),
                p.to_json(),
                p.or_minutes,
                p.target_multiplier,
                p.stop_loss_type.value,
                p.trade_direction.value,
                p.exit_time,
                p.max_or_filter_pct,
                p.entry_confirmation.value,
            ))
        self.executemany(
            """INSERT OR IGNORE INTO backtest_params
               (param_id, param_json, or_minutes, target_multiplier,
                stop_loss_type, trade_direction, exit_time,
                max_or_filter_pct, entry_confirmation)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            rows
        )
        self.commit()

    # ----------------------------------------------------------------
    # Metrics Storage
    # ----------------------------------------------------------------

    def insert_metrics_batch(self, run_id: int, metrics_rows: list) -> int:
        """
        Bulk insert metrics. Uses INSERT OR IGNORE for idempotent reruns.

        Args:
            run_id: backtest run ID
            metrics_rows: list of tuples matching backtest_metrics schema
                (param_id, stock_code, or_minutes, target_multiplier,
                 stop_loss_type, trade_direction, exit_time,
                 max_or_filter_pct, entry_confirmation,
                 total_trades, winning_trades, losing_trades, win_rate,
                 total_pnl, net_pnl, avg_pnl_per_trade, avg_winner,
                 avg_loser, profit_factor, max_drawdown, max_drawdown_pct,
                 max_consecutive_losses, sharpe_ratio, sortino_ratio,
                 expectancy, avg_r_multiple, calmar_ratio, best_trade,
                 worst_trade, avg_holding_minutes, composite_score)

        Returns:
            Number of rows inserted.
        """
        if not metrics_rows:
            return 0

        self.connect()
        full_rows = [(run_id, *row) for row in metrics_rows]

        self.executemany(
            """INSERT OR IGNORE INTO backtest_metrics
               (run_id, param_id, stock_code,
                or_minutes, target_multiplier, stop_loss_type,
                trade_direction, exit_time, max_or_filter_pct,
                entry_confirmation,
                total_trades, winning_trades, losing_trades, win_rate,
                total_pnl, net_pnl, avg_pnl_per_trade, avg_winner,
                avg_loser, profit_factor, max_drawdown, max_drawdown_pct,
                max_consecutive_losses, sharpe_ratio, sortino_ratio,
                expectancy, avg_r_multiple, calmar_ratio, best_trade,
                worst_trade, avg_holding_minutes, composite_score)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                       ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            full_rows
        )
        self.commit()
        return len(full_rows)

    def insert_trades_batch(self, run_id: int, trades: list) -> int:
        """
        Bulk insert individual trade records.

        Args:
            run_id: backtest run ID
            trades: list of tuples matching backtest_trades schema

        Returns:
            Number of rows inserted.
        """
        if not trades:
            return 0

        self.connect()
        full_rows = [(run_id, *t) for t in trades]

        self.executemany(
            """INSERT INTO backtest_trades
               (run_id, param_id, stock_code, date, direction,
                entry_time, entry_price, exit_time, exit_price,
                quantity, stop_loss_initial, stop_loss_final,
                target_price, or_high, or_low, exit_reason,
                gross_pnl, costs, net_pnl, risk_amount, r_multiple)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                       ?, ?, ?, ?, ?, ?)""",
            full_rows
        )
        self.commit()
        return len(full_rows)

    # ----------------------------------------------------------------
    # Progress Tracking
    # ----------------------------------------------------------------

    def mark_stock_in_progress(self, run_id: int, stock_code: str):
        """Mark a stock as currently being processed."""
        self.connect()
        self.execute(
            """UPDATE backtest_progress SET status = 'in_progress'
               WHERE run_id = ? AND stock_code = ?""",
            (run_id, stock_code)
        )
        self.commit()

    def mark_stock_complete(self, run_id: int, stock_code: str,
                            combos_tested: int, total_trades: int,
                            elapsed: float):
        """Mark a stock as completed."""
        self.connect()
        now = datetime.now().isoformat()
        self.execute(
            """UPDATE backtest_progress
               SET status = 'completed', combos_tested = ?,
                   total_trades_found = ?, elapsed_seconds = ?,
                   completed_at = ?
               WHERE run_id = ? AND stock_code = ?""",
            (combos_tested, total_trades, elapsed, now, run_id, stock_code)
        )
        self.commit()

    def get_completed_stocks(self, run_id: int) -> list:
        """Get list of stock codes already completed for resume."""
        self.connect()
        self.execute(
            """SELECT stock_code FROM backtest_progress
               WHERE run_id = ? AND status = 'completed'""",
            (run_id,)
        )
        return [row[0] for row in self.cur.fetchall()]

    def get_progress(self, run_id: int) -> list:
        """Get all progress entries for a run."""
        self.connect()
        self.execute(
            """SELECT * FROM backtest_progress
               WHERE run_id = ? ORDER BY status, stock_code""",
            (run_id,)
        )
        return [dict(row) for row in self.cur.fetchall()]

    # ----------------------------------------------------------------
    # Query Methods (for ranking/reporting)
    # ----------------------------------------------------------------

    def get_all_metrics(self, run_id: int) -> list:
        """Get all metrics rows for a run."""
        self.connect()
        self.execute(
            "SELECT * FROM backtest_metrics WHERE run_id = ? ORDER BY composite_score DESC",
            (run_id,)
        )
        return [dict(row) for row in self.cur.fetchall()]

    def get_metrics_for_stock(self, run_id: int, stock_code: str) -> list:
        """Get all metrics for a specific stock in a run."""
        self.connect()
        self.execute(
            """SELECT * FROM backtest_metrics
               WHERE run_id = ? AND stock_code = ?
               ORDER BY composite_score DESC""",
            (run_id, stock_code)
        )
        return [dict(row) for row in self.cur.fetchall()]

    def get_metrics_for_params(self, run_id: int, param_id: str) -> list:
        """Get metrics across all stocks for a specific parameter set."""
        self.connect()
        self.execute(
            """SELECT * FROM backtest_metrics
               WHERE run_id = ? AND param_id = ?
               ORDER BY net_pnl DESC""",
            (run_id, param_id)
        )
        return [dict(row) for row in self.cur.fetchall()]

    def get_top_strategies(self, run_id: int, metric: str = "composite_score",
                           limit: int = 20) -> list:
        """
        Get top strategies aggregated across all stocks.
        Returns param_id + average metric value.
        """
        self.connect()
        self.execute(
            f"""SELECT param_id, or_minutes, target_multiplier,
                       stop_loss_type, trade_direction, exit_time,
                       max_or_filter_pct, entry_confirmation,
                       AVG({metric}) as avg_metric,
                       AVG(net_pnl) as avg_net_pnl,
                       AVG(win_rate) as avg_win_rate,
                       AVG(profit_factor) as avg_profit_factor,
                       AVG(sharpe_ratio) as avg_sharpe,
                       COUNT(*) as num_stocks
                FROM backtest_metrics
                WHERE run_id = ?
                GROUP BY param_id
                ORDER BY avg_metric DESC
                LIMIT ?""",
            (run_id, limit)
        )
        return [dict(row) for row in self.cur.fetchall()]

    def get_top_stocks(self, run_id: int, metric: str = "net_pnl",
                       limit: int = 20, param_id: str = None) -> list:
        """
        Get top stocks by metric.
        Optionally filter by specific strategy (param_id).
        """
        self.connect()
        if param_id:
            self.execute(
                f"""SELECT stock_code,
                           AVG({metric}) as avg_metric,
                           AVG(net_pnl) as avg_net_pnl,
                           AVG(win_rate) as avg_win_rate,
                           COUNT(*) as num_strategies
                    FROM backtest_metrics
                    WHERE run_id = ? AND param_id = ?
                    GROUP BY stock_code
                    ORDER BY avg_metric DESC
                    LIMIT ?""",
                (run_id, param_id, limit)
            )
        else:
            self.execute(
                f"""SELECT stock_code,
                           AVG({metric}) as avg_metric,
                           AVG(net_pnl) as avg_net_pnl,
                           AVG(win_rate) as avg_win_rate,
                           COUNT(*) as num_strategies
                    FROM backtest_metrics
                    WHERE run_id = ?
                    GROUP BY stock_code
                    ORDER BY avg_metric DESC
                    LIMIT ?""",
                (run_id, limit)
            )
        return [dict(row) for row in self.cur.fetchall()]

    def get_best_pairs(self, run_id: int, metric: str = "composite_score",
                       limit: int = 50) -> list:
        """Get top (stock, strategy) combinations."""
        self.connect()
        self.execute(
            f"""SELECT * FROM backtest_metrics
                WHERE run_id = ?
                ORDER BY {metric} DESC
                LIMIT ?""",
            (run_id, limit)
        )
        return [dict(row) for row in self.cur.fetchall()]

    def get_metrics_count(self, run_id: int) -> int:
        """Get total number of metrics rows for a run."""
        self.connect()
        self.execute(
            "SELECT COUNT(*) FROM backtest_metrics WHERE run_id = ?",
            (run_id,)
        )
        return self.cur.fetchone()[0]
