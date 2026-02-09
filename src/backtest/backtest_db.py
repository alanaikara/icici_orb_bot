import sqlite3
import os
import logging
from datetime import datetime

logger = logging.getLogger("ICICI_ORB_Bot")


class BacktestDatabase:
    """
    SQLite database manager for backtest OHLC data.
    Follows the same pattern as PortfolioDatabase in database_manager.py.
    """

    def __init__(self, db_path="Data/backtest.db"):
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
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row
            self.cur = self.conn.cursor()
            # WAL mode for better concurrent read performance during backtesting
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
            logger.error(f"Backtest DB error: {e} | Query: {query}")
            raise

    def executemany(self, query, params_list):
        """Batch execute for performance (used for bulk OHLC inserts)."""
        if not self.conn:
            self.connect()
        try:
            self.cur.executemany(query, params_list)
            return self.cur
        except sqlite3.Error as e:
            logger.error(f"Backtest DB batch error: {e}")
            raise

    def commit(self):
        """Commit current transaction."""
        if self.conn:
            self.conn.commit()

    def initialize_database(self):
        """Create tables from schema file or inline fallback."""
        try:
            self.connect()

            # Try loading schema from file
            schema_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(
                    os.path.abspath(__file__)))),
                'Data', 'backtest_schema.sql'
            )

            if os.path.exists(schema_path):
                with open(schema_path, 'r') as f:
                    self.conn.executescript(f.read())
                logger.info(f"Backtest database initialized from {schema_path}")
            else:
                self._create_tables_inline()
                logger.info("Backtest database initialized with inline schema")

            self.commit()
        except sqlite3.Error as e:
            logger.error(f"Error initializing backtest database: {e}")
            raise
        finally:
            self.close()

    def _create_tables_inline(self):
        """Fallback inline table creation if schema file not found."""
        self.conn.executescript("""
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

            CREATE TABLE IF NOT EXISTS api_usage (
                date TEXT PRIMARY KEY,
                calls_made INTEGER DEFAULT 0,
                last_call_time TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_ohlc_stock_datetime ON ohlc_data(stock_code, datetime);
            CREATE INDEX IF NOT EXISTS idx_ohlc_datetime ON ohlc_data(datetime);
            CREATE INDEX IF NOT EXISTS idx_ohlc_stock ON ohlc_data(stock_code);
            CREATE INDEX IF NOT EXISTS idx_download_status ON download_progress(status);
        """)

    # ----------------------------------------------------------------
    # OHLC Data Operations
    # ----------------------------------------------------------------

    def insert_ohlc_batch(self, records):
        """
        Insert a batch of OHLC records. Uses INSERT OR IGNORE for idempotency.

        Args:
            records: list of tuples (stock_code, datetime, open, high, low, close, volume)

        Returns:
            Number of rows actually inserted (excludes duplicates).
        """
        if not records:
            return 0

        self.connect()
        before_count = self._get_ohlc_count()
        self.executemany(
            """INSERT OR IGNORE INTO ohlc_data
               (stock_code, datetime, open, high, low, close, volume)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            records
        )
        self.commit()
        after_count = self._get_ohlc_count()
        return after_count - before_count

    def _get_ohlc_count(self):
        """Get total OHLC record count."""
        self.execute("SELECT COUNT(*) FROM ohlc_data")
        return self.cur.fetchone()[0]

    def get_stock_record_count(self, stock_code):
        """Get OHLC record count for a specific stock."""
        self.connect()
        self.execute(
            "SELECT COUNT(*) FROM ohlc_data WHERE stock_code = ?",
            (stock_code,)
        )
        return self.cur.fetchone()[0]

    def get_ohlc_data(self, stock_code, start_date=None, end_date=None):
        """
        Retrieve OHLC data for backtesting.

        Args:
            stock_code: Stock symbol
            start_date: Optional start date filter (ISO8601)
            end_date: Optional end date filter (ISO8601)

        Returns:
            List of dicts with OHLCV data.
        """
        self.connect()
        query = "SELECT * FROM ohlc_data WHERE stock_code = ?"
        params = [stock_code]

        if start_date:
            query += " AND datetime >= ?"
            params.append(start_date)
        if end_date:
            query += " AND datetime <= ?"
            params.append(end_date)

        query += " ORDER BY datetime"
        self.execute(query, params)
        return [dict(row) for row in self.cur.fetchall()]

    # ----------------------------------------------------------------
    # Download Progress Operations
    # ----------------------------------------------------------------

    def get_download_progress(self, stock_code):
        """Get download progress for a specific stock."""
        self.connect()
        self.execute(
            "SELECT * FROM download_progress WHERE stock_code = ?",
            (stock_code,)
        )
        row = self.cur.fetchone()
        return dict(row) if row else None

    def get_all_progress(self):
        """Get download progress for all stocks, ordered by status then name."""
        self.connect()
        self.execute(
            "SELECT * FROM download_progress ORDER BY status, stock_code"
        )
        return [dict(row) for row in self.cur.fetchall()]

    def init_stock_progress(self, stock_code, first_date, last_date):
        """
        Initialize progress tracking for a stock. Idempotent - won't overwrite existing progress.
        """
        now = datetime.now().isoformat()
        self.connect()
        self.execute(
            """INSERT OR IGNORE INTO download_progress
               (stock_code, first_target_date, last_target_date,
                status, total_records, total_api_calls, created_at, updated_at)
               VALUES (?, ?, ?, 'pending', 0, 0, ?, ?)""",
            (stock_code, first_date, last_date, now, now)
        )
        self.commit()

    def update_stock_progress(self, stock_code, last_downloaded_date,
                              status, records_added=0, calls_made=0, error=None):
        """Update download progress for a stock after a chunk is processed."""
        now = datetime.now().isoformat()
        self.connect()
        self.execute(
            """UPDATE download_progress
               SET last_downloaded_date = ?,
                   status = ?,
                   total_records = total_records + ?,
                   total_api_calls = total_api_calls + ?,
                   last_error = ?,
                   updated_at = ?
               WHERE stock_code = ?""",
            (last_downloaded_date, status, records_added,
             calls_made, error, now, stock_code)
        )
        self.commit()

    def reset_stock_progress(self, stock_code):
        """Reset a stock's download progress to start over."""
        now = datetime.now().isoformat()
        self.connect()
        self.execute(
            """UPDATE download_progress
               SET status = 'pending',
                   last_downloaded_date = NULL,
                   total_records = 0,
                   total_api_calls = 0,
                   last_error = NULL,
                   updated_at = ?
               WHERE stock_code = ?""",
            (now, stock_code)
        )
        self.commit()

    def reset_errored_stocks(self):
        """Reset all stocks with error status back to pending."""
        now = datetime.now().isoformat()
        self.connect()
        self.execute(
            """UPDATE download_progress
               SET status = 'pending',
                   last_error = NULL,
                   updated_at = ?
               WHERE status = 'error'""",
            (now,)
        )
        self.commit()

    # ----------------------------------------------------------------
    # API Usage Operations
    # ----------------------------------------------------------------

    def get_daily_api_calls(self, date_str):
        """Get the number of API calls made on a specific date."""
        self.connect()
        self.execute(
            "SELECT calls_made FROM api_usage WHERE date = ?",
            (date_str,)
        )
        row = self.cur.fetchone()
        return row[0] if row else 0

    def increment_daily_api_calls(self, date_str):
        """Increment today's API call counter."""
        now = datetime.now().isoformat()
        self.connect()
        self.execute(
            """INSERT INTO api_usage (date, calls_made, last_call_time)
               VALUES (?, 1, ?)
               ON CONFLICT(date) DO UPDATE
               SET calls_made = calls_made + 1, last_call_time = ?""",
            (date_str, now, now)
        )
        self.commit()

    # ----------------------------------------------------------------
    # Summary / Stats
    # ----------------------------------------------------------------

    def get_total_records(self):
        """Get total number of OHLC records across all stocks."""
        self.connect()
        self.execute("SELECT COUNT(*) FROM ohlc_data")
        return self.cur.fetchone()[0]

    def get_records_per_stock(self):
        """Get record count grouped by stock."""
        self.connect()
        self.execute(
            """SELECT stock_code, COUNT(*) as count
               FROM ohlc_data
               GROUP BY stock_code
               ORDER BY stock_code"""
        )
        return [dict(row) for row in self.cur.fetchall()]
