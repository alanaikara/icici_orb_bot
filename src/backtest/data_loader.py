"""
Data loader for ORB backtesting.

Loads 1-minute OHLC data from SQLite into pandas DataFrames and
precomputes derived structures (opening ranges, ATR, avg volume)
that are reused across many parameter combinations.
"""

import sqlite3
import logging
import pandas as pd
import numpy as np

logger = logging.getLogger("ICICI_ORB_Bot")


class StockData:
    """
    Holds all precomputed data for a single stock.

    Loaded once per stock, reused across all parameter combinations.
    ~30 MB memory per stock.
    """

    def __init__(self, stock_code: str):
        self.stock_code = stock_code
        self.df: pd.DataFrame = None           # Full OHLC DataFrame
        self.trading_days: list[str] = []       # Sorted unique date strings
        self.day_groups: dict[str, pd.DataFrame] = {}  # date -> DataFrame slice

        # Precomputed opening ranges per OR duration
        # Key: or_minutes, Value: dict[date -> (or_high, or_low, or_avg_vol, or_pct)]
        self.opening_ranges: dict[int, dict[str, tuple]] = {}

        # Precomputed daily ATR (14-period Wilder's, from daily bars)
        self.daily_atr: dict[str, float] = {}

        # Previous day close (for gap filter if needed later)
        self.prev_close: dict[str, float] = {}


class DataLoader:
    """
    Loads OHLC data from SQLite and precomputes derived data structures.
    """

    def __init__(self, db_path: str = "Data/backtest.db"):
        self.db_path = db_path

    def load_stock(
        self,
        stock_code: str,
        start_date: str = None,
        end_date: str = None,
        or_minutes_list: list[int] = None,
    ) -> StockData:
        """
        Load and precompute all data for a single stock.

        Args:
            stock_code: ISEC stock code (e.g., 'RELIND')
            start_date: Optional start date filter 'YYYY-MM-DD'
            end_date: Optional end date filter 'YYYY-MM-DD'
            or_minutes_list: List of OR durations to precompute (default: all 7)

        Returns:
            StockData with everything in memory.
        """
        if or_minutes_list is None:
            or_minutes_list = [5, 10, 15, 20, 30, 45, 60]

        stock_data = StockData(stock_code)

        # 1. Load raw data from SQLite
        stock_data.df = self._load_ohlc(stock_code, start_date, end_date)
        if stock_data.df.empty:
            logger.warning(f"No data loaded for {stock_code}")
            return stock_data

        # 2. Group by date
        stock_data.trading_days = sorted(stock_data.df['date_str'].unique().tolist())
        stock_data.day_groups = {
            date: group for date, group in stock_data.df.groupby('date_str')
        }

        logger.info(
            f"Loaded {stock_code}: {len(stock_data.df)} candles, "
            f"{len(stock_data.trading_days)} trading days"
        )

        # 3. Precompute opening ranges for all required durations
        for om in or_minutes_list:
            stock_data.opening_ranges[om] = self._compute_opening_ranges(
                stock_data.day_groups, om
            )

        # 4. Compute daily ATR
        stock_data.daily_atr = self._compute_daily_atr(
            stock_data.day_groups, stock_data.trading_days
        )

        # 5. Compute previous day close
        stock_data.prev_close = self._compute_prev_close(
            stock_data.day_groups, stock_data.trading_days
        )

        return stock_data

    def _load_ohlc(
        self, stock_code: str, start_date: str = None, end_date: str = None
    ) -> pd.DataFrame:
        """
        Load OHLC data from SQLite into a DataFrame.

        Filters:
        - Only market hours (09:15 to 15:29)
        - Only candles with volume > 0 (filters pre-market noise)
        """
        conn = sqlite3.connect(self.db_path)

        query = """
            SELECT datetime, open, high, low, close, volume
            FROM ohlc_data
            WHERE stock_code = ?
              AND time(datetime) >= '09:15:00'
              AND time(datetime) <= '15:29:00'
              AND volume > 0
        """
        params = [stock_code]

        if start_date:
            query += " AND datetime >= ?"
            params.append(f"{start_date} 00:00:00")
        if end_date:
            query += " AND datetime <= ?"
            params.append(f"{end_date} 23:59:59")

        query += " ORDER BY datetime"

        df = pd.read_sql_query(query, conn, params=params)
        conn.close()

        if df.empty:
            return df

        # Add derived columns for fast filtering
        df['date_str'] = df['datetime'].str[:10]       # 'YYYY-MM-DD'
        df['time_str'] = df['datetime'].str[11:19]     # 'HH:MM:SS'

        return df

    def _compute_opening_ranges(
        self, day_groups: dict[str, pd.DataFrame], or_minutes: int
    ) -> dict[str, tuple]:
        """
        For each trading day, compute OR high/low from first N minutes.

        A trading day's OR candles are from 09:15:00 to 09:15 + or_minutes - 1.
        For or_minutes=15: candles at 09:15 through 09:29 (15 candles).

        Returns:
            dict[date_str -> (or_high, or_low, or_avg_vol, or_pct)]
        """
        # Calculate OR end time string
        total_minutes = 9 * 60 + 15 + or_minutes  # minutes from midnight
        end_h, end_m = divmod(total_minutes, 60)
        # OR includes candles from 09:15:00 to (or_end - 1 minute):00
        # e.g., or_minutes=15: 09:15 to 09:29 (15 candles)
        end_h_adj, end_m_adj = divmod(total_minutes - 1, 60)
        or_end_time = f"{end_h_adj:02d}:{end_m_adj:02d}:00"

        result = {}
        for date_str, day_df in day_groups.items():
            # Get OR candles
            or_candles = day_df[day_df['time_str'] <= or_end_time]

            if or_candles.empty or len(or_candles) < 2:
                continue  # Not enough data for this day

            or_high = or_candles['high'].max()
            or_low = or_candles['low'].min()
            or_avg_vol = or_candles['volume'].mean()

            # OR percentage: range as % of midpoint
            midpoint = (or_high + or_low) / 2
            or_pct = ((or_high - or_low) / midpoint * 100) if midpoint > 0 else 0

            result[date_str] = (or_high, or_low, or_avg_vol, or_pct)

        return result

    def _compute_daily_atr(
        self,
        day_groups: dict[str, pd.DataFrame],
        trading_days: list[str],
        period: int = 14,
    ) -> dict[str, float]:
        """
        Compute ATR using daily High-Low-Close derived from minute data.
        Uses Wilder's smoothing over `period` days.

        Returns:
            dict[date_str -> atr_value]
        """
        if len(trading_days) < period + 1:
            return {}

        # Build daily OHLC from minute data
        daily_highs = []
        daily_lows = []
        daily_closes = []

        for date_str in trading_days:
            day_df = day_groups[date_str]
            daily_highs.append(day_df['high'].max())
            daily_lows.append(day_df['low'].min())
            daily_closes.append(day_df['close'].iloc[-1])

        # Compute True Range
        true_ranges = []
        for i in range(len(trading_days)):
            high = daily_highs[i]
            low = daily_lows[i]

            if i == 0:
                tr = high - low
            else:
                prev_close = daily_closes[i - 1]
                tr = max(
                    high - low,
                    abs(high - prev_close),
                    abs(low - prev_close)
                )
            true_ranges.append(tr)

        # Wilder's smoothed ATR
        result = {}
        atr = sum(true_ranges[:period]) / period  # Initial ATR

        for i in range(period, len(trading_days)):
            atr = (atr * (period - 1) + true_ranges[i]) / period
            result[trading_days[i]] = atr

        # Also set ATR for the first `period` days using simple average
        for i in range(min(period, len(trading_days))):
            result[trading_days[i]] = sum(true_ranges[:i + 1]) / (i + 1)

        return result

    def _compute_prev_close(
        self,
        day_groups: dict[str, pd.DataFrame],
        trading_days: list[str],
    ) -> dict[str, float]:
        """
        Compute previous day's closing price for each trading day.
        Used for gap filter (Phase 2).

        Returns:
            dict[date_str -> prev_day_close]
        """
        result = {}
        for i in range(1, len(trading_days)):
            prev_day = trading_days[i - 1]
            prev_df = day_groups[prev_day]
            result[trading_days[i]] = prev_df['close'].iloc[-1]
        return result
