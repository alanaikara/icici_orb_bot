"""
Per-stock state machine for the Fib-MACD live strategy.

Each stock progresses through these states every trading day:

  WAITING_OR       → collecting 30-min opening range candles
  WAITING_BREAKOUT → OR closed, watching for first breakout
  WAITING_SWING    → breakout fired, tracking swing high/low
  WAITING_FIB      → swing confirmed, watching for 61.8% touch
  WAITING_ENTRY    → fib touched, waiting for MACD confirm + close
  IN_TRADE         → position open, monitoring SL / target
  DONE             → trade completed or time exit reached, no more today
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Optional

import numpy as np
import pandas as pd

from live.strategy_config import StrategyConfig, StockConfig

logger = logging.getLogger("ICICI_ORB_Bot")

FIB_786 = 0.786


class State(Enum):
    WAITING_OR       = auto()
    WAITING_BREAKOUT = auto()
    WAITING_SWING    = auto()
    WAITING_FIB      = auto()
    WAITING_ENTRY    = auto()
    IN_TRADE         = auto()
    DONE             = auto()


@dataclass
class TradeSetup:
    direction: str          # 'LONG' or 'SHORT'
    or_high: float
    or_low: float
    swing_value: float
    fib_entry: float        # 61.8% retracement level
    stop_loss: float        # just beyond 78.6% level
    touched_fib: bool = False
    entry_price: float = 0.0
    quantity: int = 0
    target_price: float = 0.0


@dataclass
class OpenTrade:
    direction: str
    entry_price: float
    quantity: int
    stop_loss: float
    target_price: float
    entry_time: str
    sl_order_id: str = ""
    entry_order_id: str = ""


@dataclass
class StockState:
    stock_code: str
    config: StrategyConfig
    stock_cfg: StockConfig

    state: State = State.WAITING_OR

    # Raw 1-min candles collected today
    candles_1m: list[dict] = field(default_factory=list)

    # Opening range
    or_high: float = 0.0
    or_low: float  = 0.0
    or_close_time: str = ""   # time when OR period ends

    # Breakout
    breakout_dir: Optional[str] = None   # 'LONG' or 'SHORT'

    # Swing tracking
    swing_peak: float = 0.0    # running max (LONG) or min (SHORT) after breakout
    swing_value: float = 0.0   # confirmed swing value
    swing_bars_waited: int = 0

    # Setup
    setup: Optional[TradeSetup] = None

    # Open trade
    trade: Optional[OpenTrade] = None

    # 5-min MACD (updated each candle)
    macd_5m: pd.DataFrame = field(default_factory=pd.DataFrame)

    def reset_for_day(self):
        self.state         = State.WAITING_OR
        self.candles_1m    = []
        self.or_high       = 0.0
        self.or_low        = 0.0
        self.or_close_time = ""
        self.breakout_dir  = None
        self.swing_peak    = 0.0
        self.swing_value   = 0.0
        self.swing_bars_waited = 0
        self.setup         = None
        self.trade         = None
        self.macd_5m       = pd.DataFrame()

    # ── Candle ingestion ──────────────────────────────────────────────────────

    def on_candle(self, candle: dict) -> Optional[str]:
        """
        Feed one 1-min candle. Returns an action string or None:
          'enter_long'  / 'enter_short'   → place entry order
          'exit_trade'                    → close position
          None                            → no action needed
        """
        self.candles_1m.append(candle)
        time_str = candle["datetime"][11:16]   # 'HH:MM'

        if self.state == State.WAITING_OR:
            return self._handle_or(candle, time_str)

        if self.state == State.WAITING_BREAKOUT:
            return self._handle_breakout(candle, time_str)

        if self.state == State.WAITING_SWING:
            return self._handle_swing(candle, time_str)

        if self.state == State.WAITING_FIB:
            return self._handle_fib(candle, time_str)

        if self.state == State.WAITING_ENTRY:
            return self._handle_entry(candle, time_str)

        if self.state == State.IN_TRADE:
            return self._handle_in_trade(candle, time_str)

        return None

    # ── State handlers ────────────────────────────────────────────────────────

    def _handle_or(self, candle: dict, time_str: str) -> Optional[str]:
        """Collect OR candles until OR period closes."""
        # OR end time: 09:15 + or_minutes
        total_mins = 9 * 60 + 15 + self.config.or_minutes
        end_h, end_m = divmod(total_mins, 60)
        or_end = f"{end_h:02d}:{end_m:02d}"

        if time_str < or_end:
            return None  # still within OR

        # OR just closed — compute OR high/low from collected candles
        or_candles = [c for c in self.candles_1m if c["datetime"][11:16] < or_end]
        if len(or_candles) < 2:
            self.state = State.DONE
            return None

        self.or_high = max(c["high"] for c in or_candles)
        self.or_low  = min(c["low"]  for c in or_candles)
        self.or_close_time = or_end
        self.state = State.WAITING_BREAKOUT
        logger.info(f"{self.stock_code} OR closed: H={self.or_high:.2f} L={self.or_low:.2f}")

        # Process current candle as first post-OR candle
        return self._handle_breakout(candle, time_str)

    def _handle_breakout(self, candle: dict, time_str: str) -> Optional[str]:
        """Watch for first breakout of OR high/low."""
        if time_str >= self.config.exit_time:
            self.state = State.DONE
            return None

        allow_long  = self.stock_cfg.direction in ('long_only', 'both')
        allow_short = self.stock_cfg.direction in ('short_only', 'both')

        # Immediate breakout: wick through OR boundary
        if allow_long and candle["high"] > self.or_high:
            self.breakout_dir = 'LONG'
            self.swing_peak   = candle["high"]
            self.state        = State.WAITING_SWING
            logger.info(f"{self.stock_code} LONG breakout at {time_str}")
            return None

        if allow_short and candle["low"] < self.or_low:
            self.breakout_dir = 'SHORT'
            self.swing_peak   = candle["low"]
            self.state        = State.WAITING_SWING
            logger.info(f"{self.stock_code} SHORT breakout at {time_str}")
            return None

        return None

    def _handle_swing(self, candle: dict, time_str: str) -> Optional[str]:
        """Track swing high/low after breakout; confirm on 0.3% retrace."""
        if time_str >= self.config.exit_time:
            self.state = State.DONE
            return None

        self.swing_bars_waited += 1
        if self.swing_bars_waited > self.config.max_wait_bars:
            logger.info(f"{self.stock_code} swing timeout — skipping today")
            self.state = State.DONE
            return None

        confirm_pct = self.config.swing_confirm_pct

        if self.breakout_dir == 'LONG':
            if candle["high"] > self.swing_peak:
                self.swing_peak = candle["high"]
            if candle["close"] < self.swing_peak * (1 - confirm_pct):
                self.swing_value = self.swing_peak
                self._build_setup()
        else:
            if candle["low"] < self.swing_peak:
                self.swing_peak = candle["low"]
            if candle["close"] > self.swing_peak * (1 + confirm_pct):
                self.swing_value = self.swing_peak
                self._build_setup()

        return None

    def _handle_fib(self, candle: dict, time_str: str) -> Optional[str]:
        """Wait for price to wick into the 61.8% fib level."""
        if time_str >= self.config.exit_time:
            self.state = State.DONE
            return None

        setup = self.setup
        # Invalidation: blown through stop zone
        if self.breakout_dir == 'LONG' and candle["close"] <= setup.stop_loss:
            logger.info(f"{self.stock_code} invalidated — closed below SL zone")
            self.state = State.DONE
            return None
        if self.breakout_dir == 'SHORT' and candle["close"] >= setup.stop_loss:
            logger.info(f"{self.stock_code} invalidated — closed above SL zone")
            self.state = State.DONE
            return None

        if self.breakout_dir == 'LONG' and candle["low"] <= setup.fib_entry:
            setup.touched_fib = True
            self.state = State.WAITING_ENTRY
            logger.info(f"{self.stock_code} fib {setup.fib_entry:.2f} touched — watching for bounce")

        if self.breakout_dir == 'SHORT' and candle["high"] >= setup.fib_entry:
            setup.touched_fib = True
            self.state = State.WAITING_ENTRY
            logger.info(f"{self.stock_code} fib {setup.fib_entry:.2f} touched — watching for bounce")

        return None

    def _handle_entry(self, candle: dict, time_str: str) -> Optional[str]:
        """
        Fib has been touched. Enter on first candle that CLOSES above/below
        fib level AND MACD confirms direction.
        """
        if time_str >= self.config.exit_time:
            self.state = State.DONE
            return None

        setup = self.setup

        # Re-check invalidation
        if self.breakout_dir == 'LONG' and candle["close"] <= setup.stop_loss:
            self.state = State.DONE
            return None
        if self.breakout_dir == 'SHORT' and candle["close"] >= setup.stop_loss:
            self.state = State.DONE
            return None

        # Entry condition: close bounces back through fib level
        if self.breakout_dir == 'LONG' and candle["close"] > setup.fib_entry:
            if self._check_macd(time_str, 'LONG'):
                return self._fire_entry(candle, time_str, 'LONG')

        if self.breakout_dir == 'SHORT' and candle["close"] < setup.fib_entry:
            if self._check_macd(time_str, 'SHORT'):
                return self._fire_entry(candle, time_str, 'SHORT')

        return None

    def _handle_in_trade(self, candle: dict, time_str: str) -> Optional[str]:
        """Monitor SL and target while in a position."""
        trade = self.trade

        # Force exit at exit_time
        if time_str >= self.config.exit_time:
            logger.info(f"{self.stock_code} time exit at {time_str}")
            self.state = State.DONE
            return "exit_trade"

        if trade.direction == 'LONG':
            if candle["low"] <= trade.stop_loss:
                logger.info(f"{self.stock_code} SL hit at {trade.stop_loss:.2f}")
                self.state = State.DONE
                return "exit_trade"
            if trade.target_price > 0 and candle["high"] >= trade.target_price:
                logger.info(f"{self.stock_code} target hit at {trade.target_price:.2f}")
                self.state = State.DONE
                return "exit_trade"
        else:
            if candle["high"] >= trade.stop_loss:
                logger.info(f"{self.stock_code} SL hit at {trade.stop_loss:.2f}")
                self.state = State.DONE
                return "exit_trade"
            if trade.target_price > 0 and candle["low"] <= trade.target_price:
                logger.info(f"{self.stock_code} target hit at {trade.target_price:.2f}")
                self.state = State.DONE
                return "exit_trade"

        return None

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _build_setup(self):
        """Compute fib levels from OR anchor and confirmed swing."""
        cfg = self.config
        if self.breakout_dir == 'LONG':
            anchor_lo = self.or_low
            anchor_hi = self.swing_value
            rng       = anchor_hi - anchor_lo
            if rng <= 0:
                self.state = State.DONE
                return
            fib_entry   = anchor_hi - cfg.fib_entry_pct * rng
            fib_786     = anchor_hi - FIB_786 * rng
            stop_loss   = fib_786 * (1 - cfg.sl_buffer_pct)
        else:
            anchor_hi = self.or_high
            anchor_lo = self.swing_value
            rng       = anchor_hi - anchor_lo
            if rng <= 0:
                self.state = State.DONE
                return
            fib_entry   = anchor_lo + cfg.fib_entry_pct * rng
            fib_786     = anchor_lo + FIB_786 * rng
            stop_loss   = fib_786 * (1 + cfg.sl_buffer_pct)

        self.setup = TradeSetup(
            direction   = self.breakout_dir,
            or_high     = self.or_high,
            or_low      = self.or_low,
            swing_value = self.swing_value,
            fib_entry   = round(fib_entry, 2),
            stop_loss   = round(stop_loss, 2),
        )
        self.state = State.WAITING_FIB
        logger.info(
            f"{self.stock_code} setup: {self.breakout_dir} "
            f"fib={self.setup.fib_entry:.2f} sl={self.setup.stop_loss:.2f}"
        )

    def _fire_entry(self, candle: dict, time_str: str, direction: str) -> str:
        """Compute position size, store trade, return action."""
        setup   = self.setup
        entry   = setup.fib_entry
        sl      = setup.stop_loss
        risk_ps = abs(entry - sl)

        quantity = min(
            int(self.config.max_risk_per_trade / risk_ps),
            int(self.config.capital / entry),
        )
        if quantity <= 0:
            self.state = State.DONE
            return None

        target = (
            entry + risk_ps * self.stock_cfg.target_r
            if direction == 'LONG'
            else entry - risk_ps * self.stock_cfg.target_r
        )

        self.trade = OpenTrade(
            direction    = direction,
            entry_price  = entry,
            quantity     = quantity,
            stop_loss    = sl,
            target_price = round(target, 2),
            entry_time   = time_str,
        )
        self.state = State.IN_TRADE
        logger.info(
            f"{self.stock_code} ENTRY {direction} @ {entry:.2f} "
            f"qty={quantity} sl={sl:.2f} tgt={target:.2f}"
        )
        return f"enter_{direction.lower()}"

    def _check_macd(self, time_str: str, direction: str) -> bool:
        """Check 5-min MACD condition at current time."""
        condition = self.config.macd_condition
        if condition == "none":
            return True
        if self.macd_5m.empty or len(self.macd_5m) < 2:
            return True  # No data — don't filter

        # Get most recent 5-min bar at or before time_str
        t = time_str + ":00"
        relevant = self.macd_5m[self.macd_5m["time_str"] <= t]
        if len(relevant) < 2:
            return True

        cur  = relevant.iloc[-1]
        prev = relevant.iloc[-2]

        if condition == "macd_cross":
            return cur["macd"] > cur["signal"] if direction == "LONG" \
                   else cur["macd"] < cur["signal"]
        if condition == "histogram_positive":
            return cur["histogram"] > 0 if direction == "LONG" \
                   else cur["histogram"] < 0
        if condition == "histogram_rising":
            return cur["histogram"] > prev["histogram"] if direction == "LONG" \
                   else cur["histogram"] < prev["histogram"]
        return True

    def update_macd(self, macd_df: pd.DataFrame):
        """Called by the data feed with fresh 5-min MACD data."""
        self.macd_5m = macd_df
