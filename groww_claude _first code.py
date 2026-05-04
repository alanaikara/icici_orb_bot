"""
Fib-MACD Opening Range Breakout Strategy — Groww Cloud

Single-file script. Copy this entire file into Groww Cloud.

Strategy:
  1. Build 30-min Opening Range (OR) — 09:15 to 09:44
  2. Wait for price to break out of OR high or low
  3. Track the post-breakout swing high (LONG) or swing low (SHORT)
  4. Enter when price retraces to 61.8% Fibonacci level AND closes back through it
     with MACD (5-min) confirming direction
  5. Stop-loss just below the 78.6% Fib level
  6. Target: 1.5× risk (1.5R)
  7. Force-exit all positions at 15:14

Dependencies (install in Groww Cloud):
    growwapi
    pandas
    numpy
"""

# ┌─────────────────────────────────────────────────────────────────────────────┐
# │                          CONFIGURATION ZONE                                │
# │   Edit this section to adjust the strategy. Nothing below needs changing.  │
# └─────────────────────────────────────────────────────────────────────────────┘

# ── Credentials ───────────────────────────────────────────────────────────────
# In Groww Cloud: paste your keys directly here (the script runs in your account).
# Locally: set as environment variables and read with os.environ.get().
import os
GROWW_API_KEY = "x"
GROWW_SECRET  = "x"


# ── Risk per trade ─────────────────────────────────────────────────────────────
# DEFAULT_RISK is the max ₹ you're willing to lose on any single trade.
# To size up on a specific stock, add it to RISK_OVERRIDES below.

DEFAULT_RISK: float = 200   # ₹ per trade (default for all stocks)

# Per-stock risk overrides — increase these to trade bigger on specific stocks.
# To revert a stock to DEFAULT_RISK, remove it from this dict or set to None.
# Example: "SBIN": 2000 means you'll risk ₹2,000 on SBIN trades.
RISK_OVERRIDES: dict[str, float] = {
    # "SBIN":        2_000,
    # "HDFCBANK":    1_500,
    # "TATAMOTORS":  1_200,
}

# ── Capital cap per position (prevents oversizing in very cheap stocks) ────────
CAPITAL_PER_TRADE: float = 10_000   # ₹ max capital per position

# ── Strategy parameters (from Run 7 backtest, Sharpe > 2.5) ───────────────────
OR_MINUTES       = 30         # Opening range duration in minutes
FIB_ENTRY_PCT    = 0.618      # Enter at 61.8% retracement
FIB_STOP_PCT     = 0.786      # Stop reference at 78.6% retracement
SL_BUFFER_PCT    = 0.001      # 0.1% buffer beyond stop level
SWING_CONFIRM    = 0.003      # 0.3% retrace confirms swing is done
MACD_CONDITION   = "macd_cross"   # "macd_cross" | "histogram_positive" | "none"
MACD_FAST        = 12
MACD_SLOW        = 26
MACD_SIGNAL      = 9
TARGET_R         = 1.5        # Reward:Risk ratio for target
EXIT_TIME        = "15:14"    # Force-exit all positions before close
MAX_WAIT_BARS    = 60         # Max 1-min bars to wait for fib touch after swing

# ── Mode ───────────────────────────────────────────────────────────────────────
DRY_RUN = False  # True = log signals only, no real orders. Set False for live.

# ── Portfolio: 34 stocks from backtest Run 7 (Sharpe > 2.5) ──────────────────
# Format: ("NSE_SYMBOL", "direction", target_r)
# direction: "long_only" | "short_only" | "both"
PORTFOLIO = [
    ("LTIM",        "long_only",  1.5),
    ("TATAMOTORS",  "short_only", 1.5),
    ("SHRIRAMFIN",  "short_only", 1.5),
    ("ADANIENT",    "short_only", 1.5),
    ("M&M",         "both",       1.5),
    ("ONGC",        "both",       1.5),
    ("NTPC",        "long_only",  1.5),
    ("MARUTI",      "short_only", 1.5),
    ("EICHERMOT",   "short_only", 1.5),
    ("TECHM",       "long_only",  1.5),
    ("INDUSINDBK",  "short_only", 1.5),
    ("HINDALCO",    "both",       1.5),
    ("BAJFINANCE",  "short_only", 1.5),
    ("HCLTECH",     "short_only", 1.5),
    ("HDFCBANK",    "both",       1.5),
    ("POWERGRID",   "long_only",  1.5),
    ("BAJAJFINSV",  "short_only", 1.5),
    ("JSWSTEEL",    "both",       1.5),
    ("SUNPHARMA",   "long_only",  1.5),
    ("TITAN",       "long_only",  1.5),
    ("SBILIFE",     "long_only",  1.5),
    ("GRASIM",      "short_only", 1.5),
    ("HEROMOTOCO",  "short_only", 1.5),
    ("ADANIPORTS",  "both",       1.5),
    ("AXISBANK",    "short_only", 1.5),
    ("COALINDIA",   "short_only", 1.5),
    ("BHARTIARTL",  "long_only",  1.5),
    ("LT",          "short_only", 1.5),
    ("BPCL",        "both",       1.5),
    ("CIPLA",       "long_only",  1.5),
    ("TATACONSUM",  "short_only", 1.5),
    ("TATASTEEL",   "short_only", 1.5),
    ("KOTAKBANK",   "long_only",  1.5),
    ("SBIN",        "both",       1.5),
]

# ─────────────────────────────────────────────────────────────────────────────
#   END OF CONFIGURATION — do not edit below unless you know what you're doing
# ─────────────────────────────────────────────────────────────────────────────

import logging
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Optional

import numpy as np
import pandas as pd
from growwapi import GrowwAPI

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("GrowwFibMACD")

# ── Market hours ──────────────────────────────────────────────────────────────
MARKET_OPEN   = "09:15"
MARKET_CLOSE  = "15:30"
POLL_INTERVAL = 60   # seconds between candle polls

# ── Groww API constants ────────────────────────────────────────────────────────
# Confirmed from Groww sample script — these live on the GrowwAPI instance.
# We resolve them after the first connect() call in GrowwBroker.
# Defaults (strings) are fallbacks if the SDK version changes constant names.
_NSE    = "NSE"
_CASH   = "CASH"
_DAY    = "DAY"
_MIS    = "MIS"      # Margin Intraday Square-off — confirmed in annexures
_MARKET = "MARKET"
_LIMIT  = "LIMIT"
_SL     = "SL"       # Stop Loss — annexures confirm "SL" not "STOP_LOSS"
_SL_M   = "SL_M"     # Stop Loss Market
_BUY    = "BUY"
_SELL   = "SELL"

FIB_786 = 0.786


# ═══════════════════════════════════════════════════════════════════════════════
#  DATA CLASSES
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class StockCfg:
    symbol:     str
    direction:  str           # 'long_only' | 'short_only' | 'both'
    target_r:   float = 1.5
    risk:       float = DEFAULT_RISK


@dataclass
class TradeSetup:
    direction:   str
    or_high:     float
    or_low:      float
    swing_value: float
    fib_entry:   float
    stop_loss:   float
    touched_fib: bool  = False


@dataclass
class OpenTrade:
    direction:       str
    entry_price:     float
    quantity:        int
    stop_loss:       float
    target_price:    float
    entry_time:      str
    sl_order_id:     str = ""
    entry_order_id:  str = ""


@dataclass
class OrderResult:
    success:  bool
    order_id: str
    message:  str


# ═══════════════════════════════════════════════════════════════════════════════
#  STATE MACHINE — per stock
# ═══════════════════════════════════════════════════════════════════════════════

class State(Enum):
    WAITING_OR       = auto()
    WAITING_BREAKOUT = auto()
    WAITING_SWING    = auto()
    WAITING_FIB      = auto()
    WAITING_ENTRY    = auto()
    IN_TRADE         = auto()
    DONE             = auto()


class StockState:
    """
    Fib-MACD state machine for a single stock.
    Progresses through states each time on_candle() is called.
    Returns an action string when an order should be placed, else None.
    """

    def __init__(self, cfg: StockCfg):
        self.cfg      = cfg
        self.symbol   = cfg.symbol
        self.reset()

    def reset(self):
        self.state            = State.WAITING_OR
        self.candles_1m:  list[dict] = []
        self.or_high          = 0.0
        self.or_low           = 0.0
        self.breakout_dir:    Optional[str] = None
        self.swing_peak       = 0.0
        self.swing_value      = 0.0
        self.swing_bars       = 0
        self.setup:           Optional[TradeSetup] = None
        self.trade:           Optional[OpenTrade]  = None
        self.macd_5m:         pd.DataFrame = pd.DataFrame()

    def on_candle(self, candle: dict) -> Optional[str]:
        """Feed one 1-min candle. Returns 'enter_long'/'enter_short'/'exit_trade'/None."""
        self.candles_1m.append(candle)
        t = candle["datetime"][11:16]   # "HH:MM"

        if self.state == State.WAITING_OR:       return self._handle_or(candle, t)
        if self.state == State.WAITING_BREAKOUT: return self._handle_breakout(candle, t)
        if self.state == State.WAITING_SWING:    return self._handle_swing(candle, t)
        if self.state == State.WAITING_FIB:      return self._handle_fib(candle, t)
        if self.state == State.WAITING_ENTRY:    return self._handle_entry(candle, t)
        if self.state == State.IN_TRADE:         return self._handle_in_trade(candle, t)
        return None

    # ── State handlers ────────────────────────────────────────────────────────

    def _handle_or(self, candle, t):
        total_mins = 9 * 60 + 15 + OR_MINUTES
        end_h, end_m = divmod(total_mins, 60)
        or_end = f"{end_h:02d}:{end_m:02d}"

        if t < or_end:
            return None

        or_candles = [c for c in self.candles_1m if c["datetime"][11:16] < or_end]
        if len(or_candles) < 2:
            self.state = State.DONE
            return None

        self.or_high = max(c["high"] for c in or_candles)
        self.or_low  = min(c["low"]  for c in or_candles)
        self.state   = State.WAITING_BREAKOUT
        logger.info(f"{self.symbol} OR closed: H={self.or_high:.2f} L={self.or_low:.2f}")
        return self._handle_breakout(candle, t)

    def _handle_breakout(self, candle, t):
        if t >= EXIT_TIME:
            self.state = State.DONE
            return None

        allow_long  = self.cfg.direction in ("long_only", "both")
        allow_short = self.cfg.direction in ("short_only", "both")

        if allow_long and candle["high"] > self.or_high:
            self.breakout_dir = "LONG"
            self.swing_peak   = candle["high"]
            self.state        = State.WAITING_SWING
            logger.info(f"{self.symbol} LONG breakout at {t}")
            return None

        if allow_short and candle["low"] < self.or_low:
            self.breakout_dir = "SHORT"
            self.swing_peak   = candle["low"]
            self.state        = State.WAITING_SWING
            logger.info(f"{self.symbol} SHORT breakout at {t}")
            return None

        return None

    def _handle_swing(self, candle, t):
        if t >= EXIT_TIME:
            self.state = State.DONE
            return None

        self.swing_bars += 1
        if self.swing_bars > MAX_WAIT_BARS:
            logger.info(f"{self.symbol} swing timeout — skipping today")
            self.state = State.DONE
            return None

        if self.breakout_dir == "LONG":
            if candle["high"] > self.swing_peak:
                self.swing_peak = candle["high"]
            if candle["close"] < self.swing_peak * (1 - SWING_CONFIRM):
                self.swing_value = self.swing_peak
                self._build_setup()
        else:
            if candle["low"] < self.swing_peak:
                self.swing_peak = candle["low"]
            if candle["close"] > self.swing_peak * (1 + SWING_CONFIRM):
                self.swing_value = self.swing_peak
                self._build_setup()

        return None

    def _handle_fib(self, candle, t):
        if t >= EXIT_TIME:
            self.state = State.DONE
            return None

        s = self.setup

        # Invalidation — price blew through stop zone
        if self.breakout_dir == "LONG" and candle["close"] <= s.stop_loss:
            logger.info(f"{self.symbol} invalidated (below SL zone)")
            self.state = State.DONE
            return None
        if self.breakout_dir == "SHORT" and candle["close"] >= s.stop_loss:
            logger.info(f"{self.symbol} invalidated (above SL zone)")
            self.state = State.DONE
            return None

        if self.breakout_dir == "LONG" and candle["low"] <= s.fib_entry:
            s.touched_fib = True
            self.state    = State.WAITING_ENTRY
            logger.info(f"{self.symbol} fib {s.fib_entry:.2f} touched — watching for bounce")

        if self.breakout_dir == "SHORT" and candle["high"] >= s.fib_entry:
            s.touched_fib = True
            self.state    = State.WAITING_ENTRY
            logger.info(f"{self.symbol} fib {s.fib_entry:.2f} touched — watching for bounce")

        return None

    def _handle_entry(self, candle, t):
        if t >= EXIT_TIME:
            self.state = State.DONE
            return None

        s = self.setup

        if self.breakout_dir == "LONG" and candle["close"] <= s.stop_loss:
            self.state = State.DONE
            return None
        if self.breakout_dir == "SHORT" and candle["close"] >= s.stop_loss:
            self.state = State.DONE
            return None

        # Entry: candle CLOSES back through fib level with MACD confirmation
        if self.breakout_dir == "LONG" and candle["close"] > s.fib_entry:
            if self._check_macd(t, "LONG"):
                return self._fire_entry(t, "LONG")

        if self.breakout_dir == "SHORT" and candle["close"] < s.fib_entry:
            if self._check_macd(t, "SHORT"):
                return self._fire_entry(t, "SHORT")

        return None

    def _handle_in_trade(self, candle, t):
        trade = self.trade

        if t >= EXIT_TIME:
            logger.info(f"{self.symbol} time exit at {t}")
            self.state = State.DONE
            return "exit_trade"

        if trade.direction == "LONG":
            if candle["low"] <= trade.stop_loss:
                logger.info(f"{self.symbol} SL hit at {trade.stop_loss:.2f}")
                self.state = State.DONE
                return "exit_trade"
            if trade.target_price > 0 and candle["high"] >= trade.target_price:
                logger.info(f"{self.symbol} target hit at {trade.target_price:.2f}")
                self.state = State.DONE
                return "exit_trade"
        else:
            if candle["high"] >= trade.stop_loss:
                logger.info(f"{self.symbol} SL hit at {trade.stop_loss:.2f}")
                self.state = State.DONE
                return "exit_trade"
            if trade.target_price > 0 and candle["low"] <= trade.target_price:
                logger.info(f"{self.symbol} target hit at {trade.target_price:.2f}")
                self.state = State.DONE
                return "exit_trade"

        return None

    # ── Helpers ────────────────────────────────────────────────────────────────

    def _build_setup(self):
        if self.breakout_dir == "LONG":
            anchor_lo = self.or_low
            anchor_hi = self.swing_value
            rng       = anchor_hi - anchor_lo
            if rng <= 0:
                self.state = State.DONE
                return
            fib_entry = anchor_hi - FIB_ENTRY_PCT * rng
            fib_786   = anchor_hi - FIB_786 * rng
            stop_loss = fib_786 * (1 - SL_BUFFER_PCT)
        else:
            anchor_hi = self.or_high
            anchor_lo = self.swing_value
            rng       = anchor_hi - anchor_lo
            if rng <= 0:
                self.state = State.DONE
                return
            fib_entry = anchor_lo + FIB_ENTRY_PCT * rng
            fib_786   = anchor_lo + FIB_786 * rng
            stop_loss = fib_786 * (1 + SL_BUFFER_PCT)

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
            f"{self.symbol} setup {self.breakout_dir}: "
            f"fib={self.setup.fib_entry:.2f} sl={self.setup.stop_loss:.2f}"
        )

    def _fire_entry(self, t: str, direction: str) -> str:
        s       = self.setup
        entry   = s.fib_entry
        sl      = s.stop_loss
        risk_ps = abs(entry - sl)

        qty = min(
            int(self.cfg.risk / risk_ps),
            int(CAPITAL_PER_TRADE / entry),
        )
        if qty <= 0:
            self.state = State.DONE
            return None

        target = (
            entry + risk_ps * self.cfg.target_r
            if direction == "LONG"
            else entry - risk_ps * self.cfg.target_r
        )

        self.trade = OpenTrade(
            direction    = direction,
            entry_price  = entry,
            quantity     = qty,
            stop_loss    = sl,
            target_price = round(target, 2),
            entry_time   = t,
        )
        self.state = State.IN_TRADE
        logger.info(
            f"{self.symbol} ENTRY {direction} @ {entry:.2f} "
            f"qty={qty} sl={sl:.2f} tgt={target:.2f} "
            f"risk=₹{risk_ps*qty:.0f}"
        )
        return f"enter_{direction.lower()}"

    def _check_macd(self, t: str, direction: str) -> bool:
        if MACD_CONDITION == "none":
            return True
        if self.macd_5m.empty or len(self.macd_5m) < 2:
            return True   # No data available — don't filter

        ts = t + ":00"
        relevant = self.macd_5m[self.macd_5m["time_str"] <= ts]
        if len(relevant) < 2:
            return True

        cur  = relevant.iloc[-1]
        prev = relevant.iloc[-2]

        if MACD_CONDITION == "macd_cross":
            return (cur["macd"] > cur["signal"]) if direction == "LONG" \
                   else (cur["macd"] < cur["signal"])
        if MACD_CONDITION == "histogram_positive":
            return (cur["histogram"] > 0) if direction == "LONG" \
                   else (cur["histogram"] < 0)
        if MACD_CONDITION == "histogram_rising":
            return (cur["histogram"] > prev["histogram"]) if direction == "LONG" \
                   else (cur["histogram"] < prev["histogram"])
        return True

    def update_macd(self, macd_df: pd.DataFrame):
        self.macd_5m = macd_df


# ═══════════════════════════════════════════════════════════════════════════════
#  GROWW BROKER
# ═══════════════════════════════════════════════════════════════════════════════

def _ref_id() -> str:
    """12-char unique order reference (Groww needs 8-20 alphanum chars)."""
    return f"FM{uuid.uuid4().hex[:10].upper()}"


def _attr(obj, key):
    """Read key from dict or object attribute — handles both SDK response styles."""
    if isinstance(obj, dict):
        return obj.get(key)
    return getattr(obj, key, None)


class GrowwBroker:
    """Groww Trade API wrapper. All calls are thin adapters over growwapi SDK."""

    def __init__(self, api_key: str, secret: str):
        self._api_key = api_key
        self._secret  = secret
        self._g: Optional[GrowwAPI] = None

    def connect(self) -> bool:
        """
        Authenticate with Groww using API key + secret.
        The access token is tied to the current session (refreshed daily by Groww Cloud).
        """
        try:
            token   = GrowwAPI.get_access_token(api_key=self._api_key, secret=self._secret)
            self._g = GrowwAPI(token)

            # Pull real constant values from the live instance (confirmed from Groww sample)
            # Annexures confirm: SL="SL", SL_M="SL_M", MIS="MIS"
            global _NSE, _CASH, _DAY, _MIS, _MARKET, _LIMIT, _SL, _SL_M, _BUY, _SELL
            _NSE    = getattr(self._g, "EXCHANGE_NSE",                    _NSE)
            _CASH   = getattr(self._g, "SEGMENT_CASH",                    _CASH)
            _DAY    = getattr(self._g, "VALIDITY_DAY",                    _DAY)
            _MIS    = getattr(self._g, "PRODUCT_MIS",                     _MIS)
            _MARKET = getattr(self._g, "ORDER_TYPE_MARKET",               _MARKET)
            _LIMIT  = getattr(self._g, "ORDER_TYPE_LIMIT",                _LIMIT)
            _SL     = getattr(self._g, "ORDER_TYPE_STOP_LOSS",            _SL)
            _SL_M   = getattr(self._g, "ORDER_TYPE_STOP_LOSS_MARKET",     _SL_M)
            _BUY    = getattr(self._g, "TRANSACTION_TYPE_BUY",            _BUY)
            _SELL   = getattr(self._g, "TRANSACTION_TYPE_SELL",           _SELL)

            logger.info("Groww broker connected ✓")
            return True
        except Exception as e:
            logger.error(f"Groww connect failed: {e}")
            return False

    def get_candles(self, symbol: str,
                    from_dt: str, to_dt: str) -> list[dict]:
        """
        Fetch today's 1-minute intraday OHLCV candles using the new Groww
        historical candles API (requires Historical Data subscription).

        Rate limit: 10 req/sec. We fire 34 calls per tick (one per stock, 1-min only).
        120ms sleep keeps burst rate at ~8/sec. Total: ~4s per tick.
        5-min MACD is computed by resampling these candles — no separate 5-min API call.
        """
        time.sleep(0.12)   # rate limiter — keeps calls under 10/sec

        try:
            resp = self._g.get_historical_candles(
                exchange        = self._g.EXCHANGE_NSE,
                segment         = self._g.SEGMENT_CASH,
                groww_symbol    = f"NSE-{symbol}",
                start_time      = from_dt,
                end_time        = to_dt,
                candle_interval = self._g.CANDLE_INTERVAL_MIN_1,
            )
            # Each candle: [epoch_seconds, open, high, low, close, volume]
            raw = _attr(resp, "candles") or []
            candles = []
            for c in raw:
                try:
                    ts = datetime.fromtimestamp(float(c[0]))
                    candles.append({
                        "datetime": ts.strftime("%Y-%m-%d %H:%M:%S"),
                        "open":   float(c[1]),
                        "high":   float(c[2]),
                        "low":    float(c[3]),
                        "close":  float(c[4]),
                        "volume": int(c[5] or 0),
                    })
                except Exception:
                    continue
            return candles
        except Exception as e:
            logger.error(f"get_candles({symbol}): {e}")
            return []

    def place_market_order(self, symbol: str, action: str, qty: int) -> OrderResult:
        txn = _BUY if action.lower() == "buy" else _SELL
        try:
            resp = self._g.place_order(
                trading_symbol     = symbol,
                quantity           = qty,
                validity           = _DAY,
                exchange           = _NSE,
                segment            = _CASH,
                product            = _MIS,
                order_type         = _MARKET,
                transaction_type   = txn,
                price              = 0,
                order_reference_id = _ref_id(),
            )
            oid = _attr(resp, "groww_order_id") or ""
            if oid:
                return OrderResult(True, oid, _attr(resp, "order_status") or "")
            return OrderResult(False, "", _attr(resp, "remark") or str(resp))
        except Exception as e:
            return OrderResult(False, "", str(e))

    def place_stoploss_order(self, symbol: str, action: str, qty: int,
                             trigger: float, limit: float) -> OrderResult:
        """
        Stop-loss limit order.
        order_type = SL  (annexures confirmed: "SL" = Stop Loss with limit price)
        trigger_price = level that activates the order
        price         = worst-case limit price (slippage buffer)
        """
        txn = _BUY if action.lower() == "buy" else _SELL
        try:
            resp = self._g.place_order(
                trading_symbol     = symbol,
                quantity           = qty,
                validity           = _DAY,
                exchange           = _NSE,
                segment            = _CASH,
                product            = _MIS,
                order_type         = _SL,       # "SL" confirmed from annexures
                transaction_type   = txn,
                price              = round(limit, 2),
                trigger_price      = round(trigger, 2),
                order_reference_id = _ref_id(),
            )
            oid = _attr(resp, "groww_order_id") or ""
            if oid:
                return OrderResult(True, oid, _attr(resp, "order_status") or "")
            return OrderResult(False, "", _attr(resp, "remark") or str(resp))
        except Exception as e:
            return OrderResult(False, "", str(e))

    def get_available_cash(self) -> float:
        """Return MIS (intraday) balance available using the real margin API."""
        try:
            resp = self._g.get_available_margin_details()
            return float(_attr(resp, "mis_balance_available") or 0)
        except Exception as e:
            logger.warning(f"get_available_cash failed: {e}")
            return 0.0

    def cancel_order(self, order_id: str) -> bool:
        try:
            resp = self._g.cancel_order(groww_order_id=order_id, segment=_CASH)
            status = str(_attr(resp, "order_status") or "")
            return "CANCEL" in status.upper() or bool(_attr(resp, "groww_order_id"))
        except Exception as e:
            logger.error(f"cancel_order({order_id}): {e}")
            return False


# ═══════════════════════════════════════════════════════════════════════════════
#  LIVE TRADER ORCHESTRATOR
#
#  Groww Cloud "Daily" deployment runs this script as a LONG-RUNNING PROCESS
#  from Start time (09:15) to End time (15:30). The while-loop polls every
#  60 seconds — exactly right for this deployment model.
# ═══════════════════════════════════════════════════════════════════════════════

def _ema(values: np.ndarray, period: int) -> np.ndarray:
    return pd.Series(values).ewm(span=period, adjust=False).mean().values


class LiveTrader:
    """
    Main trading loop — runs for the full trading session.
    Every 60 seconds:
      1. Fetch today's 1-min candles for each stock (only the new candle)
      2. Feed it into the stock's state machine
      3. Place entry / exit orders if the state machine signals
      4. Refresh 5-min MACD for all stocks
    """

    def __init__(self, broker: GrowwBroker, stock_states: dict[str, StockState]):
        self.broker = broker
        self.states = stock_states

    def run(self):
        if not self.broker.connect():
            logger.error("Broker connection failed — aborting")
            return

        for s in self.states.values():
            s.reset()

        logger.info(f"Trading {len(self.states)} stocks | DRY_RUN={DRY_RUN}")

        while True:
            now      = datetime.now()
            time_str = now.strftime("%H:%M")

            # Market not open yet — Groww Cloud starts at 09:15 so this is
            # only hit if the script starts slightly early.
            if time_str < MARKET_OPEN:
                logger.info(f"Waiting for market open ({time_str})...")
                time.sleep(15)
                continue

            # Market closed or forced exit time
            if time_str >= MARKET_CLOSE:
                logger.info("Market closed — emergency exit all positions")
                self._emergency_exit_all()
                break

            self._tick(now)

            # Sleep to the next minute boundary
            sleep_secs = 60 - (now.second % 60)
            time.sleep(sleep_secs)

        self._print_summary()

    # ── Per-tick logic ─────────────────────────────────────────────────────────

    def _tick(self, now: datetime):
        today   = now.strftime("%Y-%m-%d")
        from_dt = f"{today} 09:15:00"
        to_dt   = now.strftime("%Y-%m-%d %H:%M:%S")

        for symbol, state in self.states.items():
            if state.state == State.DONE:
                continue

            candles = self.broker.get_candles(symbol, from_dt, to_dt)
            if not candles:
                continue

            # Compute 5-min MACD by resampling 1-min data — no extra API call
            self._update_macd_from_1m(state, candles)

            latest    = candles[-1]
            last_seen = state.candles_1m[-1]["datetime"] if state.candles_1m else ""
            if latest["datetime"] == last_seen:
                continue  # No new candle yet

            action = state.on_candle(latest)

            if action and action.startswith("enter_"):
                self._handle_entry(state)
            elif action == "exit_trade":
                self._handle_exit(state)

        self._log_status()

    # ── Order handling ─────────────────────────────────────────────────────────

    def _handle_entry(self, state: StockState):
        trade     = state.trade
        symbol    = state.symbol
        direction = trade.direction
        qty       = trade.quantity
        buy_sell  = "buy" if direction == "LONG" else "sell"

        if DRY_RUN:
            logger.info(
                f"[DRY RUN] {symbol} {buy_sell.upper()} {qty} "
                f"@ ~{trade.entry_price:.2f} | SL {trade.stop_loss:.2f} "
                f"| TGT {trade.target_price:.2f}"
            )
            trade.entry_order_id = "DRY_ENTRY"
            return

        result = self.broker.place_market_order(symbol, buy_sell, qty)
        if not result.success:
            logger.error(f"{symbol} entry FAILED: {result.message}")
            state.state = State.DONE
            state.trade = None
            return
        trade.entry_order_id = result.order_id
        logger.info(f"{symbol} entry placed: {result.order_id}")

        sl_side  = "sell" if direction == "LONG" else "buy"
        sl_limit = round(trade.stop_loss * (0.995 if direction == "LONG" else 1.005), 2)
        sl_res   = self.broker.place_stoploss_order(
            symbol, sl_side, qty,
            trigger = trade.stop_loss,
            limit   = sl_limit,
        )
        if sl_res.success:
            trade.sl_order_id = sl_res.order_id
            logger.info(f"{symbol} SL placed: {sl_res.order_id}")
        else:
            logger.warning(f"{symbol} SL FAILED: {sl_res.message} — monitor manually!")

    def _handle_exit(self, state: StockState):
        if not state.trade:
            return
        symbol    = state.symbol
        direction = state.trade.direction
        qty       = state.trade.quantity
        exit_side = "sell" if direction == "LONG" else "buy"

        if DRY_RUN:
            logger.info(f"[DRY RUN] {symbol} EXIT {exit_side.upper()} {qty}")
            return

        if state.trade.sl_order_id and state.trade.sl_order_id != "DRY_ENTRY":
            self.broker.cancel_order(state.trade.sl_order_id)

        result = self.broker.place_market_order(symbol, exit_side, qty)
        if result.success:
            logger.info(f"{symbol} exit placed: {result.order_id}")
        else:
            logger.error(f"{symbol} EXIT FAILED: {result.message} — MANUAL ACTION REQUIRED!")

    def _emergency_exit_all(self):
        for state in self.states.values():
            if state.state == State.IN_TRADE and state.trade:
                logger.warning(f"Emergency exit: {state.symbol}")
                self._handle_exit(state)
                state.state = State.DONE

    # ── MACD (computed from resampled 1-min data — no extra API call) ─────────

    def _update_macd_from_1m(self, state: StockState, candles_1m: list[dict]):
        """
        Resample 1-min candles to 5-min bars and compute MACD.
        Avoids a separate 5-min API call (and any candle_interval format issues).
        """
        if len(candles_1m) < 5:
            return
        try:
            df = pd.DataFrame(candles_1m)
            df["dt"] = pd.to_datetime(df["datetime"])
            df = df.set_index("dt").sort_index()

            # 5-min bar close = last 1-min close in that 5-min window
            df_5m           = df["close"].resample("5min").last().dropna()
            closes          = df_5m.values
            if len(closes) < 2:
                return

            ema_fast        = _ema(closes, MACD_FAST)
            ema_slow        = _ema(closes, MACD_SLOW)
            macd_line       = ema_fast - ema_slow
            signal_line     = _ema(macd_line, MACD_SIGNAL)

            result = pd.DataFrame({
                "time_str":  df_5m.index.strftime("%H:%M:%S"),
                "macd":      macd_line,
                "signal":    signal_line,
                "histogram": macd_line - signal_line,
            })
            state.update_macd(result)
        except Exception as e:
            logger.warning(f"MACD resample failed for {state.symbol}: {e}")

    # ── Status logging ────────────────────────────────────────────────────────

    def _log_status(self):
        t_str    = datetime.now().strftime("%H:%M:%S")
        in_trade = [s for s in self.states.values() if s.state == State.IN_TRADE]
        watching = [s for s in self.states.values() if s.state not in (State.DONE, State.IN_TRADE)]
        done     = [s for s in self.states.values() if s.state == State.DONE]
        logger.info(
            f"[{t_str}] IN_TRADE={len(in_trade)} | "
            f"WATCHING={len(watching)} | DONE={len(done)}"
        )
        for s in in_trade:
            t = s.trade
            logger.info(
                f"  {s.symbol} {t.direction} {t.quantity}x "
                f"entry={t.entry_price:.2f} sl={t.stop_loss:.2f} tgt={t.target_price:.2f}"
            )

    def _print_summary(self):
        logger.info("=" * 60)
        logger.info("END OF DAY SUMMARY")
        logger.info("=" * 60)
        for s in self.states.values():
            if s.trade:
                t = s.trade
                logger.info(
                    f"  {s.symbol}: {t.direction} {t.quantity}x @ "
                    f"{t.entry_price:.2f} | {s.state.name}"
                )
        no_trade = [s.symbol for s in self.states.values()
                    if s.trade is None and s.state == State.DONE]
        logger.info(f"No trade today: {no_trade}")


# ═══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    print("=" * 60)
    print("  Fib-MACD Live Trader — Groww Cloud")
    print("=" * 60)
    print(f"  Mode:         {'DRY RUN' if DRY_RUN else 'LIVE TRADING'}")
    print(f"  Stocks:       {len(PORTFOLIO)}")
    print(f"  Default risk: Rs {DEFAULT_RISK:,.0f}/trade")
    print(f"  Capital cap:  Rs {CAPITAL_PER_TRADE:,.0f}/position")
    print(f"  OR duration:  {OR_MINUTES} min")
    print(f"  Fib entry:    {FIB_ENTRY_PCT*100:.1f}%")
    print(f"  MACD:         {MACD_CONDITION}")
    print(f"  Exit time:    {EXIT_TIME}")
    print("=" * 60)
    if not DRY_RUN:
        print("  *** LIVE MODE — real orders will be placed! ***")

    stocks: dict[str, StockState] = {}
    for (symbol, direction, target_r) in PORTFOLIO:
        risk = RISK_OVERRIDES.get(symbol) or DEFAULT_RISK
        cfg  = StockCfg(symbol=symbol, direction=direction,
                        target_r=target_r, risk=risk)
        stocks[symbol] = StockState(cfg)

    broker = GrowwBroker(GROWW_API_KEY, GROWW_SECRET)
    LiveTrader(broker, stocks).run()


if __name__ == "__main__":
    main()
