"""Fib-MACD ORB strategy -- Groww Cloud. Single-file. Deps: growwapi, pandas, numpy."""

# -- CONFIGURATION -------------------------------------------------------------
# Credentials
GROWW_API_KEY = "your_api_key"   # <- replace
GROWW_SECRET  = "your_secret"    # <- replace

# Risk per trade
DEFAULT_RISK: float = 1000           # Rs per trade (per stock)
RISK_OVERRIDES: dict[str, float] = {}  # e.g. {"SBIN": 2_000}

CAPITAL_PER_TRADE:    float = 3_00_000  # Rs max capital per position
MAX_TRADES_PER_DAY:   int   = 10
MIS_LEVERAGE_DIVISOR: float = 0.20      # ~=5x intraday leverage
ENTRY_FILL_WAIT_S:    float = 30        # max seconds to wait for entry fill before cancelling
MIN_MACD_BARS:        int   = 2
MACD_WARMUP_DAYS:     int   = 5         # prior trading days for MACD EMA continuity

STATE_DIR: str = "/tmp"   # use "." for local testing

# -- Strategy parameters (Run 7 backtest, Sharpe > 2.5) ------------------------
OR_MINUTES       = 30         # Opening range duration in minutes
FIB_ENTRY_PCT    = 0.618      # Enter at 61.8% retracement
SL_BUFFER_PCT    = 0.001      # 0.1% buffer beyond 78.6% stop reference
SWING_CONFIRM    = 0.003      # 0.3% retrace confirms swing is done
MACD_CONDITION   = "macd_cross"   # "macd_cross" | "histogram_positive" | "none"
MACD_FAST        = 12
MACD_SLOW        = 26
MACD_SIGNAL      = 9
TARGET_R         = 1.5        # Reward:Risk ratio for target
NO_NEW_ENTRY_TIME = "14:30"   # After this, state machine stops progressing toward new entries
EXIT_TIME        = "15:00"    # Force-exit all IN_TRADE positions at this time
MAX_WAIT_BARS    = 60         # Max 1-min bars to wait for fib touch after swing

DRY_RUN = True             # log signals only when True
LATE_START_MODE = False    # True = replay 09:15->now through state machine on startup

# Portfolio: 34 stocks from Run 7 backtest. Format: (NSE_SYMBOL, direction, target_r)
# direction in {long_only, short_only, both}
PORTFOLIO = [
    # ("LTIM",        "long_only",  1.5),  # symbol mismatch on Groww (LTIMINDTREE?)
    # ("TATAMOTORS",  "short_only", 1.5),  # symbol mismatch post-demerger
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

# -- END OF CONFIGURATION ------------------------------------------------------

import json
import logging
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import Optional

import numpy as np
import pandas as pd
from growwapi import GrowwAPI

# -- Logging -------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("GrowwFibMACD")

# -- Market hours --------------------------------------------------------------
MARKET_OPEN   = "09:15"
MARKET_CLOSE  = "15:18"   # emergency_exit_all fires here as safety net before broker auto-squares MIS at 15:20
                          # (state machine force-exits at EXIT_TIME=15:00, this catches any stragglers)
LTP_POLL_SECS = 15   # poll LTP every 15 s -> 4 samples per 1-min candle

_oe_h, _oe_m = divmod(9 * 60 + 15 + OR_MINUTES, 60)
OR_END = f"{_oe_h:02d}:{_oe_m:02d}"   # "09:45" -- first minute after the opening range

# -- Groww API constants --------------------------------------------------------
# Confirmed from Groww sample script -- these live on the GrowwAPI instance.
# We resolve them after the first connect() call in GrowwBroker.
# Defaults (strings) are fallbacks if the SDK version changes constant names.
_NSE    = "NSE"
_CASH   = "CASH"
_DAY    = "DAY"
_MIS    = "MIS"      # Margin Intraday Square-off -- confirmed in annexures
_MARKET = "MARKET"
_LIMIT  = "LIMIT"
_SL_M   = "SL_M"     # Stop Loss Market (used in OCO stop_loss leg)
_BUY    = "BUY"
_SELL   = "SELL"
_OCO    = "OCO"      # Smart-order type for One-Cancels-Other (SL+target)

FIB_786 = 0.786

# Order lifecycle terminal states (Groww annexures)
_FILLED_STATES = ("EXECUTED", "COMPLETED", "DELIVERY_AWAITED")
_DEAD_STATES   = ("REJECTED", "FAILED", "CANCELLED")


# ===============================================================================
#  DATA CLASSES
# ===============================================================================

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


@dataclass
class OpenTrade:
    direction:       str
    entry_price:     float
    quantity:        int
    stop_loss:       float
    target_price:    float
    entry_time:      str
    entry_order_id:  str = ""
    oco_order_id:    str = ""    # Groww smart-order id covering SL+target as one OCO


@dataclass
class OrderResult:
    success:  bool
    order_id: str
    message:  str


# ===============================================================================
#  STATE MACHINE -- per stock
# ===============================================================================

class State(Enum):
    WAITING_OR       = auto()
    WAITING_BREAKOUT = auto()
    WAITING_SWING    = auto()
    WAITING_FIB      = auto()
    WAITING_ENTRY    = auto()
    IN_TRADE         = auto()
    DONE             = auto()


class StockState:
    """Per-stock Fib-MACD state machine. on_candle() returns 'enter_*'/'exit_trade'/None."""

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
        self.no_trade_reason: str = ""   # set when state=DONE without a trade
        # Pre-today 1-min candles fetched at startup. Prepended to every
        # MACD computation so EMA(26) is continuous across day boundaries --
        # matches backtest behaviour where MACD spans the full history.
        self.warmup_candles_1m: list[dict] = []

    def _done(self, reason: str):
        """Mark state DONE and record the reason if no trade was placed."""
        self.state = State.DONE
        if self.trade is None and not self.no_trade_reason:
            self.no_trade_reason = reason

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

    # -- State handlers --------------------------------------------------------

    def _handle_or(self, candle, t):
        if t < OR_END:
            return None

        or_candles = [c for c in self.candles_1m if c["datetime"][11:16] < OR_END]
        if len(or_candles) < 2:
            self._done("OR build failed (<2 candles)")
            return None

        self.or_high = max(c["high"] for c in or_candles)
        self.or_low  = min(c["low"]  for c in or_candles)
        self.state   = State.WAITING_BREAKOUT
        logger.info(f"{self.symbol} OR closed: H={self.or_high:.2f} L={self.or_low:.2f}")
        return self._handle_breakout(candle, t)

    def _handle_breakout(self, candle, t):
        if t >= NO_NEW_ENTRY_TIME:
            self._done(f"no breakout by {NO_NEW_ENTRY_TIME}")
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
        if t >= NO_NEW_ENTRY_TIME:
            self._done(f"swing in progress past {NO_NEW_ENTRY_TIME}")
            return None

        self.swing_bars += 1
        if self.swing_bars > MAX_WAIT_BARS:
            logger.info(f"{self.symbol} swing timeout -- skipping today")
            self._done(f"swing timeout after {MAX_WAIT_BARS} bars")
            return None

        if self.breakout_dir == "LONG":
            if candle["high"] > self.swing_peak:
                self.swing_peak = candle["high"]
            if candle["close"] < self.swing_peak * (1 - SWING_CONFIRM):
                logger.info(f"{self.symbol} LONG swing confirmed: peak={self.swing_peak:.2f} bar={self.swing_bars}")
                self.swing_value = self.swing_peak
                self._build_setup()
        else:
            if candle["low"] < self.swing_peak:
                self.swing_peak = candle["low"]
            if candle["close"] > self.swing_peak * (1 + SWING_CONFIRM):
                logger.info(f"{self.symbol} SHORT swing confirmed: trough={self.swing_peak:.2f} bar={self.swing_bars}")
                self.swing_value = self.swing_peak
                self._build_setup()

        return None

    def _handle_fib(self, candle, t):
        if t >= NO_NEW_ENTRY_TIME:
            self._done(f"fib not touched by {NO_NEW_ENTRY_TIME}")
            return None

        s = self.setup

        # Invalidation -- price blew through 78.6% stop zone
        if self.breakout_dir == "LONG" and candle["close"] <= s.stop_loss:
            logger.info(f"{self.symbol} LONG invalidated: close={candle['close']:.2f} <= sl_zone={s.stop_loss:.2f}")
            self._done("setup invalidated (78.6% breach)")
            return None
        if self.breakout_dir == "SHORT" and candle["close"] >= s.stop_loss:
            logger.info(f"{self.symbol} SHORT invalidated: close={candle['close']:.2f} >= sl_zone={s.stop_loss:.2f}")
            self._done("setup invalidated (78.6% breach)")
            return None

        # Same-candle entry possible: a strong bounce wicks fib + closes back through.
        if self.breakout_dir == "LONG" and candle["low"] <= s.fib_entry:
            self.state = State.WAITING_ENTRY
            logger.info(f"{self.symbol} fib {s.fib_entry:.2f} touched")
            return self._handle_entry(candle, t)

        if self.breakout_dir == "SHORT" and candle["high"] >= s.fib_entry:
            self.state = State.WAITING_ENTRY
            logger.info(f"{self.symbol} fib {s.fib_entry:.2f} touched")
            return self._handle_entry(candle, t)

        return None

    def _handle_entry(self, candle, t):
        if t >= NO_NEW_ENTRY_TIME:
            self._done(f"entry not fired by {NO_NEW_ENTRY_TIME}")
            return None

        s = self.setup

        if self.breakout_dir == "LONG" and candle["close"] <= s.stop_loss:
            self._done("setup invalidated (78.6% breach during entry wait)")
            return None
        if self.breakout_dir == "SHORT" and candle["close"] >= s.stop_loss:
            self._done("setup invalidated (78.6% breach during entry wait)")
            return None

        # Entry: candle CLOSES back through fib level with MACD confirmation
        if self.breakout_dir == "LONG" and candle["close"] > s.fib_entry:
            if self._check_macd(t, "LONG"):
                return self._fire_entry(t, "LONG")
            logger.info(f"{self.symbol} LONG entry BLOCKED by MACD at {t} (close={candle['close']:.2f} fib={s.fib_entry:.2f})")

        if self.breakout_dir == "SHORT" and candle["close"] < s.fib_entry:
            if self._check_macd(t, "SHORT"):
                return self._fire_entry(t, "SHORT")
            logger.info(f"{self.symbol} SHORT entry BLOCKED by MACD at {t} (close={candle['close']:.2f} fib={s.fib_entry:.2f})")

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

    # -- Helpers ----------------------------------------------------------------

    def _build_setup(self):
        if self.breakout_dir == "LONG":
            anchor_lo = self.or_low
            anchor_hi = self.swing_value
            rng       = anchor_hi - anchor_lo
            if rng <= 0:
                self._done("invalid swing range (LONG, rng<=0)")
                return
            fib_entry = anchor_hi - FIB_ENTRY_PCT * rng
            fib_786   = anchor_hi - FIB_786 * rng
            stop_loss = fib_786 * (1 - SL_BUFFER_PCT)
        else:
            anchor_hi = self.or_high
            anchor_lo = self.swing_value
            rng       = anchor_hi - anchor_lo
            if rng <= 0:
                self._done("invalid swing range (SHORT, rng<=0)")
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

        risk_cap    = int(self.cfg.risk / risk_ps) if risk_ps > 0 else 0
        capital_cap = int(CAPITAL_PER_TRADE / entry) if entry > 0 else 0
        qty         = min(risk_cap, capital_cap)

        logger.debug(
            f"{self.symbol} sizing: entry={entry:.2f} sl={sl:.2f} "
            f"risk_ps={risk_ps:.2f} | "
            f"risk_cap={risk_cap} (Rs{self.cfg.risk}/Rs{risk_ps:.2f}/share) | "
            f"capital_cap={capital_cap} (Rs{CAPITAL_PER_TRADE}/Rs{entry:.2f}/share) | "
            f"limiting factor={'RISK' if risk_cap <= capital_cap else 'CAPITAL'} -> qty={qty}"
        )
        if qty <= 0:
            logger.info(f"{self.symbol} qty=0 (risk_ps={risk_ps:.2f} too large or entry=0) -- skipping entry")
            self._done(f"qty=0 (risk_ps={risk_ps:.2f} too wide)")
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
            f"risk=Rs{risk_ps*qty:.0f}"
        )
        return f"enter_{direction.lower()}"

    def _check_macd(self, t: str, direction: str) -> bool:
        if MACD_CONDITION == "none":
            logger.debug(f"{self.symbol} MACD check: condition='none' -> PASS (always)")
            return True
        # With MACD_WARMUP_DAYS prior days seeding the EMA, only 2 bars in
        # today's session are needed before MACD is reliable (matches backtest).
        if self.macd_5m.empty or len(self.macd_5m) < MIN_MACD_BARS:
            logger.info(
                f"{self.symbol} MACD check: only {len(self.macd_5m)} bars "
                f"(< MIN_MACD_BARS={MIN_MACD_BARS}) -> PASS (no filter)"
            )
            return True

        # Build full datetime cutoff so the filter works correctly with
        # multi-day MACD series (warmup days + today). Use the most recent
        # candle's date so this is correct in both live and replay modes.
        cur_date = self.candles_1m[-1]["datetime"][:10] if self.candles_1m else \
                   datetime.now().strftime("%Y-%m-%d")
        ts = f"{cur_date} {t}:00"
        relevant = self.macd_5m[self.macd_5m["time_str"] <= ts]
        if len(relevant) < 2:
            logger.info(
                f"{self.symbol} MACD check: only {len(relevant)} relevant bars at {t} -> PASS (no filter)"
            )
            return True

        cur = relevant.iloc[-1]

        macd_val = float(cur["macd"])
        sig_val  = float(cur["signal"])
        hist_val = float(cur["histogram"])

        if MACD_CONDITION == "macd_cross":
            passed = (macd_val > sig_val) if direction == "LONG" else (macd_val < sig_val)
            logger.info(
                f"{self.symbol} MACD cross check ({direction}) at {t}: "
                f"macd={macd_val:.4f} signal={sig_val:.4f} hist={hist_val:.4f} "
                f"-> {'PASS OK' if passed else 'FAIL X'}"
            )
            return passed

        if MACD_CONDITION == "histogram_positive":
            passed = (hist_val > 0) if direction == "LONG" else (hist_val < 0)
            logger.info(
                f"{self.symbol} MACD histogram_positive ({direction}) at {t}: "
                f"hist={hist_val:.4f} -> {'PASS OK' if passed else 'FAIL X'}"
            )
            return passed

        return True


# ===============================================================================
#  GROWW BROKER
# ===============================================================================

def _ref_id() -> str:
    """12-char unique order reference (Groww needs 8-20 alphanum chars)."""
    return f"FM{uuid.uuid4().hex[:10].upper()}"


def _round_tick(price: float) -> float:
    """Round price to nearest NSE tick size (variable by price band) to avoid exchange rejection."""
    if price <= 250:
        tick = 0.01
    elif price <= 1000:
        tick = 0.05
    elif price <= 5000:
        tick = 0.10
    elif price <= 10000:
        tick = 0.50
    elif price <= 20000:
        tick = 1.00
    else:
        tick = 5.00
    return round(round(price / tick) * tick, 2)


def _attr(obj, key):
    """Read key from dict or object attribute -- handles both SDK response styles."""
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
        """Authenticate with Groww and pull real SDK constant values."""
        try:
            token   = GrowwAPI.get_access_token(api_key=self._api_key, secret=self._secret)
            self._g = GrowwAPI(token)

            global _NSE, _CASH, _DAY, _MIS, _MARKET, _LIMIT, _SL_M, _BUY, _SELL, _OCO
            _NSE    = getattr(self._g, "EXCHANGE_NSE",                    _NSE)
            _CASH   = getattr(self._g, "SEGMENT_CASH",                    _CASH)
            _DAY    = getattr(self._g, "VALIDITY_DAY",                    _DAY)
            _MIS    = getattr(self._g, "PRODUCT_MIS",                     _MIS)
            _MARKET = getattr(self._g, "ORDER_TYPE_MARKET",               _MARKET)
            _LIMIT  = getattr(self._g, "ORDER_TYPE_LIMIT",                _LIMIT)
            _SL_M   = getattr(self._g, "ORDER_TYPE_STOP_LOSS_MARKET",     _SL_M)
            _BUY    = getattr(self._g, "TRANSACTION_TYPE_BUY",            _BUY)
            _SELL   = getattr(self._g, "TRANSACTION_TYPE_SELL",           _SELL)
            _OCO    = getattr(self._g, "SMART_ORDER_TYPE_OCO",            _OCO)

            logger.info("Groww broker connected OK")
            logger.info(
                f"API constants -> NSE={_NSE!r} CASH={_CASH!r} DAY={_DAY!r} "
                f"MIS={_MIS!r} MARKET={_MARKET!r} LIMIT={_LIMIT!r} "
                f"SL_M={_SL_M!r} BUY={_BUY!r} SELL={_SELL!r} OCO={_OCO!r}"
            )
            return True
        except Exception as e:
            logger.error(f"Groww connect failed: {e}")
            return False

    def get_candles_historical(self, symbol: str,
                               from_dt: str, to_dt: str,
                               _stagger_s: float = 0.0) -> list[dict]:
        """Fetch 1-min OHLCV. _stagger_s spaces parallel callers; one retry on transient errors."""
        if _stagger_s > 0:
            logger.debug(f"get_candles_historical({symbol}): stagger sleep {_stagger_s:.1f}s")
            time.sleep(_stagger_s)

        logger.debug(f"get_candles_historical({symbol}): {from_dt}  ->  {to_dt} (sleeping 1.5s)")
        time.sleep(1.5)   # conservative rate-limit guard

        for attempt in (1, 2):
            try:
                resp = self._g.get_historical_candles(
                    exchange        = self._g.EXCHANGE_NSE,
                    segment         = self._g.SEGMENT_CASH,
                    groww_symbol    = f"NSE-{symbol}",
                    start_time      = from_dt,
                    end_time        = to_dt,
                    candle_interval = self._g.CANDLE_INTERVAL_MIN_1,
                )
                break   # success -- exit retry loop
            except Exception as e:
                err_str = str(e).lower()
                is_transient = any(kw in err_str for kw in
                                   ("timeout", "timed out", "connection", "remote",
                                    "aborted", "rate limit"))
                if attempt == 1 and is_transient:
                    retry_sleep = 10 if "rate limit" in err_str else 3
                    logger.warning(
                        f"get_candles_historical({symbol}): transient error "
                        f"(attempt 1) -- retrying in {retry_sleep}s: {e}"
                    )
                    time.sleep(retry_sleep)
                    continue
                logger.error(f"get_candles_historical({symbol}): {e}")
                return []

        try:
            raw = _attr(resp, "candles") or []
            candles      = []
            parse_errs   = 0
            premarket_skipped = 0
            for c in raw:
                # Groww returns 2 pre-market entries per day where OHLC is None
                # but volume is set (bookkeeping data). These are expected and
                # NOT errors -- silently skip them.
                if (len(c) >= 5
                        and c[1] is None and c[2] is None
                        and c[3] is None and c[4] is None):
                    premarket_skipped += 1
                    continue
                try:
                    # Groww returns timestamps in two formats depending on
                    # API version: ISO-8601 string ("2026-04-30T09:15:00")
                    # or epoch seconds (numeric). Handle both.
                    ts_raw = c[0]
                    if isinstance(ts_raw, str):
                        # Strip trailing Z if present, and tolerate timezone offsets
                        s = ts_raw.replace("Z", "+00:00")
                        ts = datetime.fromisoformat(s)
                        # Drop tzinfo for consistent local-naive output
                        if ts.tzinfo is not None:
                            ts = ts.replace(tzinfo=None)
                    else:
                        ts = datetime.fromtimestamp(float(ts_raw))
                    # Groww occasionally returns open=None on the 09:15 candle.
                    # Use close as fallback so the candle is kept, not dropped.
                    close_val = float(c[4])
                    candles.append({
                        "datetime": ts.strftime("%Y-%m-%d %H:%M:%S"),
                        "open":   float(c[1]) if c[1] is not None else close_val,
                        "high":   float(c[2]),
                        "low":    float(c[3]),
                        "close":  close_val,
                        "volume": int(c[5] or 0),
                    })
                except Exception as parse_err:
                    parse_errs += 1
                    if parse_errs <= 2:   # log only first couple to avoid spam
                        logger.warning(
                            f"get_candles_historical({symbol}) parse failure on "
                            f"candle {c!r}: {parse_err}"
                        )
                    continue
            if premarket_skipped:
                logger.debug(f"get_candles_historical({symbol}): skipped {premarket_skipped} pre-market None-OHLC entries")
            if parse_errs and parse_errs == len(raw) - premarket_skipped:
                logger.error(f"get_candles_historical({symbol}): ALL real candles failed to parse -- timestamp format change?")
            if candles:
                logger.debug(f"get_candles_historical({symbol}): {len(candles)} candles [{candles[0]['datetime'][11:16]} - {candles[-1]['datetime'][11:16]}]")
            else:
                # Common causes: subscription expired, wrong symbol prefix, date format, or response wrapped in payload.candles.
                logger.warning(f"get_candles_historical({symbol}): 0 candles returned (check date range / subscription)")
                logger.warning(f"get_candles_historical({symbol}) raw type={type(resp).__name__}: {str(resp)[:500]}")
            return candles
        except Exception as e:
            logger.error(f"get_candles_historical({symbol}) response parse: {e}")
            return []

    def get_ltp_batch(self, symbols: list[str]) -> dict[str, float]:
        """LTP for up to 50 symbols in one call. Returns {symbol: ltp}."""
        if not symbols:
            return {}
        exchange_syms = tuple(f"NSE_{s}" for s in symbols[:50])
        logger.debug(
            f"get_ltp_batch: requesting {len(exchange_syms)} symbols -- "
            f"{', '.join(exchange_syms[:5])}{'...' if len(exchange_syms) > 5 else ''}"
        )
        try:
            resp = self._g.get_ltp(
                segment                  = self._g.SEGMENT_CASH,
                exchange_trading_symbols = exchange_syms,
            )
            result: dict[str, float] = {}
            if isinstance(resp, dict):
                for k, v in resp.items():
                    sym = k.replace("NSE_", "", 1)   # "NSE_SBIN" -> "SBIN"
                    if isinstance(v, (int, float)):
                        result[sym] = float(v)
                    elif isinstance(v, dict):
                        result[sym] = float(v.get("ltp") or 0)
                    else:
                        result[sym] = float(_attr(v, "ltp") or 0)
            # Warn on symbols that came back missing or zero
            missing = [s for s in symbols if result.get(s, 0) == 0]
            if missing:
                logger.warning(f"get_ltp_batch: {len(missing)} symbols missing/zero: {missing}")
            logger.debug(f"get_ltp_batch: {len(result)} prices received -- sample: {dict(list(result.items())[:4])}")
            return result
        except Exception as e:
            logger.error(f"get_ltp_batch: {e}")
            return {}

    def place_market_order(self, symbol: str, action: str, qty: int,
                           ref_id: Optional[str] = None) -> OrderResult:
        """Market order. Stable ref_id makes retries idempotent (Groww rejects duplicate refs)."""
        txn = _BUY if action.lower() == "buy" else _SELL
        ref = ref_id or _ref_id()
        logger.info(f"place_market_order -> {symbol} txn={txn} qty={qty} order_type={_MARKET} product={_MIS} ref={ref}")
        try:
            # price intentionally omitted for MARKET -- Groww docs say price/trigger_price shouldn't be sent.
            resp = self._g.place_order(
                trading_symbol     = symbol,
                quantity           = qty,
                validity           = _DAY,
                exchange           = _NSE,
                segment            = _CASH,
                product            = _MIS,
                order_type         = _MARKET,
                transaction_type   = txn,
                order_reference_id = ref,
            )
            logger.debug(f"place_market_order({symbol}) raw response: {resp}")
            oid = _attr(resp, "groww_order_id") or ""
            if oid:
                logger.info(f"place_market_order({symbol}): order accepted -- groww_order_id={oid}")
                return OrderResult(True, oid, _attr(resp, "order_status") or "")
            msg = _attr(resp, "remark") or str(resp)
            logger.error(f"place_market_order({symbol}): REJECTED -- {msg}")
            return OrderResult(False, "", msg)
        except Exception as e:
            logger.error(f"place_market_order({symbol}): exception -- {e}")
            return OrderResult(False, "", str(e))

    def place_oco_order(self, symbol: str, direction: str, qty: int,
                        sl_trigger: float, target_price: float,
                        ref_id: Optional[str] = None) -> OrderResult:
        """OCO smart order: SL_M (price=None) + LIMIT target.
        net_position_quantity is SIGNED -- +qty for LONG, -qty for SHORT. Sending
        +qty for SHORT makes Groww fire SELL legs that ADD to the short."""
        exit_side  = _SELL if direction == "LONG" else _BUY
        net_pos    = qty if direction == "LONG" else -qty
        sl_trig_r  = _round_tick(sl_trigger)
        tgt_r      = _round_tick(target_price)
        ref        = ref_id or _ref_id()
        ctx = (f"ref={ref} sl_trigger={sl_trig_r:.2f} target={tgt_r:.2f} "
               f"qty={qty} net_pos={net_pos:+d} exit_side={exit_side}")
        logger.info(f"place_oco_order REQUEST ({symbol} {direction}): {ctx}")
        try:
            resp = self._g.create_smart_order(
                smart_order_type      = _OCO,
                reference_id          = ref,
                segment               = _CASH,
                trading_symbol        = symbol,
                quantity              = qty,
                product_type          = _MIS,
                exchange              = _NSE,
                duration              = _DAY,
                net_position_quantity = net_pos,
                transaction_type      = exit_side,
                target = {"trigger_price": f"{tgt_r:.2f}", "order_type": _LIMIT,
                          "price": f"{tgt_r:.2f}"},
                stop_loss = {"trigger_price": f"{sl_trig_r:.2f}",
                             "order_type": _SL_M, "price": None},
            )
            logger.info(f"place_oco_order RESPONSE ({symbol}): {resp}")
            oid    = _attr(resp, "smart_order_id") or ""
            status = _attr(resp, "status") or ""
            if oid:
                logger.info(f"place_oco_order ACCEPTED ({symbol}): smart_order_id={oid} status={status!r} | {ctx}")
                return OrderResult(True, oid, status)
            msg = _attr(resp, "remark") or str(resp)
            logger.error(f"place_oco_order REJECTED ({symbol}): {msg} | {ctx}")
            return OrderResult(False, "", msg)
        except Exception as e:
            logger.error(f"place_oco_order EXCEPTION ({symbol}): {e!r} | {ctx}")
            return OrderResult(False, "", str(e))

    def cancel_smart_order(self, smart_order_id: str) -> bool:
        """Cancel an OCO/GTT smart order. Returns True when payload status is CANCELLED."""
        if not smart_order_id:
            return False
        logger.info(f"cancel_smart_order: attempting to cancel {smart_order_id}")
        try:
            resp = self._g.cancel_smart_order(
                segment          = _CASH,
                smart_order_type = _OCO,
                smart_order_id   = smart_order_id,
            )
            status = str(_attr(resp, "status") or "")
            ok     = "CANCEL" in status.upper() or bool(_attr(resp, "smart_order_id"))
            if ok:
                logger.info(f"cancel_smart_order({smart_order_id}): OK status={status!r}")
            else:
                logger.warning(f"cancel_smart_order({smart_order_id}): uncertain -- status={status!r} resp={resp}")
            return ok
        except Exception as e:
            logger.error(f"cancel_smart_order({smart_order_id}): {e}")
            return False

    def get_available_cash(self) -> float:
        """Return MIS (intraday) balance available using the real margin API."""
        try:
            resp = self._g.get_available_margin_details()
            return float(_attr(resp, "mis_balance_available") or 0)
        except Exception as e:
            logger.warning(f"get_available_cash failed: {e}")
            return 0.0

    def get_live_qty(self, symbol: str) -> Optional[int]:
        """Net MIS qty: +long, -short, 0 flat, None on API error (caller must NOT skip exit on None)."""
        try:
            resp = self._g.get_position_for_trading_symbol(
                trading_symbol = symbol,
                segment        = self._g.SEGMENT_CASH,
            )
            if resp is None:
                logger.debug(f"get_live_qty({symbol}): no position record -- returning 0 (flat)")
                return 0   # no position found
            credit = int(_attr(resp, "credit_quantity") or 0)
            debit  = int(_attr(resp, "debit_quantity")  or 0)
            net    = credit - debit
            logger.debug(f"get_live_qty({symbol}): credit={credit} debit={debit} -> net={net}")
            return net
        except Exception as e:
            logger.warning(f"get_live_qty({symbol}): {e}")
            return None   # unknown -- don't skip the exit

    def get_all_mis_positions(self) -> list[dict]:
        """Open MIS positions as [{symbol, net_qty}]. net_qty > 0 long, < 0 short. [] on error/flat."""
        try:
            resp  = self._g.get_positions_for_user(segment=self._g.SEGMENT_CASH)
            items = resp if isinstance(resp, list) else (
                    _attr(resp, "positions") or _attr(resp, "data") or [])
            result = []
            for p in items:
                if str(_attr(p, "product") or "").upper() != "MIS":
                    continue
                credit = int(_attr(p, "credit_quantity") or 0)
                debit  = int(_attr(p, "debit_quantity")  or 0)
                net    = credit - debit
                if net == 0:
                    continue
                result.append({"symbol": _attr(p, "trading_symbol"), "net_qty": net})
            if result:
                detail = ", ".join(f"{p['symbol']}={p['net_qty']:+d}" for p in result)
                logger.info(f"get_all_mis_positions: {len(result)} open MIS position(s) -- {detail}")
            else:
                logger.info("get_all_mis_positions: no open MIS positions found")
            return result
        except Exception as e:
            logger.error(f"get_all_mis_positions: {e}")
            return []

    def get_avg_fill_price(self, order_id: str) -> float:
        """Weighted-avg fill price via GET /order/trades/{id}. Returns 0 on no-trades / error."""
        try:
            resp = self._g.get_trade_list_for_order(
                groww_order_id = order_id,
                segment        = _CASH,
            )
            trades = _attr(resp, "trade_list") or []
            if not trades:
                return 0.0
            total_qty   = 0
            total_value = 0.0
            for tr in trades:
                px = float(_attr(tr, "price")    or 0)
                qy = int(  _attr(tr, "quantity") or 0)
                if px > 0 and qy > 0:
                    total_value += px * qy
                    total_qty   += qy
            return round(total_value / total_qty, 2) if total_qty > 0 else 0.0
        except Exception as e:
            logger.error(f"get_avg_fill_price({order_id}): {e}")
            return 0.0

    def get_order_status(self, order_id: str) -> tuple[str, int, float]:
        """One call: (status, filled_quantity, average_fill_price). ("UNKNOWN",0,0) on error."""
        try:
            resp   = self._g.get_order_status(groww_order_id=order_id, segment=_CASH)
            status = str(_attr(resp, "order_status") or "UNKNOWN").upper()
            filled = int(_attr(resp, "filled_quantity") or 0)
            avg_px = float(_attr(resp, "average_fill_price") or 0)
            logger.debug(f"get_order_status({order_id}): {status} filled={filled} avg={avg_px}")
            return status, filled, avg_px
        except Exception as e:
            logger.warning(f"get_order_status({order_id}): {e}")
            return "UNKNOWN", 0, 0.0

    def cancel_order(self, order_id: str) -> bool:
        logger.info(f"cancel_order: attempting to cancel order_id={order_id}")
        try:
            resp   = self._g.cancel_order(groww_order_id=order_id, segment=_CASH)
            status = str(_attr(resp, "order_status") or "")
            ok     = "CANCEL" in status.upper() or bool(_attr(resp, "groww_order_id"))
            if ok:
                logger.info(f"cancel_order({order_id}): OK status={status!r}")
            else:
                logger.warning(f"cancel_order({order_id}): uncertain -- status={status!r} resp={resp}")
            return ok
        except Exception as e:
            logger.error(f"cancel_order({order_id}): {e}")
            return False


# ===============================================================================
#  LIVE TRADER ORCHESTRATOR
#
#  Groww Cloud "Daily" deployment runs this script as a LONG-RUNNING PROCESS
#  from Start time (09:15) to End time (15:30). The while-loop polls every
#  60 seconds -- exactly right for this deployment model.
# ===============================================================================

def _ema(values: np.ndarray, period: int) -> np.ndarray:
    return pd.Series(values).ewm(span=period, adjust=False).mean().values


class LiveTrader:
    """Main trading loop. 09:15-09:44 wait; 09:45 build OR; 09:45-15:14 poll LTP/15s + emit candle/60s."""

    def __init__(self, broker: GrowwBroker, stock_states: dict[str, StockState]):
        self.broker       = broker
        self.states       = stock_states
        self._or_built    = False
        self._prev_ltps:  dict[str, float] = {}   # last LTP -> used as next candle open
        self._min_ltps:   dict[str, list]  = {}   # LTP samples in current minute
        self._last_min:   str              = ""   # last minute we emitted a candle for
        self._last_pnl_log_min: str        = ""   # last 15-min slot we logged P&L for
        self._trades_today: int            = 0    # circuit-breaker counter
        self._replaying:  bool             = False  # True during late-start replay

    def run(self):
        # LATE_START_MODE is safe with live trading: self._replaying=True blocks
        # all order placement during the historical replay. Orders only fire
        # after replay completes and _replaying is set back to False.
        if LATE_START_MODE:
            logger.warning(
                "LATE_START_MODE=True -- replaying historical candles to rebuild "
                f"state. {'DRY RUN: no orders during replay or after.' if DRY_RUN else 'LIVE: no orders during replay; live orders fire after replay completes.'}"
            )

        if not self.broker.connect():
            logger.error("Broker connection failed -- aborting")
            return

        # Always start with a clean per-stock state object -- _load_state will
        # then overlay any persisted state on top.
        for s in self.states.values():
            s.reset()
        self._or_built     = False
        self._prev_ltps    = {}
        self._min_ltps     = {sym: [] for sym in self.states}
        self._last_min     = ""
        self._last_pnl_log_min = ""
        self._trades_today = 0

        # -- Crash recovery: load state if we restarted mid-session ------------
        restored = self._load_state()
        if restored:
            self._reconcile_with_broker()
            # warmup_candles_1m is NOT persisted (would bloat state file).
            # Re-fetch it so MACD stays continuous after restart.
            self._rehydrate_after_restart()

        logger.info(
            f"Trading {len(self.states)} stocks | DRY_RUN={DRY_RUN} | "
            f"max_trades={MAX_TRADES_PER_DAY} | min_macd_bars={MIN_MACD_BARS} | "
            f"restored_state={restored}"
        )

        while True:
            now      = datetime.now()
            time_str = now.strftime("%H:%M")

            if time_str < MARKET_OPEN:
                logger.info(f"Waiting for market open ({time_str})...")
                time.sleep(30)
                continue

            if time_str >= MARKET_CLOSE:
                logger.info("Market closed -- emergency exit all positions")
                self._emergency_exit_all()
                break

            self._tick(now)
            time.sleep(LTP_POLL_SECS)   # poll LTP every LTP_POLL_SECS seconds

        self._print_summary()

    # -- Per-tick logic (runs every 15 s) ---------------------------------------

    def _tick(self, now: datetime):
        today    = now.strftime("%Y-%m-%d")
        time_str = now.strftime("%H:%M")

        # -- Phase 1: build OR once after the opening range period ends --------
        if not self._or_built:
            if time_str < OR_END:
                return   # still in OR period -- nothing to do
            self._build_or(today, OR_END)
            return

        # -- Phase 2: poll LTP for all active stocks -- ONE API call ------------
        active = [s for s, st in self.states.items() if st.state != State.DONE]
        if not active:
            logger.info("_tick: all stocks DONE -- nothing to poll")
            return

        logger.debug(f"_tick: polling LTP for {len(active)} active stock(s) at {time_str}")
        ltp_map = self.broker.get_ltp_batch(active)

        missing_ltp = []
        for sym in active:
            ltp = ltp_map.get(sym, 0.0)
            if ltp > 0:
                self._min_ltps.setdefault(sym, []).append(ltp)
            else:
                missing_ltp.append(sym)
        if missing_ltp:
            logger.warning(f"_tick: {len(missing_ltp)} stocks returned 0/no LTP: {missing_ltp}")

        # -- 15-min P&L heartbeat for open positions ---------------------------
        # Fires at :00, :15, :30, :45. Once per slot, even if _tick runs
        # multiple times during the same minute.
        cur_min = now.strftime("%H:%M")
        if cur_min[-2:] in ("00", "15", "30", "45") and cur_min != self._last_pnl_log_min:
            self._log_open_pnl(ltp_map)
            self._last_pnl_log_min = cur_min

        # -- Phase 3: at the start of each new minute, emit synthetic candles --
        # Emit a candle for the minute that just COMPLETED (_last_min), using
        # samples that were accumulated during that minute.  cur_min is then
        # recorded so we know when the next minute boundary arrives.
        if cur_min != self._last_min:
            if self._last_min:                        # skip the very first tick
                self._emit_candles(today, self._last_min)
            self._last_min = cur_min

    # -- OR build (called once at or_end) ---------------------------------------

    def _build_or(self, today: str, or_end: str):
        """Called once when OR ends. Loads warmup + today candles to seed MACD & build OR."""
        # -- Multi-day fetch: warmup (prior days) + today ----------------------
        # Backtest computes MACD on a continuous close series spanning the full
        # data history, so EMA(26) is fully stable by the time today starts.
        # We replicate that by fetching MACD_WARMUP_DAYS of prior 1-min candles
        # in addition to today's data, all in one API call per stock (within
        # Groww's 7-day historical window limit).
        today_dt        = datetime.strptime(today, "%Y-%m-%d")
        warmup_start_dt = today_dt - timedelta(days=MACD_WARMUP_DAYS)
        from_dt         = warmup_start_dt.strftime("%Y-%m-%d 09:15:00")

        if LATE_START_MODE:
            to_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            logger.warning(
                f"! LATE_START_MODE active -- replaying {today} 09:15 -> {to_dt[11:]} "
                f"with {MACD_WARMUP_DAYS}d MACD warmup ({from_dt[:10]} -> today). "
                f"No real orders will be fired during replay."
            )
            self._replaying = True
        else:
            to_dt = f"{today} {or_end}:59"
            logger.info(
                f"OR period ended ({or_end}) -- loading historical candles "
                f"({MACD_WARMUP_DAYS}d MACD warmup + today)..."
            )

        loaded_ok       = 0
        replay_signals  = []   # collected during replay for end-of-replay summary
        for symbol, state in self.states.items():
            all_candles = self.broker.get_candles_historical(symbol, from_dt, to_dt)
            if not all_candles:
                logger.warning(f"{symbol}: no candles returned at all -- marking DONE")
                state.state = State.DONE
                continue

            # Split: warmup days vs today. Warmup goes only to MACD,
            # today's candles drive the state machine.
            warmup_candles = [c for c in all_candles if c["datetime"][:10] != today]
            today_candles  = [c for c in all_candles if c["datetime"][:10] == today]

            if not today_candles:
                logger.warning(
                    f"{symbol}: warmup loaded ({len(warmup_candles)} candles) "
                    f"but no today candles -- DONE"
                )
                state.state = State.DONE
                continue

            state.warmup_candles_1m = warmup_candles
            logger.info(
                f"{symbol}: {len(warmup_candles)} warmup + "
                f"{len(today_candles)} today candles loaded"
            )

            # Feed only today's candles through the state machine.
            # MACD nuance: live mode refreshes MACD before each on_candle call
            # when state is WAITING_FIB / WAITING_ENTRY. We mirror that here so
            # replay-mode MACD filtering is identical to what would have run
            # live. EMA continuity: pass warmup + today_so_far each time.
            for i, c in enumerate(today_candles):
                if (LATE_START_MODE
                        and state.state in (State.WAITING_FIB, State.WAITING_ENTRY)):
                    self._update_macd_from_1m(
                        state, warmup_candles + today_candles[:i + 1]
                    )
                action = state.on_candle(c)
                if not LATE_START_MODE:
                    continue   # normal path: OR not yet built, no actions expected
                # Replay path: log signals without placing real orders
                if action and action.startswith("enter_"):
                    tr = state.trade
                    msg = (
                        f"REPLAY {symbol}: would enter {tr.direction} "
                        f"@ {tr.entry_price:.2f} qty={tr.quantity} "
                        f"sl={tr.stop_loss:.2f} tgt={tr.target_price:.2f} "
                        f"at {c['datetime'][11:16]}"
                    )
                    logger.info(msg)
                    replay_signals.append(msg)
                elif action == "exit_trade":
                    msg = f"REPLAY {symbol}: would exit at {c['datetime'][11:16]} ({state.state.name})"
                    logger.info(msg)
                    replay_signals.append(msg)

            # Final MACD computation uses the FULL multi-day series so EMAs
            # are continuous and stable for live polling that follows.
            self._update_macd_from_1m(state, all_candles)
            self._prev_ltps[symbol] = today_candles[-1]["close"]
            self._min_ltps[symbol]  = []
            loaded_ok += 1
            if state.or_high > 0:
                logger.info(
                    f"{symbol} OR loaded: H={state.or_high:.2f} L={state.or_low:.2f} "
                    f"range={state.or_high - state.or_low:.2f} "
                    f"({len(today_candles)} today candles, end_state={state.state.name})"
                )

        logger.info(f"Historical candles loaded for {loaded_ok}/{len(self.states)} stocks")

        # -- Replay summary (LATE_START_MODE only) ----------------------------
        if LATE_START_MODE:
            self._replaying = False
            logger.warning("=" * 60)
            logger.warning(f"REPLAY COMPLETE -- {len(replay_signals)} signal(s) fired during replay:")
            for sig in replay_signals:
                logger.warning(f"  {sig}")
            logger.warning("=" * 60)
            # End-of-replay state breakdown
            self._log_status()
            # Set _last_min to the most recent minute so live polling resumes correctly
            self._last_min = datetime.now().strftime("%H:%M")
            self._or_built = True
            self._save_state()
            return

        # -- Normal path: synthesize trigger candle at or_end using fresh LTP --
        logger.info("Fetching LTP batch for OR trigger candles...")
        ltp_map = self.broker.get_ltp_batch(list(self.states.keys()))
        for symbol, state in self.states.items():
            if state.state == State.DONE:
                continue
            ltp = ltp_map.get(symbol) or self._prev_ltps.get(symbol, 0)
            if ltp <= 0:
                logger.warning(f"{symbol}: no LTP for trigger candle -- skipping")
                continue
            logger.debug(f"{symbol} trigger candle: LTP={ltp:.2f}")
            trigger = {
                "datetime": f"{today} {or_end}:00",
                "open":  ltp, "high": ltp, "low": ltp, "close": ltp,
                "volume": 0,
            }
            action = state.on_candle(trigger)
            self._prev_ltps[symbol] = ltp
            if action and action.startswith("enter_"):
                logger.info(f"{symbol} IMMEDIATE breakout/entry on OR trigger candle!")
                self._handle_entry(state)
            elif action == "exit_trade":
                self._handle_exit(state)

        active_n = sum(1 for s in self.states.values() if s.state != State.DONE)
        logger.info(f"OR built -- {active_n}/{len(self.states)} stocks active")
        self._or_built  = True
        self._last_min  = or_end
        self._save_state()                      # checkpoint: OR build complete

    # -- Emit synthetic candles from accumulated LTP samples -------------------

    def _emit_candles(self, today: str, cur_min: str):
        """Build a 1-min synthetic candle from LTP samples and feed each stock's state machine.
        Refreshes MACD in parallel for near-entry stocks first."""
        # -- Parallel MACD refresh for near-entry stocks -----------------------
        # Each get_candles_historical call sleeps 1.5s + HTTP round-trip.
        # Running them concurrently keeps total delay ~= one call instead of Nx.
        near_entry = [
            sym for sym, st in self.states.items()
            if st.state in (State.WAITING_FIB, State.WAITING_ENTRY)
        ]
        if near_entry:
            from_dt = f"{today} 09:15:00"
            to_dt   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # Stagger requests 0.5s apart so all threads don't hit the API
            # simultaneously -- thundering herd causes timeouts/disconnects.
            def _fetch(idx_sym):
                idx, sym = idx_sym
                return sym, self.broker.get_candles_historical(
                    sym, from_dt, to_dt, _stagger_s=idx * 1.0
                )

            with ThreadPoolExecutor(max_workers=len(near_entry)) as pool:
                futures = {
                    pool.submit(_fetch, (idx, sym)): sym
                    for idx, sym in enumerate(near_entry)
                }
                for fut in as_completed(futures):
                    sym, hist = fut.result()
                    st = self.states[sym]
                    if hist:
                        self._update_macd_from_1m(st, st.warmup_candles_1m + hist)
                    else:
                        logger.warning(f"{sym}: MACD refresh returned no candles -- using stale MACD")

        # -- Emit synthetic candles --------------------------------------------
        for symbol, state in self.states.items():
            if state.state == State.DONE:
                continue

            samples = self._min_ltps.get(symbol) or []
            if not samples:
                if state.state == State.IN_TRADE:
                    # Never skip a candle while in a live position -- synthesize a
                    # flat candle from prev_ltp so SL/target checks keep running.
                    prev_ltp = self._prev_ltps.get(symbol)
                    if prev_ltp:
                        samples = [prev_ltp]
                        logger.warning(
                            f"{symbol}: no LTP samples while IN_TRADE -- "
                            f"synthesising flat candle from prev={prev_ltp:.2f}"
                        )
                    else:
                        logger.warning(f"{symbol}: IN_TRADE but no LTP or prev -- candle skipped")
                        continue
                else:
                    logger.debug(f"{symbol}: no LTP samples for minute {cur_min} -- candle skipped")
                    continue

            prev   = self._prev_ltps.get(symbol, samples[0])
            candle = {
                "datetime": f"{today} {cur_min}:00",
                "open":  prev,
                "high":  max(samples + [prev]),
                "low":   min(samples + [prev]),
                "close": samples[-1],
                "volume": 0,
            }
            logger.debug(
                f"{symbol} synthetic candle {cur_min}: "
                f"O={candle['open']:.2f} H={candle['high']:.2f} "
                f"L={candle['low']:.2f} C={candle['close']:.2f} "
                f"({len(samples)} LTP samples) state={state.state.name}"
            )

            # -- OCO is now server-side via create_smart_order: when SL trigger
            # fires or target limit fills, Groww auto-cancels the other leg.
            # The candle-poll SL/TP check in _handle_in_trade still runs as a
            # backup; if Groww's OCO already fired, get_live_qty in
            # _handle_exit returns 0 and skips the redundant market exit.

            action = state.on_candle(candle)
            self._prev_ltps[symbol] = candle["close"]
            self._min_ltps[symbol]  = []   # reset for next minute

            if action and action.startswith("enter_"):
                self._handle_entry(state)
            elif action == "exit_trade":
                self._handle_exit(state)

        self._log_status()
        self._save_state()                      # minute-boundary checkpoint

    # -- Order handling ---------------------------------------------------------

    def _handle_entry(self, state: StockState):
        trade     = state.trade
        symbol    = state.symbol
        direction = trade.direction
        qty       = trade.quantity
        buy_sell  = "buy" if direction == "LONG" else "sell"

        if self._replaying:
            logger.debug(f"{symbol} _handle_entry called during replay -- skipping order placement")
            return

        if DRY_RUN:
            logger.info(
                f"[DRY RUN] {symbol} {buy_sell.upper()} {qty} "
                f"@ ~{trade.entry_price:.2f} | SL {trade.stop_loss:.2f} "
                f"| TGT {trade.target_price:.2f}"
            )
            trade.entry_order_id = "DRY_ENTRY"
            self._trades_today += 1
            self._save_state()
            return

        # -- Pre-flight: daily loss / trade-count circuit breaker --------------
        if self._trades_today >= MAX_TRADES_PER_DAY:
            logger.warning(f"{symbol} entry SKIPPED -- MAX_TRADES_PER_DAY ({MAX_TRADES_PER_DAY}) reached")
            state.trade = None
            state._done(f"MAX_TRADES_PER_DAY={MAX_TRADES_PER_DAY} reached")
            return

        # -- Pre-flight: margin check ------------------------------------------
        required_margin = trade.entry_price * qty * MIS_LEVERAGE_DIVISOR
        avail_cash      = self.broker.get_available_cash()
        if avail_cash > 0 and avail_cash < required_margin:
            logger.error(
                f"{symbol} entry SKIPPED -- insufficient margin "
                f"(need ~=Rs{required_margin:.0f}, have Rs{avail_cash:.0f})"
            )
            state.trade = None
            state._done(f"insufficient margin (need Rs{required_margin:.0f}, have Rs{avail_cash:.0f})")
            return

        # Snapshot pre-entry position for the fill-confirmation fallback inside
        # _wait_for_fill (position-delta when status API misbehaves).
        pre_qty = self.broker.get_live_qty(symbol)
        if pre_qty is None:
            logger.warning(f"{symbol} could not read pre-entry position -- assuming 0")
            pre_qty = 0

        # -- Place MARKET entry -- guaranteed fill, accept slippage. ------------
        # Actual fill price comes from status poll; SL/target shift around it below.
        entry_ref = f"E{symbol[:6]}{uuid.uuid4().hex[:6].upper()}"
        result    = self.broker.place_market_order(
            symbol, buy_sell, qty, ref_id=entry_ref,
        )
        if not result.success:
            logger.error(f"{symbol} entry FAILED: {result.message}")
            state.trade = None
            state._done(f"entry order rejected: {result.message[:80]}")
            return
        trade.entry_order_id = result.order_id
        self._trades_today  += 1
        self._save_state()                      # checkpoint: entry placed
        logger.info(
            f"{symbol} MARKET entry placed: {result.order_id} "
            f"(ref={entry_ref}, pre_qty={pre_qty})"
        )

        # -- Wait for fill via ORDER STATUS (terminal-state aware) -------------
        # get_order_status returns status + filled_quantity + average_fill_price
        # in one call; rejections detected in ~1s instead of burning the full wait.
        filled_qty, avg_fill_px = self._wait_for_fill(
            symbol, direction, expected_qty=qty,
            pre_qty=pre_qty, order_id=result.order_id,
            max_wait_s=ENTRY_FILL_WAIT_S,
        )
        if filled_qty == 0:
            logger.warning(f"{symbol} MARKET entry did not fill within {ENTRY_FILL_WAIT_S}s -- abandoning")
            state.trade = None
            state._done(f"entry did not fill in {ENTRY_FILL_WAIT_S}s")
            self._save_state()
            return
        if filled_qty < qty:
            logger.warning(
                f"{symbol} partial fill: {filled_qty}/{qty} -- sizing SL/target to actual fill"
            )
            trade.quantity = filled_qty
            qty            = filled_qty

        # -- Fill price: status response already had it; trade-list is fallback --
        if avg_fill_px <= 0:
            avg_fill_px = self.broker.get_avg_fill_price(result.order_id)
        logger.info(f"{symbol} fill price: avg_fill_px={avg_fill_px:.2f}")

        # -- Reconcile planned vs actual fill price ----------------------------
        # Better fill (favourable slippage) -> keep planned fib SL/target, R:R expands.
        # Worse fill -> shift around fill, preserving risk_ps and 1.5R.
        planned_entry = trade.entry_price
        planned_sl    = trade.stop_loss
        planned_tgt   = trade.target_price
        risk_ps       = abs(planned_entry - planned_sl)
        target_r      = state.cfg.target_r
        if avg_fill_px > 0 and risk_ps > 0:
            if direction == "LONG":
                better_fill       = avg_fill_px < planned_entry
                planned_sl_intact = avg_fill_px > planned_sl
            else:
                better_fill       = avg_fill_px > planned_entry
                planned_sl_intact = avg_fill_px < planned_sl
            if better_fill and planned_sl_intact:
                logger.info(
                    f"{symbol} BETTER fill: planned={planned_entry:.2f}->actual={avg_fill_px:.2f} "
                    f"| keep sl={planned_sl:.2f} tgt={planned_tgt:.2f}"
                )
                trade.entry_price = avg_fill_px
            else:
                if direction == "LONG":
                    new_sl  = round(avg_fill_px - risk_ps, 2)
                    new_tgt = round(avg_fill_px + risk_ps * target_r, 2)
                else:
                    new_sl  = round(avg_fill_px + risk_ps, 2)
                    new_tgt = round(avg_fill_px - risk_ps * target_r, 2)
                logger.info(
                    f"{symbol} WORSE fill: planned={planned_entry:.2f}->actual={avg_fill_px:.2f} "
                    f"| sl {planned_sl:.2f}->{new_sl:.2f} tgt {planned_tgt:.2f}->{new_tgt:.2f} (risk={risk_ps:.2f})"
                )
                trade.entry_price  = avg_fill_px
                trade.stop_loss    = new_sl
                trade.target_price = new_tgt
        else:
            logger.warning(
                f"{symbol} no fill price -- planned entry={planned_entry:.2f} sl={planned_sl:.2f} tgt={planned_tgt:.2f}"
            )

        # -- Place OCO (SL_M + LIMIT target). Attempt 2 uses a fresh ref since
        # Groww stores rejected refs (and rejects duplicate retries).
        logger.info(
            f"{symbol} OCO arming: {direction} qty={qty} entry={trade.entry_price:.2f} "
            f"sl={trade.stop_loss:.2f} tgt={trade.target_price:.2f} entry_id={trade.entry_order_id}"
        )
        oco_ref = f"O{symbol[:6]}{uuid.uuid4().hex[:6].upper()}"
        for attempt in (1, 2):
            ref = oco_ref if attempt == 1 else f"O{symbol[:6]}{uuid.uuid4().hex[:6].upper()}"
            oco_res = self.broker.place_oco_order(
                symbol, direction, qty,
                sl_trigger   = trade.stop_loss,
                target_price = trade.target_price,
                ref_id       = ref,
            )
            if oco_res.success:
                trade.oco_order_id = oco_res.order_id
                self._save_state()
                logger.info(f"{symbol} OCO ARMED OK (attempt {attempt}): {oco_res.order_id} ref={ref}")
                return
            logger.warning(f"{symbol} OCO attempt {attempt}/2 FAILED: {oco_res.message} (ref={ref})")
            time.sleep(0.5)

        # Last-resort: OCO failed twice -> market-exit to avoid naked exposure.
        exit_side = "sell" if direction == "LONG" else "buy"
        logger.error(
            f"{symbol} OCO PLACEMENT FAILED TWICE -- emergency-exiting "
            f"(entry_id={trade.entry_order_id} qty={qty} {direction})"
        )
        emer_ref      = f"X{symbol[:6]}{uuid.uuid4().hex[:6].upper()}"
        emergency_res = self.broker.place_market_order(symbol, exit_side, qty, ref_id=emer_ref)
        if emergency_res.success:
            logger.warning(f"{symbol} emergency exit placed: {emergency_res.order_id} side={exit_side} qty={qty}")
        else:
            logger.error(
                f"{symbol} EMERGENCY EXIT ALSO FAILED -- MANUAL ACTION REQUIRED! "
                f"side={exit_side} qty={qty} | {emergency_res.message}"
            )
        state.state = State.DONE
        self._save_state()

    def _wait_for_fill(self, symbol: str, direction: str,
                       expected_qty: int, pre_qty: int,
                       order_id: str,
                       max_wait_s: float) -> tuple[int, float]:
        """Poll get_order_status (terminal-aware) until fill / dead state / timeout.
        Position delta is fallback when status keeps erroring. On timeout cancel +
        recheck status AND position so a cancel-race fill isn't lost.
        Returns (filled_qty, avg_fill_price); avg=0.0 means unknown."""
        deadline      = time.time() + max_wait_s
        status_errors = 0
        last_status   = ""
        while time.time() < deadline:
            status, filled, avg_px = self.broker.get_order_status(order_id)
            last_status = status
            if status in _FILLED_STATES:
                f = filled or expected_qty   # some fills report status before qty populates
                logger.info(f"{symbol} fill confirmed: {status} filled={f} avg={avg_px:.2f}")
                return min(f, expected_qty), avg_px
            if status in _DEAD_STATES:
                logger.warning(f"{symbol} order {order_id} terminal: {status} (filled={filled})")
                return min(filled, expected_qty), avg_px
            if status == "UNKNOWN":
                status_errors += 1
                if status_errors >= 3:
                    cur = self.broker.get_live_qty(symbol)
                    if cur is not None:
                        delta = (cur - pre_qty) if direction == "LONG" else (pre_qty - cur)
                        if delta >= expected_qty:
                            logger.info(f"{symbol} fill confirmed via position delta: {delta}")
                            return expected_qty, 0.0
            time.sleep(1.0)

        # Timeout -- cancel, recheck status + position (cancel-race safe).
        logger.info(f"{symbol} no terminal status within {max_wait_s}s (last={last_status!r}) -- cancelling")
        self.broker.cancel_order(order_id)
        time.sleep(1.0)
        status, filled, avg_px = self.broker.get_order_status(order_id)
        if status in _FILLED_STATES or filled > 0:
            f = filled or (expected_qty if status in _FILLED_STATES else 0)
            logger.info(f"{symbol} post-cancel status: {status} filled={f} avg={avg_px:.2f}")
            return min(f, expected_qty), avg_px
        cur = self.broker.get_live_qty(symbol)
        if cur is None:
            logger.warning(f"{symbol} could not verify after cancel -- assuming no fill")
            return 0, 0.0
        delta = (cur - pre_qty) if direction == "LONG" else (pre_qty - cur)
        filled = max(0, min(delta, expected_qty))
        logger.info(f"{symbol} post-cancel position read: filled={filled} (pre={pre_qty} cur={cur})")
        return filled, 0.0

    def _handle_exit(self, state: StockState):
        if not state.trade:
            return
        symbol    = state.symbol
        direction = state.trade.direction
        qty       = state.trade.quantity
        exit_side = "sell" if direction == "LONG" else "buy"

        if self._replaying:
            logger.debug(f"{symbol} _handle_exit called during replay -- skipping order placement")
            return

        if DRY_RUN:
            logger.info(f"[DRY RUN] {symbol} EXIT {exit_side.upper()} {qty}")
            return

        # Verify broker position before exit -- guard against SL/manual close -> naked position.
        live_qty = self.broker.get_live_qty(symbol)
        if live_qty == 0:
            logger.info(f"{symbol}: position already flat (SL fired or manual close) -- skipping exit")
            return
        if live_qty is None:
            logger.warning(
                f"{symbol}: could not verify live position -- using state.trade.quantity={qty}"
            )
            exit_qty = qty
        else:
            # Sanity-check: our recorded direction vs broker sign
            expected_sign = 1 if direction == "LONG" else -1
            if live_qty * expected_sign < 0:
                logger.error(
                    f"{symbol}: broker position SIGN MISMATCH "
                    f"(live={live_qty:+d}, expected {direction}) -- refusing exit"
                )
                return
            # Use broker's actual qty -- handles partial fills correctly
            exit_qty = abs(live_qty)
            if exit_qty != qty:
                logger.warning(
                    f"{symbol}: broker qty ({exit_qty}) != state qty ({qty}) -- "
                    f"using broker qty for exit"
                )

        # Cancel pending OCO smart order before manual market exit. Cancelling
        # an already-fired OCO is harmless.
        if state.trade.oco_order_id:
            self.broker.cancel_smart_order(state.trade.oco_order_id)

        # Stable exit ref so a transient retry doesn't double-exit.
        # Attempt 2 uses a fresh ref in case attempt 1 was stored by Groww despite failure.
        exit_ref = f"X{symbol[:6]}{uuid.uuid4().hex[:6].upper()}"
        for attempt in (1, 2):
            ref    = exit_ref if attempt == 1 else f"X{symbol[:6]}{uuid.uuid4().hex[:6].upper()}"
            result = self.broker.place_market_order(
                symbol, exit_side, exit_qty, ref_id=ref
            )
            if result.success:
                logger.info(f"{symbol} exit placed (attempt {attempt}): {result.order_id} (qty={exit_qty})")
                break
            logger.error(
                f"{symbol} EXIT FAILED (attempt {attempt}/2): {result.message}"
                + (" -- MANUAL ACTION REQUIRED!" if attempt == 2 else " -- retrying...")
            )
            if attempt == 1:
                time.sleep(1.0)
        self._save_state()                      # checkpoint: exit attempted

    def _emergency_exit_all(self):
        """Force-exit all open positions. Reads broker state (not in-memory) so manual closes are handled."""
        if DRY_RUN:
            in_trade = [s for s in self.states.values() if s.state == State.IN_TRADE]
            for st in in_trade:
                t = st.trade
                exit_side = "SELL" if t.direction == "LONG" else "BUY"
                logger.info(f"[DRY RUN] Emergency exit: {st.symbol} {exit_side} {t.quantity}")
                st.state = State.DONE
            return

        # Step 1 -- cancel pending OCO smart orders
        for st in self.states.values():
            if st.state == State.IN_TRADE and st.trade:
                if st.trade.oco_order_id:
                    self.broker.cancel_smart_order(st.trade.oco_order_id)

        # Step 2 -- read actual open positions from broker
        open_positions = self.broker.get_all_mis_positions()
        if not open_positions:
            logger.info("Emergency exit: no open MIS positions found on broker")
        else:
            logger.warning(f"Emergency exit: {len(open_positions)} open position(s) found")

        # Step 3 -- market-exit each open position (stable ref -> idempotent)
        for pos in open_positions:
            sym     = pos["symbol"]
            net_qty = pos["net_qty"]
            side    = "sell" if net_qty > 0 else "buy"
            qty     = abs(net_qty)
            ref     = f"EOD{sym[:5]}{uuid.uuid4().hex[:6].upper()}"
            logger.warning(f"Emergency exit: {sym} {side.upper()} {qty} (ref={ref})")
            res = self.broker.place_market_order(sym, side, qty, ref_id=ref)
            if res.success:
                logger.info(f"{sym} emergency exit placed: {res.order_id}")
            else:
                logger.error(f"{sym} EMERGENCY EXIT FAILED: {res.message} -- MANUAL ACTION REQUIRED!")

        # Mark all in-trade states as done
        for st in self.states.values():
            if st.state == State.IN_TRADE:
                st.state = State.DONE
        self._save_state()

    # -- MACD (computed from resampled 1-min data -- no extra API call) ---------

    def _update_macd_from_1m(self, state: StockState, candles_1m: list[dict]):
        """Resample 1-min candles to 5-min bars and compute MACD (no extra API call)."""
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

            # Full datetime ("YYYY-MM-DD HH:MM:SS") so multi-day series can be
            # filtered correctly in _check_macd. Previously time_str was HH:MM:SS
            # only, which would collide across days when warmup data is included.
            result = pd.DataFrame({
                "time_str":  df_5m.index.strftime("%Y-%m-%d %H:%M:%S"),
                "macd":      macd_line,
                "signal":    signal_line,
                "histogram": macd_line - signal_line,
            })
            state.macd_5m = result
            logger.debug(
                f"{state.symbol} MACD updated: {len(closes)} 5-min bars | "
                f"latest macd={macd_line[-1]:.4f} signal={signal_line[-1]:.4f} "
                f"hist={(macd_line - signal_line)[-1]:.4f}"
            )
        except Exception as e:
            logger.warning(f"MACD resample failed for {state.symbol}: {e}")

    # -- Status logging --------------------------------------------------------

    def _log_status(self):
        t_str    = datetime.now().strftime("%H:%M:%S")
        by_state: dict[str, list] = {}
        for s in self.states.values():
            # Split DONE into TRADED (we entered today) vs NO_TRADE (setup
            # never materialised, got blocked, or invalidated before entry).
            if s.state == State.DONE:
                bucket = "TRADED" if s.trade is not None else "NO_TRADE"
            else:
                bucket = s.state.name
            by_state.setdefault(bucket, []).append(s.symbol)

        # Build summary line: each bucket -> count (with symbols for short lists)
        parts = []
        order = [
            "WAITING_OR", "WAITING_BREAKOUT", "WAITING_SWING",
            "WAITING_FIB", "WAITING_ENTRY", "IN_TRADE", "TRADED", "NO_TRADE",
        ]
        for name in order:
            syms = by_state.get(name, [])
            if not syms:
                continue
            if len(syms) <= 4:
                parts.append(f"{name}={len(syms)}({','.join(syms)})")
            else:
                parts.append(f"{name}={len(syms)}")
        logger.info(f"[{t_str}] STATUS -- {' | '.join(parts)}")

        # Detailed line for every open trade
        for s in self.states.values():
            if s.state == State.IN_TRADE and s.trade:
                t = s.trade
                logger.info(
                    f"  > {s.symbol} {t.direction} {t.quantity}x "
                    f"entry={t.entry_price:.2f} sl={t.stop_loss:.2f} "
                    f"tgt={t.target_price:.2f} "
                    f"oco_id={t.oco_order_id or 'none'}"
                )

    def _log_open_pnl(self, ltp_map: dict[str, float]):
        """Log unrealized P&L per open position using this tick's LTP (entry_price is planned fib)."""
        in_trade = [s for s in self.states.values()
                    if s.state == State.IN_TRADE and s.trade]
        if not in_trade:
            logger.info(f"[{datetime.now():%H:%M:%S}] P&L heartbeat -- no open positions")
            return

        total = 0.0
        lines = []
        missing = []
        for s in in_trade:
            t   = s.trade
            ltp = ltp_map.get(s.symbol, 0.0) or self._prev_ltps.get(s.symbol, 0.0)
            if ltp <= 0:
                missing.append(s.symbol)
                continue
            pnl = (ltp - t.entry_price) * t.quantity if t.direction == "LONG" \
                  else (t.entry_price - ltp) * t.quantity
            total += pnl
            lines.append(
                f"  > {s.symbol} {t.direction} {t.quantity}x "
                f"entry={t.entry_price:.2f} ltp={ltp:.2f} "
                f"pnl=Rs{pnl:+,.0f}"
            )

        logger.info(
            f"[{datetime.now():%H:%M:%S}] P&L heartbeat -- "
            f"{len(in_trade)} open | TOTAL Rs{total:+,.0f}"
        )
        for line in lines:
            logger.info(line)
        if missing:
            logger.warning(f"P&L heartbeat: no LTP for {missing} -- skipped")

    # -- Persistence (crash recovery) ------------------------------------------

    def _state_file_path(self) -> str:
        """Path to today's state file. New file each calendar day."""
        return os.path.join(STATE_DIR, f"groww_state_{datetime.now():%Y-%m-%d}.json")

    def _save_state(self):
        """Snapshot state to disk atomically (write tmp + rename). ~5KB JSON dump, no API calls."""
        path     = self._state_file_path()
        tmp_path = path + ".tmp"
        snapshot = {
            "date":         datetime.now().strftime("%Y-%m-%d"),
            "saved_at":     datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "or_built":     self._or_built,
            "last_min":     self._last_min,
            "trades_today": self._trades_today,
            "prev_ltps":    self._prev_ltps,
            "stocks":       {},
        }
        for sym, s in self.states.items():
            entry = {
                "state":        s.state.name,
                "or_high":      s.or_high,
                "or_low":       s.or_low,
                "breakout_dir": s.breakout_dir,
                "swing_peak":   s.swing_peak,
                "swing_value":  s.swing_value,
                "swing_bars":   s.swing_bars,
                "no_trade_reason": s.no_trade_reason,
            }
            if s.setup is not None:
                entry["setup"] = {
                    "direction":   s.setup.direction,
                    "or_high":     s.setup.or_high,
                    "or_low":      s.setup.or_low,
                    "swing_value": s.setup.swing_value,
                    "fib_entry":   s.setup.fib_entry,
                    "stop_loss":   s.setup.stop_loss,
                }
            if s.trade is not None:
                entry["trade"] = {
                    "direction":      s.trade.direction,
                    "entry_price":    s.trade.entry_price,
                    "quantity":       s.trade.quantity,
                    "stop_loss":      s.trade.stop_loss,
                    "target_price":   s.trade.target_price,
                    "entry_time":     s.trade.entry_time,
                    "entry_order_id":  s.trade.entry_order_id,
                    "oco_order_id":    s.trade.oco_order_id,
                }
            snapshot["stocks"][sym] = entry

        try:
            with open(tmp_path, "w") as f:
                json.dump(snapshot, f, indent=2)
            os.replace(tmp_path, path)   # atomic rename
            logger.debug(f"_save_state: snapshot written to {path}")
        except Exception as e:
            logger.warning(f"_save_state failed: {e}")

    def _load_state(self) -> bool:
        """Load today's state file. Caller MUST follow with _reconcile_with_broker()."""
        path = self._state_file_path()
        if not os.path.exists(path):
            logger.info(f"_load_state: no state file at {path} -- fresh start")
            return False

        try:
            with open(path) as f:
                snap = json.load(f)
        except Exception as e:
            logger.warning(f"_load_state: corrupt state file ({e}) -- fresh start")
            return False

        today = datetime.now().strftime("%Y-%m-%d")
        if snap.get("date") != today:
            logger.info(f"_load_state: stale state file ({snap.get('date')} != {today}) -- fresh start")
            return False

        self._or_built     = bool(snap.get("or_built", False))
        self._last_min     = str(snap.get("last_min", ""))
        self._trades_today = int(snap.get("trades_today", 0))
        self._prev_ltps    = {k: float(v) for k, v in (snap.get("prev_ltps") or {}).items()}

        restored = 0
        for sym, entry in (snap.get("stocks") or {}).items():
            s = self.states.get(sym)
            if s is None:
                continue   # stock no longer in portfolio
            try:
                s.state        = State[entry["state"]]
                s.or_high      = float(entry.get("or_high", 0))
                s.or_low       = float(entry.get("or_low",  0))
                s.breakout_dir = entry.get("breakout_dir")
                s.swing_peak   = float(entry.get("swing_peak",  0))
                s.swing_value  = float(entry.get("swing_value", 0))
                s.swing_bars   = int(entry.get("swing_bars",   0))
                s.no_trade_reason = str(entry.get("no_trade_reason", ""))
                if entry.get("setup"):
                    su = entry["setup"]
                    s.setup = TradeSetup(
                        direction   = su["direction"],
                        or_high     = float(su["or_high"]),
                        or_low      = float(su["or_low"]),
                        swing_value = float(su["swing_value"]),
                        fib_entry   = float(su["fib_entry"]),
                        stop_loss   = float(su["stop_loss"]),
                    )
                if entry.get("trade"):
                    tr = entry["trade"]
                    s.trade = OpenTrade(
                        direction       = tr["direction"],
                        entry_price     = float(tr["entry_price"]),
                        quantity        = int(tr["quantity"]),
                        stop_loss       = float(tr["stop_loss"]),
                        target_price    = float(tr["target_price"]),
                        entry_time      = tr.get("entry_time", ""),
                        entry_order_id  = tr.get("entry_order_id", ""),
                        oco_order_id    = tr.get("oco_order_id", ""),
                    )
                restored += 1
            except Exception as e:
                logger.warning(f"_load_state: failed to restore {sym}: {e}")

        logger.info(
            f"_load_state: restored {restored}/{len(self.states)} stocks from {path} | "
            f"or_built={self._or_built} trades_today={self._trades_today}"
        )
        return True

    def _reconcile_with_broker(self):
        """Reconcile in-memory IN_TRADE vs broker: flat->DONE, sign-mismatch->DONE, qty-diff->adopt broker."""
        if DRY_RUN:
            logger.info("_reconcile_with_broker: DRY_RUN -- skipping broker reconciliation")
            return

        in_trade_syms = [
            sym for sym, s in self.states.items()
            if s.state == State.IN_TRADE and s.trade is not None
        ]
        if not in_trade_syms:
            logger.info("_reconcile_with_broker: no IN_TRADE stocks to reconcile")
            return

        broker_positions = {p["symbol"]: p["net_qty"] for p in self.broker.get_all_mis_positions()}
        logger.info(
            f"_reconcile_with_broker: comparing {len(in_trade_syms)} stored IN_TRADE "
            f"vs {len(broker_positions)} broker MIS positions"
        )

        for sym in in_trade_syms:
            s        = self.states[sym]
            tr       = s.trade
            net_qty  = broker_positions.get(sym, 0)
            expected = tr.quantity if tr.direction == "LONG" else -tr.quantity

            if net_qty == 0:
                logger.warning(
                    f"  {sym}: state IN_TRADE but broker FLAT -- SL fired or manual close. Marking DONE."
                )
                s.state = State.DONE
                continue

            if (net_qty > 0) != (expected > 0):
                logger.error(
                    f"  {sym}: SIGN MISMATCH -- state {tr.direction} qty {tr.quantity}, "
                    f"broker net {net_qty:+d}. Manual review required. Marking DONE."
                )
                s.state = State.DONE
                continue

            if abs(net_qty) != abs(expected):
                logger.warning(
                    f"  {sym}: qty mismatch -- state {tr.quantity}, broker {abs(net_qty)}. "
                    f"Adopting broker qty."
                )
                tr.quantity = abs(net_qty)
            else:
                logger.info(f"  {sym}: state and broker agree ({tr.direction} {tr.quantity})")

        self._save_state()

    def _rehydrate_after_restart(self):
        """Re-fetch warmup data and re-seed MACD after _load_state (warmup is not persisted)."""
        today        = datetime.now().strftime("%Y-%m-%d")
        today_dt     = datetime.strptime(today, "%Y-%m-%d")
        warmup_start = (today_dt - timedelta(days=MACD_WARMUP_DAYS)).strftime("%Y-%m-%d 09:15:00")
        to_dt        = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        logger.info(
            f"_rehydrate_after_restart: refetching {MACD_WARMUP_DAYS}d MACD warmup "
            f"({warmup_start} -> {to_dt}) for restored stocks..."
        )
        n = 0
        for symbol, state in self.states.items():
            if state.state == State.DONE:
                continue
            candles = self.broker.get_candles_historical(symbol, warmup_start, to_dt)
            if not candles:
                logger.warning(f"_rehydrate_after_restart: {symbol} no candles -- MACD stale")
                continue
            state.warmup_candles_1m = [c for c in candles if c["datetime"][:10] != today]
            self._update_macd_from_1m(state, candles)
            n += 1
        logger.info(f"_rehydrate_after_restart: rehydrated {n} stocks")

    def _print_summary(self):
        logger.info("=" * 60)
        logger.info("END OF DAY SUMMARY")
        logger.info("=" * 60)
        traded = [s for s in self.states.values() if s.trade]
        logger.info(f"TRADED ({len(traded)}):")
        for s in traded:
            t = s.trade
            logger.info(
                f"  {s.symbol}: {t.direction} {t.quantity}x @ {t.entry_price:.2f} | "
                f"sl={t.stop_loss:.2f} tgt={t.target_price:.2f} | final_state={s.state.name}"
            )
        no_trade = [s for s in self.states.values()
                    if s.trade is None and s.state == State.DONE]
        logger.info(f"NO_TRADE ({len(no_trade)}):")
        for s in no_trade:
            reason = s.no_trade_reason or "unknown"
            logger.info(f"  {s.symbol}: {reason}")
        still_active = [s.symbol for s in self.states.values()
                        if s.state not in (State.DONE,)]
        if still_active:
            logger.info(f"Still active at shutdown: {still_active}")


# ===============================================================================
#  ENTRY POINT
# ===============================================================================

def main():
    print("=" * 60)
    print("  Fib-MACD Live Trader -- Groww Cloud")
    print("=" * 60)
    print(f"  Mode:         {'DRY RUN' if DRY_RUN else 'LIVE TRADING'}")
    print(f"  Stocks:       {len(PORTFOLIO)}")
    print(f"  Default risk: Rs {DEFAULT_RISK:,.0f}/trade")
    print(f"  Capital cap:  Rs {CAPITAL_PER_TRADE:,.0f}/position")
    print(f"  OR duration:  {OR_MINUTES} min")
    print(f"  Fib entry:    {FIB_ENTRY_PCT*100:.1f}%")
    print(f"  MACD:         {MACD_CONDITION}")
    print(f"  No new entry: {NO_NEW_ENTRY_TIME}")
    print(f"  Exit time:    {EXIT_TIME}")
    print("=" * 60)
    if not DRY_RUN:
        print("  *** LIVE MODE -- real orders will be placed! ***")

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
