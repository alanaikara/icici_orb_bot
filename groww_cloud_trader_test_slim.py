"""Fib-MACD ORB strategy — TEST/REPLAY runner (slim). DRY_RUN=True, LATE_START_MODE=True."""

# ── CONFIG ────────────────────────────────────────────────────────────────────
GROWW_API_KEY = "your_api_key"
GROWW_SECRET  = "your_secret_key"

DEFAULT_RISK         = 200
RISK_OVERRIDES       = {}
CAPITAL_PER_TRADE    = 10_000
MAX_TRADES_PER_DAY   = 10
MIS_LEVERAGE_DIVISOR = 0.20
SL_LIMIT_BUFFER      = 0.005
MIN_MACD_BARS        = 2
MACD_WARMUP_DAYS     = 5
STATE_DIR            = "/tmp"

OR_MINUTES     = 30
FIB_ENTRY_PCT  = 0.618
FIB_786        = 0.786
SL_BUFFER_PCT  = 0.001
SWING_CONFIRM  = 0.003
MACD_CONDITION = "macd_cross"
MACD_FAST, MACD_SLOW, MACD_SIGNAL = 12, 26, 9
TARGET_R       = 1.5
EXIT_TIME      = "15:14"
MAX_WAIT_BARS  = 60

DRY_RUN         = True   # FORCED TRUE
LATE_START_MODE = True   # FORCED TRUE — replay 09:15→now

PORTFOLIO = [
    ("SHRIRAMFIN","short_only",1.5),("ADANIENT","short_only",1.5),
    ("M&M","both",1.5),("ONGC","both",1.5),("NTPC","long_only",1.5),
    ("MARUTI","short_only",1.5),("EICHERMOT","short_only",1.5),
    ("TECHM","long_only",1.5),("INDUSINDBK","short_only",1.5),
    ("HINDALCO","both",1.5),("BAJFINANCE","short_only",1.5),
    ("HCLTECH","short_only",1.5),("HDFCBANK","both",1.5),
    ("POWERGRID","long_only",1.5),("BAJAJFINSV","short_only",1.5),
    ("JSWSTEEL","both",1.5),("SUNPHARMA","long_only",1.5),
    ("TITAN","long_only",1.5),("SBILIFE","long_only",1.5),
    ("GRASIM","short_only",1.5),("HEROMOTOCO","short_only",1.5),
    ("ADANIPORTS","both",1.5),("AXISBANK","short_only",1.5),
    ("COALINDIA","short_only",1.5),("BHARTIARTL","long_only",1.5),
    ("LT","short_only",1.5),("BPCL","both",1.5),
    ("CIPLA","long_only",1.5),("TATACONSUM","short_only",1.5),
    ("TATASTEEL","short_only",1.5),("KOTAKBANK","long_only",1.5),
    ("SBIN","both",1.5),
]

# ── IMPORTS ───────────────────────────────────────────────────────────────────
import json, logging, os, time, uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import Optional
import numpy as np
import pandas as pd
from growwapi import GrowwAPI

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger("GrowwFibMACD")

MARKET_OPEN, MARKET_CLOSE = "00:00", "23:59"   # relaxed for test
LTP_POLL_SECS = 15

_NSE, _CASH, _DAY, _MIS = "NSE", "CASH", "DAY", "MIS"
_MARKET, _LIMIT, _SL    = "MARKET", "LIMIT", "SL"
_BUY, _SELL             = "BUY", "SELL"


# ── DATA ──────────────────────────────────────────────────────────────────────
@dataclass
class StockCfg:
    symbol: str; direction: str; target_r: float = 1.5; risk: float = DEFAULT_RISK

@dataclass
class TradeSetup:
    direction: str; or_high: float; or_low: float; swing_value: float
    fib_entry: float; stop_loss: float; touched_fib: bool = False

@dataclass
class OpenTrade:
    direction: str; entry_price: float; quantity: int
    stop_loss: float; target_price: float; entry_time: str
    sl_order_id: str = ""; target_order_id: str = ""; entry_order_id: str = ""

@dataclass
class OrderResult:
    success: bool; order_id: str; message: str


class State(Enum):
    WAITING_OR=auto(); WAITING_BREAKOUT=auto(); WAITING_SWING=auto()
    WAITING_FIB=auto(); WAITING_ENTRY=auto(); IN_TRADE=auto(); DONE=auto()


# ── STATE MACHINE ─────────────────────────────────────────────────────────────
class StockState:
    def __init__(self, cfg: StockCfg):
        self.cfg, self.symbol = cfg, cfg.symbol
        self.reset()

    def reset(self):
        self.state = State.WAITING_OR
        self.candles_1m = []
        self.or_high = self.or_low = 0.0
        self.breakout_dir = None
        self.swing_peak = self.swing_value = 0.0
        self.swing_bars = 0
        self.setup = self.trade = None
        self.macd_5m = pd.DataFrame()
        self.warmup_candles_1m = []
        self.last_exit_reason = ""

    def on_candle(self, c):
        self.candles_1m.append(c)
        t = c["datetime"][11:16]
        s = self.state
        if s == State.WAITING_OR:       return self._h_or(c, t)
        if s == State.WAITING_BREAKOUT: return self._h_brk(c, t)
        if s == State.WAITING_SWING:    return self._h_sw(c, t)
        if s == State.WAITING_FIB:      return self._h_fib(c, t)
        if s == State.WAITING_ENTRY:    return self._h_ent(c, t)
        if s == State.IN_TRADE:         return self._h_in(c, t)
        return None

    def _h_or(self, c, t):
        tot = 9*60+15+OR_MINUTES; eh, em = divmod(tot, 60)
        oe = f"{eh:02d}:{em:02d}"
        if t < oe: return None
        ors = [x for x in self.candles_1m if x["datetime"][11:16] < oe]
        if len(ors) < 2: self.state = State.DONE; return None
        self.or_high = max(x["high"] for x in ors)
        self.or_low  = min(x["low"]  for x in ors)
        self.state = State.WAITING_BREAKOUT
        logger.info(f"{self.symbol} OR closed: H={self.or_high:.2f} L={self.or_low:.2f}")
        return self._h_brk(c, t)

    def _h_brk(self, c, t):
        if t >= EXIT_TIME: self.state = State.DONE; return None
        al = self.cfg.direction in ("long_only", "both")
        ash = self.cfg.direction in ("short_only", "both")
        if al and c["high"] > self.or_high:
            self.breakout_dir, self.swing_peak = "LONG", c["high"]
            self.state = State.WAITING_SWING
            logger.info(f"{self.symbol} LONG breakout at {t}"); return None
        if ash and c["low"] < self.or_low:
            self.breakout_dir, self.swing_peak = "SHORT", c["low"]
            self.state = State.WAITING_SWING
            logger.info(f"{self.symbol} SHORT breakout at {t}"); return None
        return None

    def _h_sw(self, c, t):
        if t >= EXIT_TIME: self.state = State.DONE; return None
        self.swing_bars += 1
        if self.swing_bars > MAX_WAIT_BARS:
            logger.info(f"{self.symbol} swing timeout"); self.state = State.DONE; return None
        if self.breakout_dir == "LONG":
            if c["high"] > self.swing_peak: self.swing_peak = c["high"]
            if c["close"] < self.swing_peak * (1 - SWING_CONFIRM):
                logger.info(f"{self.symbol} LONG swing confirmed: peak={self.swing_peak:.2f}")
                self.swing_value = self.swing_peak; self._build_setup()
        else:
            if c["low"] < self.swing_peak: self.swing_peak = c["low"]
            if c["close"] > self.swing_peak * (1 + SWING_CONFIRM):
                logger.info(f"{self.symbol} SHORT swing confirmed: trough={self.swing_peak:.2f}")
                self.swing_value = self.swing_peak; self._build_setup()
        return None

    def _h_fib(self, c, t):
        if t >= EXIT_TIME: self.state = State.DONE; return None
        s = self.setup
        if self.breakout_dir == "LONG" and c["close"] <= s.stop_loss:
            logger.info(f"{self.symbol} LONG invalidated: close={c['close']:.2f} ≤ {s.stop_loss:.2f}")
            self.state = State.DONE; return None
        if self.breakout_dir == "SHORT" and c["close"] >= s.stop_loss:
            logger.info(f"{self.symbol} SHORT invalidated: close={c['close']:.2f} ≥ {s.stop_loss:.2f}")
            self.state = State.DONE; return None
        if self.breakout_dir == "LONG" and c["low"] <= s.fib_entry:
            s.touched_fib = True; self.state = State.WAITING_ENTRY
            logger.info(f"{self.symbol} fib {s.fib_entry:.2f} touched")
        if self.breakout_dir == "SHORT" and c["high"] >= s.fib_entry:
            s.touched_fib = True; self.state = State.WAITING_ENTRY
            logger.info(f"{self.symbol} fib {s.fib_entry:.2f} touched")
        return None

    def _h_ent(self, c, t):
        if t >= EXIT_TIME: self.state = State.DONE; return None
        s = self.setup
        if self.breakout_dir == "LONG" and c["close"] <= s.stop_loss:
            self.state = State.DONE; return None
        if self.breakout_dir == "SHORT" and c["close"] >= s.stop_loss:
            self.state = State.DONE; return None
        if self.breakout_dir == "LONG" and c["close"] > s.fib_entry:
            if self._macd(t, "LONG"): return self._fire(t, "LONG")
            logger.info(f"{self.symbol} LONG entry BLOCKED by MACD at {t}")
        if self.breakout_dir == "SHORT" and c["close"] < s.fib_entry:
            if self._macd(t, "SHORT"): return self._fire(t, "SHORT")
            logger.info(f"{self.symbol} SHORT entry BLOCKED by MACD at {t}")
        return None

    def _h_in(self, c, t):
        tr = self.trade
        if t >= EXIT_TIME:
            logger.info(f"{self.symbol} TIME exit at {t}")
            self.state = State.DONE; self.last_exit_reason = "TIME"; return "exit_trade"
        if tr.direction == "LONG":
            if c["low"] <= tr.stop_loss:
                logger.info(f"{self.symbol} SL hit @ {tr.stop_loss:.2f}")
                self.state = State.DONE; self.last_exit_reason = "SL"; return "exit_trade"
            if tr.target_price > 0 and c["high"] >= tr.target_price:
                logger.info(f"{self.symbol} TARGET hit @ {tr.target_price:.2f}")
                self.state = State.DONE; self.last_exit_reason = "TARGET"; return "exit_trade"
        else:
            if c["high"] >= tr.stop_loss:
                logger.info(f"{self.symbol} SL hit @ {tr.stop_loss:.2f}")
                self.state = State.DONE; self.last_exit_reason = "SL"; return "exit_trade"
            if tr.target_price > 0 and c["low"] <= tr.target_price:
                logger.info(f"{self.symbol} TARGET hit @ {tr.target_price:.2f}")
                self.state = State.DONE; self.last_exit_reason = "TARGET"; return "exit_trade"
        return None

    def _build_setup(self):
        if self.breakout_dir == "LONG":
            lo, hi = self.or_low, self.swing_value
            rng = hi - lo
            if rng <= 0: self.state = State.DONE; return
            fe = hi - FIB_ENTRY_PCT * rng
            f786 = hi - FIB_786 * rng
            sl = f786 * (1 - SL_BUFFER_PCT)
        else:
            hi, lo = self.or_high, self.swing_value
            rng = hi - lo
            if rng <= 0: self.state = State.DONE; return
            fe = lo + FIB_ENTRY_PCT * rng
            f786 = lo + FIB_786 * rng
            sl = f786 * (1 + SL_BUFFER_PCT)
        self.setup = TradeSetup(self.breakout_dir, self.or_high, self.or_low,
                                self.swing_value, round(fe,2), round(sl,2))
        self.state = State.WAITING_FIB
        logger.info(f"{self.symbol} setup {self.breakout_dir}: fib={self.setup.fib_entry:.2f} sl={self.setup.stop_loss:.2f}")

    def _fire(self, t, d):
        s = self.setup
        e, sl = s.fib_entry, s.stop_loss
        rps = abs(e - sl)
        rc = int(self.cfg.risk / rps) if rps > 0 else 0
        cc = int(CAPITAL_PER_TRADE / e) if e > 0 else 0
        q = min(rc, cc)
        if q <= 0: self.state = State.DONE; return None
        tgt = e + rps*self.cfg.target_r if d == "LONG" else e - rps*self.cfg.target_r
        self.trade = OpenTrade(d, e, q, sl, round(tgt,2), t)
        self.state = State.IN_TRADE
        logger.info(f"{self.symbol} ENTRY {d} @ {e:.2f} qty={q} sl={sl:.2f} tgt={tgt:.2f}")
        return f"enter_{d.lower()}"

    def _macd(self, t, d):
        if MACD_CONDITION == "none": return True
        if self.macd_5m.empty or len(self.macd_5m) < MIN_MACD_BARS: return True
        cd = self.candles_1m[-1]["datetime"][:10] if self.candles_1m else datetime.now().strftime("%Y-%m-%d")
        ts = f"{cd} {t}:00"
        rel = self.macd_5m[self.macd_5m["time_str"] <= ts]
        if len(rel) < 2: return True
        cur, prev = rel.iloc[-1], rel.iloc[-2]
        m, sg, h = float(cur["macd"]), float(cur["signal"]), float(cur["histogram"])
        if MACD_CONDITION == "macd_cross":
            ok = (m > sg) if d == "LONG" else (m < sg)
            logger.info(f"{self.symbol} MACD cross ({d}) at {t}: m={m:.4f} s={sg:.4f} h={h:.4f} → {'PASS' if ok else 'FAIL'}")
            return ok
        if MACD_CONDITION == "histogram_positive":
            return (h > 0) if d == "LONG" else (h < 0)
        return True

    def update_macd(self, df): self.macd_5m = df


# ── BROKER ────────────────────────────────────────────────────────────────────
def _ref_id(p="FM"): return f"{p}{uuid.uuid4().hex[:10].upper()}"
def _at(o, k): return o.get(k) if isinstance(o, dict) else getattr(o, k, None)


class GrowwBroker:
    def __init__(self, key, sec):
        self._k, self._s, self._g = key, sec, None

    def connect(self):
        try:
            t = GrowwAPI.get_access_token(api_key=self._k, secret=self._s)
            self._g = GrowwAPI(t)
            global _NSE,_CASH,_DAY,_MIS,_MARKET,_LIMIT,_SL,_BUY,_SELL
            _NSE   = getattr(self._g, "EXCHANGE_NSE",         _NSE)
            _CASH  = getattr(self._g, "SEGMENT_CASH",         _CASH)
            _DAY   = getattr(self._g, "VALIDITY_DAY",         _DAY)
            _MIS   = getattr(self._g, "PRODUCT_MIS",          _MIS)
            _MARKET= getattr(self._g, "ORDER_TYPE_MARKET",    _MARKET)
            _LIMIT = getattr(self._g, "ORDER_TYPE_LIMIT",     _LIMIT)
            _SL    = getattr(self._g, "ORDER_TYPE_STOP_LOSS", _SL)
            _BUY   = getattr(self._g, "TRANSACTION_TYPE_BUY", _BUY)
            _SELL  = getattr(self._g, "TRANSACTION_TYPE_SELL",_SELL)
            logger.info("Groww connected ✓")
            return True
        except Exception as e:
            logger.error(f"connect failed: {e}"); return False

    def get_candles_historical(self, sym, frm, to):
        time.sleep(1.5)
        try:
            r = self._g.get_historical_candles(
                exchange=self._g.EXCHANGE_NSE, segment=self._g.SEGMENT_CASH,
                groww_symbol=f"NSE-{sym}", start_time=frm, end_time=to,
                candle_interval=self._g.CANDLE_INTERVAL_MIN_1)
            raw = _at(r, "candles") or []
            cs, errs, pre = [], 0, 0
            for c in raw:
                if len(c) >= 5 and c[1] is None and c[2] is None and c[3] is None and c[4] is None:
                    pre += 1; continue
                try:
                    tr = c[0]
                    if isinstance(tr, str):
                        ts = datetime.fromisoformat(tr.replace("Z","+00:00"))
                        if ts.tzinfo: ts = ts.replace(tzinfo=None)
                    else:
                        ts = datetime.fromtimestamp(float(tr))
                    cs.append({"datetime": ts.strftime("%Y-%m-%d %H:%M:%S"),
                               "open": float(c[1]), "high": float(c[2]),
                               "low": float(c[3]), "close": float(c[4]),
                               "volume": int(c[5] or 0)})
                except Exception:
                    errs += 1
            if not cs:
                logger.warning(f"get_candles_historical({sym}): 0 candles (raw {len(raw)}, premarket {pre}, errs {errs})")
            return cs
        except Exception as e:
            logger.error(f"get_candles_historical({sym}): {e}"); return []

    def get_ltp_batch(self, syms):
        if not syms: return {}
        es = tuple(f"NSE_{s}" for s in syms[:50])
        try:
            r = self._g.get_ltp(segment=self._g.SEGMENT_CASH, exchange_trading_symbols=es)
            res = {}
            if isinstance(r, dict):
                for k, v in r.items():
                    sym = k.replace("NSE_","",1)
                    if isinstance(v,(int,float)): res[sym] = float(v)
                    elif isinstance(v, dict):     res[sym] = float(v.get("ltp") or 0)
                    else:                         res[sym] = float(_at(v,"ltp") or 0)
            miss = [s for s in syms if res.get(s,0) == 0]
            if miss: logger.warning(f"get_ltp_batch: {len(miss)} missing: {miss}")
            return res
        except Exception as e:
            logger.error(f"get_ltp_batch: {e}"); return {}

    def place_market_order(self, sym, act, qty, ref_id=None):
        txn = _BUY if act.lower() == "buy" else _SELL
        ref = ref_id or _ref_id("M")
        logger.info(f"market_order → {sym} {txn} {qty} ref={ref}")
        try:
            r = self._g.place_order(trading_symbol=sym, quantity=qty, validity=_DAY,
                exchange=_NSE, segment=_CASH, product=_MIS, order_type=_MARKET,
                transaction_type=txn, order_reference_id=ref)
            oid = _at(r,"groww_order_id") or ""
            if oid:
                logger.info(f"  accepted oid={oid}")
                return OrderResult(True, oid, _at(r,"order_status") or "")
            msg = _at(r,"remark") or str(r)
            logger.error(f"  rejected: {msg}")
            return OrderResult(False, "", msg)
        except Exception as e:
            logger.error(f"  exception: {e}"); return OrderResult(False, "", str(e))

    def place_limit_order(self, sym, act, qty, price, ref_id=None):
        txn = _BUY if act.lower() == "buy" else _SELL
        ref = ref_id or _ref_id("L")
        logger.info(f"limit_order → {sym} {txn} {qty} @ {price:.2f} ref={ref}")
        try:
            r = self._g.place_order(trading_symbol=sym, quantity=qty, validity=_DAY,
                exchange=_NSE, segment=_CASH, product=_MIS, order_type=_LIMIT,
                transaction_type=txn, price=round(price,2), order_reference_id=ref)
            oid = _at(r,"groww_order_id") or ""
            if oid: return OrderResult(True, oid, _at(r,"order_status") or "")
            return OrderResult(False, "", _at(r,"remark") or str(r))
        except Exception as e:
            return OrderResult(False, "", str(e))

    def place_stoploss_order(self, sym, act, qty, trig, lim, ref_id=None):
        txn = _BUY if act.lower() == "buy" else _SELL
        ref = ref_id or _ref_id("S")
        logger.info(f"sl_order → {sym} {txn} {qty} trig={trig:.2f} lim={lim:.2f} ref={ref}")
        try:
            r = self._g.place_order(trading_symbol=sym, quantity=qty, validity=_DAY,
                exchange=_NSE, segment=_CASH, product=_MIS, order_type=_SL,
                transaction_type=txn, price=round(lim,2), trigger_price=round(trig,2),
                order_reference_id=ref)
            oid = _at(r,"groww_order_id") or ""
            if oid: return OrderResult(True, oid, _at(r,"order_status") or "")
            return OrderResult(False, "", _at(r,"remark") or str(r))
        except Exception as e:
            return OrderResult(False, "", str(e))

    def get_available_cash(self):
        try:
            r = self._g.get_available_margin_details()
            return float(_at(r,"mis_balance_available") or 0)
        except Exception: return 0.0

    def get_live_qty(self, sym):
        try:
            r = self._g.get_position_for_trading_symbol(trading_symbol=sym, segment=self._g.SEGMENT_CASH)
            if r is None: return 0
            return int(_at(r,"credit_quantity") or 0) - int(_at(r,"debit_quantity") or 0)
        except Exception: return None

    def get_all_mis_positions(self):
        try:
            r = self._g.get_positions_for_user(segment=self._g.SEGMENT_CASH)
            items = r if isinstance(r, list) else (_at(r,"positions") or _at(r,"data") or [])
            res = []
            for p in items:
                if str(_at(p,"product") or "").upper() != "MIS": continue
                net = int(_at(p,"credit_quantity") or 0) - int(_at(p,"debit_quantity") or 0)
                if net == 0: continue
                res.append({"symbol": _at(p,"trading_symbol"), "net_qty": net})
            return res
        except Exception as e:
            logger.error(f"get_all_mis_positions: {e}"); return []

    def get_order_status(self, oid):
        try:
            r = self._g.get_order_status(groww_order_id=oid, segment=_CASH)
            return str(_at(r,"order_status") or "UNKNOWN").upper()
        except Exception: return "UNKNOWN"

    def cancel_order(self, oid):
        try:
            r = self._g.cancel_order(groww_order_id=oid, segment=_CASH)
            s = str(_at(r,"order_status") or "")
            return "CANCEL" in s.upper() or bool(_at(r,"groww_order_id"))
        except Exception: return False


# ── LIVE TRADER ───────────────────────────────────────────────────────────────
def _ema(v, p): return pd.Series(v).ewm(span=p, adjust=False).mean().values


class LiveTrader:
    def __init__(self, broker, states):
        self.broker, self.states = broker, states
        self._or_built = False
        self._prev_ltps = {}; self._min_ltps = {}; self._last_min = ""
        self._trades_today = 0
        self._replaying = False

    def run(self):
        if LATE_START_MODE and not DRY_RUN:
            logger.error("LATE_START_MODE requires DRY_RUN=True"); return
        if not self.broker.connect(): return
        for s in self.states.values(): s.reset()
        self._or_built = False; self._prev_ltps = {}
        self._min_ltps = {s: [] for s in self.states}
        self._last_min = ""; self._trades_today = 0
        restored = self._load_state()
        if restored:
            self._reconcile_with_broker()
            self._rehydrate_after_restart()
        logger.info(f"Trading {len(self.states)} stocks | DRY_RUN={DRY_RUN} | restored={restored}")
        while True:
            now = datetime.now(); ts = now.strftime("%H:%M")
            if ts < MARKET_OPEN: time.sleep(30); continue
            if ts >= MARKET_CLOSE:
                logger.info("Market closed — emergency exit")
                self._emergency_exit_all(); break
            self._tick(now); time.sleep(LTP_POLL_SECS)
        self._print_summary()

    def _tick(self, now):
        today = now.strftime("%Y-%m-%d"); ts = now.strftime("%H:%M")
        oem = 9*60+15+OR_MINUTES; oeh, oemn = divmod(oem, 60)
        oe = f"{oeh:02d}:{oemn:02d}"
        if not self._or_built:
            if ts < oe and not LATE_START_MODE: return
            self._build_or(today, oe); return
        active = [s for s, st in self.states.items() if st.state != State.DONE]
        if not active: return
        ltp = self.broker.get_ltp_batch(active)
        for s in active:
            v = ltp.get(s, 0.0)
            if v > 0: self._min_ltps.setdefault(s, []).append(v)
        cm = now.strftime("%H:%M")
        if cm != self._last_min:
            if self._last_min: self._emit_candles(today, self._last_min)
            self._last_min = cm

    def _build_or(self, today, oe):
        td = datetime.strptime(today, "%Y-%m-%d")
        ws = (td - timedelta(days=MACD_WARMUP_DAYS)).strftime("%Y-%m-%d 09:15:00")
        if LATE_START_MODE:
            to = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            logger.warning(f"⚠ LATE_START_MODE — replay {today} 09:15 → {to[11:]} ({MACD_WARMUP_DAYS}d warmup)")
            self._replaying = True
        else:
            to = f"{today} {oe}:59"
        loaded, sigs = 0, []
        for sym, st in self.states.items():
            ac = self.broker.get_candles_historical(sym, ws, to)
            if not ac:
                logger.warning(f"{sym}: no candles — DONE"); st.state = State.DONE; continue
            wm = [c for c in ac if c["datetime"][:10] != today]
            tc = [c for c in ac if c["datetime"][:10] == today]
            if not tc:
                logger.warning(f"{sym}: warmup ok ({len(wm)}) but no today — DONE")
                st.state = State.DONE; continue
            st.warmup_candles_1m = wm
            logger.info(f"{sym}: {len(wm)}+{len(tc)} candles loaded")
            for i, c in enumerate(tc):
                if LATE_START_MODE and st.state in (State.WAITING_FIB, State.WAITING_ENTRY):
                    self._update_macd_from_1m(st, wm + tc[:i+1])
                a = st.on_candle(c)
                if not LATE_START_MODE: continue
                if a and a.startswith("enter_"):
                    tr = st.trade
                    m = (f"REPLAY {sym}: enter {tr.direction} @ {tr.entry_price:.2f} "
                         f"qty={tr.quantity} sl={tr.stop_loss:.2f} tgt={tr.target_price:.2f} at {c['datetime'][11:16]}")
                    logger.info(m); sigs.append(m)
                elif a == "exit_trade":
                    rsn = st.last_exit_reason or "?"
                    m = f"REPLAY {sym}: exit at {c['datetime'][11:16]} → {rsn}"
                    logger.info(m); sigs.append(m)
            self._update_macd_from_1m(st, ac)
            self._prev_ltps[sym] = tc[-1]["close"]; self._min_ltps[sym] = []
            loaded += 1
            if st.or_high > 0:
                logger.info(f"{sym} OR: H={st.or_high:.2f} L={st.or_low:.2f} state={st.state.name}")
        logger.info(f"Loaded {loaded}/{len(self.states)} stocks")
        if LATE_START_MODE:
            self._replaying = False
            logger.warning(f"REPLAY COMPLETE — {len(sigs)} signal(s)")
            for s in sigs: logger.warning(f"  {s}")
            self._log_status()
            self._last_min = datetime.now().strftime("%H:%M")
            self._or_built = True; self._save_state(); return
        ltp = self.broker.get_ltp_batch(list(self.states.keys()))
        for sym, st in self.states.items():
            if st.state == State.DONE: continue
            p = ltp.get(sym) or self._prev_ltps.get(sym, 0)
            if p <= 0: continue
            tg = {"datetime": f"{today} {oe}:00", "open": p, "high": p, "low": p, "close": p, "volume": 0}
            a = st.on_candle(tg); self._prev_ltps[sym] = p
            if a and a.startswith("enter_"): self._handle_entry(st)
            elif a == "exit_trade": self._handle_exit(st)
        self._or_built = True; self._last_min = oe; self._save_state()

    def _emit_candles(self, today, cm):
        for sym, st in self.states.items():
            if st.state == State.DONE: continue
            samp = self._min_ltps.get(sym) or []
            if not samp: continue
            prev = self._prev_ltps.get(sym, samp[0])
            c = {"datetime": f"{today} {cm}:00", "open": prev,
                 "high": max(samp+[prev]), "low": min(samp+[prev]),
                 "close": samp[-1], "volume": 0}
            if st.state in (State.WAITING_FIB, State.WAITING_ENTRY):
                frm = f"{today} 09:15:00"
                to = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                h = self.broker.get_candles_historical(sym, frm, to)
                if h: self._update_macd_from_1m(st, st.warmup_candles_1m + h)
            a = st.on_candle(c)
            self._prev_ltps[sym] = c["close"]; self._min_ltps[sym] = []
            if a and a.startswith("enter_"): self._handle_entry(st)
            elif a == "exit_trade": self._handle_exit(st)
        self._check_oco_fills()
        self._log_status(); self._save_state()

    def _handle_entry(self, st):
        tr = st.trade; sym = st.symbol; d = tr.direction; q = tr.quantity
        bs = "buy" if d == "LONG" else "sell"
        if self._replaying: return
        if DRY_RUN:
            logger.info(f"[DRY RUN] {sym} {bs.upper()} {q} @ ~{tr.entry_price:.2f} sl={tr.stop_loss:.2f} tgt={tr.target_price:.2f}")
            tr.entry_order_id = "DRY"; self._trades_today += 1; self._save_state(); return
        if self._trades_today >= MAX_TRADES_PER_DAY:
            logger.warning(f"{sym} MAX_TRADES_PER_DAY reached"); st.state = State.DONE; st.trade = None; return
        rm = tr.entry_price * q * MIS_LEVERAGE_DIVISOR
        cash = self.broker.get_available_cash()
        if cash > 0 and cash < rm:
            logger.error(f"{sym} insufficient margin (need {rm:.0f}, have {cash:.0f})")
            st.state = State.DONE; st.trade = None; return
        eref = f"E{sym[:6]}{uuid.uuid4().hex[:6].upper()}"
        r = self.broker.place_market_order(sym, bs, q, ref_id=eref)
        if not r.success:
            logger.error(f"{sym} entry FAILED: {r.message}"); st.state = State.DONE; st.trade = None; return
        tr.entry_order_id = r.order_id; self._trades_today += 1; self._save_state()
        logger.info(f"{sym} entry placed: {r.order_id}")
        fq = self._wait_for_fill(r.order_id, q, 3)
        if fq == 0:
            logger.error(f"{sym} entry NOT filled — abandoning"); st.state = State.DONE; st.trade = None; return
        if fq < q: tr.quantity = fq; q = fq
        ss = "sell" if d == "LONG" else "buy"
        sref = f"S{sym[:6]}{uuid.uuid4().hex[:6].upper()}"
        sl_lim = round(tr.stop_loss * (1-SL_LIMIT_BUFFER if d=="LONG" else 1+SL_LIMIT_BUFFER), 2)
        sl_ok = False
        for att in (1, 2):
            sr = self.broker.place_stoploss_order(sym, ss, q, tr.stop_loss, sl_lim, ref_id=sref)
            if sr.success:
                tr.sl_order_id = sr.order_id; self._save_state()
                logger.info(f"{sym} SL placed: {sr.order_id}"); sl_ok = True; break
            logger.warning(f"{sym} SL fail attempt {att}: {sr.message}"); time.sleep(0.5)
        if sl_ok:
            tref = f"T{sym[:6]}{uuid.uuid4().hex[:6].upper()}"
            tr_res = self.broker.place_limit_order(sym, ss, q, tr.target_price, ref_id=tref)
            if tr_res.success:
                tr.target_order_id = tr_res.order_id; self._save_state()
                logger.info(f"{sym} TARGET placed: {tr_res.order_id} @ {tr.target_price:.2f}")
            else:
                logger.warning(f"{sym} TARGET fail: {tr_res.message}")
            return
        logger.error(f"{sym} SL FAILED — emergency exit")
        eref2 = f"X{sym[:6]}{uuid.uuid4().hex[:6].upper()}"
        self.broker.place_market_order(sym, ss, q, ref_id=eref2)
        st.state = State.DONE; self._save_state()

    def _wait_for_fill(self, oid, exp, mw=3):
        deadline = time.time() + mw
        while time.time() < deadline:
            s = self.broker.get_order_status(oid)
            if s in ("EXECUTED","COMPLETE","FILLED"): return exp
            if s in ("REJECTED","CANCELLED","FAILED"): return 0
            time.sleep(0.5)
        return exp

    def _handle_exit(self, st):
        if not st.trade: return
        sym = st.symbol; d = st.trade.direction; q = st.trade.quantity
        es = "sell" if d == "LONG" else "buy"
        if self._replaying: return
        if DRY_RUN:
            logger.info(f"[DRY RUN] {sym} EXIT {es.upper()} {q}"); return
        lq = self.broker.get_live_qty(sym)
        if lq == 0:
            logger.info(f"{sym} already flat — skipping exit"); return
        if lq is None:
            eq = q
        else:
            es_sign = 1 if d == "LONG" else -1
            if lq * es_sign < 0:
                logger.error(f"{sym} sign mismatch — refusing exit"); return
            eq = abs(lq)
        if st.trade.sl_order_id:     self.broker.cancel_order(st.trade.sl_order_id)
        if st.trade.target_order_id: self.broker.cancel_order(st.trade.target_order_id)
        eref = f"X{sym[:6]}{uuid.uuid4().hex[:6].upper()}"
        r = self.broker.place_market_order(sym, es, eq, ref_id=eref)
        if r.success: logger.info(f"{sym} exit placed: {r.order_id} reason={st.last_exit_reason or 'manual'}")
        else: logger.error(f"{sym} EXIT FAILED: {r.message}")
        self._save_state()

    def _emergency_exit_all(self):
        if DRY_RUN:
            for st in self.states.values():
                if st.state == State.IN_TRADE: st.state = State.DONE
            return
        for st in self.states.values():
            if st.state == State.IN_TRADE and st.trade:
                if st.trade.sl_order_id:     self.broker.cancel_order(st.trade.sl_order_id)
                if st.trade.target_order_id: self.broker.cancel_order(st.trade.target_order_id)
        ops = self.broker.get_all_mis_positions()
        for p in ops:
            sm, n = p["symbol"], p["net_qty"]
            sd = "sell" if n > 0 else "buy"
            ref = f"EOD{sm[:5]}{uuid.uuid4().hex[:6].upper()}"
            r = self.broker.place_market_order(sm, sd, abs(n), ref_id=ref)
            if r.success: logger.info(f"{sm} EOD exit: {r.order_id}")
            else: logger.error(f"{sm} EOD exit FAILED: {r.message}")
        for st in self.states.values():
            if st.state == State.IN_TRADE: st.state = State.DONE
        self._save_state()

    def _update_macd_from_1m(self, st, candles_1m):
        if len(candles_1m) < 5: return
        try:
            df = pd.DataFrame(candles_1m)
            df["dt"] = pd.to_datetime(df["datetime"])
            df = df.set_index("dt").sort_index()
            d5 = df["close"].resample("5min").last().dropna()
            cl = d5.values
            if len(cl) < 2: return
            ef = _ema(cl, MACD_FAST); es = _ema(cl, MACD_SLOW)
            ml = ef - es; sl = _ema(ml, MACD_SIGNAL)
            r = pd.DataFrame({"time_str": d5.index.strftime("%Y-%m-%d %H:%M:%S"),
                              "macd": ml, "signal": sl, "histogram": ml - sl})
            st.update_macd(r)
        except Exception as e:
            logger.warning(f"MACD fail {st.symbol}: {e}")

    def _check_oco_fills(self):
        if DRY_RUN or self._replaying: return
        FILLED = {"EXECUTED","COMPLETE","FILLED"}
        for sym, st in self.states.items():
            if st.state != State.IN_TRADE or not st.trade: continue
            tr = st.trade
            if not (tr.sl_order_id and tr.target_order_id): continue
            ss = self.broker.get_order_status(tr.sl_order_id)
            ts = self.broker.get_order_status(tr.target_order_id)
            sf, tf = ss in FILLED, ts in FILLED
            if sf and not tf:
                logger.info(f"{sym} SL fired @ broker — cancelling target")
                self.broker.cancel_order(tr.target_order_id)
                st.state = State.DONE; st.last_exit_reason = "SL"; self._save_state()
            elif tf and not sf:
                logger.info(f"{sym} TARGET fired @ broker — cancelling SL")
                self.broker.cancel_order(tr.sl_order_id)
                st.state = State.DONE; st.last_exit_reason = "TARGET"; self._save_state()
            elif sf and tf:
                logger.error(f"{sym} BOTH filled — manual review")
                st.state = State.DONE; st.last_exit_reason = "BOTH_FILLED"; self._save_state()

    def _log_status(self):
        ts = datetime.now().strftime("%H:%M:%S")
        bs = {}
        for s in self.states.values(): bs.setdefault(s.state.name, []).append(s.symbol)
        order = ["WAITING_OR","WAITING_BREAKOUT","WAITING_SWING","WAITING_FIB","WAITING_ENTRY","IN_TRADE","DONE"]
        parts = []
        for n in order:
            sy = bs.get(n, [])
            if not sy: continue
            parts.append(f"{n}={len(sy)}({','.join(sy)})" if len(sy) <= 4 else f"{n}={len(sy)}")
        logger.info(f"[{ts}] {' | '.join(parts)}")
        for s in self.states.values():
            if s.state == State.IN_TRADE and s.trade:
                t = s.trade
                logger.info(f"  ↳ {s.symbol} {t.direction} {t.quantity}x e={t.entry_price:.2f} sl={t.stop_loss:.2f} tgt={t.target_price:.2f}")

    def _print_summary(self):
        logger.info("="*60); logger.info("END OF DAY SUMMARY"); logger.info("="*60)
        for s in self.states.values():
            if s.trade:
                t = s.trade; r = s.last_exit_reason or "OPEN"
                logger.info(f"  {s.symbol}: {t.direction} {t.quantity}x @ {t.entry_price:.2f} → {r}")
        nt = [s.symbol for s in self.states.values() if s.trade is None and s.state == State.DONE]
        logger.info(f"No trade today: {nt}")

    # ── persistence ───────────────────────────────────────────────────────────
    def _state_file_path(self):
        return os.path.join(STATE_DIR, f"groww_state_test_{datetime.now():%Y-%m-%d}.json")

    def _save_state(self):
        p = self._state_file_path(); tp = p + ".tmp"
        snap = {"date": datetime.now().strftime("%Y-%m-%d"),
                "or_built": self._or_built, "last_min": self._last_min,
                "trades_today": self._trades_today, "prev_ltps": self._prev_ltps,
                "stocks": {}}
        for sym, s in self.states.items():
            e = {"state": s.state.name, "or_high": s.or_high, "or_low": s.or_low,
                 "breakout_dir": s.breakout_dir, "swing_peak": s.swing_peak,
                 "swing_value": s.swing_value, "swing_bars": s.swing_bars,
                 "last_exit_reason": s.last_exit_reason}
            if s.setup is not None:
                e["setup"] = {"direction": s.setup.direction,
                    "or_high": s.setup.or_high, "or_low": s.setup.or_low,
                    "swing_value": s.setup.swing_value, "fib_entry": s.setup.fib_entry,
                    "stop_loss": s.setup.stop_loss, "touched_fib": s.setup.touched_fib}
            if s.trade is not None:
                e["trade"] = {"direction": s.trade.direction,
                    "entry_price": s.trade.entry_price, "quantity": s.trade.quantity,
                    "stop_loss": s.trade.stop_loss, "target_price": s.trade.target_price,
                    "entry_time": s.trade.entry_time, "sl_order_id": s.trade.sl_order_id,
                    "target_order_id": s.trade.target_order_id,
                    "entry_order_id": s.trade.entry_order_id}
            snap["stocks"][sym] = e
        try:
            with open(tp, "w") as f: json.dump(snap, f)
            os.replace(tp, p)
        except Exception as e:
            logger.warning(f"_save_state fail: {e}")

    def _load_state(self):
        p = self._state_file_path()
        if not os.path.exists(p): return False
        try:
            snap = json.load(open(p))
        except Exception as e:
            logger.warning(f"_load_state corrupt: {e}"); return False
        if snap.get("date") != datetime.now().strftime("%Y-%m-%d"): return False
        self._or_built = bool(snap.get("or_built", False))
        self._last_min = str(snap.get("last_min", ""))
        self._trades_today = int(snap.get("trades_today", 0))
        self._prev_ltps = {k: float(v) for k, v in (snap.get("prev_ltps") or {}).items()}
        for sym, e in (snap.get("stocks") or {}).items():
            s = self.states.get(sym)
            if s is None: continue
            try:
                s.state = State[e["state"]]
                s.or_high = float(e.get("or_high", 0)); s.or_low = float(e.get("or_low", 0))
                s.breakout_dir = e.get("breakout_dir")
                s.swing_peak = float(e.get("swing_peak", 0))
                s.swing_value = float(e.get("swing_value", 0))
                s.swing_bars = int(e.get("swing_bars", 0))
                s.last_exit_reason = e.get("last_exit_reason", "")
                if e.get("setup"):
                    su = e["setup"]
                    s.setup = TradeSetup(su["direction"], float(su["or_high"]),
                        float(su["or_low"]), float(su["swing_value"]),
                        float(su["fib_entry"]), float(su["stop_loss"]),
                        bool(su.get("touched_fib", False)))
                if e.get("trade"):
                    tr = e["trade"]
                    s.trade = OpenTrade(tr["direction"], float(tr["entry_price"]),
                        int(tr["quantity"]), float(tr["stop_loss"]),
                        float(tr["target_price"]), tr.get("entry_time", ""),
                        tr.get("sl_order_id", ""), tr.get("target_order_id", ""),
                        tr.get("entry_order_id", ""))
            except Exception as ex:
                logger.warning(f"restore {sym} fail: {ex}")
        return True

    def _reconcile_with_broker(self):
        if DRY_RUN: return
        in_tr = [s for s, st in self.states.items() if st.state == State.IN_TRADE and st.trade]
        if not in_tr: return
        bp = {p["symbol"]: p["net_qty"] for p in self.broker.get_all_mis_positions()}
        for sym in in_tr:
            s = self.states[sym]; tr = s.trade
            n = bp.get(sym, 0); exp = tr.quantity if tr.direction == "LONG" else -tr.quantity
            if n == 0:
                logger.warning(f"{sym} state IN_TRADE but broker FLAT — DONE"); s.state = State.DONE
            elif (n > 0) != (exp > 0):
                logger.error(f"{sym} sign mismatch — DONE"); s.state = State.DONE
            elif abs(n) != abs(exp):
                logger.warning(f"{sym} qty mismatch — adopting broker {abs(n)}"); tr.quantity = abs(n)
        self._save_state()

    def _rehydrate_after_restart(self):
        today = datetime.now().strftime("%Y-%m-%d")
        td = datetime.strptime(today, "%Y-%m-%d")
        ws = (td - timedelta(days=MACD_WARMUP_DAYS)).strftime("%Y-%m-%d 09:15:00")
        to = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for sym, st in self.states.items():
            if st.state == State.DONE: continue
            cs = self.broker.get_candles_historical(sym, ws, to)
            if not cs: continue
            st.warmup_candles_1m = [c for c in cs if c["datetime"][:10] != today]
            self._update_macd_from_1m(st, cs)


# ── ENTRY ─────────────────────────────────────────────────────────────────────
def main():
    print("="*60)
    print(f"  Fib-MACD TEST | DRY={DRY_RUN} | LATE_START={LATE_START_MODE}")
    print(f"  Stocks: {len(PORTFOLIO)} | Risk: ₹{DEFAULT_RISK} | Cap: ₹{CAPITAL_PER_TRADE}")
    print(f"  Hours: {MARKET_OPEN}–{MARKET_CLOSE} | Exit: {EXIT_TIME}")
    print("="*60)
    if not DRY_RUN:
        print("DRY_RUN=False in TEST file — refusing to run"); return
    stocks = {}
    for sym, d, tr in PORTFOLIO:
        risk = RISK_OVERRIDES.get(sym) or DEFAULT_RISK
        stocks[sym] = StockState(StockCfg(sym, d, tr, risk))
    LiveTrader(GrowwBroker(GROWW_API_KEY, GROWW_SECRET), stocks).run()


if __name__ == "__main__":
    main()
