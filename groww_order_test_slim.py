"""Groww order pipeline smoke test (slim). Edit config + set LIVE=True."""

# ── CONFIG ────────────────────────────────────────────────────────────────────
GROWW_API_KEY = "your_api_key"      # ← replace
GROWW_SECRET  = "your_secret_key"   # ← replace
TEST_STOCK     = "ONGC"
TEST_DIRECTION = "LONG"             # "LONG" | "SHORT"
TEST_QTY       = 1
SL_DIST_PCT    = 0.05               # SL 5% away (won't fire)
TGT_DIST_PCT   = 0.05               # target 5% away (won't fill)
PAUSE_SECS     = 30                 # pause for visual UI check
LIVE           = False              # MUST be True to actually place orders

# ── IMPORTS ───────────────────────────────────────────────────────────────────
import logging, time, uuid
from typing import Optional
from growwapi import GrowwAPI

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("OrderTest")

# Groww annexure values; overridden from SDK on connect()
NSE, CASH, DAY, MIS = "NSE", "CASH", "DAY", "MIS"
MARKET, LIMIT, SL_  = "MARKET", "LIMIT", "SL"
BUY, SELL           = "BUY", "SELL"


def _ref(p): return f"{p}{uuid.uuid4().hex[:10].upper()}"
def _at(o, k): return o.get(k) if isinstance(o, dict) else getattr(o, k, None)


class B:
    """Minimal Groww broker."""
    def __init__(self, key, sec):
        self.k, self.s, self.g = key, sec, None

    def connect(self):
        try:
            tok = GrowwAPI.get_access_token(api_key=self.k, secret=self.s)
            self.g = GrowwAPI(tok)
            global NSE, CASH, DAY, MIS, MARKET, LIMIT, SL_, BUY, SELL
            NSE    = getattr(self.g, "EXCHANGE_NSE",            NSE)
            CASH   = getattr(self.g, "SEGMENT_CASH",            CASH)
            DAY    = getattr(self.g, "VALIDITY_DAY",            DAY)
            MIS    = getattr(self.g, "PRODUCT_MIS",             MIS)
            MARKET = getattr(self.g, "ORDER_TYPE_MARKET",       MARKET)
            LIMIT  = getattr(self.g, "ORDER_TYPE_LIMIT",        LIMIT)
            SL_    = getattr(self.g, "ORDER_TYPE_STOP_LOSS",    SL_)
            BUY    = getattr(self.g, "TRANSACTION_TYPE_BUY",    BUY)
            SELL   = getattr(self.g, "TRANSACTION_TYPE_SELL",   SELL)
            log.info(f"connected ✓ MIS={MIS} SL={SL_} LIMIT={LIMIT}")
            return True
        except Exception as e:
            log.error(f"connect failed: {e}"); return False

    def ltp(self, sym):
        try:
            r = self.g.get_ltp(segment=self.g.SEGMENT_CASH,
                               exchange_trading_symbols=(f"NSE_{sym}",))
            if isinstance(r, dict):
                v = r.get(f"NSE_{sym}")
                if isinstance(v, (int, float)): return float(v)
                if isinstance(v, dict): return float(v.get("ltp") or 0)
                return float(_at(v, "ltp") or 0)
        except Exception as e:
            log.error(f"ltp({sym}): {e}")
        return 0.0

    def _order(self, **kw):
        log.info(f"order → {kw}")
        try:
            r = self.g.place_order(**kw)
            log.info(f"  resp: {r}")
            oid = _at(r, "groww_order_id") or ""
            return (oid, _at(r, "order_status") or "", _at(r, "remark") or str(r))
        except Exception as e:
            return ("", "", str(e))

    def market(self, sym, side, qty, ref=None):
        return self._order(
            trading_symbol=sym, quantity=qty, validity=DAY, exchange=NSE,
            segment=CASH, product=MIS, order_type=MARKET,
            transaction_type=BUY if side == "buy" else SELL,
            order_reference_id=ref or _ref("M"))

    def limit(self, sym, side, qty, price, ref=None):
        return self._order(
            trading_symbol=sym, quantity=qty, validity=DAY, exchange=NSE,
            segment=CASH, product=MIS, order_type=LIMIT,
            transaction_type=BUY if side == "buy" else SELL,
            price=round(price, 2),
            order_reference_id=ref or _ref("L"))

    def sl(self, sym, side, qty, trig, lim, ref=None):
        return self._order(
            trading_symbol=sym, quantity=qty, validity=DAY, exchange=NSE,
            segment=CASH, product=MIS, order_type=SL_,
            transaction_type=BUY if side == "buy" else SELL,
            price=round(lim, 2), trigger_price=round(trig, 2),
            order_reference_id=ref or _ref("S"))

    def cancel(self, oid):
        log.info(f"cancel: {oid}")
        try:
            r = self.g.cancel_order(groww_order_id=oid, segment=CASH)
            s = str(_at(r, "order_status") or "")
            log.info(f"  status: {s!r}")
            return "CANCEL" in s.upper() or bool(_at(r, "groww_order_id"))
        except Exception as e:
            log.error(f"cancel({oid}): {e}"); return False

    def status(self, oid):
        try:
            r = self.g.get_order_status(groww_order_id=oid, segment=CASH)
            return str(_at(r, "order_status") or "UNKNOWN").upper()
        except Exception as e:
            log.warning(f"status({oid}): {e}"); return "UNKNOWN"

    def qty(self, sym) -> Optional[int]:
        try:
            r = self.g.get_position_for_trading_symbol(
                trading_symbol=sym, segment=self.g.SEGMENT_CASH)
            if r is None: return 0
            return int(_at(r, "credit_quantity") or 0) - int(_at(r, "debit_quantity") or 0)
        except Exception as e:
            log.warning(f"qty({sym}): {e}"); return None


def main():
    print("="*60)
    print(f"Order Smoke Test | {TEST_STOCK} {TEST_DIRECTION} qty={TEST_QTY} | LIVE={LIVE}")
    print("="*60)

    if not LIVE:
        print("LIVE=False → no orders placed. Set LIVE=True to run.")
        return
    if TEST_DIRECTION not in ("LONG", "SHORT"):
        log.error("TEST_DIRECTION must be LONG or SHORT"); return

    b = B(GROWW_API_KEY, GROWW_SECRET)
    if not b.connect():
        return

    # Step 0 — LTP & plan
    p = b.ltp(TEST_STOCK)
    if p <= 0:
        log.error(f"no LTP for {TEST_STOCK} — abort"); return
    log.info(f"[0] {TEST_STOCK} LTP=₹{p:.2f}")

    if TEST_DIRECTION == "LONG":
        ent_side, exit_side = "buy",  "sell"
        sl_t   = round(p * (1 - SL_DIST_PCT),  2)
        sl_l   = round(sl_t * 0.995,           2)
        tgt_p  = round(p * (1 + TGT_DIST_PCT), 2)
    else:
        ent_side, exit_side = "sell", "buy"
        sl_t   = round(p * (1 + SL_DIST_PCT),  2)
        sl_l   = round(sl_t * 1.005,           2)
        tgt_p  = round(p * (1 - TGT_DIST_PCT), 2)
    log.info(f"plan: entry={ent_side} sl_trig={sl_t} sl_lim={sl_l} target={tgt_p}")

    eid = sid = tid = ""
    try:
        # Step 1 — entry
        log.info(f"[1] MARKET {ent_side.upper()}")
        oid, st, msg = b.market(TEST_STOCK, ent_side, TEST_QTY)
        if not oid: log.error(f"entry rejected: {msg}"); return
        eid = oid
        log.info(f"  entry_id={eid}")
        time.sleep(3)
        log.info(f"  status: {b.status(eid)}  qty: {b.qty(TEST_STOCK)}")
        if b.qty(TEST_STOCK) == 0:
            log.error("flat after entry — abort"); return

        # Step 2 — SL
        log.info(f"[2] SL trigger={sl_t} limit={sl_l}")
        oid, st, msg = b.sl(TEST_STOCK, exit_side, TEST_QTY, sl_t, sl_l)
        if oid: sid = oid; log.info(f"  sl_id={sid}  status: {b.status(sid)}")
        else:   log.error(f"  SL failed: {msg}")

        # Step 3 — target
        log.info(f"[3] TARGET LIMIT @ {tgt_p}")
        oid, st, msg = b.limit(TEST_STOCK, exit_side, TEST_QTY, tgt_p)
        if oid: tid = oid; log.info(f"  tgt_id={tid}  status: {b.status(tid)}")
        else:   log.error(f"  TARGET failed: {msg}")

        # Step 4 — pause for UI
        log.info(f"[4] verify in Groww UI: entry={eid} sl={sid} tgt={tid}")
        for s in range(PAUSE_SECS, 0, -5):
            log.info(f"  …{s}s"); time.sleep(5)

        # Step 5 — cancel both
        log.info("[5] cancel SL + TARGET")
        if sid: log.info(f"  sl cancel: {'✓' if b.cancel(sid) else '✗'}")
        if tid: log.info(f"  tgt cancel: {'✓' if b.cancel(tid) else '✗'}")

    finally:
        # Step 6 — ALWAYS close
        log.info("[6] CLEANUP — close position")
        q = b.qty(TEST_STOCK)
        log.info(f"  pre-cleanup qty: {q}")
        if q is None:
            cq, cs = TEST_QTY, ("sell" if TEST_DIRECTION == "LONG" else "buy")
            log.warning("qty unknown — forcing market exit")
        elif q == 0:
            cq, cs = 0, ""
            log.info("  already flat")
        else:
            cq, cs = abs(q), ("sell" if q > 0 else "buy")
        if cq:
            oid, st, msg = b.market(TEST_STOCK, cs, cq)
            if oid:
                log.info(f"  exit_id={oid}")
                time.sleep(3)
                f = b.qty(TEST_STOCK)
                log.info(f"  final qty: {f}")
                if f != 0:
                    log.error(f"⚠ NOT FLAT (qty={f}) — manual action required")
            else:
                log.error(f"⚠ EXIT FAILED: {msg} — MANUAL ACTION REQUIRED")

    print("="*60)
    print("DONE — verify Groww UI: orders + positions=flat")
    print("="*60)


if __name__ == "__main__":
    main()
