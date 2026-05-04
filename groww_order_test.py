"""
Groww Order Pipeline Smoke Test
================================

Walks ONE share of a single stock through the full bracket-order flow so
you can confirm in the Groww UI that every order type is being accepted
and routed correctly:

    1. MARKET entry  (BUY for LONG, SELL for SHORT)
    2. STOP-LOSS order (set 5% away — won't fire during test)
    3. LIMIT target order (set 5% away — won't fire during test)
    4. ~30s pause so you can verify all 3 orders in Groww web UI
    5. Cancel SL + cancel target
    6. MARKET exit  (close the position)
    7. Verify position is flat

The SL trigger and target prices are deliberately set far from LTP so
neither leg accidentally fires during the test. The cleanup step in the
`finally` block ALWAYS runs — if anything errors mid-test, the script
still attempts to flatten the position.

USAGE
-----
1. Paste your Groww credentials below.
2. (Optional) change TEST_STOCK / TEST_DIRECTION / TEST_QTY.
3. Set `LIVE = True` to actually place real orders.
4. Run in Groww Cloud (or locally) and watch the log.

WHAT YOU SHOULD SEE
-------------------
Every step prints the order_id Groww assigned. After step 4 (the pause),
open Groww web UI → Orders tab. You should see:
   - 1 MARKET filled order (the entry)
   - 1 SL order in OPEN/PENDING state
   - 1 LIMIT order in OPEN/PENDING state

After the script completes, open Positions → should be flat.
"""

# ────────────────────────────────────────────────────────────────────────────
#                            CONFIGURATION ZONE
# ────────────────────────────────────────────────────────────────────────────

# ── Credentials (paste here, or pull from env vars before deploying) ─────────
GROWW_API_KEY = "your_api_key"      # ← replace
GROWW_SECRET  = "your_secret_key"   # ← replace

# ── What to test ─────────────────────────────────────────────────────────────
TEST_STOCK     = "ONGC"      # liquid, low-priced (≈₹250) → small capital risk
TEST_DIRECTION = "LONG"      # "LONG" (buy then sell) | "SHORT" (sell then buy)
TEST_QTY       = 1           # one share is enough to verify the API

# ── Safety distance for SL/target (% of LTP, well outside any 1-min move) ────
SL_DISTANCE_PCT     = 0.05   # 5% — SL won't fire during a ~1 min test
TARGET_DISTANCE_PCT = 0.05   # 5% — target won't fill during a ~1 min test

# ── Pause between placing orders and cancelling them, so you can see them ────
VERIFY_PAUSE_SECS = 30

# ── HARD SAFETY: must be explicitly flipped to True to place real orders ─────
LIVE = False

# ────────────────────────────────────────────────────────────────────────────
#                              END OF CONFIG
# ────────────────────────────────────────────────────────────────────────────

import logging
import time
import uuid
from dataclasses import dataclass
from typing import Optional

try:
    import pyotp                                       # noqa: F401  (kept for future TOTP)
except ImportError:
    pass

try:
    from growwapi import GrowwAPI
except ImportError:
    raise SystemExit("growwapi not installed — run: pip install growwapi")


# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("OrderTest")


# ── Groww API constants (resolved on connect) ─────────────────────────────────
_NSE    = "NSE"
_CASH   = "CASH"
_DAY    = "DAY"
_MIS    = "MIS"
_MARKET = "MARKET"
_LIMIT  = "LIMIT"
_SL     = "SL"
_BUY    = "BUY"
_SELL   = "SELL"


@dataclass
class OrderResult:
    success:  bool
    order_id: str
    message:  str


def _ref(prefix: str) -> str:
    """Generate a unique order_reference_id (8–20 alphanumeric chars)."""
    return f"{prefix}{uuid.uuid4().hex[:10].upper()}"


def _attr(obj, key):
    if isinstance(obj, dict):
        return obj.get(key)
    return getattr(obj, key, None)


# ════════════════════════════════════════════════════════════════════════════
#  MINIMAL GROWW BROKER (subset of the live trader's GrowwBroker)
# ════════════════════════════════════════════════════════════════════════════

class GrowwBroker:
    def __init__(self, api_key: str, secret: str):
        self._api_key = api_key
        self._secret  = secret
        self._g: Optional[GrowwAPI] = None

    def connect(self) -> bool:
        try:
            token   = GrowwAPI.get_access_token(api_key=self._api_key, secret=self._secret)
            self._g = GrowwAPI(token)
            global _NSE, _CASH, _DAY, _MIS, _MARKET, _LIMIT, _SL, _BUY, _SELL
            _NSE    = getattr(self._g, "EXCHANGE_NSE",                _NSE)
            _CASH   = getattr(self._g, "SEGMENT_CASH",                _CASH)
            _DAY    = getattr(self._g, "VALIDITY_DAY",                _DAY)
            _MIS    = getattr(self._g, "PRODUCT_MIS",                 _MIS)
            _MARKET = getattr(self._g, "ORDER_TYPE_MARKET",           _MARKET)
            _LIMIT  = getattr(self._g, "ORDER_TYPE_LIMIT",            _LIMIT)
            _SL     = getattr(self._g, "ORDER_TYPE_STOP_LOSS",        _SL)
            _BUY    = getattr(self._g, "TRANSACTION_TYPE_BUY",        _BUY)
            _SELL   = getattr(self._g, "TRANSACTION_TYPE_SELL",       _SELL)
            logger.info("Groww connected ✓")
            logger.info(
                f"API constants → NSE={_NSE!r} CASH={_CASH!r} MIS={_MIS!r} "
                f"MARKET={_MARKET!r} LIMIT={_LIMIT!r} SL={_SL!r}"
            )
            return True
        except Exception as e:
            logger.error(f"Groww connect failed: {e}")
            return False

    # ── Market data ──────────────────────────────────────────────────────────
    def get_ltp(self, symbol: str) -> float:
        try:
            resp = self._g.get_ltp(
                segment                  = self._g.SEGMENT_CASH,
                exchange_trading_symbols = (f"NSE_{symbol}",),
            )
            if isinstance(resp, dict):
                v = resp.get(f"NSE_{symbol}")
                if isinstance(v, (int, float)):
                    return float(v)
                if isinstance(v, dict):
                    return float(v.get("ltp") or 0)
                return float(_attr(v, "ltp") or 0)
        except Exception as e:
            logger.error(f"get_ltp({symbol}): {e}")
        return 0.0

    # ── Orders ───────────────────────────────────────────────────────────────
    def place_market_order(self, symbol, action, qty, ref_id=None):
        txn = _BUY if action.lower() == "buy" else _SELL
        ref = ref_id or _ref("M")
        logger.info(f"place_market_order → {symbol} {txn} {qty} ref={ref}")
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
                order_reference_id = ref,
            )
            logger.info(f"  raw resp: {resp}")
            oid = _attr(resp, "groww_order_id") or ""
            if oid:
                return OrderResult(True, oid, _attr(resp, "order_status") or "")
            return OrderResult(False, "", _attr(resp, "remark") or str(resp))
        except Exception as e:
            return OrderResult(False, "", str(e))

    def place_limit_order(self, symbol, action, qty, price, ref_id=None):
        txn = _BUY if action.lower() == "buy" else _SELL
        ref = ref_id or _ref("L")
        logger.info(f"place_limit_order → {symbol} {txn} {qty} @ limit={price:.2f} ref={ref}")
        try:
            resp = self._g.place_order(
                trading_symbol     = symbol,
                quantity           = qty,
                validity           = _DAY,
                exchange           = _NSE,
                segment            = _CASH,
                product            = _MIS,
                order_type         = _LIMIT,
                transaction_type   = txn,
                price              = round(price, 2),
                order_reference_id = ref,
            )
            logger.info(f"  raw resp: {resp}")
            oid = _attr(resp, "groww_order_id") or ""
            if oid:
                return OrderResult(True, oid, _attr(resp, "order_status") or "")
            return OrderResult(False, "", _attr(resp, "remark") or str(resp))
        except Exception as e:
            return OrderResult(False, "", str(e))

    def place_stoploss_order(self, symbol, action, qty, trigger, limit, ref_id=None):
        txn = _BUY if action.lower() == "buy" else _SELL
        ref = ref_id or _ref("S")
        logger.info(
            f"place_stoploss_order → {symbol} {txn} {qty} "
            f"trigger={trigger:.2f} limit={limit:.2f} ref={ref}"
        )
        try:
            resp = self._g.place_order(
                trading_symbol     = symbol,
                quantity           = qty,
                validity           = _DAY,
                exchange           = _NSE,
                segment            = _CASH,
                product            = _MIS,
                order_type         = _SL,
                transaction_type   = txn,
                price              = round(limit, 2),
                trigger_price      = round(trigger, 2),
                order_reference_id = ref,
            )
            logger.info(f"  raw resp: {resp}")
            oid = _attr(resp, "groww_order_id") or ""
            if oid:
                return OrderResult(True, oid, _attr(resp, "order_status") or "")
            return OrderResult(False, "", _attr(resp, "remark") or str(resp))
        except Exception as e:
            return OrderResult(False, "", str(e))

    def cancel_order(self, order_id) -> bool:
        logger.info(f"cancel_order: {order_id}")
        try:
            resp = self._g.cancel_order(groww_order_id=order_id, segment=_CASH)
            status = str(_attr(resp, "order_status") or "")
            logger.info(f"  status: {status!r}")
            return "CANCEL" in status.upper() or bool(_attr(resp, "groww_order_id"))
        except Exception as e:
            logger.error(f"cancel_order({order_id}): {e}")
            return False

    def get_order_status(self, order_id) -> str:
        try:
            resp = self._g.get_order_status(groww_order_id=order_id, segment=_CASH)
            return str(_attr(resp, "order_status") or "UNKNOWN").upper()
        except Exception as e:
            logger.warning(f"get_order_status({order_id}): {e}")
            return "UNKNOWN"

    def get_live_qty(self, symbol) -> Optional[int]:
        """credit_qty - debit_qty. >0 = long, <0 = short, 0 = flat, None = error."""
        try:
            resp = self._g.get_position_for_trading_symbol(
                trading_symbol = symbol,
                segment        = self._g.SEGMENT_CASH,
            )
            if resp is None:
                return 0
            credit = int(_attr(resp, "credit_quantity") or 0)
            debit  = int(_attr(resp, "debit_quantity")  or 0)
            return credit - debit
        except Exception as e:
            logger.warning(f"get_live_qty({symbol}): {e}")
            return None


# ════════════════════════════════════════════════════════════════════════════
#  TEST FLOW
# ════════════════════════════════════════════════════════════════════════════

def banner(title: str):
    line = "─" * 60
    logger.info(line)
    logger.info(f"  {title}")
    logger.info(line)


def main():
    print("=" * 64)
    print("  Groww Order Pipeline Smoke Test")
    print("=" * 64)
    print(f"  Stock:        {TEST_STOCK}")
    print(f"  Direction:    {TEST_DIRECTION}")
    print(f"  Quantity:     {TEST_QTY}")
    print(f"  SL distance:  {SL_DISTANCE_PCT*100:.1f}% (won't fire)")
    print(f"  TGT distance: {TARGET_DISTANCE_PCT*100:.1f}% (won't fill)")
    print(f"  LIVE mode:    {LIVE}")
    print("=" * 64)

    if not LIVE:
        print()
        print("  ⚠  LIVE = False — no orders will be placed.")
        print("     Set LIVE = True at top of this file to run the smoke test.")
        return

    if TEST_DIRECTION not in ("LONG", "SHORT"):
        logger.error(f"TEST_DIRECTION must be LONG or SHORT, got {TEST_DIRECTION!r}")
        return
    if TEST_QTY < 1:
        logger.error("TEST_QTY must be ≥ 1")
        return

    broker = GrowwBroker(GROWW_API_KEY, GROWW_SECRET)
    if not broker.connect():
        logger.error("Aborting — could not connect to Groww")
        return

    # ── Step 0: Get LTP and compute SL / target ──────────────────────────────
    banner("STEP 0 — Fetch LTP")
    ltp = broker.get_ltp(TEST_STOCK)
    if ltp <= 0:
        logger.error(f"No LTP for {TEST_STOCK} — aborting (symbol mismatch?)")
        return
    logger.info(f"{TEST_STOCK} LTP = ₹{ltp:.2f}")

    if TEST_DIRECTION == "LONG":
        entry_side = "buy"
        exit_side  = "sell"
        sl_trigger = round(ltp * (1 - SL_DISTANCE_PCT), 2)
        sl_limit   = round(sl_trigger * 0.995,          2)   # 0.5% extra slippage
        target_px  = round(ltp * (1 + TARGET_DISTANCE_PCT), 2)
    else:  # SHORT
        entry_side = "sell"
        exit_side  = "buy"
        sl_trigger = round(ltp * (1 + SL_DISTANCE_PCT), 2)
        sl_limit   = round(sl_trigger * 1.005,          2)
        target_px  = round(ltp * (1 - TARGET_DISTANCE_PCT), 2)

    logger.info(f"Plan: {TEST_DIRECTION} {TEST_QTY} {TEST_STOCK}")
    logger.info(f"  entry:    market {entry_side.upper()} → ~₹{ltp:.2f}")
    logger.info(f"  SL:       trigger {sl_trigger}, limit {sl_limit}")
    logger.info(f"  TARGET:   limit {target_px}")
    logger.info(f"  exit:     market {exit_side.upper()}")

    entry_id = sl_id = tgt_id = ""
    try:
        # ── Step 1: Place MARKET entry ────────────────────────────────────────
        banner(f"STEP 1 — Place MARKET entry ({entry_side.upper()} {TEST_QTY})")
        ent_res = broker.place_market_order(TEST_STOCK, entry_side, TEST_QTY)
        if not ent_res.success:
            logger.error(f"Entry rejected: {ent_res.message}")
            return
        entry_id = ent_res.order_id
        logger.info(f"✓ Entry order_id: {entry_id}")
        logger.info("  Waiting 3s for fill…")
        time.sleep(3)
        logger.info(f"  Status: {broker.get_order_status(entry_id)}")
        live_qty = broker.get_live_qty(TEST_STOCK)
        logger.info(f"  Live position qty: {live_qty}")
        if live_qty == 0:
            logger.error("Position is flat after entry — order did not fill. Aborting.")
            return

        # ── Step 2: Place STOP-LOSS order ─────────────────────────────────────
        banner(f"STEP 2 — Place STOP-LOSS (trigger {sl_trigger}, limit {sl_limit})")
        sl_res = broker.place_stoploss_order(
            TEST_STOCK, exit_side, TEST_QTY,
            trigger=sl_trigger, limit=sl_limit,
        )
        if sl_res.success:
            sl_id = sl_res.order_id
            logger.info(f"✓ SL order_id: {sl_id}")
            time.sleep(1)
            logger.info(f"  Status: {broker.get_order_status(sl_id)}")
        else:
            logger.error(f"SL placement failed: {sl_res.message}")

        # ── Step 3: Place TARGET LIMIT order ──────────────────────────────────
        banner(f"STEP 3 — Place TARGET LIMIT @ {target_px}")
        tgt_res = broker.place_limit_order(
            TEST_STOCK, exit_side, TEST_QTY, price=target_px,
        )
        if tgt_res.success:
            tgt_id = tgt_res.order_id
            logger.info(f"✓ TARGET order_id: {tgt_id}")
            time.sleep(1)
            logger.info(f"  Status: {broker.get_order_status(tgt_id)}")
        else:
            logger.error(f"TARGET placement failed: {tgt_res.message}")

        # ── Step 4: Pause for visual verification in Groww UI ─────────────────
        banner(f"STEP 4 — Verify in Groww UI ({VERIFY_PAUSE_SECS}s pause)")
        logger.info("Open Groww web → Orders. You should see:")
        logger.info(f"  • MARKET filled order id={entry_id}")
        if sl_id:
            logger.info(f"  • SL pending     order id={sl_id}")
        if tgt_id:
            logger.info(f"  • LIMIT pending  order id={tgt_id}")
        for sec in range(VERIFY_PAUSE_SECS, 0, -5):
            logger.info(f"  …continuing in {sec}s")
            time.sleep(5)

        # ── Step 5: Cancel SL + TARGET ────────────────────────────────────────
        banner("STEP 5 — Cancel SL and TARGET orders")
        if sl_id:
            ok = broker.cancel_order(sl_id)
            logger.info(f"  SL cancel: {'✓' if ok else '✗'}")
        if tgt_id:
            ok = broker.cancel_order(tgt_id)
            logger.info(f"  TARGET cancel: {'✓' if ok else '✗'}")

    finally:
        # ── Step 6: ALWAYS try to close the position ──────────────────────────
        banner("STEP 6 — CLEANUP: close position (market exit)")
        live_qty = broker.get_live_qty(TEST_STOCK)
        logger.info(f"Pre-cleanup live qty: {live_qty}")
        if live_qty is None:
            logger.warning("Could not read position — assuming open. Forcing market exit.")
            close_qty  = TEST_QTY
            close_side = "sell" if TEST_DIRECTION == "LONG" else "buy"
        elif live_qty == 0:
            logger.info("Already flat — nothing to close.")
            close_qty = 0
            close_side = ""
        else:
            close_qty  = abs(live_qty)
            close_side = "sell" if live_qty > 0 else "buy"

        if close_qty > 0:
            res = broker.place_market_order(TEST_STOCK, close_side, close_qty)
            if res.success:
                logger.info(f"✓ Exit order_id: {res.order_id}")
                time.sleep(3)
                final_qty = broker.get_live_qty(TEST_STOCK)
                logger.info(f"  Final position: {final_qty}")
                if final_qty != 0:
                    logger.error(
                        f"⚠  Position is NOT flat after exit "
                        f"(qty={final_qty}). Manual action required!"
                    )
            else:
                logger.error(
                    f"⚠  EXIT FAILED: {res.message} — MANUAL ACTION REQUIRED. "
                    f"Stock={TEST_STOCK}, qty={close_qty}, side={close_side}"
                )

    print()
    print("=" * 64)
    print("  ORDER PIPELINE SMOKE TEST COMPLETE")
    print("=" * 64)
    print("  Verify in Groww UI:")
    print(f"    Orders → entry filled, SL cancelled, target cancelled, exit filled")
    print(f"    Positions → {TEST_STOCK} should be flat (qty=0)")
    print("=" * 64)


if __name__ == "__main__":
    main()
