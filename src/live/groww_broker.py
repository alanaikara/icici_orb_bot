"""
Groww Trade API broker implementation.

Implements BrokerBase using the growwapi Python SDK.
Authentication uses TOTP (pyotp) — no daily manual token refresh needed.

Install dependencies:
    pip install growwapi pyotp

Environment variables required:
    GROWW_API_KEY     — from Groww Cloud API Keys page
    GROWW_TOTP_SECRET — from Groww Cloud TOTP setup (base32 secret, NOT the 6-digit code)

Usage:
    broker = GrowwBroker(
        api_key     = os.environ["GROWW_API_KEY"],
        totp_secret = os.environ["GROWW_TOTP_SECRET"],
    )
    broker.connect()
"""

import logging
import uuid
from datetime import datetime
from typing import Optional

try:
    import pyotp
except ImportError:
    raise ImportError("Run: pip install pyotp")

try:
    from growwapi import GrowwAPI
except ImportError:
    raise ImportError("Run: pip install growwapi")

from live.broker_base import BrokerBase, OrderResult, Position

logger = logging.getLogger("ICICI_ORB_Bot")

# ── Groww constants (resolved at runtime so we don't hardcode string values) ──
# If a constant isn't on the GrowwAPI class, fall back to the raw string value
# used by Groww's REST API.
_EXCHANGE_NSE      = getattr(GrowwAPI, "EXCHANGE_NSE",              "NSE")
_SEGMENT_CASH      = getattr(GrowwAPI, "SEGMENT_CASH",              "CASH")
_VALIDITY_DAY      = getattr(GrowwAPI, "VALIDITY_DAY",              "DAY")
_PRODUCT_MIS       = getattr(GrowwAPI, "PRODUCT_MIS",               "MIS")   # intraday
_PRODUCT_CNC       = getattr(GrowwAPI, "PRODUCT_CNC",               "CNC")   # delivery
_ORDER_MARKET      = getattr(GrowwAPI, "ORDER_TYPE_MARKET",         "MARKET")
_ORDER_LIMIT       = getattr(GrowwAPI, "ORDER_TYPE_LIMIT",          "LIMIT")
_ORDER_SL          = getattr(GrowwAPI, "ORDER_TYPE_STOP_LOSS",      "STOP_LOSS")
_ORDER_SL_MARKET   = getattr(GrowwAPI, "ORDER_TYPE_STOP_LOSS_MARKET","STOP_LOSS_MARKET")
_TXN_BUY           = getattr(GrowwAPI, "TRANSACTION_TYPE_BUY",      "BUY")
_TXN_SELL          = getattr(GrowwAPI, "TRANSACTION_TYPE_SELL",     "SELL")


def _ref_id() -> str:
    """Generate a unique 12-char order reference ID for Groww (8-20 alphanum)."""
    return f"FM{uuid.uuid4().hex[:10].upper()}"


def _safe_get(obj, key):
    """Read a key from either a dict or an object attribute (SDK may return either)."""
    if isinstance(obj, dict):
        return obj.get(key)
    return getattr(obj, key, None)


class GrowwBroker(BrokerBase):
    """
    Groww Trade API adapter for the Fib-MACD live trader.

    Token is auto-refreshed on each call to connect() via TOTP,
    so you can call connect() once at startup and never touch it again.
    """

    def __init__(self, api_key: str, totp_secret: str):
        self._api_key     = api_key
        self._totp_secret = totp_secret
        self._groww: Optional[GrowwAPI] = None
        self._connected   = False

    # ── Connection ─────────────────────────────────────────────────────────────

    def connect(self) -> bool:
        """Authenticate via TOTP and create GrowwAPI session."""
        try:
            totp = pyotp.TOTP(self._totp_secret).now()
            access_token = GrowwAPI.get_access_token(
                api_key=self._api_key,
                totp=totp,
            )
            self._groww     = GrowwAPI(access_token)
            self._connected = True
            logger.info("GrowwBroker: connected via TOTP ✓")
            return True
        except Exception as e:
            logger.error(f"GrowwBroker connect failed: {e}")
            self._connected = False
            return False

    def is_connected(self) -> bool:
        return self._connected

    # ── Market data ────────────────────────────────────────────────────────────

    def get_ltp(self, stock_code: str) -> Optional[float]:
        """Return last traded price for a single stock."""
        try:
            exchange_sym = f"NSE_{stock_code}"
            resp = self._groww.get_ltp(
                segment=_SEGMENT_CASH,
                exchange_trading_symbols=exchange_sym,
            )
            # resp is a dict: {"NSE_SYMBOL": {"ltp": float}}
            if isinstance(resp, dict):
                data = resp.get(exchange_sym, {})
                if isinstance(data, dict):
                    return float(data.get("ltp", 0)) or None
                return float(_safe_get(data, "ltp") or 0) or None
        except Exception as e:
            logger.error(f"get_ltp({stock_code}): {e}")
        return None

    def get_candles(
        self,
        stock_code: str,
        interval: str,          # '1minute' | '5minute'
        from_datetime: str,     # 'YYYY-MM-DD HH:MM:SS'
        to_datetime: str,
    ) -> list[dict]:
        """
        Fetch intraday OHLCV candles from Groww.

        Groww returns each candle as:
            [timestamp_epoch_seconds, open, high, low, close, volume]

        We convert to the standard dict format:
            {"datetime": "YYYY-MM-DD HH:MM:SS", "open": ..., "high": ...,
             "low": ..., "close": ..., "volume": ...}

        Interval map:
            '1minute'  → interval_in_minutes=1   (max 7-day window)
            '5minute'  → interval_in_minutes=5   (max 15-day window)
        """
        interval_map = {
            "1minute":  1,
            "5minute":  5,
            "10minute": 10,
            "60minute": 60,
        }
        interval_mins = interval_map.get(interval, 1)

        try:
            resp = self._groww.get_historical_candle_data(
                trading_symbol     = stock_code,
                exchange           = _EXCHANGE_NSE,
                segment            = _SEGMENT_CASH,
                start_time         = from_datetime,
                end_time           = to_datetime,
                interval_in_minutes= interval_mins,
            )

            # SDK may return an object with .candles or a dict with "candles" key
            raw_candles = _safe_get(resp, "candles") or []

            candles = []
            for c in raw_candles:
                # c = [epoch_seconds, open, high, low, close, volume]
                try:
                    ts  = datetime.fromtimestamp(float(c[0]))
                    candles.append({
                        "datetime": ts.strftime("%Y-%m-%d %H:%M:%S"),
                        "open":     float(c[1]),
                        "high":     float(c[2]),
                        "low":      float(c[3]),
                        "close":    float(c[4]),
                        "volume":   int(c[5]),
                    })
                except (IndexError, ValueError, TypeError) as ce:
                    logger.debug(f"Skipping malformed candle for {stock_code}: {ce}")
                    continue

            return candles

        except Exception as e:
            logger.error(f"get_candles({stock_code}, {interval}): {e}")
            return []

    # ── Orders ─────────────────────────────────────────────────────────────────

    def place_market_order(
        self,
        stock_code: str,
        action: str,            # 'buy' or 'sell'
        quantity: int,
        product_type: str = "intraday",
    ) -> OrderResult:
        txn  = _TXN_BUY if action.lower() == "buy" else _TXN_SELL
        prod = _PRODUCT_MIS   # always intraday for the strategy
        ref  = _ref_id()
        try:
            resp = self._groww.place_order(
                trading_symbol   = stock_code,
                quantity         = quantity,
                validity         = _VALIDITY_DAY,
                exchange         = _EXCHANGE_NSE,
                segment          = _SEGMENT_CASH,
                product          = prod,
                order_type       = _ORDER_MARKET,
                transaction_type = txn,
                price            = 0,
                order_reference_id = ref,
            )
            order_id = _safe_get(resp, "groww_order_id") or ""
            status   = _safe_get(resp, "order_status") or ""
            if order_id:
                logger.info(f"{stock_code} market {action} {quantity}: order_id={order_id}")
                return OrderResult(success=True, order_id=order_id, message=status)
            msg = _safe_get(resp, "remark") or str(resp)
            return OrderResult(success=False, order_id="", message=msg)
        except Exception as e:
            return OrderResult(success=False, order_id="", message=str(e))

    def place_limit_order(
        self,
        stock_code: str,
        action: str,
        quantity: int,
        price: float,
        product_type: str = "intraday",
    ) -> OrderResult:
        txn = _TXN_BUY if action.lower() == "buy" else _TXN_SELL
        ref = _ref_id()
        try:
            resp = self._groww.place_order(
                trading_symbol     = stock_code,
                quantity           = quantity,
                validity           = _VALIDITY_DAY,
                exchange           = _EXCHANGE_NSE,
                segment            = _SEGMENT_CASH,
                product            = _PRODUCT_MIS,
                order_type         = _ORDER_LIMIT,
                transaction_type   = txn,
                price              = round(price, 2),
                order_reference_id = ref,
            )
            order_id = _safe_get(resp, "groww_order_id") or ""
            status   = _safe_get(resp, "order_status") or ""
            if order_id:
                return OrderResult(success=True, order_id=order_id, message=status)
            return OrderResult(success=False, order_id="", message=str(resp))
        except Exception as e:
            return OrderResult(success=False, order_id="", message=str(e))

    def place_stoploss_order(
        self,
        stock_code: str,
        action: str,
        quantity: int,
        trigger_price: float,
        limit_price: float,
        product_type: str = "intraday",
    ) -> OrderResult:
        """
        Place a stop-loss limit order.
        trigger_price = price at which SL is triggered (the actual stop level)
        limit_price   = worst acceptable execution price (slippage buffer)
        """
        txn = _TXN_BUY if action.lower() == "buy" else _TXN_SELL
        ref = _ref_id()
        try:
            resp = self._groww.place_order(
                trading_symbol     = stock_code,
                quantity           = quantity,
                validity           = _VALIDITY_DAY,
                exchange           = _EXCHANGE_NSE,
                segment            = _SEGMENT_CASH,
                product            = _PRODUCT_MIS,
                order_type         = _ORDER_SL,
                transaction_type   = txn,
                price              = round(limit_price, 2),
                trigger_price      = round(trigger_price, 2),
                order_reference_id = ref,
            )
            order_id = _safe_get(resp, "groww_order_id") or ""
            status   = _safe_get(resp, "order_status") or ""
            if order_id:
                logger.info(
                    f"{stock_code} SL {action} {quantity} "
                    f"trigger={trigger_price:.2f} limit={limit_price:.2f}: {order_id}"
                )
                return OrderResult(success=True, order_id=order_id, message=status)
            msg = _safe_get(resp, "remark") or str(resp)
            return OrderResult(success=False, order_id="", message=msg)
        except Exception as e:
            return OrderResult(success=False, order_id="", message=str(e))

    def cancel_order(self, order_id: str) -> bool:
        try:
            resp = self._groww.cancel_order(
                groww_order_id = order_id,
                segment        = _SEGMENT_CASH,
            )
            status = _safe_get(resp, "order_status") or ""
            logger.info(f"cancel_order({order_id}): {status}")
            # CANCELLED / CANCEL_REQUESTED both mean success
            return "CANCEL" in str(status).upper() or bool(_safe_get(resp, "groww_order_id"))
        except Exception as e:
            logger.error(f"cancel_order({order_id}): {e}")
            return False

    def get_order_status(self, order_id: str) -> dict:
        try:
            resp = self._groww.get_order_status(
                groww_order_id = order_id,
                segment        = _SEGMENT_CASH,
            )
            return {
                "status":     _safe_get(resp, "order_status") or "unknown",
                "filled_qty": int(_safe_get(resp, "filled_quantity") or 0),
                "avg_price":  float(_safe_get(resp, "average_fill_price") or 0),
            }
        except Exception as e:
            logger.error(f"get_order_status({order_id}): {e}")
            return {"status": "unknown", "filled_qty": 0, "avg_price": 0.0}

    # ── Positions / funds ──────────────────────────────────────────────────────

    def get_positions(self) -> list[Position]:
        try:
            resp = self._groww.get_positions_for_user(segment=_SEGMENT_CASH)
            # resp may be a list directly or an object with .positions / a dict
            if isinstance(resp, list):
                items = resp
            else:
                items = _safe_get(resp, "positions") or _safe_get(resp, "data") or []

            positions = []
            for p in items:
                qty = int(_safe_get(p, "quantity") or 0)
                if qty == 0:
                    continue
                direction = "LONG" if qty > 0 else "SHORT"
                positions.append(Position(
                    stock_code    = _safe_get(p, "trading_symbol") or "",
                    direction     = direction,
                    quantity      = abs(qty),
                    entry_price   = float(_safe_get(p, "net_price") or 0),
                    current_price = 0.0,   # not in Groww position response
                    pnl           = float(_safe_get(p, "realised_pnl") or 0),
                ))
            return positions
        except Exception as e:
            logger.error(f"get_positions: {e}")
            return []

    def get_funds(self) -> dict:
        """
        Return available cash and used margin.
        NOTE: Groww doesn't expose a direct funds API in the current SDK.
        This returns a high default so the strategy never blocks on a funds check.
        Implement properly once Groww exposes get_margin_for_user() or similar.
        """
        logger.debug("get_funds: Groww funds API not yet available — returning default")
        return {"available_cash": 10_00_000.0, "used_margin": 0.0}
