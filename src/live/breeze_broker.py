"""
ICICI Breeze Connect broker implementation.

Implements BrokerBase using the existing ICICIDirectAPI client.
Swap this out for Zerodha/Upstox/etc. by writing a new BrokerBase subclass.
"""

import logging
from datetime import datetime
from typing import Optional

from live.broker_base import BrokerBase, OrderResult, Position
from api.icici_api import ICICIDirectAPI

logger = logging.getLogger("ICICI_ORB_Bot")

# Breeze product type strings
PRODUCT_INTRADAY  = "intraday"
PRODUCT_DELIVERY  = "delivery"

# Breeze action strings
ACTION_BUY  = "buy"
ACTION_SELL = "sell"

# Breeze order type strings
ORDER_MARKET   = "market"
ORDER_LIMIT    = "limit"
ORDER_STOPLOSS = "stoploss"


class BreezeBroker(BrokerBase):
    """
    ICICI Direct (Breeze Connect) broker adapter.
    """

    def __init__(self, app_key: str, secret_key: str, session_token: str):
        self._api = ICICIDirectAPI(app_key, secret_key, session_token)
        self._connected = False

    # ── Connection ────────────────────────────────────────────────────────────

    def connect(self) -> bool:
        try:
            result = self._api.get_customer_details(
                self._api.session_token, self._api.app_key
            )
            self._connected = result.get("Status") == 200
            return self._connected
        except Exception as e:
            logger.error(f"Breeze connect failed: {e}")
            return False

    def is_connected(self) -> bool:
        return self._connected

    # ── Market data ───────────────────────────────────────────────────────────

    def get_ltp(self, stock_code: str) -> Optional[float]:
        try:
            resp = self._api.breeze.get_quotes(
                stock_code=stock_code,
                exchange_code="NSE",
                product_type="cash",
                expiry_date="",
                right="",
                strike_price="",
            )
            if resp and resp.get("Success"):
                return float(resp["Success"][0].get("ltp", 0))
        except Exception as e:
            logger.error(f"get_ltp({stock_code}): {e}")
        return None

    def get_candles(
        self,
        stock_code: str,
        interval: str,
        from_datetime: str,
        to_datetime: str,
    ) -> list[dict]:
        """
        Fetch intraday candles via Breeze get_historical_data_v2.
        interval: '1minute' | '5minute'
        """
        try:
            resp = self._api.get_historical_data_v2(
                stock_code=stock_code,
                exchange_code="NSE",
                from_date=from_datetime,
                to_date=to_datetime,
                interval=interval,
            )
            if resp and isinstance(resp, list):
                candles = []
                for c in resp:
                    candles.append({
                        "datetime": c.get("datetime", ""),
                        "open":   float(c.get("open",  0)),
                        "high":   float(c.get("high",  0)),
                        "low":    float(c.get("low",   0)),
                        "close":  float(c.get("close", 0)),
                        "volume": int(c.get("volume",  0)),
                    })
                return candles
        except Exception as e:
            logger.error(f"get_candles({stock_code}): {e}")
        return []

    # ── Orders ────────────────────────────────────────────────────────────────

    def place_market_order(
        self,
        stock_code: str,
        action: str,
        quantity: int,
        product_type: str = PRODUCT_INTRADAY,
    ) -> OrderResult:
        try:
            resp = self._api.breeze.place_order(
                stock_code=stock_code,
                exchange_code="NSE",
                product="cash",
                action=action,
                order_type=ORDER_MARKET,
                quantity=str(quantity),
                price="0",
                validity="day",
                disclosed_quantity="0",
                expiry_date="",
                right="",
                strike_price="",
                user_remark=f"fib_macd_{action}",
            )
            if resp and resp.get("Status") == 200:
                order_id = resp["Success"].get("order_id", "")
                return OrderResult(success=True, order_id=order_id, message="OK")
            msg = resp.get("Error", "Unknown error") if resp else "No response"
            return OrderResult(success=False, order_id="", message=str(msg))
        except Exception as e:
            return OrderResult(success=False, order_id="", message=str(e))

    def place_limit_order(
        self,
        stock_code: str,
        action: str,
        quantity: int,
        price: float,
        product_type: str = PRODUCT_INTRADAY,
    ) -> OrderResult:
        try:
            resp = self._api.breeze.place_order(
                stock_code=stock_code,
                exchange_code="NSE",
                product="cash",
                action=action,
                order_type=ORDER_LIMIT,
                quantity=str(quantity),
                price=str(round(price, 2)),
                validity="day",
                disclosed_quantity="0",
                expiry_date="",
                right="",
                strike_price="",
                user_remark=f"fib_macd_{action}_lmt",
            )
            if resp and resp.get("Status") == 200:
                order_id = resp["Success"].get("order_id", "")
                return OrderResult(success=True, order_id=order_id, message="OK")
            msg = resp.get("Error", "Unknown error") if resp else "No response"
            return OrderResult(success=False, order_id="", message=str(msg))
        except Exception as e:
            return OrderResult(success=False, order_id="", message=str(e))

    def place_stoploss_order(
        self,
        stock_code: str,
        action: str,
        quantity: int,
        trigger_price: float,
        limit_price: float,
        product_type: str = PRODUCT_INTRADAY,
    ) -> OrderResult:
        try:
            resp = self._api.breeze.place_order(
                stock_code=stock_code,
                exchange_code="NSE",
                product="cash",
                action=action,
                order_type=ORDER_STOPLOSS,
                quantity=str(quantity),
                price=str(round(limit_price, 2)),
                stoploss=str(round(trigger_price, 2)),
                validity="day",
                disclosed_quantity="0",
                expiry_date="",
                right="",
                strike_price="",
                user_remark=f"fib_macd_{action}_sl",
            )
            if resp and resp.get("Status") == 200:
                order_id = resp["Success"].get("order_id", "")
                return OrderResult(success=True, order_id=order_id, message="OK")
            msg = resp.get("Error", "Unknown error") if resp else "No response"
            return OrderResult(success=False, order_id="", message=str(msg))
        except Exception as e:
            return OrderResult(success=False, order_id="", message=str(e))

    def cancel_order(self, order_id: str) -> bool:
        try:
            resp = self._api.breeze.cancel_order(
                exchange_code="NSE",
                order_id=order_id,
            )
            return bool(resp and resp.get("Status") == 200)
        except Exception as e:
            logger.error(f"cancel_order({order_id}): {e}")
            return False

    def get_order_status(self, order_id: str) -> dict:
        try:
            resp = self._api.breeze.get_order_detail(
                exchange_code="NSE",
                order_id=order_id,
            )
            if resp and resp.get("Success"):
                o = resp["Success"][0]
                return {
                    "status":     o.get("status", "unknown"),
                    "filled_qty": int(o.get("quantity", 0)),
                    "avg_price":  float(o.get("average_price", 0)),
                }
        except Exception as e:
            logger.error(f"get_order_status({order_id}): {e}")
        return {"status": "unknown", "filled_qty": 0, "avg_price": 0.0}

    # ── Positions / funds ─────────────────────────────────────────────────────

    def get_positions(self) -> list[Position]:
        try:
            resp = self._api.breeze.get_portfolio_positions()
            positions = []
            if resp and resp.get("Success"):
                for p in resp["Success"]:
                    qty = int(p.get("quantity", 0))
                    if qty == 0:
                        continue
                    direction = "LONG" if qty > 0 else "SHORT"
                    positions.append(Position(
                        stock_code    = p.get("stock_code", ""),
                        direction     = direction,
                        quantity      = abs(qty),
                        entry_price   = float(p.get("average_price", 0)),
                        current_price = float(p.get("ltp", 0)),
                        pnl           = float(p.get("pnl", 0)),
                    ))
            return positions
        except Exception as e:
            logger.error(f"get_positions: {e}")
            return []

    def get_funds(self) -> dict:
        try:
            resp = self._api.breeze.get_funds()
            if resp and resp.get("Success"):
                f = resp["Success"]
                return {
                    "available_cash": float(f.get("net_balance", 0)),
                    "used_margin":    float(f.get("block_by_trade", 0)),
                }
        except Exception as e:
            logger.error(f"get_funds: {e}")
        return {"available_cash": 0.0, "used_margin": 0.0}
