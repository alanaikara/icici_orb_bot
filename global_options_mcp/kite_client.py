"""Zerodha Kite Connect adapter for long-dated option research and orders.

Safety model
------------
* Read tools (profile, holdings, positions, chains) always work.
* Orders are PREVIEW ONLY unless env LIVE_ORDERS=true.
* Even with LIVE_ORDERS=true, the caller must pass confirm=True.
* Only LIMIT orders, product NRML (carry forward), exchange NFO.
* Hard caps: MAX_ORDER_PREMIUM (Rs per order) and MAX_LOTS_PER_ORDER.
"""
from __future__ import annotations

import datetime as dt
import os
import time
from dataclasses import dataclass, field
from typing import Any

from .analysis import nearest_strikes, safe_float
from .options_math import bs_greeks, implied_vol

# NFO underlying name -> NSE spot quote symbol
INDEX_SPOT = {
    "NIFTY": "NSE:NIFTY 50",
    "BANKNIFTY": "NSE:NIFTY BANK",
    "FINNIFTY": "NSE:NIFTY FIN SERVICE",
    "MIDCPNIFTY": "NSE:NIFTY MID SELECT",
    "NIFTYNXT50": "NSE:NIFTY NEXT 50",
}


def _env_bool(name: str, default: bool = False) -> bool:
    return os.getenv(name, str(default)).strip().lower() in ("1", "true", "yes", "on")


@dataclass
class OrderLimits:
    live_orders: bool = field(default_factory=lambda: _env_bool("LIVE_ORDERS", False))
    max_order_premium: float = field(
        default_factory=lambda: float(os.getenv("MAX_ORDER_PREMIUM", "50000")))
    max_lots_per_order: int = field(
        default_factory=lambda: int(os.getenv("MAX_LOTS_PER_ORDER", "2")))


class KiteNotConfigured(RuntimeError):
    pass


class ZerodhaClient:
    """Thin wrapper over `kiteconnect.KiteConnect`. `kite` can be injected for tests."""

    def __init__(self, kite: Any | None = None, limits: OrderLimits | None = None,
                 risk_free_rate: float | None = None):
        self._kite = kite
        self.limits = limits or OrderLimits()
        self.r = risk_free_rate if risk_free_rate is not None else float(
            os.getenv("RISK_FREE_RATE", "0.065"))
        self._instruments: list[dict] | None = None
        self._instruments_day: dt.date | None = None

    # ---------------------------------------------------------- session --
    @property
    def kite(self):
        if self._kite is None:
            api_key = os.getenv("KITE_API_KEY")
            token = os.getenv("KITE_ACCESS_TOKEN")
            if not api_key or not token:
                raise KiteNotConfigured(
                    "Set KITE_API_KEY and KITE_ACCESS_TOKEN (run: python -m global_options_mcp.kite_login)."
                )
            from kiteconnect import KiteConnect

            k = KiteConnect(api_key=api_key)
            k.set_access_token(token)
            self._kite = k
        return self._kite

    # ----------------------------------------------------------- account --
    def profile(self) -> dict:
        p = self.kite.profile()
        return {k: p.get(k) for k in ("user_id", "user_name", "broker", "exchanges", "products")}

    def margins(self) -> dict:
        return self.kite.margins()

    def holdings(self) -> list[dict]:
        return self.kite.holdings()

    def positions(self) -> dict:
        return self.kite.positions()

    def orders(self) -> list[dict]:
        return self.kite.orders()

    # ------------------------------------------------------- instruments --
    def nfo_instruments(self) -> list[dict]:
        today = dt.date.today()
        if self._instruments is None or self._instruments_day != today:
            self._instruments = self.kite.instruments("NFO")
            self._instruments_day = today
        return self._instruments

    def _options_for(self, underlying: str) -> list[dict]:
        u = underlying.upper()
        return [i for i in self.nfo_instruments()
                if i.get("name") == u and i.get("instrument_type") in ("CE", "PE")]

    @staticmethod
    def _as_date(x) -> dt.date:
        if isinstance(x, dt.datetime):
            return x.date()
        if isinstance(x, dt.date):
            return x
        return dt.date.fromisoformat(str(x)[:10])

    def option_expiries(self, underlying: str, min_days: int = 0) -> list[dict]:
        today = dt.date.today()
        seen: dict[dt.date, int] = {}
        for i in self._options_for(underlying):
            d = self._as_date(i["expiry"])
            seen[d] = seen.get(d, 0) + 1
        out = []
        for d in sorted(seen):
            days = (d - today).days
            if days >= min_days:
                out.append({"expiry": d.isoformat(), "days": days, "contracts": seen[d]})
        return out

    def spot_symbol(self, underlying: str) -> str:
        u = underlying.upper()
        return INDEX_SPOT.get(u, f"NSE:{u}")

    def spot(self, underlying: str) -> float:
        sym = self.spot_symbol(underlying)
        q = self.kite.ltp([sym])
        return float(q[sym]["last_price"])

    # ------------------------------------------------------ option chain --
    def option_chain(self, underlying: str, expiry: str, width: int = 10,
                     q_div: float = 0.0) -> dict:
        """Chain around ATM with live quote, bid/ask, OI, model IV and Greeks."""
        exp = dt.date.fromisoformat(expiry)
        opts = [i for i in self._options_for(underlying) if self._as_date(i["expiry"]) == exp]
        if not opts:
            raise ValueError(f"No {underlying} options for expiry {expiry}. "
                             "Call kite_option_expiries first.")
        spot = self.spot(underlying)
        strikes = nearest_strikes(spot, [float(i["strike"]) for i in opts], width)
        chosen = [i for i in opts if float(i["strike"]) in strikes]
        keys = [f"NFO:{i['tradingsymbol']}" for i in chosen]
        quotes: dict = {}
        for n in range(0, len(keys), 250):  # Kite allows ~500 per call; stay safe
            quotes.update(self.kite.quote(keys[n:n + 250]))

        days = max((exp - dt.date.today()).days, 0) + 0.5  # count expiry day
        T = days / 365.0
        rows: dict[float, dict] = {}
        for i in chosen:
            K = float(i["strike"])
            qt = quotes.get(f"NFO:{i['tradingsymbol']}", {})
            depth = qt.get("depth", {})
            bid = safe_float((depth.get("buy") or [{}])[0].get("price"))
            ask = safe_float((depth.get("sell") or [{}])[0].get("price"))
            ltp = safe_float(qt.get("last_price"))
            mid = (bid + ask) / 2 if bid and ask else ltp
            kind = "call" if i["instrument_type"] == "CE" else "put"
            iv = implied_vol(mid, spot, K, T, self.r, kind, q_div) if mid else None
            g = bs_greeks(spot, K, T, self.r, iv, kind, q_div).as_dict() if iv else None
            leg = {
                "tradingsymbol": i["tradingsymbol"], "lot_size": i.get("lot_size"),
                "ltp": ltp, "bid": bid, "ask": ask, "mid": mid,
                "spread_pct": round((ask - bid) / mid * 100, 2) if bid and ask and mid else None,
                "oi": qt.get("oi"), "volume": qt.get("volume"),
                "iv": round(iv, 4) if iv else None, "greeks": g,
            }
            rows.setdefault(K, {"strike": K})[i["instrument_type"]] = leg
        lot = chosen[0].get("lot_size") if chosen else None
        return {
            "underlying": underlying.upper(), "spot": spot, "expiry": expiry,
            "days_to_expiry": round(days, 1), "lot_size": lot, "risk_free_rate": self.r,
            "chain": [rows[k] for k in sorted(rows)],
            "as_of": time.strftime("%Y-%m-%d %H:%M:%S"),
        }

    # ------------------------------------------------------------ orders --
    def place_option_order(self, tradingsymbol: str, side: str, lots: int,
                           limit_price: float, confirm: bool = False,
                           tag: str = "gomcp") -> dict:
        side_u = side.upper()
        if side_u not in ("BUY", "SELL"):
            raise ValueError("side must be BUY or SELL")
        if lots <= 0 or limit_price <= 0:
            raise ValueError("lots and limit_price must be positive")
        inst = next((i for i in self.nfo_instruments()
                     if i.get("tradingsymbol") == tradingsymbol), None)
        if inst is None or inst.get("instrument_type") not in ("CE", "PE"):
            raise ValueError(f"{tradingsymbol} is not an NFO option")
        lot_size = int(inst.get("lot_size") or 1)
        qty = lots * lot_size
        premium = qty * limit_price

        problems = []
        if lots > self.limits.max_lots_per_order:
            problems.append(f"lots {lots} > MAX_LOTS_PER_ORDER {self.limits.max_lots_per_order}")
        if premium > self.limits.max_order_premium:
            problems.append(f"premium Rs {premium:,.0f} > MAX_ORDER_PREMIUM "
                            f"Rs {self.limits.max_order_premium:,.0f}")
        if side_u == "SELL":
            held = self._net_qty(tradingsymbol)
            if held < qty:
                problems.append("SELL allowed only to close an existing long "
                                f"(net qty held {held}, requested {qty}); naked selling is blocked")

        preview = {
            "exchange": "NFO", "tradingsymbol": tradingsymbol, "transaction_type": side_u,
            "quantity": qty, "lots": lots, "lot_size": lot_size, "order_type": "LIMIT",
            "price": limit_price, "product": "NRML", "validity": "DAY",
            "approx_premium": round(premium, 2),
        }
        if problems:
            return {"status": "rejected_by_guard", "problems": problems, "order": preview}
        if not self.limits.live_orders:
            return {"status": "preview_only",
                    "reason": "LIVE_ORDERS is not true. Nothing was sent to Zerodha.",
                    "order": preview}
        if not confirm:
            return {"status": "needs_confirmation",
                    "reason": "Call again with confirm=true after the human approves.",
                    "order": preview}
        order_id = self.kite.place_order(
            variety="regular", exchange="NFO", tradingsymbol=tradingsymbol,
            transaction_type=side_u, quantity=qty, product="NRML",
            order_type="LIMIT", price=limit_price, validity="DAY", tag=tag[:20],
        )
        return {"status": "placed", "order_id": order_id, "order": preview}

    def _net_qty(self, tradingsymbol: str) -> int:
        try:
            pos = self.kite.positions().get("net", [])
        except Exception:
            return 0
        return sum(int(p.get("quantity", 0)) for p in pos if p.get("tradingsymbol") == tradingsymbol)
