"""Shared "long option ideas" logic for the MCP server and the dashboard."""
from __future__ import annotations

from .analysis import macro_regime, rank_long_option_candidates
from .global_data import GlobalData
from .kite_client import ZerodhaClient
from .options_math import historical_vol

YAHOO_SYMBOL = {"NIFTY": "^NSEI", "BANKNIFTY": "^NSEBANK", "FINNIFTY": "NIFTY_FIN_SERVICE.NS"}


def yahoo_symbol(underlying: str) -> str:
    u = underlying.upper()
    return YAHOO_SYMBOL.get(u, f"{u}.NS")


def underlying_hv(data: GlobalData, underlying: str) -> float | None:
    return historical_vol(data.closes(yahoo_symbol(underlying), "1y"))


def long_option_ideas(zerodha: ZerodhaClient, data: GlobalData, underlying: str, expiry: str,
                      view: str = "bullish", capital: float | None = None,
                      max_risk_pct: float = 2.0, width: int = 12,
                      max_spread_pct: float = 5.0, include_macro: bool = True) -> dict:
    """Live chain + historical vol (+ macro regime) -> ranked strikes.

    Raises broker errors; callers decide how to show them.
    """
    chain = zerodha.option_chain(underlying, expiry, width)
    side = "CE" if view.lower().startswith("bull") else "PE"
    iv_map, prem, dropped = {}, {}, []
    for row in chain["chain"]:
        leg = row.get(side)
        if not leg or not leg.get("iv"):
            continue
        if leg.get("spread_pct") is not None and leg["spread_pct"] > max_spread_pct:
            dropped.append({"strike": row["strike"], "spread_pct": leg["spread_pct"]})
            continue
        iv_map[row["strike"]] = leg["iv"]
        prem[row["strike"]] = leg.get("ask") or leg.get("mid")  # buyers pay the ask

    hv = underlying_hv(data, underlying)
    ranked = rank_long_option_candidates(
        chain["spot"], list(iv_map), chain["days_to_expiry"],
        "call" if side == "CE" else "put", iv_map, prem, hv, zerodha.r,
        lot_size=int(chain.get("lot_size") or 1), capital=capital, max_risk_pct=max_risk_pct)
    ranked["historical_vol_1y"] = round(hv, 4) if hv else None
    ranked["dropped_illiquid"] = dropped
    ranked["lot_size"] = chain.get("lot_size")
    ranked["expiry"] = expiry
    if include_macro:
        ranked["macro"] = macro_regime(data.stats)
    legs = {r["strike"]: (r.get(side) or {}) for r in chain["chain"]}
    for c in ranked["candidates"]:
        leg = legs.get(c["strike"], {})
        c["tradingsymbol"] = leg.get("tradingsymbol")
        c["bid"], c["ask"] = leg.get("bid"), leg.get("ask")
        c["spread_pct"], c["oi"] = leg.get("spread_pct"), leg.get("oi")
    return ranked


# ------------------------------------------------- helpers for dashboard --

def model_strikes(spot: float, step: float, count: int = 10) -> list[float]:
    """`count` strikes each side of ATM, on a `step` grid."""
    if step <= 0:
        raise ValueError("step must be positive")
    atm = round(spot / step) * step
    return [atm + i * step for i in range(-count, count + 1) if atm + i * step > 0]


def payoff_curves(kind: str, strike: float, premium: float, qty: float, spot: float,
                  days: float, iv: float, r: float = 0.065,
                  short_strike: float | None = None, short_premium: float = 0.0,
                  span_pct: float = 0.3, points: int = 61) -> list[dict]:
    """Rows of {spot, structure, when, pnl} for charting.

    structure: "Long option" and, when short_strike is set, "Debit spread".
    when: "At expiry" and "Today (model)" (Black-Scholes value now, same IV).
    """
    from .options_math import Leg, bs_price

    k = "call" if kind.lower() in ("call", "c", "ce") else "put"
    T = max(days, 0.0) / 365.0
    lo, hi = spot * (1 - span_pct), spot * (1 + span_pct)
    grid = [lo + (hi - lo) * i / (points - 1) for i in range(points)]
    structures = {"Long option": [Leg(k, "buy", strike, premium, qty)]}
    if short_strike:
        structures["Debit spread"] = [Leg(k, "buy", strike, premium, qty),
                                      Leg(k, "sell", short_strike, short_premium, qty)]
    rows = []
    for name, legs in structures.items():
        for s in grid:
            rows.append({"spot": round(s, 2), "structure": name, "when": "At expiry",
                         "pnl": round(sum(l.value_at_expiry(s) for l in legs), 2)})
            if T > 0 and iv > 0:
                now = sum(l.sign() * (bs_price(s, l.strike, T, r, iv, k) - l.premium) * l.qty
                          for l in legs)
                rows.append({"spot": round(s, 2), "structure": name, "when": "Today (model)",
                             "pnl": round(now, 2)})
    return rows
