"""Decision support for long-term (long-dated) option positions.

Two parts:
1. `macro_regime`  - turns global market stats into a risk-on / risk-off score
                     with plain-language reasons (India-centric view).
2. `rank_long_option_candidates` - scores strikes for a long call / long put
                     view: cost of carry (theta), breakeven distance,
                     probability, vega exposure vs. IV/HV.

This is a research tool. It does not give financial advice.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, asdict
from typing import Callable, Iterable

from .options_math import bs_greeks, expected_move

StatsLookup = Callable[[str], "object | None"]  # symbol -> SymbolStats | None


@dataclass
class Signal:
    name: str
    score: int          # +1 supportive, 0 neutral, -1 adverse (for Indian equities)
    value: float | None
    reason: str


def _get(lookup: StatsLookup, sym: str, field: str):
    st = lookup(sym)
    return getattr(st, field, None) if st is not None else None


def macro_regime(lookup: StatsLookup) -> dict:
    """Score the global backdrop for Indian equity index longs.

    Each rule gives +1 / 0 / -1. Missing data gives 0 and is reported.
    """
    sig: list[Signal] = []

    def trend(sym: str, label: str):
        v = _get(lookup, sym, "vs_200dma_pct")
        if v is None:
            sig.append(Signal(label, 0, None, "no data"))
        elif v > 2:
            sig.append(Signal(label, 1, v, f"{v:+.1f}% above 200-day average (uptrend)"))
        elif v < -2:
            sig.append(Signal(label, -1, v, f"{v:+.1f}% below 200-day average (downtrend)"))
        else:
            sig.append(Signal(label, 0, v, f"{v:+.1f}% vs 200-day average (flat)"))

    trend("^NSEI", "Nifty trend")
    trend("^GSPC", "S&P 500 trend")
    trend("HG=F", "Copper trend (global growth)")

    vix = _get(lookup, "^VIX", "last")
    if vix is None:
        sig.append(Signal("US VIX", 0, None, "no data"))
    elif vix < 16:
        sig.append(Signal("US VIX", 1, vix, f"VIX {vix:.1f}: calm global risk appetite"))
    elif vix > 25:
        sig.append(Signal("US VIX", -1, vix, f"VIX {vix:.1f}: global stress"))
    else:
        sig.append(Signal("US VIX", 0, vix, f"VIX {vix:.1f}: normal"))

    def rising_is_bad(sym: str, label: str, threshold_pct: float, what: str):
        v = _get(lookup, sym, "chg_3m_pct")
        if v is None:
            sig.append(Signal(label, 0, None, "no data"))
        elif v > threshold_pct:
            sig.append(Signal(label, -1, v, f"{what} up {v:+.1f}% in 3 months (headwind for India)"))
        elif v < -threshold_pct:
            sig.append(Signal(label, 1, v, f"{what} down {v:+.1f}% in 3 months (tailwind for India)"))
        else:
            sig.append(Signal(label, 0, v, f"{what} {v:+.1f}% in 3 months (stable)"))

    rising_is_bad("DX-Y.NYB", "US dollar (DXY)", 3.0, "Dollar index")
    rising_is_bad("INR=X", "USD/INR", 2.0, "USD/INR")
    rising_is_bad("BZ=F", "Brent crude", 15.0, "Brent crude")
    # ^TNX quotes the yield in percent (4.3 = 4.3 %); 3m pct change of ~10 % ~ +40 bp.
    rising_is_bad("^TNX", "US 10Y yield", 10.0, "US 10-year yield")

    total = sum(s.score for s in sig)
    available = sum(1 for s in sig if s.value is not None)
    if available == 0:
        label = "unknown"
    elif total >= 3:
        label = "risk-on"
    elif total <= -3:
        label = "risk-off"
    else:
        label = "neutral"

    bias = {
        "risk-on": "Backdrop supports long calls / bull call spreads on Indian indices.",
        "risk-off": "Backdrop favours protective puts or waiting; avoid large naked long calls.",
        "neutral": "Mixed backdrop: prefer defined-risk debit spreads and smaller size.",
        "unknown": "No data available.",
    }[label]

    return {
        "regime": label,
        "score": total,
        "max_score": len(sig),
        "signals_with_data": available,
        "bias": bias,
        "signals": [asdict(s) for s in sig],
    }


# ------------------------------------------------------- strike ranking --

@dataclass
class Candidate:
    strike: float
    kind: str
    premium: float
    iv: float
    delta: float
    theta_per_day: float
    theta_pct_of_premium_per_month: float
    vega_per_1pct: float
    breakeven: float
    breakeven_move_pct: float
    breakeven_in_sd: float
    prob_itm: float
    lots_for_budget: int
    capital_at_risk: float
    score: float
    notes: list[str]


def rank_long_option_candidates(
    spot: float,
    strikes: Iterable[float],
    days_to_expiry: float,
    kind: str = "call",
    iv: float | dict | None = None,
    premiums: dict | None = None,
    hv: float | None = None,
    r: float = 0.065,
    q: float = 0.0,
    lot_size: int = 1,
    capital: float | None = None,
    max_risk_pct: float = 2.0,
) -> dict:
    """Rank strikes for a long option position.

    `iv` may be one number or a {strike: iv} map (from a live chain).
    `premiums` ({strike: price}) overrides the model price when given.
    Score favours: delta 0.55-0.80 (stock-replacement zone), low monthly
    theta as % of premium, breakeven within ~1 SD, and cheap IV vs HV.
    """
    if days_to_expiry <= 0:
        raise ValueError("days_to_expiry must be positive")
    T = days_to_expiry / 365.0
    kind_l = "call" if kind.lower() in ("call", "c", "ce") else "put"
    budget = capital * max_risk_pct / 100.0 if capital else None

    rows: list[Candidate] = []
    for K in sorted(set(float(k) for k in strikes)):
        sigma = iv.get(K) if isinstance(iv, dict) else iv
        if isinstance(iv, dict) and sigma is None:
            sigma = iv.get(int(K)) if K.is_integer() else None
        if not sigma or sigma <= 0:
            continue
        g = bs_greeks(spot, K, T, r, sigma, kind_l, q)
        prem = None
        if premiums:
            prem = premiums.get(K) or (premiums.get(int(K)) if K.is_integer() else None)
        prem = float(prem) if prem else g.price
        if prem <= 0:
            continue

        breakeven = K + prem if kind_l == "call" else K - prem
        be_move = (breakeven / spot - 1.0) * 100.0
        sd = expected_move(spot, sigma, days_to_expiry)
        be_sd = (breakeven - spot) / sd if kind_l == "call" else (spot - breakeven) / sd
        theta_month_pct = abs(g.theta_per_day) * 30 / prem * 100.0
        abs_delta = abs(g.delta)

        notes: list[str] = []
        score = 0.0
        # delta zone
        if 0.55 <= abs_delta <= 0.80:
            score += 3; notes.append("delta in stock-replacement zone (0.55-0.80)")
        elif 0.40 <= abs_delta < 0.55:
            score += 1.5; notes.append("near-the-money: more leverage, more decay")
        elif abs_delta > 0.80:
            score += 1; notes.append("deep ITM: low leverage, behaves like the underlying")
        else:
            score -= 1; notes.append("OTM lottery zone: low probability, fast decay")
        # carry cost
        if theta_month_pct < 3:
            score += 2; notes.append(f"low decay ({theta_month_pct:.1f}%/month of premium)")
        elif theta_month_pct < 6:
            score += 1
        else:
            score -= 1; notes.append(f"high decay ({theta_month_pct:.1f}%/month of premium)")
        # breakeven distance
        if be_sd <= 0.5:
            score += 2
        elif be_sd <= 1.0:
            score += 1
        else:
            score -= 1; notes.append(f"breakeven needs {be_sd:.1f} SD move")
        # vol value
        if hv:
            ratio = sigma / hv
            if ratio < 0.9:
                score += 1; notes.append(f"IV {sigma:.0%} cheap vs HV {hv:.0%}")
            elif ratio > 1.3:
                score -= 1; notes.append(f"IV {sigma:.0%} rich vs HV {hv:.0%}: consider a spread")

        cost_per_lot = prem * lot_size
        lots = int(budget // cost_per_lot) if budget and cost_per_lot > 0 else 0
        rows.append(Candidate(
            strike=K, kind=kind_l, premium=round(prem, 2), iv=round(sigma, 4),
            delta=round(g.delta, 3), theta_per_day=round(g.theta_per_day, 3),
            theta_pct_of_premium_per_month=round(theta_month_pct, 2),
            vega_per_1pct=round(g.vega_per_1pct, 3), breakeven=round(breakeven, 2),
            breakeven_move_pct=round(be_move, 2), breakeven_in_sd=round(be_sd, 2),
            prob_itm=round(g.prob_itm, 3), lots_for_budget=lots,
            capital_at_risk=round(lots * cost_per_lot, 2), score=round(score, 2),
            notes=notes,
        ))

    # Ties: prefer delta nearest 0.70 (middle of the stock-replacement zone).
    rows.sort(key=lambda c: (-c.score, abs(abs(c.delta) - 0.70)))
    return {
        "spot": spot,
        "days_to_expiry": days_to_expiry,
        "kind": kind_l,
        "one_sd_move": round(expected_move(spot, (iv if isinstance(iv, (int, float)) else
                                                 (sum(iv.values()) / len(iv) if iv else 0)) or 0,
                                           days_to_expiry), 2),
        "risk_budget": round(budget, 2) if budget else None,
        "candidates": [asdict(c) for c in rows],
        "disclaimer": "Model output for research only. Check liquidity (bid-ask, OI) before any order.",
    }


def years_to_days(years: float) -> float:
    return years * 365.0


def nearest_strikes(spot: float, all_strikes: Iterable[float], width: int = 10) -> list[float]:
    s = sorted(set(all_strikes))
    if not s:
        return []
    idx = min(range(len(s)), key=lambda i: abs(s[i] - spot))
    return s[max(0, idx - width): idx + width + 1]


def safe_float(x) -> float | None:
    try:
        v = float(x)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None
