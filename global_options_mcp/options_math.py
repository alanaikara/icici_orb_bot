"""Pure-Python option maths: Black-Scholes price, Greeks, implied volatility,
historical volatility and multi-leg payoff.

No scipy dependency. All functions use annualised inputs:
    S      spot price
    K      strike
    T      time to expiry in years (days / 365)
    r      risk-free rate (0.065 = 6.5 %)
    sigma  volatility (0.18 = 18 %)
    q      continuous dividend yield
"""
from __future__ import annotations

import math
from dataclasses import dataclass, asdict
from typing import Iterable, Sequence

TRADING_DAYS = 252


def norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


def _check_kind(kind: str) -> str:
    k = kind.lower()
    if k in ("c", "call", "ce"):
        return "call"
    if k in ("p", "put", "pe"):
        return "put"
    raise ValueError(f"kind must be call/put (CE/PE), got {kind!r}")


def _d1_d2(S: float, K: float, T: float, r: float, sigma: float, q: float):
    if S <= 0 or K <= 0:
        raise ValueError("S and K must be positive")
    if T <= 0 or sigma <= 0:
        raise ValueError("T and sigma must be positive")
    vol_t = sigma * math.sqrt(T)
    d1 = (math.log(S / K) + (r - q + 0.5 * sigma * sigma) * T) / vol_t
    return d1, d1 - vol_t


def bs_price(S: float, K: float, T: float, r: float, sigma: float,
             kind: str = "call", q: float = 0.0) -> float:
    kind = _check_kind(kind)
    if T <= 0:
        return max(0.0, S - K) if kind == "call" else max(0.0, K - S)
    d1, d2 = _d1_d2(S, K, T, r, sigma, q)
    df_r, df_q = math.exp(-r * T), math.exp(-q * T)
    if kind == "call":
        return S * df_q * norm_cdf(d1) - K * df_r * norm_cdf(d2)
    return K * df_r * norm_cdf(-d2) - S * df_q * norm_cdf(-d1)


@dataclass
class Greeks:
    price: float
    delta: float
    gamma: float
    vega_per_1pct: float      # price change for +1 vol point
    theta_per_day: float      # price change per calendar day
    rho_per_1pct: float       # price change for +1 % rate
    prob_itm: float           # risk-neutral N(d2) / N(-d2)

    def as_dict(self) -> dict:
        return {k: round(v, 6) for k, v in asdict(self).items()}


def bs_greeks(S: float, K: float, T: float, r: float, sigma: float,
              kind: str = "call", q: float = 0.0) -> Greeks:
    kind = _check_kind(kind)
    d1, d2 = _d1_d2(S, K, T, r, sigma, q)
    df_r, df_q = math.exp(-r * T), math.exp(-q * T)
    sqrt_t = math.sqrt(T)
    price = bs_price(S, K, T, r, sigma, kind, q)
    gamma = df_q * norm_pdf(d1) / (S * sigma * sqrt_t)
    vega = S * df_q * norm_pdf(d1) * sqrt_t
    common_theta = -S * df_q * norm_pdf(d1) * sigma / (2 * sqrt_t)
    if kind == "call":
        delta = df_q * norm_cdf(d1)
        theta = common_theta - r * K * df_r * norm_cdf(d2) + q * S * df_q * norm_cdf(d1)
        rho = K * T * df_r * norm_cdf(d2)
        p_itm = norm_cdf(d2)
    else:
        delta = df_q * (norm_cdf(d1) - 1.0)
        theta = common_theta + r * K * df_r * norm_cdf(-d2) - q * S * df_q * norm_cdf(-d1)
        rho = -K * T * df_r * norm_cdf(-d2)
        p_itm = norm_cdf(-d2)
    return Greeks(price, delta, gamma, vega / 100.0, theta / 365.0, rho / 100.0, p_itm)


def implied_vol(price: float, S: float, K: float, T: float, r: float,
                kind: str = "call", q: float = 0.0,
                lo: float = 1e-4, hi: float = 5.0, tol: float = 1e-6,
                max_iter: int = 200) -> float | None:
    """Bisection solver. Returns None when the price is outside no-arbitrage bounds."""
    kind = _check_kind(kind)
    if price <= 0 or T <= 0:
        return None
    intrinsic = max(0.0, S * math.exp(-q * T) - K * math.exp(-r * T)) if kind == "call" \
        else max(0.0, K * math.exp(-r * T) - S * math.exp(-q * T))
    if price < intrinsic - 1e-9:
        return None
    p_lo, p_hi = bs_price(S, K, T, r, lo, kind, q), bs_price(S, K, T, r, hi, kind, q)
    if not (p_lo - tol <= price <= p_hi + tol):
        return None
    for _ in range(max_iter):
        mid = 0.5 * (lo + hi)
        p_mid = bs_price(S, K, T, r, mid, kind, q)
        if abs(p_mid - price) < tol:
            return mid
        if p_mid < price:
            lo = mid
        else:
            hi = mid
    return 0.5 * (lo + hi)


def historical_vol(closes: Sequence[float], window: int | None = None) -> float | None:
    """Annualised close-to-close volatility from daily closes."""
    data = [c for c in closes if c and c > 0]
    if window:
        data = data[-(window + 1):]
    if len(data) < 3:
        return None
    rets = [math.log(b / a) for a, b in zip(data[:-1], data[1:])]
    mean = sum(rets) / len(rets)
    var = sum((x - mean) ** 2 for x in rets) / (len(rets) - 1)
    return math.sqrt(var) * math.sqrt(TRADING_DAYS)


def expected_move(S: float, sigma: float, days: float) -> float:
    """One-standard-deviation move over `days` calendar days."""
    return S * sigma * math.sqrt(days / 365.0)


# ---------------------------------------------------------------- payoff --

@dataclass
class Leg:
    kind: str          # call / put / stock
    side: str          # buy / sell
    strike: float = 0.0
    premium: float = 0.0
    qty: float = 1.0   # units (lot size x lots)

    def sign(self) -> int:
        s = self.side.lower()
        if s in ("buy", "long", "b"):
            return 1
        if s in ("sell", "short", "s"):
            return -1
        raise ValueError(f"side must be buy/sell, got {self.side!r}")

    def value_at_expiry(self, S: float) -> float:
        k = self.kind.lower()
        if k == "stock":
            intrinsic, cost = S, self.premium
        else:
            intrinsic = max(0.0, S - self.strike) if _check_kind(k) == "call" \
                else max(0.0, self.strike - S)
            cost = self.premium
        return self.sign() * (intrinsic - cost) * self.qty


def payoff_table(legs: Iterable[Leg], prices: Iterable[float]) -> list[dict]:
    legs = list(legs)
    return [{"spot": round(p, 2), "pnl": round(sum(l.value_at_expiry(p) for l in legs), 2)}
            for p in prices]


def payoff_summary(legs: Iterable[Leg], spot: float, span_pct: float = 0.5,
                   steps: int = 200) -> dict:
    """Max profit / max loss / breakevens over spot*(1±span_pct)."""
    legs = list(legs)
    lo, hi = spot * (1 - span_pct), spot * (1 + span_pct)
    grid = [lo + (hi - lo) * i / steps for i in range(steps + 1)]
    pnl = [sum(l.value_at_expiry(p) for l in legs) for p in grid]
    breakevens = []
    for (p0, v0), (p1, v1) in zip(zip(grid, pnl), zip(grid[1:], pnl[1:])):
        if v0 == 0:
            breakevens.append(round(p0, 2))
        elif v0 * v1 < 0:
            breakevens.append(round(p0 + (p1 - p0) * (-v0) / (v1 - v0), 2))
    # Unbounded upside if slope at right edge is positive
    slope_hi = pnl[-1] - pnl[-2]
    slope_lo = pnl[1] - pnl[0]
    return {
        "max_profit_in_range": round(max(pnl), 2),
        "max_loss_in_range": round(min(pnl), 2),
        "upside_unbounded": slope_hi > 1e-9,
        "downside_unbounded": slope_lo < -1e-9 and pnl[0] < 0,
        "breakevens": breakevens,
        "net_premium": round(sum(-l.sign() * l.premium * l.qty for l in legs
                                 if l.kind.lower() != "stock"), 2),
        "range": [round(lo, 2), round(hi, 2)],
    }
