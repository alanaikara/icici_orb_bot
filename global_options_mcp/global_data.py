"""Global market data: indices, volatility, FX, rates, commodities, crypto.

Source: Yahoo Finance via `yfinance` (no API key). The fetcher is
injectable so tests and other data vendors can replace it.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable, Sequence

from .options_math import historical_vol

# symbol -> human name, grouped by category
UNIVERSE: dict[str, dict[str, str]] = {
    "india": {
        "^NSEI": "Nifty 50",
        "^NSEBANK": "Nifty Bank",
        "^BSESN": "Sensex",
        "^INDIAVIX": "India VIX",
    },
    "us": {
        "^GSPC": "S&P 500",
        "^IXIC": "Nasdaq Composite",
        "^DJI": "Dow Jones",
        "^RUT": "Russell 2000",
        "^VIX": "CBOE VIX",
    },
    "europe": {
        "^FTSE": "FTSE 100",
        "^GDAXI": "DAX",
        "^FCHI": "CAC 40",
        "^STOXX50E": "Euro Stoxx 50",
    },
    "asia": {
        "^N225": "Nikkei 225",
        "^HSI": "Hang Seng",
        "000001.SS": "Shanghai Composite",
        "^KS11": "KOSPI",
        "^AXJO": "ASX 200",
    },
    "fx": {
        "INR=X": "USD/INR",
        "DX-Y.NYB": "US Dollar Index (DXY)",
        "EURUSD=X": "EUR/USD",
        "JPY=X": "USD/JPY",
        "CNY=X": "USD/CNY",
    },
    "rates": {
        "^IRX": "US 13-week T-bill yield",
        "^FVX": "US 5-year yield",
        "^TNX": "US 10-year yield",
        "^TYX": "US 30-year yield",
    },
    "commodities": {
        "BZ=F": "Brent crude",
        "CL=F": "WTI crude",
        "GC=F": "Gold",
        "SI=F": "Silver",
        "HG=F": "Copper",
        "NG=F": "Natural gas",
    },
    "crypto": {
        "BTC-USD": "Bitcoin",
        "ETH-USD": "Ethereum",
    },
}

ALL_SYMBOLS = {s: n for grp in UNIVERSE.values() for s, n in grp.items()}

# Fetcher signature: (symbol, period) -> list of daily closes, oldest first.
Fetcher = Callable[[str, str], Sequence[float]]


def yfinance_fetcher(symbol: str, period: str = "2y") -> list[float]:
    import yfinance as yf  # imported lazily so tests do not need it

    df = yf.Ticker(symbol).history(period=period, interval="1d", auto_adjust=True)
    if df is None or df.empty:
        return []
    return [float(x) for x in df["Close"].dropna().tolist()]


@dataclass
class SymbolStats:
    symbol: str
    name: str
    last: float
    chg_1d_pct: float | None
    chg_1w_pct: float | None
    chg_1m_pct: float | None
    chg_3m_pct: float | None
    chg_1y_pct: float | None
    vs_200dma_pct: float | None
    hv_20d: float | None
    hv_1y: float | None

    def as_dict(self) -> dict:
        out = {}
        for k, v in self.__dict__.items():
            out[k] = round(v, 4) if isinstance(v, float) else v
        return out


def _pct(closes: Sequence[float], lookback: int) -> float | None:
    if len(closes) <= lookback or closes[-1 - lookback] == 0:
        return None
    return (closes[-1] / closes[-1 - lookback] - 1.0) * 100.0


def compute_stats(symbol: str, closes: Sequence[float], name: str | None = None) -> SymbolStats | None:
    if not closes:
        return None
    last = closes[-1]
    dma200 = sum(closes[-200:]) / 200 if len(closes) >= 200 else None
    return SymbolStats(
        symbol=symbol,
        name=name or ALL_SYMBOLS.get(symbol, symbol),
        last=last,
        chg_1d_pct=_pct(closes, 1),
        chg_1w_pct=_pct(closes, 5),
        chg_1m_pct=_pct(closes, 21),
        chg_3m_pct=_pct(closes, 63),
        chg_1y_pct=_pct(closes, 252),
        vs_200dma_pct=((last / dma200 - 1.0) * 100.0) if dma200 else None,
        hv_20d=historical_vol(closes, 20),
        hv_1y=historical_vol(closes, 252),
    )


class GlobalData:
    """Cached access to global market history."""

    def __init__(self, fetcher: Fetcher | None = None, ttl_seconds: int = 900):
        self._fetch = fetcher or yfinance_fetcher
        self._ttl = ttl_seconds
        self._cache: dict[tuple[str, str], tuple[float, list[float]]] = {}

    def closes(self, symbol: str, period: str = "2y") -> list[float]:
        key = (symbol, period)
        hit = self._cache.get(key)
        if hit and time.time() - hit[0] < self._ttl:
            return hit[1]
        try:
            data = list(self._fetch(symbol, period))
        except Exception:  # vendor errors must not crash the MCP server
            data = []
        self._cache[key] = (time.time(), data)
        return data

    def stats(self, symbol: str) -> SymbolStats | None:
        return compute_stats(symbol, self.closes(symbol))

    def snapshot(self, category: str = "all") -> dict:
        cats = list(UNIVERSE) if category == "all" else [category]
        unknown = [c for c in cats if c not in UNIVERSE]
        if unknown:
            raise ValueError(f"Unknown category {unknown}. Use one of {list(UNIVERSE)} or 'all'.")
        out: dict[str, list[dict]] = {}
        missing: list[str] = []
        for cat in cats:
            rows = []
            for sym in UNIVERSE[cat]:
                st = self.stats(sym)
                if st is None:
                    missing.append(sym)
                else:
                    rows.append(st.as_dict())
            out[cat] = rows
        return {"data": out, "missing": missing, "source": "Yahoo Finance (delayed)"}
