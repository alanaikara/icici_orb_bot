"""MCP server: Zerodha + global market data + long-dated option analytics.

Run (stdio, for Claude Desktop / Claude Code):
    python -m global_options_mcp.server

Run (HTTP):
    python -m global_options_mcp.server --http --port 8765
"""
from __future__ import annotations

import argparse
from typing import Any

try:  # mcp >= 2
    from mcp.server.mcpserver import MCPServer as _Server
except ImportError:  # mcp 1.x
    from mcp.server.fastmcp import FastMCP as _Server

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from .analysis import macro_regime, rank_long_option_candidates
from .global_data import ALL_SYMBOLS, UNIVERSE, GlobalData, compute_stats
from .ideas import long_option_ideas as build_ideas
from .kite_client import KiteNotConfigured, ZerodhaClient
from .options_math import Leg, bs_greeks, implied_vol, payoff_summary, payoff_table

INSTRUCTIONS = """\
Tools for research on LONG-DATED option positions (months to years) on Indian
markets through Zerodha, with a global macro backdrop.

Suggested workflow:
1. macro_regime            -> global risk-on / risk-off score with reasons
2. global_market_snapshot  -> detail per region / asset class
3. kite_option_expiries    -> which long expiries exist (min_days=90+)
4. kite_option_chain       -> live chain with IV, Greeks, bid/ask, OI
5. long_option_ideas       -> ranked strikes for a bullish / bearish view
6. strategy_payoff         -> check max loss / breakeven of the final structure
7. kite_place_option_order -> preview first; live only if LIVE_ORDERS=true and
                              the human explicitly approves (confirm=true)

Always show the human the max loss, breakeven, bid-ask spread and open interest
before suggesting an order. Long-dated contracts can be illiquid: reject legs
with spread_pct > 5 or tiny OI. Never place an order without explicit approval.
"""

mcp = _Server("global-options", instructions=INSTRUCTIONS)
_data = GlobalData()
_zerodha = ZerodhaClient()


def _kite_call(fn, *a, **kw) -> Any:
    try:
        return fn(*a, **kw)
    except KiteNotConfigured as e:
        return {"error": str(e)}
    except Exception as e:  # surface broker errors as data, not crashes
        return {"error": f"{type(e).__name__}: {e}"}


# ------------------------------------------------------------ global data --

@mcp.tool()
def global_market_snapshot(category: str = "all") -> dict:
    """Global markets: last price, 1d/1w/1m/3m/1y % change, distance from the
    200-day average and historical volatility. category: all, india, us,
    europe, asia, fx, rates, commodities, crypto."""
    return _data.snapshot(category)


@mcp.tool()
def symbol_stats(symbol: str) -> dict:
    """Stats for any Yahoo Finance symbol (e.g. ^NSEI, RELIANCE.NS, AAPL, GC=F)."""
    st = compute_stats(symbol, _data.closes(symbol), ALL_SYMBOLS.get(symbol))
    return st.as_dict() if st else {"error": f"no data for {symbol}"}


@mcp.tool()
def list_global_symbols() -> dict:
    """The built-in global universe, grouped by category."""
    return UNIVERSE


@mcp.tool()
def macro_regime_score() -> dict:
    """Global risk-on / risk-off score for Indian equity longs, from trends
    (Nifty, S&P 500, copper), VIX, DXY, USD/INR, Brent and US 10Y yield."""
    return macro_regime(_data.stats)


# -------------------------------------------------------- option maths ----

@mcp.tool()
def black_scholes(spot: float, strike: float, days_to_expiry: float, volatility: float,
                  kind: str = "call", rate: float = 0.065, dividend_yield: float = 0.0) -> dict:
    """Black-Scholes price and Greeks. volatility as a decimal (0.15 = 15 %)."""
    g = bs_greeks(spot, strike, days_to_expiry / 365.0, rate, volatility, kind, dividend_yield)
    return g.as_dict()


@mcp.tool()
def implied_volatility(option_price: float, spot: float, strike: float, days_to_expiry: float,
                       kind: str = "call", rate: float = 0.065, dividend_yield: float = 0.0) -> dict:
    """Implied volatility from an option price."""
    iv = implied_vol(option_price, spot, strike, days_to_expiry / 365.0, rate, kind, dividend_yield)
    return {"implied_vol": round(iv, 5) if iv else None}


@mcp.tool()
def strategy_payoff(legs: list[dict], spot: float, span_pct: float = 0.4, points: int = 21) -> dict:
    """Expiry payoff for a multi-leg position.
    legs: [{"kind":"call|put|stock","side":"buy|sell","strike":24000,"premium":850,"qty":75}]"""
    parsed = [Leg(**l) for l in legs]
    lo, hi = spot * (1 - span_pct), spot * (1 + span_pct)
    grid = [lo + (hi - lo) * i / (points - 1) for i in range(points)]
    return {"summary": payoff_summary(parsed, spot, span_pct), "table": payoff_table(parsed, grid)}


@mcp.tool()
def evaluate_long_options(spot: float, strikes: list[float], days_to_expiry: float,
                          volatility: float, kind: str = "call", hist_volatility: float | None = None,
                          lot_size: int = 1, capital: float | None = None,
                          max_risk_pct: float = 2.0, rate: float = 0.065) -> dict:
    """Rank strikes for a long call/put using one IV for all strikes (offline,
    no broker needed). Use long_option_ideas for live Zerodha prices."""
    return rank_long_option_candidates(spot, strikes, days_to_expiry, kind, volatility,
                                       hv=hist_volatility, r=rate, lot_size=lot_size,
                                       capital=capital, max_risk_pct=max_risk_pct)


# ------------------------------------------------------------- zerodha ----

@mcp.tool()
def kite_profile() -> dict:
    """Zerodha account profile (checks the session works)."""
    return _kite_call(_zerodha.profile)


@mcp.tool()
def kite_margins() -> dict:
    """Available funds and used margin."""
    return _kite_call(_zerodha.margins)


@mcp.tool()
def kite_holdings() -> Any:
    """Long-term equity holdings (demat)."""
    return _kite_call(_zerodha.holdings)


@mcp.tool()
def kite_positions() -> Any:
    """Open F&O / intraday positions (net and day)."""
    return _kite_call(_zerodha.positions)


@mcp.tool()
def kite_orders() -> Any:
    """Today's orders."""
    return _kite_call(_zerodha.orders)


@mcp.tool()
def kite_option_expiries(underlying: str = "NIFTY", min_days: int = 0) -> Any:
    """All option expiries listed on NFO for an underlying (NIFTY, BANKNIFTY,
    RELIANCE, ...). Use min_days=90 to see long-dated contracts only."""
    return _kite_call(_zerodha.option_expiries, underlying, min_days)


@mcp.tool()
def kite_option_chain(underlying: str, expiry: str, width: int = 10) -> Any:
    """Live option chain around ATM: bid/ask, spread %, OI, volume, model IV
    and Greeks. expiry as YYYY-MM-DD (from kite_option_expiries)."""
    return _kite_call(_zerodha.option_chain, underlying, expiry, width)


@mcp.tool()
def long_option_ideas(underlying: str, expiry: str, view: str = "bullish",
                      capital: float | None = None, max_risk_pct: float = 2.0,
                      width: int = 12, max_spread_pct: float = 5.0) -> Any:
    """Combine the live chain, Nifty/stock historical vol and the macro regime
    into ranked long call (bullish) or long put (bearish) strikes. Legs with a
    bid-ask spread wider than max_spread_pct are dropped."""
    return _kite_call(build_ideas, _zerodha, _data, underlying, expiry, view, capital,
                      max_risk_pct, width, max_spread_pct)


@mcp.tool()
def kite_place_option_order(tradingsymbol: str, side: str, lots: int, limit_price: float,
                            confirm: bool = False) -> Any:
    """Place a LIMIT, NRML option order on NFO. Returns a preview unless
    LIVE_ORDERS=true AND confirm=true. SELL is allowed only to close an existing
    long. Lot and premium caps apply. Get explicit human approval first."""
    return _kite_call(_zerodha.place_option_order, tradingsymbol, side, lots, limit_price, confirm)


@mcp.prompt()
def long_term_option_plan(underlying: str = "NIFTY", view: str = "bullish",
                          horizon_months: int = 12, capital: float = 500000) -> str:
    """Guided plan for a long-dated option position."""
    return (
        f"Build a {view} long-dated option plan on {underlying} for about {horizon_months} months "
        f"with capital Rs {capital:,.0f}.\n"
        "1. Call macro_regime_score and global_market_snapshot('all'); summarise the backdrop.\n"
        f"2. Call kite_option_expiries('{underlying}', min_days={max(30, horizon_months * 25)}) "
        "and pick the expiry closest to the horizon that has liquid contracts.\n"
        "3. Call long_option_ideas with that expiry, view and capital (max_risk_pct 2).\n"
        "4. Compare the top long option with a debit spread using strategy_payoff.\n"
        "5. Present: structure, max loss, breakeven, theta per month, liquidity, exit rules "
        "(e.g. roll at 90 days to expiry, cut at -50% premium). Do not place orders."
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--http", action="store_true", help="serve streamable HTTP instead of stdio")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8765)
    a = ap.parse_args()
    if a.http:
        mcp.run(transport="streamable-http", host=a.host, port=a.port)
    else:
        mcp.run()


if __name__ == "__main__":
    main()
