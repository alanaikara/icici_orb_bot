"""Streamlit dashboard for long-dated option ideas.

Run from the repo root:
    streamlit run global_options_mcp/dashboard.py

Two data sources:
* Live (Zerodha) - real chain, bid/ask, OI. Needs KITE_API_KEY + KITE_ACCESS_TOKEN.
* Model          - Black-Scholes prices from spot + IV you enter. No broker needed.

The dashboard never sends orders. "Preview order" only runs the order guards.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # allow `streamlit run`

import altair as alt
import pandas as pd
import streamlit as st

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from global_options_mcp.analysis import macro_regime, rank_long_option_candidates
from global_options_mcp.global_data import UNIVERSE, GlobalData
from global_options_mcp.ideas import (long_option_ideas, model_strikes, payoff_curves,
                                      underlying_hv, yahoo_symbol)
from global_options_mcp.kite_client import ZerodhaClient
from global_options_mcp.options_math import Leg, payoff_summary

st.set_page_config(page_title="Option Ideas", page_icon="📈", layout="wide")

DEFAULT_SPOT = {"NIFTY": 24000.0, "BANKNIFTY": 52000.0}
DEFAULT_STEP = {"NIFTY": 500.0, "BANKNIFTY": 1000.0}
DEFAULT_LOT = {"NIFTY": 75, "BANKNIFTY": 35}


# --------------------------------------------------------------- cached --

@st.cache_resource
def get_data() -> GlobalData:
    return GlobalData()


@st.cache_resource
def get_zerodha() -> ZerodhaClient:
    return ZerodhaClient()


@st.cache_data(ttl=900, show_spinner="Loading global markets...")
def cached_macro() -> dict:
    return macro_regime(get_data().stats)


@st.cache_data(ttl=900, show_spinner="Loading global markets...")
def cached_snapshot(category: str) -> dict:
    return get_data().snapshot(category)


@st.cache_data(ttl=900)
def cached_hv_and_spot(underlying: str) -> tuple[float | None, float | None]:
    data = get_data()
    closes = data.closes(yahoo_symbol(underlying), "1y")
    return underlying_hv(data, underlying), (closes[-1] if closes else None)


@st.cache_data(ttl=3600, show_spinner="Loading expiries from Zerodha...")
def cached_expiries(underlying: str, min_days: int) -> list[dict]:
    return get_zerodha().option_expiries(underlying, min_days)


@st.cache_data(ttl=60, show_spinner="Loading live option chain...")
def cached_live_ideas(underlying, expiry, view, capital, risk, width, max_spread) -> dict:
    return long_option_ideas(get_zerodha(), get_data(), underlying, expiry, view, capital,
                             risk, width, max_spread, include_macro=False)


def kite_ready() -> bool:
    api_key, token = ZerodhaClient._read_credentials()  # re-reads .env after a new login
    return bool(api_key and token)


# -------------------------------------------------------------- sidebar --

st.sidebar.title("Option ideas")
source = st.sidebar.radio("Data source", ["Live (Zerodha)", "Model (no broker)"],
                          index=0 if kite_ready() else 1, key="source")
live = source.startswith("Live")
if live and not kite_ready():
    st.sidebar.error("Set KITE_API_KEY and KITE_ACCESS_TOKEN. "
                     "Run: python -m global_options_mcp.kite_login --if-needed")

underlying = st.sidebar.text_input("Underlying", "NIFTY", key="underlying").strip().upper()
view = st.sidebar.radio("View", ["bullish", "bearish"], horizontal=True, key="view")
capital = st.sidebar.number_input("Capital (Rs)", 10_000, 100_000_000, 500_000, 50_000, key="capital")
risk = st.sidebar.slider("Max risk per idea (% of capital)", 0.5, 10.0, 2.0, 0.5, key="risk")

hv, yahoo_spot = cached_hv_and_spot(underlying)

if live:
    min_days = st.sidebar.slider("Minimum days to expiry", 0, 720, 90, 15, key="min_days")
    width = st.sidebar.slider("Strikes each side of ATM", 4, 30, 12, key="width")
    max_spread = st.sidebar.slider("Max bid-ask spread %", 1.0, 20.0, 5.0, 0.5, key="max_spread")
else:
    spot = st.sidebar.number_input("Spot", 1.0, 1e7,
                                   float(round(yahoo_spot or DEFAULT_SPOT.get(underlying, 1000.0), 2)),
                                   key="spot")
    days = st.sidebar.slider("Days to expiry", 30, 1095, 365, 15, key="days")
    iv_pct = st.sidebar.number_input("Implied volatility %", 1.0, 200.0,
                                     float(round((hv or 0.15) * 100, 1)), 0.5, key="iv")
    step = st.sidebar.number_input("Strike step", 1.0, 10_000.0,
                                   DEFAULT_STEP.get(underlying, round(spot * 0.02, 0) or 10.0),
                                   key="step")
    lot = st.sidebar.number_input("Lot size", 1, 10_000, DEFAULT_LOT.get(underlying, 1), key="lot")
    rate = st.sidebar.number_input("Risk-free rate %", 0.0, 20.0, 6.5, 0.25, key="rate")

st.sidebar.caption("Research tool. Not financial advice. This page never sends orders.")


# --------------------------------------------------------------- header --

st.title(f"{underlying} · long-dated {view} ideas")

macro = cached_macro()
REGIME_ICON = {"risk-on": "🟢", "neutral": "🟡", "risk-off": "🔴", "unknown": "⚪"}
c1, c2, c3, c4 = st.columns(4)
c1.metric("Global regime", f"{REGIME_ICON[macro['regime']]} {macro['regime']}")
c2.metric("Macro score", f"{macro['score']:+d} / {macro['max_score']}",
          help="Sum of +1 / 0 / -1 signals. Missing data counts as 0.")
c3.metric("1-year historical vol", f"{hv:.1%}" if hv else "n/a")
c4.metric("Spot (Yahoo, delayed)", f"{yahoo_spot:,.2f}" if yahoo_spot else "n/a")
st.info(macro["bias"])
if view == "bullish" and macro["regime"] == "risk-off":
    st.warning("Your view is bullish but the global backdrop is risk-off. Consider smaller size or a spread.")
if view == "bearish" and macro["regime"] == "risk-on":
    st.warning("Your view is bearish but the global backdrop is risk-on.")

with st.expander("Macro signals"):
    st.dataframe(pd.DataFrame(macro["signals"]), hide_index=True,
                 column_config={"score": st.column_config.NumberColumn(format="%+d"),
                                "value": st.column_config.NumberColumn(format="%.2f")})


# --------------------------------------------------------- build ideas --

ideas: dict | None = None
error: str | None = None
if live:
    if kite_ready():
        try:
            exps = cached_expiries(underlying, min_days)
        except Exception as e:  # noqa: BLE001 - show broker error in UI
            exps, error = [], f"{type(e).__name__}: {e}"
        if exps:
            labels = {f"{e['expiry']}  ({e['days']} days)": e["expiry"] for e in exps}
            choice = st.selectbox("Expiry", list(labels), index=len(labels) - 1 if len(labels) < 3 else 0,
                                  key="expiry")
            try:
                ideas = cached_live_ideas(underlying, labels[choice], view, float(capital),
                                          float(risk), int(width), float(max_spread))
            except Exception as e:  # noqa: BLE001
                error = f"{type(e).__name__}: {e}"
        elif not error:
            error = f"No {underlying} option expiries at least {min_days} days away."
else:
    kind = "call" if view == "bullish" else "put"
    strikes = model_strikes(spot, step, 10)
    ideas = rank_long_option_candidates(spot, strikes, days, kind, iv_pct / 100, hv=hv,
                                        r=rate / 100, lot_size=int(lot), capital=float(capital),
                                        max_risk_pct=float(risk))
    ideas["lot_size"] = int(lot)
    ideas["expiry"] = f"model, {days} days"

if error:
    st.error(error)

tab_ideas, tab_payoff, tab_global = st.tabs(["Ranked strikes", "Payoff", "Global markets"])

# ---------------------------------------------------------- ideas tab ---

with tab_ideas:
    if not ideas or not ideas["candidates"]:
        st.write("No candidates. Change the inputs in the sidebar.")
    else:
        df = pd.DataFrame(ideas["candidates"])
        df["notes"] = df["notes"].apply(lambda n: "; ".join(n))
        cols = ["score", "strike", "tradingsymbol", "premium", "bid", "ask", "spread_pct", "oi",
                "delta", "iv", "theta_pct_of_premium_per_month", "breakeven", "breakeven_move_pct",
                "breakeven_in_sd", "prob_itm", "lots_for_budget", "capital_at_risk", "notes"]
        df = df[[c for c in cols if c in df.columns]]
        m1, m2, m3, m4 = st.columns(4)
        top = ideas["candidates"][0]
        m1.metric("Top strike", f"{top['strike']:,.0f} {'CE' if ideas['kind'] == 'call' else 'PE'}")
        m2.metric("Premium / lot", f"Rs {top['premium'] * (ideas.get('lot_size') or 1):,.0f}")
        m3.metric("Breakeven", f"{top['breakeven']:,.0f}", f"{top['breakeven_move_pct']:+.1f}%")
        m4.metric("Risk budget", f"Rs {ideas['risk_budget']:,.0f}" if ideas.get("risk_budget") else "n/a")
        if ideas.get("risk_budget") and not top["lots_for_budget"]:
            st.warning("One lot of the top strike costs more than your risk budget. "
                       "Raise the risk %, use a debit spread, or pick a cheaper strike.")
        st.caption(f"Expiry: {ideas.get('expiry')} · days to expiry {ideas['days_to_expiry']} · "
                   f"one-SD move ±{ideas['one_sd_move']:,.0f} · lot size {ideas.get('lot_size')}")
        st.dataframe(
            df, hide_index=True,
            column_config={
                "score": st.column_config.ProgressColumn("Score", min_value=-4, max_value=8, format="%.1f"),
                "strike": st.column_config.NumberColumn("Strike", format="%.0f"),
                "tradingsymbol": "Symbol",
                "premium": st.column_config.NumberColumn("Premium", format="%.2f"),
                "spread_pct": st.column_config.NumberColumn("Spread %", format="%.2f"),
                "oi": st.column_config.NumberColumn("OI", format="%d"),
                "delta": st.column_config.NumberColumn("Delta", format="%.2f"),
                "iv": st.column_config.NumberColumn("IV", format="percent"),
                "theta_pct_of_premium_per_month": st.column_config.NumberColumn("Decay %/month", format="%.1f"),
                "breakeven": st.column_config.NumberColumn("Breakeven", format="%.0f"),
                "breakeven_move_pct": st.column_config.NumberColumn("BE move %", format="%+.1f"),
                "breakeven_in_sd": st.column_config.NumberColumn("BE in SD", format="%.2f"),
                "prob_itm": st.column_config.NumberColumn("P(ITM)", format="percent"),
                "lots_for_budget": st.column_config.NumberColumn("Lots", format="%d"),
                "capital_at_risk": st.column_config.NumberColumn("Capital at risk", format="Rs %,.0f"),
                "notes": st.column_config.TextColumn("Notes", width="large"),
            },
        )
        if ideas.get("dropped_illiquid"):
            st.warning("Removed for wide bid-ask spread: " + ", ".join(
                f"{d['strike']:,.0f} ({d['spread_pct']}%)" for d in ideas["dropped_illiquid"]))
        st.caption(ideas["disclaimer"])

# --------------------------------------------------------- payoff tab ---

with tab_payoff:
    if not ideas or not ideas["candidates"]:
        st.write("No candidates to chart.")
    else:
        cands = {c["strike"]: c for c in ideas["candidates"]}
        strikes_sorted = sorted(cands)
        p1, p2, p3 = st.columns(3)
        long_k = p1.selectbox("Buy strike", strikes_sorted,
                              index=strikes_sorted.index(ideas["candidates"][0]["strike"]),
                              format_func=lambda k: f"{k:,.0f}", key="long_k")
        far = [k for k in strikes_sorted if (k > long_k if ideas["kind"] == "call" else k < long_k)]
        short_opts = [None] + (far if ideas["kind"] == "call" else far[::-1])
        short_k = p2.selectbox("Sell strike (debit spread, optional)", short_opts,
                               index=min(2, len(short_opts) - 1),
                               format_func=lambda k: "none" if k is None else f"{k:,.0f}", key="short_k")
        lots = p3.number_input("Lots", 1, 1000, max(1, cands[long_k]["lots_for_budget"] or 1), key="lots")

        lot_size = int(ideas.get("lot_size") or 1)
        qty = lots * lot_size
        long_c = cands[long_k]
        short_c = cands.get(short_k) if short_k else None
        # Sellers receive the bid; fall back to the model/mid premium.
        short_prem = (short_c.get("bid") or short_c["premium"]) if short_c else 0.0
        spot_now = ideas["spot"]
        rows = payoff_curves(ideas["kind"], long_k, long_c["premium"], qty, spot_now,
                             ideas["days_to_expiry"], long_c["iv"],
                             r=(rate / 100) if not live else get_zerodha().r,
                             short_strike=short_k, short_premium=short_prem)
        pdf = pd.DataFrame(rows)
        chart = alt.Chart(pdf).mark_line(strokeWidth=2.5).encode(
            x=alt.X("spot:Q", title=f"{underlying} at the date", scale=alt.Scale(zero=False)),
            y=alt.Y("pnl:Q", title="P&L (Rs)"),
            color=alt.Color("structure:N", title=None),
            strokeDash=alt.StrokeDash("when:N", title=None,
                                      sort=["At expiry", "Today (model)"]),
            tooltip=["structure", "when", alt.Tooltip("spot:Q", format=",.0f"),
                     alt.Tooltip("pnl:Q", format=",.0f")],
        )
        zero = alt.Chart(pd.DataFrame({"y": [0]})).mark_rule(opacity=0.5).encode(y="y:Q")
        now_rule = alt.Chart(pd.DataFrame({"x": [spot_now]})).mark_rule(strokeDash=[2, 2]).encode(x="x:Q")
        st.altair_chart((chart + zero + now_rule).interactive(), width="stretch")

        legs = [Leg(ideas["kind"], "buy", long_k, long_c["premium"], qty)]
        if short_k:
            legs.append(Leg(ideas["kind"], "sell", short_k, short_prem, qty))
        summ = payoff_summary(legs, spot_now, 0.6)
        s1, s2, s3, s4 = st.columns(4)
        s1.metric("Net debit", f"Rs {-summ['net_premium']:,.0f}")
        s2.metric("Max loss", f"Rs {summ['max_loss_in_range']:,.0f}")
        s3.metric("Max profit", "unlimited" if summ["upside_unbounded"] or summ["downside_unbounded"]
                  else f"Rs {summ['max_profit_in_range']:,.0f}")
        s4.metric("Breakeven", ", ".join(f"{b:,.0f}" for b in summ["breakevens"]) or "n/a")
        st.caption("Solid = at expiry. Dashed = value today from Black-Scholes with the same IV. "
                   "Dotted vertical line = current spot.")

        if live and long_c.get("tradingsymbol"):
            st.subheader("Order preview")
            st.caption("This button only runs the safety checks. It never sends an order. "
                       "Place real orders through Claude (MCP) or Kite after review.")
            limit = st.number_input("Limit price", 0.05, 1e6,
                                    float(long_c.get("ask") or long_c["premium"]), 0.05, key="limit")
            if st.button(f"Preview BUY {lots} lot(s) {long_c['tradingsymbol']}", key="preview"):
                res = get_zerodha().place_option_order(long_c["tradingsymbol"], "BUY", int(lots),
                                                       float(limit), confirm=False)
                (st.error if res["status"] == "rejected_by_guard" else st.success)(res["status"])
                st.json(res)

# --------------------------------------------------------- global tab ---

with tab_global:
    cat = st.selectbox("Category", ["all"] + list(UNIVERSE), key="cat")
    snap = cached_snapshot(cat)
    frames = []
    for group, rows_ in snap["data"].items():
        for r in rows_:
            frames.append({"group": group, **r})
    if frames:
        gdf = pd.DataFrame(frames)[["group", "name", "symbol", "last", "chg_1d_pct", "chg_1w_pct",
                                    "chg_1m_pct", "chg_3m_pct", "chg_1y_pct", "vs_200dma_pct", "hv_20d"]]
        pct = {c: st.column_config.NumberColumn(c.replace("chg_", "").replace("_pct", " %"), format="%+.2f")
               for c in ["chg_1d_pct", "chg_1w_pct", "chg_1m_pct", "chg_3m_pct", "chg_1y_pct"]}
        st.dataframe(gdf, hide_index=True, column_config={
            **pct,
            "last": st.column_config.NumberColumn("Last", format="%,.2f"),
            "vs_200dma_pct": st.column_config.NumberColumn("vs 200-DMA %", format="%+.1f"),
            "hv_20d": st.column_config.NumberColumn("HV 20d", format="percent"),
        })
    else:
        st.write("No global data. Check the internet connection (Yahoo Finance).")
    if snap["missing"]:
        st.caption("No data for: " + ", ".join(snap["missing"]))
    st.caption(snap["source"])
