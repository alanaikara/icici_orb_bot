import datetime as dt
import math

import pytest

from global_options_mcp.analysis import macro_regime, rank_long_option_candidates
from global_options_mcp.global_data import GlobalData, compute_stats
from global_options_mcp.kite_client import OrderLimits, ZerodhaClient
from global_options_mcp.options_math import (
    Leg, bs_greeks, bs_price, historical_vol, implied_vol, payoff_summary)


# ------------------------------------------------------------- maths --

def test_bs_reference_value():
    # Hull textbook value: S=K=100, T=1, r=5%, vol=20% -> call 10.4506
    assert bs_price(100, 100, 1, 0.05, 0.2, "call") == pytest.approx(10.4506, abs=1e-4)


def test_put_call_parity():
    S, K, T, r, v = 24000, 25000, 1.5, 0.065, 0.16
    c, p = bs_price(S, K, T, r, v, "CE"), bs_price(S, K, T, r, v, "PE")
    assert c - p == pytest.approx(S - K * math.exp(-r * T), abs=1e-6)


def test_greeks_sanity():
    g = bs_greeks(24000, 24000, 1.0, 0.065, 0.15, "call")
    assert 0.5 < g.delta < 0.75
    assert g.gamma > 0 and g.vega_per_1pct > 0 and g.theta_per_day < 0
    gp = bs_greeks(24000, 24000, 1.0, 0.065, 0.15, "put")
    assert g.delta - gp.delta == pytest.approx(1.0, abs=1e-9)


def test_implied_vol_round_trip():
    price = bs_price(24000, 26000, 0.75, 0.065, 0.17, "call")
    assert implied_vol(price, 24000, 26000, 0.75, 0.065, "call") == pytest.approx(0.17, abs=1e-4)
    assert implied_vol(0.0001, 24000, 20000, 0.75, 0.065, "call") is None  # below intrinsic


def test_historical_vol():
    flat = [100 * 1.001 ** i for i in range(100)]
    assert historical_vol(flat) == pytest.approx(0.0, abs=1e-9)
    zigzag = [100, 101, 100, 101, 100, 101]
    assert historical_vol(zigzag) > 0.1


def test_bull_call_spread_payoff():
    legs = [Leg("call", "buy", 24000, 1500, 75), Leg("call", "sell", 26000, 600, 75)]
    s = payoff_summary(legs, 25000)
    assert s["max_loss_in_range"] == pytest.approx(-900 * 75)
    assert s["max_profit_in_range"] == pytest.approx((2000 - 900) * 75)
    assert s["breakevens"] == [pytest.approx(24900, abs=1)]
    assert not s["upside_unbounded"]


# -------------------------------------------------------- global data --

def _series(start, drift, n=300):
    return [start * (1 + drift) ** i for i in range(n)]


def test_compute_stats_and_cache():
    calls = []

    def fake(sym, period):
        calls.append(sym)
        return _series(100, 0.001)

    gd = GlobalData(fetcher=fake)
    st = gd.stats("^NSEI")
    assert st.name == "Nifty 50" and st.vs_200dma_pct > 0 and st.chg_1y_pct > 0
    gd.stats("^NSEI")
    assert calls == ["^NSEI"]  # second call served from cache


def test_snapshot_handles_vendor_failure():
    def broken(sym, period):
        raise RuntimeError("network down")

    snap = GlobalData(fetcher=broken).snapshot("fx")
    assert snap["data"]["fx"] == [] and "INR=X" in snap["missing"]


def test_macro_regime_risk_on_and_off():
    up, down = _series(100, 0.002), _series(100, -0.002)
    vix_calm = [12.0] * 300
    good = {"^NSEI": up, "^GSPC": up, "HG=F": up, "^VIX": vix_calm,
            "DX-Y.NYB": down, "INR=X": down, "BZ=F": down, "^TNX": down}
    res = macro_regime(lambda s: compute_stats(s, good.get(s, [])))
    assert res["regime"] == "risk-on" and res["score"] >= 3

    bad = {"^NSEI": down, "^GSPC": down, "HG=F": down, "^VIX": [35.0] * 300,
           "DX-Y.NYB": up, "INR=X": up, "BZ=F": up, "^TNX": up}
    assert macro_regime(lambda s: compute_stats(s, bad.get(s, [])))["regime"] == "risk-off"
    assert macro_regime(lambda s: None)["regime"] == "unknown"


# ------------------------------------------------------------ ranking --

def test_rank_prefers_itm_low_decay_over_far_otm():
    res = rank_long_option_candidates(
        spot=24000, strikes=[21000, 22000, 23000, 24000, 26000, 28000],
        days_to_expiry=365, kind="call", iv=0.15, hv=0.16,
        lot_size=75, capital=1_000_000, max_risk_pct=10)
    top, worst = res["candidates"][0], res["candidates"][-1]
    assert top["strike"] <= 24000
    assert worst["strike"] == 28000
    assert all(c["capital_at_risk"] <= 100_000 for c in res["candidates"])


# ------------------------------------------------------------- broker --

class FakeKite:
    def __init__(self):
        self.placed = []
        exp = dt.date.today() + dt.timedelta(days=400)
        self.exp = exp
        self.inst = [
            {"name": "NIFTY", "instrument_type": t, "expiry": exp, "strike": float(k),
             "tradingsymbol": f"NIFTY{exp:%y%b}{k}{t}".upper(), "lot_size": 75}
            for k in range(22000, 26001, 500) for t in ("CE", "PE")
        ]

    def instruments(self, ex):
        return self.inst

    def ltp(self, syms):
        return {s: {"last_price": 24000.0} for s in syms}

    def quote(self, keys):
        out = {}
        for k in keys:
            i = next(x for x in self.inst if f"NFO:{x['tradingsymbol']}" == k)
            kind = "call" if i["instrument_type"] == "CE" else "put"
            p = bs_price(24000, i["strike"], 400 / 365, 0.065, 0.15, kind)
            out[k] = {"last_price": p, "oi": 1000, "volume": 10,
                      "depth": {"buy": [{"price": p * 0.99}], "sell": [{"price": p * 1.01}]}}
        return out

    def positions(self):
        return {"net": []}

    def place_order(self, **kw):
        self.placed.append(kw)
        return "ORDER123"


def test_expiries_and_chain_iv():
    fk = FakeKite()
    z = ZerodhaClient(kite=fk, limits=OrderLimits(False, 1e9, 10))
    exps = z.option_expiries("NIFTY", min_days=300)
    assert exps and exps[0]["expiry"] == fk.exp.isoformat()
    ch = z.option_chain("NIFTY", fk.exp.isoformat(), width=2)
    assert len(ch["chain"]) == 5 and ch["lot_size"] == 75
    atm = next(r for r in ch["chain"] if r["strike"] == 24000)
    assert atm["CE"]["iv"] == pytest.approx(0.15, abs=0.01)
    assert atm["CE"]["spread_pct"] == pytest.approx(2.0, abs=0.01)


def _sym(fk, k=24000, t="CE"):
    return next(i["tradingsymbol"] for i in fk.inst if i["strike"] == k and i["instrument_type"] == t)


def test_order_guards():
    fk = FakeKite()
    sym = _sym(fk)
    # default: preview only
    r = ZerodhaClient(kite=fk, limits=OrderLimits(False, 1e9, 10)).place_option_order(sym, "BUY", 1, 1500)
    assert r["status"] == "preview_only" and not fk.placed
    # live but not confirmed
    z = ZerodhaClient(kite=fk, limits=OrderLimits(True, 1e9, 10))
    assert z.place_option_order(sym, "BUY", 1, 1500)["status"] == "needs_confirmation"
    # caps
    zc = ZerodhaClient(kite=fk, limits=OrderLimits(True, 50_000, 2))
    assert zc.place_option_order(sym, "BUY", 3, 100, confirm=True)["status"] == "rejected_by_guard"
    assert zc.place_option_order(sym, "BUY", 1, 1500, confirm=True)["status"] == "rejected_by_guard"
    # naked sell blocked
    assert z.place_option_order(sym, "SELL", 1, 1500, confirm=True)["status"] == "rejected_by_guard"
    # live + confirmed -> placed as LIMIT NRML
    ok = z.place_option_order(sym, "BUY", 1, 1500, confirm=True)
    assert ok["status"] == "placed" and fk.placed[-1]["order_type"] == "LIMIT"
    assert fk.placed[-1]["product"] == "NRML" and fk.placed[-1]["quantity"] == 75


# ---------------------------------------------------- dashboard helpers --

from global_options_mcp.global_data import GlobalData as _GD
from global_options_mcp.ideas import long_option_ideas, model_strikes, payoff_curves


def test_model_strikes_grid():
    s = model_strikes(24037, 500, 2)
    assert s == [23000, 23500, 24000, 24500, 25000]
    with pytest.raises(ValueError):
        model_strikes(24000, 0)


def test_payoff_curves_shapes():
    rows = payoff_curves("call", 24000, 2273, 75, 24000, 365, 0.15,
                         short_strike=26000, short_premium=1271, points=11)
    assert {r["structure"] for r in rows} == {"Long option", "Debit spread"}
    assert {r["when"] for r in rows} == {"At expiry", "Today (model)"}
    exp_spread = [r for r in rows if r["structure"] == "Debit spread" and r["when"] == "At expiry"]
    assert min(r["pnl"] for r in exp_spread) == pytest.approx(-1002 * 75)
    # today's model value at spot equals ~0 P&L when premium is the model price
    today = [r for r in rows if r["structure"] == "Long option" and r["when"] == "Today (model)"
             and r["spot"] == 24000]
    assert today[0]["pnl"] == pytest.approx(0, abs=75)
    assert all(r["when"] == "At expiry" for r in payoff_curves("put", 24000, 900, 1, 24000, 0, 0.15))


def test_long_option_ideas_live_path():
    fk = FakeKite()
    z = ZerodhaClient(kite=fk, limits=OrderLimits(False, 1e9, 10))
    gd = _GD(fetcher=lambda s, p: _series(100, 0.0005))
    res = long_option_ideas(z, gd, "NIFTY", fk.exp.isoformat(), "bullish",
                            capital=1_000_000, max_risk_pct=5, width=4)
    c0 = res["candidates"][0]
    assert c0["tradingsymbol"].endswith("CE") and c0["ask"] and c0["oi"] == 1000
    assert res["lot_size"] == 75 and "macro" in res
    bear = long_option_ideas(z, gd, "NIFTY", fk.exp.isoformat(), "bearish", width=2,
                             include_macro=False)
    assert bear["kind"] == "put" and "macro" not in bear


def test_rank_tie_prefers_delta_near_070():
    res = rank_long_option_candidates(24000, model_strikes(24000, 500, 10), 365, "call", 0.15)
    top = res["candidates"][0]
    tied = [c for c in res["candidates"] if c["score"] == top["score"]]
    assert abs(top["delta"] - 0.70) == min(abs(c["delta"] - 0.70) for c in tied)
