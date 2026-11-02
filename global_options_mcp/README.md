# Global Options MCP

An MCP server that connects Claude to **your Zerodha account**, **global market
data** and **option analytics**, so that you can research **long-dated option
positions** (months to years) on Indian markets.

> Research tool only. Not financial advice. Orders are blocked by default.

## What it does

| Area | Tools |
|---|---|
| Global data (Yahoo Finance, no key) | `global_market_snapshot`, `symbol_stats`, `list_global_symbols` |
| Macro backdrop | `macro_regime_score`: risk-on / risk-off score from Nifty, S&P 500, copper trends, VIX, DXY, USD/INR, Brent, US 10Y |
| Option maths (offline) | `black_scholes`, `implied_volatility`, `strategy_payoff`, `evaluate_long_options` |
| Zerodha (Kite Connect) | `kite_profile`, `kite_margins`, `kite_holdings`, `kite_positions`, `kite_orders` |
| Live options | `kite_option_expiries`, `kite_option_chain` (bid/ask, spread %, OI, IV, Greeks) |
| Decision support | `long_option_ideas`: live chain + historical vol + macro score → ranked strikes |
| Orders (guarded) | `kite_place_option_order` |
| Prompt | `long_term_option_plan`: step-by-step plan |

## Order safety

1. `LIVE_ORDERS` is `false` by default. Then every order is a **preview only**.
2. With `LIVE_ORDERS=true`, the tool still needs `confirm=true`. Claude must ask you first.
3. Only **LIMIT** orders, product **NRML**, exchange **NFO**.
4. Caps: `MAX_LOTS_PER_ORDER` (default 2) and `MAX_ORDER_PREMIUM` (default ₹50,000).
5. **SELL** is allowed only to close a long you hold. Naked option selling is blocked.

## Set up

### 1. Get a Kite Connect app
Create an app at <https://developers.kite.trade>. Set the redirect URL to
`http://127.0.0.1/` (any URL works; you copy the token from it). Live quotes and
option chains need a Kite Connect plan that includes market data. Check the
current price on the Zerodha site.

### 2. Install
```bash
python -m venv .venv-mcp
```
```bash
.venv-mcp/bin/pip install -r global_options_mcp/requirements.txt
```

### 3. Configure `.env` (in the repo root)
```bash
cp global_options_mcp/.env.example .env
```
Fill in `KITE_API_KEY` and `KITE_API_SECRET`.

### 4. Log in (every morning, the token expires daily)
```bash
.venv-mcp/bin/python -m global_options_mcp.kite_login --write-env
```

### 5. Connect to Claude

**Claude Code:**
```bash
claude mcp add global-options -- /ABSOLUTE/PATH/icici_orb_bot/.venv-mcp/bin/python -m global_options_mcp.server
```

**Claude Desktop** (`claude_desktop_config.json`):
```json
{
  "mcpServers": {
    "global-options": {
      "command": "/ABSOLUTE/PATH/icici_orb_bot/.venv-mcp/bin/python",
      "args": ["-m", "global_options_mcp.server"],
      "cwd": "/ABSOLUTE/PATH/icici_orb_bot"
    }
  }
}
```

**HTTP mode** (for other MCP clients): `python -m global_options_mcp.server --http --port 8765`

### Optional: Zerodha's official Kite MCP
Zerodha also runs a hosted Kite MCP server (see the Zerodha / Kite MCP docs for
the current URL). You can add it next to this server: use it for account access,
and use this server for global data and option analytics. Without Kite Connect
keys, all global-data and maths tools in this server still work.

## Example questions for Claude

- "Give me the macro regime and a one-paragraph global summary."
- "Which NIFTY expiries are more than 180 days away?"
- "I am bullish on NIFTY for 12 months with ₹5 lakh. Use long_option_ideas for the
  nearest expiry after 300 days, max 2 % risk, and compare the best call with a
  bull call spread."
- "Show the payoff of buying the 24000 CE at 1850 and selling the 26000 CE at 820, lot 75."

## How the strike ranking works

`rank_long_option_candidates` (in `analysis.py`) scores each strike:

| Factor | Better |
|---|---|
| Delta | 0.55–0.80 (stock-replacement zone) |
| Theta per month as % of premium | below 3 % |
| Breakeven distance | within 0.5–1 standard deviation of expected move |
| IV vs 1-year historical vol | IV below HV (cheap volatility) |

`long_option_ideas` also drops legs with a bid-ask spread above 5 % and uses the
**ask** price, because buyers pay the ask.

## Notes on Indian long-dated options

- Index options (for example NIFTY) list longer expiries than stock options.
  Stock options usually go out only about 3 months. Use `kite_option_expiries`
  to see what is listed today.
- Far expiries can have wide spreads and small open interest. Check `spread_pct`
  and `oi` before you trade, and always use limit orders.

## Tests
```bash
.venv-mcp/bin/python -m pytest global_options_mcp/tests -q
```

## Files
```
global_options_mcp/
├── server.py        MCP server (tools + prompt)
├── kite_client.py   Zerodha adapter + order guards
├── kite_login.py    daily access-token helper
├── global_data.py   global universe + stats (Yahoo Finance)
├── analysis.py      macro regime + strike ranking
├── options_math.py  Black-Scholes, Greeks, IV, HV, payoff
├── docs/            HTML explainer + MP4 video
└── tests/           offline tests (fake broker + fake data)
```
