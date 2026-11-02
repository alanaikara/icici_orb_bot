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
Create an app at <https://developers.kite.trade>. Set the **redirect URL** to
`http://127.0.0.1:5000/` and leave the postback URL empty. Live quotes and
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
.venv-mcp/bin/python -m global_options_mcp.kite_login --if-needed
```
The browser opens the Kite login page. You log in (password + 2FA). The script
catches the redirect on `http://127.0.0.1:5000/`, saves `KITE_ACCESS_TOKEN` to
`.env` and exits. The MCP server and dashboard pick up the new token on the next
call, with no restart.

| Option | Use |
|---|---|
| `--if-needed` | Skip the login if today's token still works |
| `--check` | Only test the saved token (exit code 0 = valid, 2 = expired) |
| `--manual` | Paste the redirect URL yourself (for a remote machine) |
| `--no-browser` | Print the login URL, do not open a browser |
| `KITE_REDIRECT_URL` | Use another local redirect URL; it must match the Kite console |

**Why the password and 2FA stay manual:** Zerodha requires a manual login once a
day. A script that stores your password and TOTP secret also gives full account
access to anyone who reads that file. This tool never stores them.

#### Open the login automatically every weekday morning
The scheduled job opens the browser at 8:45. You only log in.

**macOS** (launchd): save as `~/Library/LaunchAgents/com.user.kitelogin.plist`, then run
`launchctl load ~/Library/LaunchAgents/com.user.kitelogin.plist`.
```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>Label</key><string>com.user.kitelogin</string>
  <key>WorkingDirectory</key><string>/ABSOLUTE/PATH/icici_orb_bot</string>
  <key>ProgramArguments</key><array>
    <string>/ABSOLUTE/PATH/icici_orb_bot/.venv-mcp/bin/python</string>
    <string>-m</string><string>global_options_mcp.kite_login</string><string>--if-needed</string>
  </array>
  <key>StartCalendarInterval</key><array>
    <dict><key>Weekday</key><integer>1</integer><key>Hour</key><integer>8</integer><key>Minute</key><integer>45</integer></dict>
    <dict><key>Weekday</key><integer>2</integer><key>Hour</key><integer>8</integer><key>Minute</key><integer>45</integer></dict>
    <dict><key>Weekday</key><integer>3</integer><key>Hour</key><integer>8</integer><key>Minute</key><integer>45</integer></dict>
    <dict><key>Weekday</key><integer>4</integer><key>Hour</key><integer>8</integer><key>Minute</key><integer>45</integer></dict>
    <dict><key>Weekday</key><integer>5</integer><key>Hour</key><integer>8</integer><key>Minute</key><integer>45</integer></dict>
  </array>
  <key>StandardOutPath</key><string>/tmp/kitelogin.log</string>
  <key>StandardErrorPath</key><string>/tmp/kitelogin.log</string>
</dict></plist>
```

**Windows** (Task Scheduler, run in PowerShell):
```powershell
$a = New-ScheduledTaskAction -Execute "C:\PATH\icici_orb_bot\.venv-mcp\Scripts\python.exe" -Argument "-m global_options_mcp.kite_login --if-needed" -WorkingDirectory "C:\PATH\icici_orb_bot"
$t = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At 8:45am
Register-ScheduledTask -TaskName "KiteLogin" -Action $a -Trigger $t
```

**Linux desktop** (`crontab -e`; cron needs your display to open a browser):
```
45 8 * * 1-5 cd /ABSOLUTE/PATH/icici_orb_bot && DISPLAY=:0 .venv-mcp/bin/python -m global_options_mcp.kite_login --if-needed >> /tmp/kitelogin.log 2>&1
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

## Dashboard (Streamlit)

```bash
.venv-mcp/bin/streamlit run global_options_mcp/dashboard.py
```

- **Data source**: *Live (Zerodha)* uses the real chain (bid/ask, spread, OI). *Model* uses
  Black-Scholes prices from spot and IV that you enter, so it works without Kite keys.
- **Header**: global regime, macro score, 1-year historical vol, spot. A warning shows when your
  view disagrees with the global regime.
- **Ranked strikes**: the same ranking as the `long_option_ideas` MCP tool, with lots and capital at
  risk for your budget.
- **Payoff**: long option compared with a debit spread, at expiry and today (model value).
- **Global markets**: the full global table by category.
- **Order preview**: runs the order guards only. The dashboard never sends orders.

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
├── dashboard.py     Streamlit dashboard for option ideas
├── ideas.py         shared idea-building logic (server + dashboard)
├── kite_client.py   Zerodha adapter + order guards
├── kite_login.py    daily login: auto-captures the redirect, saves the token
├── global_data.py   global universe + stats (Yahoo Finance)
├── analysis.py      macro regime + strike ranking
├── options_math.py  Black-Scholes, Greeks, IV, HV, payoff
├── docs/            HTML explainer + MP4 video
└── tests/           offline tests (fake broker + fake data)
```
