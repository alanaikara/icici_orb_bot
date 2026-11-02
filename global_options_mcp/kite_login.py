"""Daily Zerodha login helper.

Kite access tokens expire every day (around 6 AM). Run once each morning:

    python -m global_options_mcp.kite_login

1. Opens the Kite login URL.
2. You log in; Zerodha redirects to your app's redirect URL with ?request_token=...
3. Paste the request_token (or the full redirect URL) here.
4. The script prints KITE_ACCESS_TOKEN and, with --write-env, saves it to .env.
"""
from __future__ import annotations

import argparse
import os
import re
import sys
import webbrowser
from pathlib import Path


def _update_env(path: Path, key: str, value: str) -> None:
    lines = path.read_text().splitlines() if path.exists() else []
    out, done = [], False
    for line in lines:
        if line.startswith(f"{key}="):
            out.append(f"{key}={value}")
            done = True
        else:
            out.append(line)
    if not done:
        out.append(f"{key}={value}")
    path.write_text("\n".join(out) + "\n")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--write-env", metavar="PATH", nargs="?", const=".env",
                    help="save KITE_ACCESS_TOKEN into this .env file (default .env)")
    ap.add_argument("--no-browser", action="store_true")
    args = ap.parse_args(argv)

    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass
    api_key, secret = os.getenv("KITE_API_KEY"), os.getenv("KITE_API_SECRET")
    if not api_key or not secret:
        print("Set KITE_API_KEY and KITE_API_SECRET first (env or .env).", file=sys.stderr)
        return 1

    from kiteconnect import KiteConnect

    kite = KiteConnect(api_key=api_key)
    url = kite.login_url()
    print(f"Login URL:\n  {url}\n")
    if not args.no_browser:
        webbrowser.open(url)
    raw = input("Paste request_token or the full redirect URL: ").strip()
    m = re.search(r"request_token=([A-Za-z0-9]+)", raw)
    request_token = m.group(1) if m else raw

    session = kite.generate_session(request_token, api_secret=secret)
    token = session["access_token"]
    print(f"\nKITE_ACCESS_TOKEN={token}\n(valid until ~6 AM tomorrow)")
    if args.write_env:
        _update_env(Path(args.write_env), "KITE_ACCESS_TOKEN", token)
        print(f"Saved to {args.write_env}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
