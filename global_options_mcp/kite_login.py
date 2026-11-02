"""Daily Zerodha login with automatic token capture.

Kite access tokens expire every day (around 6 AM). Run once each morning:

    python -m global_options_mcp.kite_login              # login, save token to .env
    python -m global_options_mcp.kite_login --if-needed  # skip if today's token still works
    python -m global_options_mcp.kite_login --check      # only test the saved token

How it works
1. A small web server starts on your redirect URL (default http://127.0.0.1:5000/).
2. Your browser opens the Kite login page.
3. You log in yourself (password + 2FA). Zerodha requires a manual daily login,
   so this script never stores or types your password or TOTP.
4. Zerodha redirects to the local server. The script reads the request_token,
   exchanges it for an access token and saves KITE_ACCESS_TOKEN to .env.
5. The MCP server and the dashboard read .env again on the next call, so they
   do not need a restart.

Set KITE_REDIRECT_URL if your Kite app uses another local URL. It must match the
redirect URL in the Kite developer console exactly. Use --manual to paste the URL
yourself instead (for example on a remote machine).
"""
from __future__ import annotations

import argparse
import os
import re
import sys
import threading
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

DEFAULT_REDIRECT = "http://127.0.0.1:5000/"

_PAGE = """<!doctype html><meta charset="utf-8"><title>Kite login</title>
<body style="font-family:system-ui,sans-serif;max-width:32rem;margin:15vh auto;padding:0 16px">
<h2>{title}</h2><p>{body}</p></body>"""


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
    try:
        os.chmod(path, 0o600)  # token gives account access: owner-only
    except OSError:
        pass


def parse_request_token(raw: str) -> str | None:
    """Accept a full redirect URL, a query string, or a bare token."""
    raw = raw.strip()
    m = re.search(r"request_token=([A-Za-z0-9]+)", raw)
    if m:
        return m.group(1)
    return raw if re.fullmatch(r"[A-Za-z0-9]{8,}", raw) else None


def wait_for_request_token(redirect_url: str, timeout: float = 300,
                           on_ready=None) -> str | None:
    """Serve the redirect URL locally until Zerodha sends a request_token.

    Returns the token, or None on timeout / failed login.
    """
    u = urlparse(redirect_url)
    if u.scheme != "http" or u.hostname not in ("127.0.0.1", "localhost"):
        raise ValueError(f"Automatic capture needs a local http redirect URL, got {redirect_url!r}. "
                         "Use --manual, or set KITE_REDIRECT_URL=http://127.0.0.1:5000/")
    port = u.port if u.port is not None else 80
    expected_path = u.path or "/"
    result: dict = {}
    done = threading.Event()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802 (http.server API)
            p = urlparse(self.path)
            q = parse_qs(p.query)
            token = (q.get("request_token") or [None])[0]
            status = (q.get("status") or [""])[0]
            if p.path.rstrip("/") != expected_path.rstrip("/") or not (token or status):
                self._send(404, "Not found", "This server only waits for the Kite login redirect.")
                return
            if token and status in ("", "success"):
                result["token"] = token
                self._send(200, "Login received",
                           "You can close this tab and go back to the terminal.")
            else:
                result["error"] = status or "no request_token"
                self._send(400, "Login failed", f"Zerodha returned status '{status}'. Run the login again.")
            done.set()

        def _send(self, code, title, body):
            data = _PAGE.format(title=title, body=body).encode()
            self.send_response(code)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *a):  # keep the terminal quiet
            pass

    server = HTTPServer((u.hostname, port), Handler)
    server.timeout = 0.5
    if on_ready:
        on_ready(server.server_address[1])
    try:
        deadline = timeout
        while not done.is_set() and deadline > 0:
            server.handle_request()
            deadline -= server.timeout
    finally:
        server.server_close()
    if "error" in result:
        print(f"Login failed: {result['error']}", file=sys.stderr)
    return result.get("token")


def token_works(api_key: str, token: str | None, kite_cls=None) -> dict | None:
    """Return the profile if the token is valid, else None."""
    if not token:
        return None
    if kite_cls is None:
        from kiteconnect import KiteConnect as kite_cls
    try:
        k = kite_cls(api_key=api_key)
        k.set_access_token(token)
        return k.profile()
    except Exception:
        return None


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--env-file", default=os.getenv("KITE_ENV_FILE", ".env"),
                    help="where to save KITE_ACCESS_TOKEN (default .env)")
    ap.add_argument("--check", action="store_true", help="only test the saved token, then exit")
    ap.add_argument("--if-needed", action="store_true", help="skip login if the saved token works")
    ap.add_argument("--manual", action="store_true", help="paste the redirect URL instead of auto-capture")
    ap.add_argument("--no-browser", action="store_true", help="print the login URL but do not open it")
    ap.add_argument("--timeout", type=float, default=300, help="seconds to wait for login (default 300)")
    ap.add_argument("--write-env", nargs="?", const=None, help=argparse.SUPPRESS)  # old flag, still accepted
    args = ap.parse_args(argv)
    env_path = Path(args.write_env or args.env_file)

    from .kite_client import read_env_file

    for k, v in read_env_file(env_path).items():
        if k == "KITE_ACCESS_TOKEN" or not os.getenv(k):  # file token is the latest one
            os.environ[k] = v
    api_key, secret = os.getenv("KITE_API_KEY"), os.getenv("KITE_API_SECRET")
    if not api_key or not secret:
        print(f"Set KITE_API_KEY and KITE_API_SECRET in {env_path} first.", file=sys.stderr)
        return 1

    if args.check or args.if_needed:
        prof = token_works(api_key, os.getenv("KITE_ACCESS_TOKEN"))
        if prof:
            print(f"Saved token works for {prof.get('user_name')} ({prof.get('user_id')}).")
            return 0
        if args.check:
            print("Saved token is missing or expired. Run the login.", file=sys.stderr)
            return 2
        print("Saved token is missing or expired. Starting login...")

    from kiteconnect import KiteConnect

    kite = KiteConnect(api_key=api_key)
    url = kite.login_url()
    redirect = os.getenv("KITE_REDIRECT_URL", DEFAULT_REDIRECT)

    if args.manual:
        print(f"Login URL:\n  {url}\n")
        if not args.no_browser:
            webbrowser.open(url)
        request_token = parse_request_token(input("Paste the redirect URL or request_token: "))
    else:
        def ready(port):
            print(f"Waiting for the Kite redirect on {redirect} (port {port}) ...")
            print(f"Log in here if the browser does not open:\n  {url}\n")
            if not args.no_browser:
                webbrowser.open(url)
        try:
            request_token = wait_for_request_token(redirect, args.timeout, ready)
        except OSError as e:
            print(f"Cannot listen on {redirect}: {e}. Close the other program or use --manual.",
                  file=sys.stderr)
            return 1

    if not request_token:
        print("No request_token received (timeout or failed login).", file=sys.stderr)
        return 1

    session = kite.generate_session(request_token, api_secret=secret)
    token = session["access_token"]
    _update_env(env_path, "KITE_ACCESS_TOKEN", token)
    print(f"Logged in as {session.get('user_name', '?')}. Token saved to {env_path} "
          "(valid until about 6 AM tomorrow).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
