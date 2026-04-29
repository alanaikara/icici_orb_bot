"""
Live trader orchestrator for the Fib-MACD strategy.

Every minute:
  1. Fetch latest 1-min candle for each active stock
  2. Feed candle into the stock's state machine
  3. If state machine signals an entry/exit, place orders via broker
  4. Update 5-min MACD for each stock
  5. Log P&L and state summary

Usage:
    from live.live_trader import LiveTrader
    from live.breeze_broker import BreezeBroker
    from live.strategy_config import FIB_MACD_PORTFOLIO

    broker = BreezeBroker(app_key, secret_key, session_token)
    trader = LiveTrader(broker, FIB_MACD_PORTFOLIO)
    trader.run()
"""

import logging
import time
import threading
from datetime import datetime, date
from typing import Optional

import pandas as pd
import numpy as np

from live.broker_base import BrokerBase
from live.strategy_config import StrategyConfig
from live.stock_state import StockState, State, OpenTrade

logger = logging.getLogger("ICICI_ORB_Bot")

MARKET_OPEN  = "09:15"
MARKET_CLOSE = "15:30"


class LiveTrader:
    """
    Runs the Fib-MACD strategy live across all configured stocks.
    Polls broker for 1-min candles every minute and manages orders.
    """

    def __init__(
        self,
        broker: BrokerBase,
        config: StrategyConfig,
        dry_run: bool = True,    # True = log orders but don't place them
        poll_interval: int = 60, # seconds between candle polls
    ):
        self.broker        = broker
        self.config        = config
        self.dry_run       = dry_run
        self.poll_interval = poll_interval

        # One StockState per configured stock
        self.states: dict[str, StockState] = {
            sc.stock_code: StockState(
                stock_code = sc.stock_code,
                config     = config,
                stock_cfg  = sc,
            )
            for sc in config.stocks if sc.active
        }

        self._running = False
        logger.info(
            f"LiveTrader init: {len(self.states)} stocks, "
            f"dry_run={dry_run}"
        )

    # ── Main loop ─────────────────────────────────────────────────────────────

    def run(self):
        """Blocking main loop. Ctrl-C to stop."""
        if not self.broker.connect():
            logger.error("Broker connection failed — aborting")
            return

        logger.info("Broker connected ✓")
        self._running = True

        # Reset all states for today
        for state in self.states.values():
            state.reset_for_day()

        try:
            while self._running:
                now = datetime.now()
                time_str = now.strftime("%H:%M")

                if time_str < MARKET_OPEN:
                    logger.info(f"Market not open yet ({time_str}). Waiting...")
                    time.sleep(30)
                    continue

                if time_str >= MARKET_CLOSE:
                    logger.info("Market closed. Stopping.")
                    self._emergency_exit_all()
                    break

                self._tick()

                # Sleep until next minute boundary
                sleep_secs = self.poll_interval - (now.second % self.poll_interval)
                time.sleep(sleep_secs)

        except KeyboardInterrupt:
            logger.info("Interrupted — exiting all positions")
            self._emergency_exit_all()

        self._running = False
        self._print_summary()

    # ── Per-tick logic ────────────────────────────────────────────────────────

    def _tick(self):
        """Called once per minute. Fetches candles and processes each stock."""
        now      = datetime.now()
        today    = now.strftime("%Y-%m-%d")
        from_dt  = f"{today} 09:15:00"
        to_dt    = now.strftime("%Y-%m-%d %H:%M:%S")

        # Fetch + update 5-min MACD for all stocks (do once per tick)
        self._refresh_macd(today, from_dt, to_dt)

        for stock_code, state in self.states.items():
            if state.state == State.DONE:
                continue

            # Fetch latest 1-min candles
            candles = self.broker.get_candles(
                stock_code, "1minute", from_dt, to_dt
            )
            if not candles:
                continue

            # Only process the latest candle we haven't seen
            latest = candles[-1]
            last_seen = (
                state.candles_1m[-1]["datetime"]
                if state.candles_1m else ""
            )
            if latest["datetime"] == last_seen:
                continue

            # Feed into state machine
            action = state.on_candle(latest)

            if action and action.startswith("enter_"):
                self._handle_entry(state, action)
            elif action == "exit_trade":
                self._handle_exit(state)

        self._log_status()

    # ── Order handling ────────────────────────────────────────────────────────

    def _handle_entry(self, state: StockState, action: str):
        """Place entry order + stop-loss order."""
        trade     = state.trade
        direction = trade.direction
        qty       = trade.quantity
        stock     = state.stock_code

        # Entry: market order (fills immediately at best price)
        buy_sell = "buy" if direction == "LONG" else "sell"

        if self.dry_run:
            logger.info(
                f"[DRY RUN] {stock} {buy_sell.upper()} {qty} @ ~{trade.entry_price:.2f} "
                f"| SL {trade.stop_loss:.2f} | TGT {trade.target_price:.2f}"
            )
            trade.entry_order_id = "DRY_ENTRY"
        else:
            result = self.broker.place_market_order(stock, buy_sell, qty)
            if not result.success:
                logger.error(f"{stock} entry order FAILED: {result.message}")
                state.state = State.DONE
                state.trade = None
                return
            trade.entry_order_id = result.order_id
            logger.info(f"{stock} entry order placed: {result.order_id}")

            # Place SL order immediately after entry
            sl_side = "sell" if direction == "LONG" else "buy"
            sl_result = self.broker.place_stoploss_order(
                stock_code    = stock,
                action        = sl_side,
                quantity      = qty,
                trigger_price = trade.stop_loss,
                limit_price   = round(trade.stop_loss * (0.995 if direction == "LONG" else 1.005), 2),
            )
            if sl_result.success:
                trade.sl_order_id = sl_result.order_id
                logger.info(f"{stock} SL order placed: {sl_result.order_id}")
            else:
                logger.warning(f"{stock} SL order FAILED: {sl_result.message} — monitor manually!")

    def _handle_exit(self, state: StockState):
        """Close open position with a market order."""
        trade = state.trade
        if not trade:
            return

        stock     = state.stock_code
        direction = trade.direction
        qty       = trade.quantity
        exit_side = "sell" if direction == "LONG" else "buy"

        if self.dry_run:
            logger.info(f"[DRY RUN] {stock} EXIT {exit_side.upper()} {qty}")
        else:
            # Cancel any open SL order first
            if trade.sl_order_id and trade.sl_order_id != "DRY_ENTRY":
                self.broker.cancel_order(trade.sl_order_id)

            result = self.broker.place_market_order(stock, exit_side, qty)
            if result.success:
                logger.info(f"{stock} exit order placed: {result.order_id}")
            else:
                logger.error(f"{stock} EXIT order FAILED: {result.message} — MANUAL ACTION REQUIRED!")

    def _emergency_exit_all(self):
        """Exit all IN_TRADE positions (market close / interrupt)."""
        for state in self.states.values():
            if state.state == State.IN_TRADE and state.trade:
                logger.warning(f"Emergency exit: {state.stock_code}")
                self._handle_exit(state)
                state.state = State.DONE

    # ── MACD refresh ─────────────────────────────────────────────────────────

    def _refresh_macd(self, today: str, from_dt: str, to_dt: str):
        """Fetch 5-min candles for all stocks and recompute MACD."""
        for stock_code, state in self.states.items():
            if state.state == State.DONE:
                continue
            try:
                candles_5m = self.broker.get_candles(
                    stock_code, "5minute", from_dt, to_dt
                )
                if len(candles_5m) < 2:
                    continue
                df = pd.DataFrame(candles_5m)
                df["time_str"] = df["datetime"].str[11:19]
                closes = df["close"].values
                ema_fast    = self._ema(closes, self.config.macd_fast)
                ema_slow    = self._ema(closes, self.config.macd_slow)
                macd_line   = ema_fast - ema_slow
                signal_line = self._ema(macd_line, self.config.macd_signal)
                df["macd"]      = macd_line
                df["signal"]    = signal_line
                df["histogram"] = macd_line - signal_line
                state.update_macd(df[["time_str", "macd", "signal", "histogram"]])
            except Exception as e:
                logger.warning(f"MACD refresh failed for {stock_code}: {e}")

    @staticmethod
    def _ema(values: np.ndarray, period: int) -> np.ndarray:
        return pd.Series(values).ewm(span=period, adjust=False).mean().values

    # ── Status / logging ──────────────────────────────────────────────────────

    def _log_status(self):
        now      = datetime.now().strftime("%H:%M:%S")
        in_trade = [s for s in self.states.values() if s.state == State.IN_TRADE]
        waiting  = [s for s in self.states.values() if s.state not in (State.DONE, State.IN_TRADE)]
        done     = [s for s in self.states.values() if s.state == State.DONE]

        logger.info(
            f"[{now}] IN_TRADE={len(in_trade)} | "
            f"WATCHING={len(waiting)} | DONE={len(done)}"
        )
        for s in in_trade:
            t = s.trade
            logger.info(
                f"  ↳ {s.stock_code} {t.direction} {t.quantity}@ "
                f"{t.entry_price:.2f} → SL {t.stop_loss:.2f} TGT {t.target_price:.2f}"
            )

    def _print_summary(self):
        logger.info("\n" + "="*60)
        logger.info("END OF DAY SUMMARY")
        logger.info("="*60)
        traded = [s for s in self.states.values() if s.trade is not None]
        logger.info(f"Stocks that traded today: {len(traded)}")
        for s in traded:
            t = s.trade
            logger.info(
                f"  {s.stock_code}: {t.direction} {t.quantity} @ "
                f"{t.entry_price:.2f} | state={s.state.name}"
            )
        no_trade = [
            s.stock_code for s in self.states.values()
            if s.trade is None and s.state == State.DONE
        ]
        logger.info(f"No trade today: {no_trade}")
