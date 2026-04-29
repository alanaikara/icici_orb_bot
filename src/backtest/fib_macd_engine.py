"""
Fibonacci Retracement + MACD Confirmation ORB strategy engine.

Strategy logic:
  1. Identify Opening Range (first N minutes).
  2. Wait for price to break the OR high/low — this sets direction only.
  3. After the breakout, track the swing high/low that forms.
     Swing is "confirmed" when price retraces swing_confirm_pct from the peak.
  4. Draw Fibonacci from OR_low → swing_high (long) or OR_high → swing_low (short).
  5. Enter at the first touch of the fib_entry_pct level (50% or 61.8%)
     provided the 5-min MACD confirms momentum in the trade direction.
  6. Stop loss just below the 78.6% level (+ sl_buffer_pct buffer).
  7. Target = entry ± risk * target_r   (or end-of-day if target_r == 0).
  8. If price blows through 78.6% before entry fires, the setup is voided.
"""

import logging
from dataclasses import dataclass
from itertools import product

import numpy as np

from backtest.data_loader import StockData
from backtest.metrics import Trade

logger = logging.getLogger("ICICI_ORB_Bot")

# ── Fibonacci constants ───────────────────────────────────────────────────────
FIB_382 = 0.382   # Early pullback entry — chart marks this as key level
FIB_50  = 0.500
FIB_618 = 0.618
FIB_786 = 0.786


# ── Parameter dataclass ───────────────────────────────────────────────────────

@dataclass(frozen=True)
class FibMACDParams:
    """
    One complete parameter combination for the Fib-MACD strategy.
    Frozen so it is hashable and safe for multiprocessing.
    """
    or_minutes: int          # Opening range duration (15 or 30)
    fib_entry_pct: float     # Retracement level for entry: 0.50 or 0.618
    macd_condition: str      # 'histogram_positive' | 'histogram_rising' | 'macd_cross' | 'none'
    target_r: float          # Risk-reward target (0 = hold to exit_time)
    trade_direction: str     # 'long_only' | 'short_only' | 'both'
    exit_time: str           # 'HH:MM' — force-close time

    # Less-swept but still parameterised
    swing_confirm_pct: float = 0.003   # Drop % from peak to lock in swing high
    sl_buffer_pct: float     = 0.001   # Extra buffer below 78.6% SL level
    max_wait_bars: int       = 60      # 1-min bars to wait for swing after breakout
    breakout_type: str       = 'immediate'  # 'immediate' | 'candle_close'

    def param_id(self) -> str:
        import hashlib
        key = (
            f"{self.or_minutes}|{self.fib_entry_pct}|{self.macd_condition}|"
            f"{self.target_r}|{self.trade_direction}|{self.exit_time}|"
            f"{self.swing_confirm_pct}|{self.sl_buffer_pct}|"
            f"{self.max_wait_bars}|{self.breakout_type}"
        )
        return hashlib.md5(key.encode()).hexdigest()[:12]

    def to_dict(self) -> dict:
        return {
            "or_minutes": self.or_minutes,
            "fib_entry_pct": self.fib_entry_pct,
            "macd_condition": self.macd_condition,
            "target_r": self.target_r,
            "trade_direction": self.trade_direction,
            "exit_time": self.exit_time,
            "swing_confirm_pct": self.swing_confirm_pct,
            "sl_buffer_pct": self.sl_buffer_pct,
            "max_wait_bars": self.max_wait_bars,
            "breakout_type": self.breakout_type,
        }

    def short_description(self) -> str:
        return (
            f"OR{self.or_minutes}m | Fib{self.fib_entry_pct*100:.0f}% | "
            f"MACD:{self.macd_condition} | {self.target_r}R | "
            f"{self.trade_direction} | exit@{self.exit_time}"
        )


# ── Parameter grid ────────────────────────────────────────────────────────────

# Full sweep values
GRID_OR_MINUTES      = [15, 30]
GRID_FIB_ENTRY       = [0.382, 0.50, 0.618]   # Chart marks 38.2% and 61.8% as key levels
GRID_MACD_COND       = ['histogram_positive', 'histogram_rising', 'macd_cross', 'none']
GRID_TARGET_R        = [1.5, 2.0, 2.5, 3.0]
GRID_DIRECTION       = ['long_only', 'short_only', 'both']
GRID_EXIT_TIMES      = ['14:30', '15:00', '15:14']

# Quick-validation subset
QUICK_OR_MINUTES     = [15]
QUICK_FIB_ENTRY      = [0.618]
QUICK_MACD_COND      = ['histogram_rising', 'none']
QUICK_TARGET_R       = [2.0]
QUICK_DIRECTION      = ['both']
QUICK_EXIT_TIMES     = ['15:14']


def generate_param_grid(
    or_minutes=None, fib_entries=None, macd_conditions=None,
    targets=None, directions=None, exit_times=None,
    quick: bool = False,
) -> list[FibMACDParams]:
    """Generate all FibMACDParams combinations."""
    if quick:
        or_minutes      = QUICK_OR_MINUTES
        fib_entries     = QUICK_FIB_ENTRY
        macd_conditions = QUICK_MACD_COND
        targets         = QUICK_TARGET_R
        directions      = QUICK_DIRECTION
        exit_times      = QUICK_EXIT_TIMES
    else:
        or_minutes      = or_minutes      or GRID_OR_MINUTES
        fib_entries     = fib_entries     or GRID_FIB_ENTRY
        macd_conditions = macd_conditions or GRID_MACD_COND
        targets         = targets         or GRID_TARGET_R
        directions      = directions      or GRID_DIRECTION
        exit_times      = exit_times      or GRID_EXIT_TIMES

    params_list = []
    for orm, fib, macd, tgt, drn, et in product(
        or_minutes, fib_entries, macd_conditions, targets, directions, exit_times
    ):
        params_list.append(FibMACDParams(
            or_minutes=orm,
            fib_entry_pct=fib,
            macd_condition=macd,
            target_r=tgt,
            trade_direction=drn,
            exit_time=et,
        ))
    return params_list


# ── Core simulator ────────────────────────────────────────────────────────────

class FibMACDSimulator:
    """
    Simulates the Fib-MACD pullback strategy on one stock for all trading days.
    """

    def __init__(
        self,
        capital: float = 100_000,
        max_risk_per_trade: float = 1_000,
        brokerage_rate: float = 0.0001,   # legacy param (ignored when use_zerodha_charges=True)
        stt_rate: float = 0.00025,         # legacy param (ignored when use_zerodha_charges=True)
        slippage_pct: float = 0.0,         # 0.05% per side = 0.0005
        use_zerodha_charges: bool = False,  # Full Zerodha intraday charge structure
    ):
        self.capital              = capital
        self.max_risk_per_trade   = max_risk_per_trade
        self.brokerage_rate       = brokerage_rate
        self.stt_rate             = stt_rate
        self.slippage_pct         = slippage_pct
        self.use_zerodha_charges  = use_zerodha_charges

    # ── Public interface ──────────────────────────────────────────────────────

    def run(self, stock_data: StockData, params: FibMACDParams) -> list[Trade]:
        """Run strategy for one stock across all trading days."""
        trades = []
        for date_str in stock_data.trading_days:
            trade = self._simulate_day(stock_data, date_str, params)
            if trade is not None:
                trades.append(trade)
        return trades

    # ── Day-level logic ───────────────────────────────────────────────────────

    def _simulate_day(
        self, stock_data: StockData, date_str: str, params: FibMACDParams
    ) -> Trade | None:

        # ── 0. Fetch raw candle arrays for the day ────────────────────────────
        day_df = stock_data.day_groups.get(date_str)
        if day_df is None or day_df.empty:
            return None

        or_data = stock_data.opening_ranges.get(params.or_minutes, {})
        if date_str not in or_data:
            return None
        or_high, or_low, _or_avg_vol, _or_pct = or_data[date_str]

        # Compute time boundaries
        total_mins = 9 * 60 + 15 + params.or_minutes
        end_h, end_m = divmod(total_mins, 60)
        or_end_str  = f"{end_h:02d}:{end_m:02d}:00"
        exit_str    = f"{params.exit_time}:00"

        mask = (day_df['time_str'].values > or_end_str) & \
               (day_df['time_str'].values <= exit_str)
        idx_arr = np.where(mask)[0]
        if len(idx_arr) < 3:
            return None

        highs     = day_df['high'].values[idx_arr]
        lows      = day_df['low'].values[idx_arr]
        closes    = day_df['close'].values[idx_arr]
        times     = day_df['time_str'].values[idx_arr]
        datetimes = day_df['datetime'].values[idx_arr]
        n         = len(highs)

        allow_long  = params.trade_direction in ('long_only', 'both')
        allow_short = params.trade_direction in ('short_only', 'both')

        # ── 1. Find first breakout ────────────────────────────────────────────
        breakout_dir = None
        breakout_idx = -1

        for i in range(n):
            if allow_long and breakout_dir is None:
                if params.breakout_type == 'candle_close':
                    triggered = closes[i] > or_high
                else:
                    triggered = highs[i] > or_high
                if triggered:
                    breakout_dir = 'LONG'
                    breakout_idx = i

            if allow_short and breakout_dir is None:
                if params.breakout_type == 'candle_close':
                    triggered = closes[i] < or_low
                else:
                    triggered = lows[i] < or_low
                if triggered:
                    breakout_dir = 'SHORT'
                    breakout_idx = i

            if breakout_dir is not None:
                break

        if breakout_idx < 0:
            return None

        # ── 2. Track swing high/low after breakout ────────────────────────────
        wait_end = min(breakout_idx + params.max_wait_bars, n - 1)

        if breakout_dir == 'LONG':
            swing_result = self._find_swing_high(
                highs, closes, breakout_idx, wait_end, params.swing_confirm_pct
            )
        else:
            swing_result = self._find_swing_low(
                lows, closes, breakout_idx, wait_end, params.swing_confirm_pct
            )

        if swing_result is None:
            return None

        swing_value, swing_confirmed_idx = swing_result

        # ── 3. Calculate Fibonacci levels ─────────────────────────────────────
        if breakout_dir == 'LONG':
            anchor_lo   = or_low
            anchor_hi   = swing_value
            rng         = anchor_hi - anchor_lo
            fib_entry   = anchor_hi - params.fib_entry_pct * rng   # e.g. 61.8% retrace
            fib_786_lvl = anchor_hi - FIB_786 * rng                # stop reference
            stop_loss   = fib_786_lvl * (1 - params.sl_buffer_pct)
        else:
            anchor_hi   = or_high
            anchor_lo   = swing_value
            rng         = anchor_hi - anchor_lo
            fib_entry   = anchor_lo + params.fib_entry_pct * rng
            fib_786_lvl = anchor_lo + FIB_786 * rng
            stop_loss   = fib_786_lvl * (1 + params.sl_buffer_pct)

        if rng <= 0:
            return None

        # ── 4. Wait for fib touch + MACD confirmation ─────────────────────────
        scan_end  = n - 1
        entry_idx = -1
        entry_price = 0.0

        touched_fib = False  # Has price wicked into the fib zone yet?

        for i in range(swing_confirmed_idx, scan_end + 1):
            if breakout_dir == 'LONG':
                # Invalidation: candle closes below SL zone (blown through)
                if closes[i] <= stop_loss:
                    return None

                # Phase A: wait for wick to touch the fib level
                if not touched_fib and lows[i] <= fib_entry:
                    touched_fib = True

                # Phase B: entry on the first candle that CLOSES ABOVE fib level
                # after having touched it — the "strong bullish close" confirmation
                if touched_fib and closes[i] > fib_entry:
                    if self._check_macd(
                        stock_data.macd_5min, date_str, times[i],
                        params.macd_condition, 'LONG'
                    ):
                        entry_idx   = i
                        entry_price = fib_entry  # Limit-order style at fib level
                        break

            else:  # SHORT
                if closes[i] >= stop_loss:
                    return None

                if not touched_fib and highs[i] >= fib_entry:
                    touched_fib = True

                if touched_fib and closes[i] < fib_entry:
                    if self._check_macd(
                        stock_data.macd_5min, date_str, times[i],
                        params.macd_condition, 'SHORT'
                    ):
                        entry_idx   = i
                        entry_price = fib_entry
                        break

        if entry_idx < 0:
            return None

        # ── 5. Position sizing ────────────────────────────────────────────────
        risk_per_share = abs(entry_price - stop_loss)
        if risk_per_share <= 0:
            return None

        quantity = min(
            int(self.max_risk_per_trade / risk_per_share),
            int(self.capital / entry_price),
        )
        if quantity <= 0:
            return None

        # ── 6. Target price ───────────────────────────────────────────────────
        if params.target_r > 0:
            if breakout_dir == 'LONG':
                target_price = entry_price + risk_per_share * params.target_r
            else:
                target_price = entry_price - risk_per_share * params.target_r
        else:
            # Hold to forced exit at exit_time
            target_price = 0.0

        # ── 7. Apply slippage to entry price ─────────────────────────────────
        # Slippage worsens execution: long entries pay more, short entries get less
        if self.slippage_pct > 0:
            if breakout_dir == 'LONG':
                actual_entry = entry_price * (1 + self.slippage_pct)
            else:
                actual_entry = entry_price * (1 - self.slippage_pct)
            # Recompute risk and quantity with slipped entry
            risk_per_share = abs(actual_entry - stop_loss)
            if risk_per_share <= 0:
                return None
            quantity = min(
                int(self.max_risk_per_trade / risk_per_share),
                int(self.capital / actual_entry),
            )
            if quantity <= 0:
                return None
            # Recompute target with slipped entry
            if params.target_r > 0:
                if breakout_dir == 'LONG':
                    target_price = actual_entry + risk_per_share * params.target_r
                else:
                    target_price = actual_entry - risk_per_share * params.target_r
        else:
            actual_entry = entry_price

        # ── 8. Simulate exit ──────────────────────────────────────────────────
        exit_price, exit_idx, exit_reason = self._find_exit(
            highs, lows, closes, entry_idx, breakout_dir,
            stop_loss, target_price, n - 1,
        )

        # Apply slippage to exit price (worsens exit)
        if self.slippage_pct > 0:
            if breakout_dir == 'LONG':
                actual_exit = exit_price * (1 - self.slippage_pct)
            else:
                actual_exit = exit_price * (1 + self.slippage_pct)
        else:
            actual_exit = exit_price

        # ── 9. Build Trade record ─────────────────────────────────────────────
        return self._build_trade(
            stock_code       = stock_data.stock_code,
            date_str         = date_str,
            direction        = breakout_dir,
            entry_time       = str(datetimes[entry_idx]),
            entry_price      = actual_entry,
            exit_time        = str(datetimes[exit_idx]),
            exit_price       = actual_exit,
            quantity         = quantity,
            stop_loss        = stop_loss,
            target_price     = target_price,
            or_high          = or_high,
            or_low           = or_low,
            exit_reason      = exit_reason,
            risk_per_share   = risk_per_share,
        )

    # ── Swing detection ───────────────────────────────────────────────────────

    @staticmethod
    def _find_swing_high(
        highs: np.ndarray,
        closes: np.ndarray,
        start: int,
        end: int,
        confirm_pct: float,
    ) -> tuple[float, int] | None:
        """
        Scan forward from `start`; lock swing_high when price closes
        more than confirm_pct below the running peak.
        Returns (swing_high_value, candle_index_of_confirmation) or None.
        """
        peak = highs[start]
        for i in range(start + 1, end + 1):
            if highs[i] > peak:
                peak = highs[i]
            if closes[i] < peak * (1 - confirm_pct):
                return (peak, i)
        return None

    @staticmethod
    def _find_swing_low(
        lows: np.ndarray,
        closes: np.ndarray,
        start: int,
        end: int,
        confirm_pct: float,
    ) -> tuple[float, int] | None:
        trough = lows[start]
        for i in range(start + 1, end + 1):
            if lows[i] < trough:
                trough = lows[i]
            if closes[i] > trough * (1 + confirm_pct):
                return (trough, i)
        return None

    # ── MACD filter ───────────────────────────────────────────────────────────

    @staticmethod
    def _check_macd(
        macd_5min: dict,
        date_str: str,
        candle_time: str,
        condition: str,
        direction: str,
    ) -> bool:
        """
        Return True if the MACD condition is satisfied on the 5-min chart
        at or just before `candle_time`.

        condition values:
          'none'               — always True (no MACD filter)
          'histogram_positive' — histogram > 0 for LONG, < 0 for SHORT
          'histogram_rising'   — histogram is higher than previous bar (LONG)
                                 or lower than previous bar (SHORT)
          'macd_cross'         — MACD line above signal (LONG) or below (SHORT)
        """
        if condition == 'none':
            return True

        day_macd = macd_5min.get(date_str)
        if day_macd is None or len(day_macd) < 2:
            return True  # No data — don't filter out the trade

        # Most recent 5-min bar at or before candle_time
        relevant = day_macd[day_macd['time_str'] <= candle_time]
        if len(relevant) < 2:
            return True

        cur  = relevant.iloc[-1]
        prev = relevant.iloc[-2]

        if condition == 'histogram_positive':
            return cur['histogram'] > 0 if direction == 'LONG' else cur['histogram'] < 0

        if condition == 'histogram_rising':
            return cur['histogram'] > prev['histogram'] if direction == 'LONG' \
                   else cur['histogram'] < prev['histogram']

        if condition == 'macd_cross':
            return cur['macd'] > cur['signal'] if direction == 'LONG' \
                   else cur['macd'] < cur['signal']

        return True

    # ── Exit ──────────────────────────────────────────────────────────────────

    @staticmethod
    def _find_exit(
        highs: np.ndarray,
        lows: np.ndarray,
        closes: np.ndarray,
        entry_idx: int,
        direction: str,
        stop_loss: float,
        target_price: float,
        last_idx: int,
    ) -> tuple[float, int, str]:
        """Scan candles after entry for SL, target, or time exit."""
        for i in range(entry_idx + 1, last_idx + 1):
            if direction == 'LONG':
                if lows[i] <= stop_loss:
                    return (stop_loss, i, 'stop_loss')
                if target_price > 0 and highs[i] >= target_price:
                    return (target_price, i, 'target')
            else:
                if highs[i] >= stop_loss:
                    return (stop_loss, i, 'stop_loss')
                if target_price > 0 and lows[i] <= target_price:
                    return (target_price, i, 'target')

        return (float(closes[last_idx]), last_idx, 'time_exit')

    # ── Trade construction ────────────────────────────────────────────────────

    def _build_trade(
        self,
        stock_code: str,
        date_str: str,
        direction: str,
        entry_time: str,
        entry_price: float,
        exit_time: str,
        exit_price: float,
        quantity: int,
        stop_loss: float,
        target_price: float,
        or_high: float,
        or_low: float,
        exit_reason: str,
        risk_per_share: float,
    ) -> Trade:
        if direction == 'LONG':
            gross_pnl = (exit_price - entry_price) * quantity
        else:
            gross_pnl = (entry_price - exit_price) * quantity

        buy_value  = entry_price * quantity
        sell_value = exit_price  * quantity
        turnover   = buy_value + sell_value

        if self.use_zerodha_charges:
            # Zerodha intraday equity charges
            brokerage  = min(buy_value  * 0.0003, 20.0) \
                       + min(sell_value * 0.0003, 20.0)   # 0.03% or ₹20/order, both sides
            stt        = sell_value * 0.00025              # 0.025% sell side only
            exchange   = turnover   * 0.0000307            # NSE 0.00307%
            sebi       = turnover   * 0.000001             # ₹10/crore
            stamp      = buy_value  * 0.00003              # 0.003% buy side only
            gst        = (brokerage + exchange + sebi) * 0.18
            costs      = brokerage + stt + exchange + sebi + stamp + gst
        else:
            brokerage  = turnover * self.brokerage_rate    # legacy flat rate
            stt        = sell_value * self.stt_rate
            costs      = brokerage + stt

        net_pnl    = gross_pnl - costs
        risk_amount = risk_per_share * quantity
        r_multiple  = net_pnl / risk_amount if risk_amount > 0 else 0.0

        return Trade(
            stock_code        = stock_code,
            date              = date_str,
            direction         = direction,
            entry_time        = entry_time,
            entry_price       = round(entry_price, 2),
            exit_time         = exit_time,
            exit_price        = round(exit_price, 2),
            quantity          = quantity,
            stop_loss_initial = round(stop_loss, 2),
            stop_loss_final   = round(stop_loss, 2),
            target_price      = round(target_price, 2),
            or_high           = round(or_high, 2),
            or_low            = round(or_low, 2),
            exit_reason       = exit_reason,
            gross_pnl         = round(gross_pnl, 2),
            costs             = round(costs, 2),
            net_pnl           = round(net_pnl, 2),
            risk_amount       = round(risk_amount, 2),
            r_multiple        = round(r_multiple, 4),
        )
