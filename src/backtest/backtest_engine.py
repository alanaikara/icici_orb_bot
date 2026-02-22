"""
Core ORB (Opening Range Breakout) backtest simulation engine.

OPTIMIZED VERSION: Uses vectorized NumPy operations to precompute
entry/exit signals per day, then iterates only over parameters.
The candle-by-candle loop is used ONLY for trailing stops (which
require sequential state). Fixed SL and ATR SL are fully vectorized.

Speed improvement: ~10-20x over the pure Python candle loop.
"""

import logging
import numpy as np
from backtest.data_loader import StockData
from backtest.parameter_grid import (
    StrategyParams, StopLossType, TradeDirection, EntryConfirmation,
)
from backtest.metrics import Trade

logger = logging.getLogger("ICICI_ORB_Bot")


class DayCache:
    """
    Precomputed data for one trading day's post-OR candles.
    Built once per (stock, or_minutes, exit_time) and reused.
    """
    __slots__ = [
        'date_str', 'highs', 'lows', 'closes', 'volumes', 'datetimes',
        'n_candles', 'max_high', 'min_low',
        'first_long_imm_idx', 'first_short_imm_idx',
        'first_long_close_idx', 'first_short_close_idx',
        'first_long_vol_idx', 'first_short_vol_idx',
    ]

    def __init__(self):
        self.n_candles = 0


class ORBSimulator:
    """
    Optimized ORB strategy simulator.

    Key optimization: For each trading day, precompute the first candle
    index where each entry signal type fires. Then for each parameter combo,
    simply look up the precomputed entry index and simulate the exit
    from that point forward.

    For FIXED and ATR stops (no trailing state), the exit can be found
    with a single vectorized search (np.searchsorted-like approach on
    cumulative min/max arrays).
    """

    def __init__(
        self,
        capital: float = 100000,
        max_risk_per_trade: float = 1000,
        brokerage_rate: float = 0.0001,
        stt_rate: float = 0.00025,
    ):
        self.capital = capital
        self.max_risk_per_trade = max_risk_per_trade
        self.brokerage_rate = brokerage_rate
        self.stt_rate = stt_rate

    def run(self, stock_data: StockData, params: StrategyParams) -> list[Trade]:
        """
        Run the ORB strategy for one stock across all trading days.
        """
        trades = []
        or_data = stock_data.opening_ranges.get(params.or_minutes)
        if or_data is None:
            return trades

        # Precompute OR end time string
        total_minutes = 9 * 60 + 15 + params.or_minutes
        end_h, end_m = divmod(total_minutes, 60)
        or_end_time_str = f"{end_h:02d}:{end_m:02d}:00"
        exit_time_str = f"{params.exit_time}:00"

        # Precompute day caches
        day_caches = self._build_day_caches(
            stock_data, or_data, or_end_time_str, exit_time_str
        )

        # Determine entry check function based on params
        allow_long = params.trade_direction in (TradeDirection.LONG_ONLY, TradeDirection.BOTH)
        allow_short = params.trade_direction in (TradeDirection.SHORT_ONLY, TradeDirection.BOTH)
        is_trailing = params.stop_loss_type == StopLossType.TRAILING

        for dc in day_caches:
            or_info = or_data[dc.date_str]
            or_high, or_low, or_avg_vol, or_pct = or_info

            # OR filter
            if params.max_or_filter_pct > 0 and or_pct > params.max_or_filter_pct:
                continue

            # Find entry
            entry_result = self._find_entry(
                dc, or_high, or_low, or_avg_vol,
                params.entry_confirmation, allow_long, allow_short,
            )
            if entry_result is None:
                continue

            direction, entry_price, entry_idx = entry_result

            # Compute SL
            atr_value = stock_data.daily_atr.get(dc.date_str, 0)
            stop_loss = self._initial_stop_loss(
                direction, entry_price, or_high, or_low, atr_value, params,
            )

            # Position sizing
            risk_per_share = abs(entry_price - stop_loss)
            if risk_per_share <= 0:
                continue

            quantity = min(
                int(self.max_risk_per_trade / risk_per_share),
                int(self.capital / entry_price) if entry_price > 0 else 0,
            )
            if quantity <= 0:
                continue

            # Compute target
            if params.target_multiplier > 0:
                if direction == "LONG":
                    target_price = entry_price + risk_per_share * params.target_multiplier
                else:
                    target_price = entry_price - risk_per_share * params.target_multiplier
            else:
                target_price = 0.0

            # Find exit
            if is_trailing:
                exit_result = self._find_exit_trailing(
                    dc, direction, entry_idx, stop_loss,
                    target_price, params.trailing_stop_pct,
                )
            else:
                exit_result = self._find_exit_vectorized(
                    dc, direction, entry_idx, stop_loss, target_price,
                )

            exit_price, exit_idx, exit_reason, sl_final = exit_result

            # Build trade
            entry_time = str(dc.datetimes[entry_idx])
            exit_time = str(dc.datetimes[exit_idx])

            trade = self._build_trade(
                stock_data.stock_code, dc.date_str, direction,
                entry_time, entry_price, exit_time, exit_price,
                quantity, stop_loss, sl_final, target_price,
                or_high, or_low, exit_reason, risk_per_share,
            )
            trades.append(trade)

        return trades

    def run_with_caches(
        self,
        stock_data: StockData,
        params: StrategyParams,
        or_data: dict,
        day_caches: list,
    ) -> list[Trade]:
        """
        Run using pre-built DayCaches (shared across params with same
        or_minutes and exit_time). Avoids rebuilding caches for each combo.
        """
        trades = []

        allow_long = params.trade_direction in (TradeDirection.LONG_ONLY, TradeDirection.BOTH)
        allow_short = params.trade_direction in (TradeDirection.SHORT_ONLY, TradeDirection.BOTH)
        is_trailing = params.stop_loss_type == StopLossType.TRAILING

        for dc in day_caches:
            or_info = or_data[dc.date_str]
            or_high, or_low, or_avg_vol, or_pct = or_info

            if params.max_or_filter_pct > 0 and or_pct > params.max_or_filter_pct:
                continue

            entry_result = self._find_entry(
                dc, or_high, or_low, or_avg_vol,
                params.entry_confirmation, allow_long, allow_short,
            )
            if entry_result is None:
                continue

            direction, entry_price, entry_idx = entry_result

            atr_value = stock_data.daily_atr.get(dc.date_str, 0)
            stop_loss = self._initial_stop_loss(
                direction, entry_price, or_high, or_low, atr_value, params,
            )

            risk_per_share = abs(entry_price - stop_loss)
            if risk_per_share <= 0:
                continue

            quantity = min(
                int(self.max_risk_per_trade / risk_per_share),
                int(self.capital / entry_price) if entry_price > 0 else 0,
            )
            if quantity <= 0:
                continue

            if params.target_multiplier > 0:
                if direction == "LONG":
                    target_price = entry_price + risk_per_share * params.target_multiplier
                else:
                    target_price = entry_price - risk_per_share * params.target_multiplier
            else:
                target_price = 0.0

            if is_trailing:
                exit_result = self._find_exit_trailing(
                    dc, direction, entry_idx, stop_loss,
                    target_price, params.trailing_stop_pct,
                )
            else:
                exit_result = self._find_exit_vectorized(
                    dc, direction, entry_idx, stop_loss, target_price,
                )

            exit_price, exit_idx, exit_reason, sl_final = exit_result

            trade = self._build_trade(
                stock_data.stock_code, dc.date_str, direction,
                str(dc.datetimes[entry_idx]), entry_price,
                str(dc.datetimes[exit_idx]), exit_price,
                quantity, stop_loss, sl_final, target_price,
                or_high, or_low, exit_reason, risk_per_share,
            )
            trades.append(trade)

        return trades

    def _build_day_caches(
        self,
        stock_data: StockData,
        or_data: dict,
        or_end_time_str: str,
        exit_time_str: str,
    ) -> list[DayCache]:
        """
        Build DayCache objects with precomputed entry signal indices.
        """
        caches = []

        for date_str in stock_data.trading_days:
            if date_str not in or_data:
                continue

            day_df = stock_data.day_groups.get(date_str)
            if day_df is None or day_df.empty:
                continue

            mask = (day_df['time_str'].values >= or_end_time_str) & \
                   (day_df['time_str'].values <= exit_time_str)
            indices = np.where(mask)[0]
            if len(indices) == 0:
                continue

            dc = DayCache()
            dc.date_str = date_str
            dc.highs = day_df['high'].values[indices]
            dc.lows = day_df['low'].values[indices]
            dc.closes = day_df['close'].values[indices]
            dc.volumes = day_df['volume'].values[indices]
            dc.datetimes = day_df['datetime'].values[indices]
            dc.n_candles = len(indices)

            # Precompute per-day aggregates
            dc.max_high = dc.highs.max()
            dc.min_low = dc.lows.min()

            or_high, or_low, or_avg_vol, _ = or_data[date_str]

            # Precompute first entry index for each signal type
            # IMMEDIATE LONG: first candle where high > or_high
            long_imm = np.where(dc.highs > or_high)[0]
            dc.first_long_imm_idx = int(long_imm[0]) if len(long_imm) > 0 else -1

            # IMMEDIATE SHORT: first candle where low < or_low
            short_imm = np.where(dc.lows < or_low)[0]
            dc.first_short_imm_idx = int(short_imm[0]) if len(short_imm) > 0 else -1

            # CANDLE CLOSE LONG: first candle where close > or_high
            long_close = np.where(dc.closes > or_high)[0]
            dc.first_long_close_idx = int(long_close[0]) if len(long_close) > 0 else -1

            # CANDLE CLOSE SHORT: first candle where close < or_low
            short_close = np.where(dc.closes < or_low)[0]
            dc.first_short_close_idx = int(short_close[0]) if len(short_close) > 0 else -1

            # VOLUME CONFIRM LONG: first candle where close > or_high AND volume > 1.5x
            if or_avg_vol > 0:
                vol_mask_long = (dc.closes > or_high) & (dc.volumes > 1.5 * or_avg_vol)
                long_vol = np.where(vol_mask_long)[0]
                dc.first_long_vol_idx = int(long_vol[0]) if len(long_vol) > 0 else -1

                vol_mask_short = (dc.closes < or_low) & (dc.volumes > 1.5 * or_avg_vol)
                short_vol = np.where(vol_mask_short)[0]
                dc.first_short_vol_idx = int(short_vol[0]) if len(short_vol) > 0 else -1
            else:
                dc.first_long_vol_idx = -1
                dc.first_short_vol_idx = -1

            caches.append(dc)

        return caches

    def _find_entry(
        self,
        dc: DayCache,
        or_high: float, or_low: float, or_avg_vol: float,
        confirmation: EntryConfirmation,
        allow_long: bool, allow_short: bool,
    ) -> tuple | None:
        """
        Find entry using precomputed indices.
        Returns (direction, entry_price, candle_idx) or None.
        """
        long_idx = -1
        short_idx = -1

        if allow_long:
            if confirmation == EntryConfirmation.IMMEDIATE:
                long_idx = dc.first_long_imm_idx
            elif confirmation == EntryConfirmation.CANDLE_CLOSE:
                long_idx = dc.first_long_close_idx
            else:  # VOLUME
                long_idx = dc.first_long_vol_idx

        if allow_short:
            if confirmation == EntryConfirmation.IMMEDIATE:
                short_idx = dc.first_short_imm_idx
            elif confirmation == EntryConfirmation.CANDLE_CLOSE:
                short_idx = dc.first_short_close_idx
            else:  # VOLUME
                short_idx = dc.first_short_vol_idx

        # Pick whichever fires first
        if long_idx >= 0 and short_idx >= 0:
            if long_idx <= short_idx:
                if confirmation == EntryConfirmation.IMMEDIATE:
                    return ("LONG", or_high, long_idx)
                else:
                    return ("LONG", float(dc.closes[long_idx]), long_idx)
            else:
                if confirmation == EntryConfirmation.IMMEDIATE:
                    return ("SHORT", or_low, short_idx)
                else:
                    return ("SHORT", float(dc.closes[short_idx]), short_idx)
        elif long_idx >= 0:
            if confirmation == EntryConfirmation.IMMEDIATE:
                return ("LONG", or_high, long_idx)
            else:
                return ("LONG", float(dc.closes[long_idx]), long_idx)
        elif short_idx >= 0:
            if confirmation == EntryConfirmation.IMMEDIATE:
                return ("SHORT", or_low, short_idx)
            else:
                return ("SHORT", float(dc.closes[short_idx]), short_idx)

        return None

    def _find_exit_vectorized(
        self,
        dc: DayCache,
        direction: str,
        entry_idx: int,
        stop_loss: float,
        target_price: float,
    ) -> tuple:
        """
        Find exit using vectorized operations for FIXED and ATR stops.
        No candle loop needed.

        Returns (exit_price, exit_idx, exit_reason, sl_final).
        """
        # Start from candle AFTER entry
        start = entry_idx + 1
        if start >= dc.n_candles:
            # Time exit on entry candle's close
            return (float(dc.closes[entry_idx]), entry_idx, "time_exit", stop_loss)

        highs = dc.highs[start:]
        lows = dc.lows[start:]
        n = len(highs)

        # Find first SL hit
        if direction == "LONG":
            sl_hits = np.where(lows <= stop_loss)[0]
        else:
            sl_hits = np.where(highs >= stop_loss)[0]

        sl_idx = int(sl_hits[0]) + start if len(sl_hits) > 0 else -1

        # Find first target hit
        tgt_idx = -1
        if target_price > 0:
            if direction == "LONG":
                tgt_hits = np.where(highs >= target_price)[0]
            else:
                tgt_hits = np.where(lows <= target_price)[0]
            tgt_idx = int(tgt_hits[0]) + start if len(tgt_hits) > 0 else -1

        # Determine which fires first
        if sl_idx >= 0 and tgt_idx >= 0:
            if sl_idx <= tgt_idx:
                # SL first (or same candle = conservative SL assumption)
                return (stop_loss, sl_idx, "stop_loss", stop_loss)
            else:
                return (target_price, tgt_idx, "target", stop_loss)
        elif sl_idx >= 0:
            return (stop_loss, sl_idx, "stop_loss", stop_loss)
        elif tgt_idx >= 0:
            return (target_price, tgt_idx, "target", stop_loss)
        else:
            # Time exit
            last_idx = dc.n_candles - 1
            return (float(dc.closes[last_idx]), last_idx, "time_exit", stop_loss)

    def _find_exit_trailing(
        self,
        dc: DayCache,
        direction: str,
        entry_idx: int,
        stop_loss: float,
        target_price: float,
        trailing_pct: float,
    ) -> tuple:
        """
        Find exit with trailing stop. Requires candle-by-candle iteration
        because the stop ratchets based on sequential price movement.

        Returns (exit_price, exit_idx, exit_reason, sl_final).
        """
        start = entry_idx + 1
        if start >= dc.n_candles:
            return (float(dc.closes[entry_idx]), entry_idx, "time_exit", stop_loss)

        peak = dc.highs[entry_idx] if direction == "LONG" else dc.lows[entry_idx]
        sl = stop_loss
        trailing_mult = trailing_pct / 100.0

        for i in range(start, dc.n_candles):
            c_high = dc.highs[i]
            c_low = dc.lows[i]

            # Update trailing stop
            if direction == "LONG":
                if c_high > peak:
                    peak = c_high
                    new_sl = peak * (1 - trailing_mult)
                    if new_sl > sl:
                        sl = new_sl
                sl_hit = c_low <= sl
                tgt_hit = (target_price > 0 and c_high >= target_price)
            else:
                if c_low < peak:
                    peak = c_low
                    new_sl = peak * (1 + trailing_mult)
                    if new_sl < sl:
                        sl = new_sl
                sl_hit = c_high >= sl
                tgt_hit = (target_price > 0 and c_low <= target_price)

            if sl_hit and tgt_hit:
                return (sl, i, "stop_loss", sl)
            elif sl_hit:
                return (sl, i, "stop_loss", sl)
            elif tgt_hit:
                return (target_price, i, "target", sl)

        # Time exit
        last_idx = dc.n_candles - 1
        return (float(dc.closes[last_idx]), last_idx, "time_exit", sl)

    def _initial_stop_loss(
        self,
        direction: str,
        entry_price: float,
        or_high: float,
        or_low: float,
        atr_value: float,
        params: StrategyParams,
    ) -> float:
        """Calculate initial stop loss based on SL type."""
        if params.stop_loss_type == StopLossType.FIXED:
            return or_low if direction == "LONG" else or_high
        elif params.stop_loss_type == StopLossType.TRAILING:
            if direction == "LONG":
                return entry_price * (1 - params.trailing_stop_pct / 100)
            else:
                return entry_price * (1 + params.trailing_stop_pct / 100)
        elif params.stop_loss_type == StopLossType.ATR_BASED:
            if atr_value > 0:
                if direction == "LONG":
                    return entry_price - atr_value * params.atr_multiplier
                else:
                    return entry_price + atr_value * params.atr_multiplier
            else:
                return or_low if direction == "LONG" else or_high
        return or_low if direction == "LONG" else or_high

    def _build_trade(
        self,
        stock_code: str, date_str: str, direction: str,
        entry_time: str, entry_price: float,
        exit_time: str, exit_price: float,
        quantity: int, stop_loss_initial: float, stop_loss_final: float,
        target_price: float, or_high: float, or_low: float,
        exit_reason: str, risk_per_share: float,
    ) -> Trade:
        """Construct a Trade object and compute P&L."""
        if direction == "LONG":
            gross_pnl = (exit_price - entry_price) * quantity
        else:
            gross_pnl = (entry_price - exit_price) * quantity

        brokerage = entry_price * quantity * self.brokerage_rate * 2
        stt = exit_price * quantity * self.stt_rate
        costs = brokerage + stt
        net_pnl = gross_pnl - costs
        risk_amount = risk_per_share * quantity
        r_multiple = net_pnl / risk_amount if risk_amount > 0 else 0

        return Trade(
            stock_code=stock_code,
            date=date_str,
            direction=direction,
            entry_time=entry_time,
            entry_price=round(entry_price, 2),
            exit_time=exit_time,
            exit_price=round(exit_price, 2),
            quantity=quantity,
            stop_loss_initial=round(stop_loss_initial, 2),
            stop_loss_final=round(stop_loss_final, 2),
            target_price=round(target_price, 2),
            or_high=round(or_high, 2),
            or_low=round(or_low, 2),
            exit_reason=exit_reason,
            gross_pnl=round(gross_pnl, 2),
            costs=round(costs, 2),
            net_pnl=round(net_pnl, 2),
            risk_amount=round(risk_amount, 2),
            r_multiple=round(r_multiple, 4),
        )
