"""
Trade dataclass and performance metrics calculator for ORB backtesting.

Computes all key trading metrics from a list of trades:
win rate, profit factor, Sharpe ratio, max drawdown, expectancy, etc.
"""

import math
from dataclasses import dataclass
from datetime import datetime


@dataclass
class Trade:
    """Represents one simulated ORB trade."""
    stock_code: str
    date: str                    # Trading day 'YYYY-MM-DD'
    direction: str               # 'LONG' or 'SHORT'
    entry_time: str              # 'YYYY-MM-DD HH:MM:SS'
    entry_price: float
    exit_time: str               # 'YYYY-MM-DD HH:MM:SS'
    exit_price: float
    quantity: int
    stop_loss_initial: float
    stop_loss_final: float       # May differ from initial if trailing
    target_price: float          # 0 if no target
    or_high: float
    or_low: float
    exit_reason: str             # 'target', 'stop_loss', 'time_exit'
    gross_pnl: float
    costs: float                 # brokerage + STT
    net_pnl: float
    risk_amount: float           # risk_per_share * quantity
    r_multiple: float            # net_pnl / risk_amount

    def holding_minutes(self) -> float:
        """Calculate holding duration in minutes."""
        try:
            entry_dt = datetime.strptime(self.entry_time, "%Y-%m-%d %H:%M:%S")
            exit_dt = datetime.strptime(self.exit_time, "%Y-%m-%d %H:%M:%S")
            return (exit_dt - entry_dt).total_seconds() / 60.0
        except (ValueError, TypeError):
            return 0.0

    def to_tuple(self, param_id: str) -> tuple:
        """
        Convert to tuple for bulk DB insertion into backtest_trades.
        Returns: (param_id, stock_code, date, direction, entry_time,
                  entry_price, exit_time, exit_price, quantity,
                  stop_loss_initial, stop_loss_final, target_price,
                  or_high, or_low, exit_reason, gross_pnl, costs,
                  net_pnl, risk_amount, r_multiple)
        """
        return (
            param_id, self.stock_code, self.date, self.direction,
            self.entry_time, self.entry_price, self.exit_time,
            self.exit_price, self.quantity, self.stop_loss_initial,
            self.stop_loss_final, self.target_price, self.or_high,
            self.or_low, self.exit_reason, self.gross_pnl, self.costs,
            self.net_pnl, self.risk_amount, self.r_multiple,
        )


@dataclass
class PerformanceResult:
    """All computed metrics for one (stock, params) combination."""
    total_trades: int
    winning_trades: int
    losing_trades: int
    win_rate: float
    total_pnl: float           # Gross P&L
    net_pnl: float             # After costs
    avg_pnl_per_trade: float
    avg_winner: float
    avg_loser: float
    profit_factor: float       # gross_profits / gross_losses
    max_drawdown: float        # Absolute
    max_drawdown_pct: float    # As % of capital
    max_consecutive_losses: int
    sharpe_ratio: float        # Annualized
    sortino_ratio: float       # Annualized (downside only)
    expectancy: float          # avg_win * win_rate - avg_loss * loss_rate
    avg_r_multiple: float      # Average R achieved per trade
    calmar_ratio: float        # Annualized return / max drawdown
    best_trade: float
    worst_trade: float
    avg_holding_minutes: float
    composite_score: float     # Weighted ranking score

    def to_metrics_tuple(self) -> tuple:
        """
        Convert to tuple for bulk DB insertion into backtest_metrics.
        Returns all metric values in schema order (after the param columns).
        """
        return (
            self.total_trades, self.winning_trades, self.losing_trades,
            self.win_rate, self.total_pnl, self.net_pnl,
            self.avg_pnl_per_trade, self.avg_winner, self.avg_loser,
            self.profit_factor, self.max_drawdown, self.max_drawdown_pct,
            self.max_consecutive_losses, self.sharpe_ratio, self.sortino_ratio,
            self.expectancy, self.avg_r_multiple, self.calmar_ratio,
            self.best_trade, self.worst_trade, self.avg_holding_minutes,
            self.composite_score,
        )


class MetricsCalculator:
    """Computes performance metrics from a list of Trade objects."""

    def __init__(self, capital: float = 100000):
        self.capital = capital

    def compute(self, trades: list[Trade]) -> PerformanceResult:
        """
        Compute all performance metrics from a list of trades.

        Handles edge cases:
        - 0 trades -> all metrics = 0, score = -infinity
        - 0 losses -> profit_factor capped at 999.99
        - 0 winning trades -> avg_winner = 0
        """
        if not trades:
            return self._empty_result()

        total = len(trades)
        pnls = [t.net_pnl for t in trades]
        gross_pnls = [t.gross_pnl for t in trades]
        r_multiples = [t.r_multiple for t in trades]

        winners = [p for p in pnls if p > 0]
        losers = [p for p in pnls if p <= 0]

        winning_count = len(winners)
        losing_count = len(losers)
        win_rate = winning_count / total if total > 0 else 0

        total_pnl = sum(gross_pnls)
        net_pnl = sum(pnls)
        avg_pnl = net_pnl / total if total > 0 else 0
        avg_winner = sum(winners) / winning_count if winning_count > 0 else 0
        avg_loser = sum(losers) / losing_count if losing_count > 0 else 0

        # Profit factor
        gross_profits = sum(p for p in pnls if p > 0)
        gross_losses = abs(sum(p for p in pnls if p <= 0))
        if gross_losses > 0:
            profit_factor = gross_profits / gross_losses
        else:
            profit_factor = 999.99 if gross_profits > 0 else 0

        # Max drawdown from equity curve
        max_dd, max_dd_pct = self._compute_drawdown(pnls)

        # Max consecutive losses
        max_consec = self._max_consecutive_losses(pnls)

        # Sharpe ratio (annualized from trade-level returns)
        sharpe = self._compute_sharpe(pnls, trades)

        # Sortino ratio
        sortino = self._compute_sortino(pnls, trades)

        # Expectancy
        loss_rate = losing_count / total if total > 0 else 0
        expectancy = (avg_winner * win_rate) - (abs(avg_loser) * loss_rate)

        # Average R-multiple
        avg_r = sum(r_multiples) / total if total > 0 else 0

        # Calmar ratio
        calmar = self._compute_calmar(pnls, trades, max_dd)

        # Best / worst trade
        best_trade = max(pnls) if pnls else 0
        worst_trade = min(pnls) if pnls else 0

        # Average holding time
        holding_times = [t.holding_minutes() for t in trades]
        avg_holding = sum(holding_times) / total if total > 0 else 0

        # Composite score
        composite = self._composite_score(
            net_pnl, sharpe, profit_factor, win_rate, max_dd_pct, expectancy
        )

        return PerformanceResult(
            total_trades=total,
            winning_trades=winning_count,
            losing_trades=losing_count,
            win_rate=round(win_rate, 4),
            total_pnl=round(total_pnl, 2),
            net_pnl=round(net_pnl, 2),
            avg_pnl_per_trade=round(avg_pnl, 2),
            avg_winner=round(avg_winner, 2),
            avg_loser=round(avg_loser, 2),
            profit_factor=round(min(profit_factor, 999.99), 2),
            max_drawdown=round(max_dd, 2),
            max_drawdown_pct=round(max_dd_pct, 4),
            max_consecutive_losses=max_consec,
            sharpe_ratio=round(sharpe, 4),
            sortino_ratio=round(sortino, 4),
            expectancy=round(expectancy, 2),
            avg_r_multiple=round(avg_r, 4),
            calmar_ratio=round(calmar, 4),
            best_trade=round(best_trade, 2),
            worst_trade=round(worst_trade, 2),
            avg_holding_minutes=round(avg_holding, 1),
            composite_score=round(composite, 4),
        )

    def _empty_result(self) -> PerformanceResult:
        """Return zeroed result for no-trade scenarios."""
        return PerformanceResult(
            total_trades=0, winning_trades=0, losing_trades=0,
            win_rate=0, total_pnl=0, net_pnl=0, avg_pnl_per_trade=0,
            avg_winner=0, avg_loser=0, profit_factor=0,
            max_drawdown=0, max_drawdown_pct=0, max_consecutive_losses=0,
            sharpe_ratio=0, sortino_ratio=0, expectancy=0,
            avg_r_multiple=0, calmar_ratio=0, best_trade=0,
            worst_trade=0, avg_holding_minutes=0,
            composite_score=-999999,
        )

    def _compute_drawdown(self, pnls: list) -> tuple[float, float]:
        """
        Compute max drawdown from running equity curve.

        Returns (max_drawdown_absolute, max_drawdown_pct_of_capital).
        """
        equity = self.capital
        peak = equity
        max_dd = 0

        for pnl in pnls:
            equity += pnl
            peak = max(peak, equity)
            dd = peak - equity
            max_dd = max(max_dd, dd)

        max_dd_pct = max_dd / self.capital if self.capital > 0 else 0
        return max_dd, max_dd_pct

    def _max_consecutive_losses(self, pnls: list) -> int:
        """Count maximum consecutive losing trades."""
        max_consec = 0
        current = 0
        for pnl in pnls:
            if pnl <= 0:
                current += 1
                max_consec = max(max_consec, current)
            else:
                current = 0
        return max_consec

    def _compute_sharpe(self, pnls: list, trades: list[Trade]) -> float:
        """
        Compute annualized Sharpe ratio.

        Uses daily P&L aggregation (trades on the same day are summed).
        Risk-free rate assumed = 0.
        """
        daily_pnls = self._aggregate_daily_pnls(pnls, trades)
        if len(daily_pnls) < 2:
            return 0

        daily_returns = [p / self.capital for p in daily_pnls]
        mean_ret = sum(daily_returns) / len(daily_returns)
        variance = sum((r - mean_ret) ** 2 for r in daily_returns) / (len(daily_returns) - 1)
        std_ret = math.sqrt(variance) if variance > 0 else 0

        if std_ret == 0:
            return 0

        return (mean_ret / std_ret) * math.sqrt(252)

    def _compute_sortino(self, pnls: list, trades: list[Trade]) -> float:
        """
        Compute annualized Sortino ratio.
        Uses only downside deviation (negative returns).
        """
        daily_pnls = self._aggregate_daily_pnls(pnls, trades)
        if len(daily_pnls) < 2:
            return 0

        daily_returns = [p / self.capital for p in daily_pnls]
        mean_ret = sum(daily_returns) / len(daily_returns)

        downside = [r for r in daily_returns if r < 0]
        if not downside:
            return 999.99 if mean_ret > 0 else 0

        downside_var = sum(r ** 2 for r in downside) / len(daily_returns)
        downside_dev = math.sqrt(downside_var) if downside_var > 0 else 0

        if downside_dev == 0:
            return 0

        return (mean_ret / downside_dev) * math.sqrt(252)

    def _compute_calmar(self, pnls: list, trades: list[Trade],
                        max_dd: float) -> float:
        """
        Compute Calmar ratio: annualized return / max drawdown.
        """
        if max_dd <= 0 or not trades:
            return 0

        # Estimate trading period in years
        try:
            first_date = datetime.strptime(trades[0].date, "%Y-%m-%d")
            last_date = datetime.strptime(trades[-1].date, "%Y-%m-%d")
            days = (last_date - first_date).days
            years = days / 365.25 if days > 0 else 1
        except (ValueError, TypeError):
            years = 1

        total_return = sum(pnls)
        annual_return = total_return / years if years > 0 else total_return
        return annual_return / max_dd

    def _aggregate_daily_pnls(self, pnls: list, trades: list[Trade]) -> list:
        """
        Aggregate trade P&Ls by date.
        Multiple trades on the same day are summed.
        """
        daily = {}
        for trade, pnl in zip(trades, pnls):
            daily[trade.date] = daily.get(trade.date, 0) + pnl
        return list(daily.values())

    def _composite_score(self, net_pnl: float, sharpe: float,
                         profit_factor: float, win_rate: float,
                         max_dd_pct: float, expectancy: float) -> float:
        """
        Compute weighted composite score for strategy ranking.

        Raw values are used (normalization happens in ranking.py).
        This is a heuristic score combining key metrics:
        - Higher is better for all components
        - max_dd_pct is inverted (lower drawdown = higher score)
        """
        # Normalize components to reasonable ranges for weighting
        # Net P&L: divide by capital for scale-independence
        pnl_score = net_pnl / self.capital if self.capital > 0 else 0

        # Sharpe: already scale-independent
        sharpe_score = sharpe

        # Profit factor: cap at 10 to prevent outlier dominance
        pf_score = min(profit_factor, 10) / 10

        # Win rate: already 0-1
        wr_score = win_rate

        # Drawdown: invert so lower is better, cap at 1
        dd_score = max(0, 1 - min(max_dd_pct, 1))

        # Expectancy: divide by capital for scale
        exp_score = expectancy / (self.capital * 0.01) if self.capital > 0 else 0

        score = (
            0.25 * pnl_score +
            0.20 * sharpe_score +
            0.15 * pf_score +
            0.15 * wr_score +
            0.15 * dd_score +
            0.10 * exp_score
        )
        return score
