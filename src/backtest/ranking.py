"""
Strategy and stock ranking for ORB backtest results.

Provides methods to rank parameter combinations, identify best stocks,
find optimal (stock, strategy) pairs, and analyze parameter sensitivity.
"""

import logging
import pandas as pd

from backtest.results_db import ResultsDatabase

logger = logging.getLogger("ICICI_ORB_Bot")


class StrategyRanker:
    """
    Ranks strategies and stocks from backtest results.
    """

    def __init__(self, results_db: ResultsDatabase, run_id: int):
        self.db = results_db
        self.run_id = run_id

    def rank_strategies(
        self,
        metric: str = "composite_score",
        limit: int = 20,
        aggregate: str = "mean",
    ) -> pd.DataFrame:
        """
        Rank parameter combinations by aggregating metric across all stocks.

        Args:
            metric: Metric column name to rank by
            limit: Number of top strategies to return
            aggregate: 'mean' or 'median'

        Returns:
            DataFrame with param details + aggregated metric values.
        """
        self.db.connect()
        rows = self.db.get_top_strategies(self.run_id, metric, limit)
        self.db.close()

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        return df

    def rank_stocks(
        self,
        metric: str = "net_pnl",
        limit: int = 20,
        param_id: str = None,
    ) -> pd.DataFrame:
        """
        Rank stocks by metric.

        Args:
            metric: Metric to rank by
            limit: Number of top stocks
            param_id: If given, rank stocks for that specific strategy only

        Returns:
            DataFrame with stock rankings.
        """
        self.db.connect()
        rows = self.db.get_top_stocks(self.run_id, metric, limit, param_id)
        self.db.close()

        if not rows:
            return pd.DataFrame()

        return pd.DataFrame(rows)

    def best_pairs(
        self,
        metric: str = "composite_score",
        limit: int = 50,
    ) -> pd.DataFrame:
        """
        Find best (stock, strategy) combinations.

        Returns:
            DataFrame with top N rows sorted by metric.
        """
        self.db.connect()
        rows = self.db.get_best_pairs(self.run_id, metric, limit)
        self.db.close()

        if not rows:
            return pd.DataFrame()

        return pd.DataFrame(rows)

    def parameter_sensitivity(self) -> pd.DataFrame:
        """
        Analyze which parameters have the most impact on performance.

        For each parameter, compute the variance of mean net_pnl
        across its values while averaging over all other parameters.
        Higher variance = more impactful parameter.

        Returns:
            DataFrame with [parameter, variance_explained, best_value, worst_value].
        """
        self.db.connect()
        all_metrics = self.db.get_all_metrics(self.run_id)
        self.db.close()

        if not all_metrics:
            return pd.DataFrame()

        df = pd.DataFrame(all_metrics)

        params_to_analyze = [
            ("or_minutes", "OR Duration (min)"),
            ("target_multiplier", "Target R:R"),
            ("stop_loss_type", "Stop Loss Type"),
            ("trade_direction", "Trade Direction"),
            ("exit_time", "Exit Time"),
            ("max_or_filter_pct", "OR Size Filter (%)"),
            ("entry_confirmation", "Entry Confirmation"),
        ]

        results = []
        for param_col, param_name in params_to_analyze:
            # Group by this parameter, average net_pnl across all other params + stocks
            grouped = df.groupby(param_col)["net_pnl"].mean()

            if len(grouped) < 2:
                continue

            variance = grouped.var()
            best_val = grouped.idxmax()
            best_pnl = grouped.max()
            worst_val = grouped.idxmin()
            worst_pnl = grouped.min()
            spread = best_pnl - worst_pnl

            results.append({
                "parameter": param_name,
                "column": param_col,
                "variance": round(variance, 2),
                "spread": round(spread, 2),
                "best_value": str(best_val),
                "best_avg_pnl": round(best_pnl, 2),
                "worst_value": str(worst_val),
                "worst_avg_pnl": round(worst_pnl, 2),
            })

        result_df = pd.DataFrame(results)
        if not result_df.empty:
            result_df = result_df.sort_values("spread", ascending=False).reset_index(drop=True)

        return result_df

    def heatmap_data(
        self,
        param_x: str,
        param_y: str,
        metric: str = "net_pnl",
    ) -> pd.DataFrame:
        """
        Generate 2D heatmap data for any pair of parameters vs a metric.

        Args:
            param_x: Parameter column for x-axis
            param_y: Parameter column for y-axis
            metric: Metric to average

        Returns:
            Pivoted DataFrame suitable for heatmap visualization.
        """
        self.db.connect()
        all_metrics = self.db.get_all_metrics(self.run_id)
        self.db.close()

        if not all_metrics:
            return pd.DataFrame()

        df = pd.DataFrame(all_metrics)

        # Pivot: rows = param_y values, cols = param_x values, values = mean metric
        pivot = df.pivot_table(
            values=metric,
            index=param_y,
            columns=param_x,
            aggfunc="mean",
        )

        return pivot.round(2)

    def parameter_breakdown(self, param_col: str) -> pd.DataFrame:
        """
        Get detailed breakdown of a single parameter's impact.

        Returns DataFrame with one row per parameter value,
        showing mean metrics across all other params + stocks.
        """
        self.db.connect()
        all_metrics = self.db.get_all_metrics(self.run_id)
        self.db.close()

        if not all_metrics:
            return pd.DataFrame()

        df = pd.DataFrame(all_metrics)

        grouped = df.groupby(param_col).agg({
            "net_pnl": "mean",
            "win_rate": "mean",
            "profit_factor": "mean",
            "sharpe_ratio": "mean",
            "max_drawdown_pct": "mean",
            "total_trades": "mean",
            "expectancy": "mean",
            "composite_score": "mean",
        }).round(4)

        grouped = grouped.sort_values("composite_score", ascending=False)
        return grouped
