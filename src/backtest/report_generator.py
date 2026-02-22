"""
Report generator for ORB backtest results.

Generates text summaries, CSV exports, and optional charts
from backtest results stored in the results database.
"""

import os
import logging
import pandas as pd
from datetime import datetime

from backtest.results_db import ResultsDatabase
from backtest.ranking import StrategyRanker

logger = logging.getLogger("ICICI_ORB_Bot")


class ReportGenerator:
    """
    Generates reports from backtest results.
    """

    def __init__(
        self,
        results_db: ResultsDatabase,
        run_id: int,
        output_dir: str = "Reports/backtest",
    ):
        self.db = results_db
        self.run_id = run_id
        self.output_dir = output_dir
        self.ranker = StrategyRanker(results_db, run_id)

        os.makedirs(output_dir, exist_ok=True)

    def generate_all(self):
        """Generate all reports."""
        print(f"\nGenerating reports for Run #{self.run_id}...")
        print(f"Output directory: {self.output_dir}\n")

        summary = self.generate_text_summary()
        self.generate_csv_exports()
        self._try_generate_charts()

        print(f"\nAll reports saved to {self.output_dir}/")
        return summary

    def generate_text_summary(self) -> str:
        """
        Generate a text-based summary report.
        Prints to console and saves to output_dir/summary.txt.
        """
        lines = []
        lines.append("=" * 70)
        lines.append(f"ORB BACKTEST RESULTS — Run #{self.run_id}")
        lines.append("=" * 70)

        # Run metadata
        self.db.connect()
        run = self.db.get_run(self.run_id)
        self.db.close()

        if not run:
            msg = f"Run #{self.run_id} not found."
            print(msg)
            return msg

        lines.append(f"\nDate range:   {run.get('start_date', 'N/A')} to {run.get('end_date', 'N/A')}")
        lines.append(f"Stocks:       {run['total_stocks']}")
        lines.append(f"Param combos: {run['total_param_combos']:,}")
        lines.append(f"Simulations:  {run['total_simulations']:,}")
        lines.append(f"Elapsed:      {run['elapsed_seconds']:.1f}s ({run['elapsed_seconds']/60:.1f} min)")
        lines.append(f"Status:       {run['status']}")

        # Top 10 strategies
        lines.append(f"\n{'='*70}")
        lines.append("TOP 10 STRATEGIES (averaged across all stocks)")
        lines.append("=" * 70)

        top_strategies = self.ranker.rank_strategies(limit=10)
        if not top_strategies.empty:
            for i, row in top_strategies.iterrows():
                lines.append(f"\n  #{i+1}  Score: {row.get('avg_metric', 0):.4f}")
                lines.append(f"      OR: {row['or_minutes']}m | "
                           f"Target: {row['target_multiplier']}R | "
                           f"SL: {row['stop_loss_type']} | "
                           f"Dir: {row['trade_direction']}")
                lines.append(f"      Exit: {row['exit_time']} | "
                           f"Filter: {row['max_or_filter_pct']}% | "
                           f"Entry: {row['entry_confirmation']}")
                lines.append(f"      Avg P&L: ₹{row.get('avg_net_pnl', 0):,.2f} | "
                           f"Win%: {row.get('avg_win_rate', 0)*100:.1f}% | "
                           f"PF: {row.get('avg_profit_factor', 0):.2f} | "
                           f"Sharpe: {row.get('avg_sharpe', 0):.2f}")
        else:
            lines.append("  No results found.")

        # Top 10 stocks
        lines.append(f"\n{'='*70}")
        lines.append("TOP 10 STOCKS (averaged across all strategies)")
        lines.append("=" * 70)

        top_stocks = self.ranker.rank_stocks(limit=10)
        if not top_stocks.empty:
            for i, row in top_stocks.iterrows():
                lines.append(f"  #{i+1}  {row['stock_code']:10s} | "
                           f"Avg P&L: ₹{row.get('avg_net_pnl', 0):>10,.2f} | "
                           f"Win%: {row.get('avg_win_rate', 0)*100:>5.1f}%")
        else:
            lines.append("  No results found.")

        # Top 10 best pairs
        lines.append(f"\n{'='*70}")
        lines.append("TOP 10 BEST (STOCK, STRATEGY) PAIRS")
        lines.append("=" * 70)

        best = self.ranker.best_pairs(limit=10)
        if not best.empty:
            for i, row in best.iterrows():
                lines.append(f"  #{i+1}  {row['stock_code']:10s} | "
                           f"OR{row['or_minutes']}m {row['stop_loss_type']} "
                           f"{row['target_multiplier']}R {row['trade_direction']} "
                           f"@{row['exit_time']}")
                lines.append(f"      P&L: ₹{row['net_pnl']:>10,.2f} | "
                           f"Win%: {row['win_rate']*100:>5.1f}% | "
                           f"Trades: {row['total_trades']:>4d} | "
                           f"Sharpe: {row['sharpe_ratio']:.2f} | "
                           f"MaxDD: {row['max_drawdown_pct']*100:.1f}%")
        else:
            lines.append("  No results found.")

        # Parameter sensitivity
        lines.append(f"\n{'='*70}")
        lines.append("PARAMETER SENSITIVITY (which params matter most)")
        lines.append("=" * 70)

        sensitivity = self.ranker.parameter_sensitivity()
        if not sensitivity.empty:
            lines.append(f"  {'Parameter':<25s} {'Spread':>10s} {'Best Value':>12s} {'Best P&L':>12s} {'Worst Value':>12s} {'Worst P&L':>12s}")
            lines.append(f"  {'-'*85}")
            for _, row in sensitivity.iterrows():
                lines.append(
                    f"  {row['parameter']:<25s} "
                    f"₹{row['spread']:>9,.2f} "
                    f"{str(row['best_value']):>12s} "
                    f"₹{row['best_avg_pnl']:>10,.2f} "
                    f"{str(row['worst_value']):>12s} "
                    f"₹{row['worst_avg_pnl']:>10,.2f}"
                )
        else:
            lines.append("  No sensitivity data.")

        lines.append(f"\n{'='*70}")
        lines.append(f"Report generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("=" * 70)

        summary = "\n".join(lines)

        # Print to console
        print(summary)

        # Save to file
        filepath = os.path.join(self.output_dir, f"summary_run{self.run_id}.txt")
        with open(filepath, "w") as f:
            f.write(summary)
        print(f"\nSummary saved to {filepath}")

        return summary

    def generate_csv_exports(self):
        """Export key results to CSV for external analysis."""
        # All metrics
        self.db.connect()
        all_metrics = self.db.get_all_metrics(self.run_id)
        self.db.close()

        if all_metrics:
            df = pd.DataFrame(all_metrics)
            filepath = os.path.join(self.output_dir, f"all_metrics_run{self.run_id}.csv")
            df.to_csv(filepath, index=False)
            print(f"Exported {len(df):,} metrics rows to {filepath}")

        # Top strategies
        top_strats = self.ranker.rank_strategies(limit=100)
        if not top_strats.empty:
            filepath = os.path.join(self.output_dir, f"top_strategies_run{self.run_id}.csv")
            top_strats.to_csv(filepath, index=False)
            print(f"Exported top strategies to {filepath}")

        # Top stocks
        top_stocks = self.ranker.rank_stocks(limit=50)
        if not top_stocks.empty:
            filepath = os.path.join(self.output_dir, f"top_stocks_run{self.run_id}.csv")
            top_stocks.to_csv(filepath, index=False)
            print(f"Exported top stocks to {filepath}")

        # Best pairs
        best = self.ranker.best_pairs(limit=200)
        if not best.empty:
            filepath = os.path.join(self.output_dir, f"best_pairs_run{self.run_id}.csv")
            best.to_csv(filepath, index=False)
            print(f"Exported best pairs to {filepath}")

        # Parameter sensitivity
        sensitivity = self.ranker.parameter_sensitivity()
        if not sensitivity.empty:
            filepath = os.path.join(self.output_dir, f"sensitivity_run{self.run_id}.csv")
            sensitivity.to_csv(filepath, index=False)
            print(f"Exported parameter sensitivity to {filepath}")

        # Parameter breakdowns
        for param in ["or_minutes", "target_multiplier", "stop_loss_type",
                      "trade_direction", "exit_time", "max_or_filter_pct",
                      "entry_confirmation"]:
            breakdown = self.ranker.parameter_breakdown(param)
            if not breakdown.empty:
                filepath = os.path.join(
                    self.output_dir,
                    f"breakdown_{param}_run{self.run_id}.csv"
                )
                breakdown.to_csv(filepath)

    def _try_generate_charts(self):
        """Generate charts if matplotlib is available."""
        try:
            import matplotlib
            matplotlib.use("Agg")  # Non-interactive backend
            import matplotlib.pyplot as plt
            import matplotlib.ticker as ticker
        except ImportError:
            print("matplotlib not installed — skipping chart generation")
            print("Install with: pip install matplotlib")
            return

        self._generate_heatmaps(plt)
        self._generate_bar_charts(plt)

    def _generate_heatmaps(self, plt):
        """Generate heatmap charts for parameter pairs."""
        heatmap_pairs = [
            ("or_minutes", "target_multiplier", "net_pnl", "OR Duration vs Target R:R"),
            ("or_minutes", "stop_loss_type", "sharpe_ratio", "OR Duration vs SL Type"),
            ("or_minutes", "exit_time", "net_pnl", "OR Duration vs Exit Time"),
            ("stop_loss_type", "target_multiplier", "net_pnl", "SL Type vs Target R:R"),
        ]

        for param_x, param_y, metric, title in heatmap_pairs:
            data = self.ranker.heatmap_data(param_x, param_y, metric)
            if data.empty:
                continue

            fig, ax = plt.subplots(figsize=(10, 6))
            im = ax.imshow(data.values, cmap="RdYlGn", aspect="auto")

            ax.set_xticks(range(len(data.columns)))
            ax.set_xticklabels(data.columns, rotation=45, ha="right")
            ax.set_yticks(range(len(data.index)))
            ax.set_yticklabels(data.index)
            ax.set_xlabel(param_x)
            ax.set_ylabel(param_y)
            ax.set_title(f"{title} (avg {metric})")

            plt.colorbar(im, ax=ax, label=metric)

            # Add value labels
            for y in range(len(data.index)):
                for x in range(len(data.columns)):
                    val = data.values[y, x]
                    ax.text(x, y, f"{val:.0f}", ha="center", va="center",
                           fontsize=8, color="black")

            plt.tight_layout()
            filename = f"heatmap_{param_x}_vs_{param_y}_run{self.run_id}.png"
            filepath = os.path.join(self.output_dir, filename)
            plt.savefig(filepath, dpi=150)
            plt.close()
            print(f"Saved heatmap: {filepath}")

    def _generate_bar_charts(self, plt):
        """Generate bar charts for top stocks and parameters."""
        # Top 15 stocks by net P&L
        top_stocks = self.ranker.rank_stocks(metric="net_pnl", limit=15)
        if not top_stocks.empty:
            fig, ax = plt.subplots(figsize=(12, 6))
            colors = ["green" if v > 0 else "red" for v in top_stocks["avg_net_pnl"]]
            ax.barh(top_stocks["stock_code"], top_stocks["avg_net_pnl"], color=colors)
            ax.set_xlabel("Average Net P&L (₹)")
            ax.set_title("Top 15 Stocks by Average Net P&L")
            ax.invert_yaxis()
            plt.tight_layout()
            filepath = os.path.join(self.output_dir, f"top_stocks_run{self.run_id}.png")
            plt.savefig(filepath, dpi=150)
            plt.close()
            print(f"Saved chart: {filepath}")

        # Parameter sensitivity bar chart
        sensitivity = self.ranker.parameter_sensitivity()
        if not sensitivity.empty:
            fig, ax = plt.subplots(figsize=(10, 5))
            ax.barh(sensitivity["parameter"], sensitivity["spread"], color="steelblue")
            ax.set_xlabel("P&L Spread (Best - Worst value)")
            ax.set_title("Parameter Sensitivity Analysis")
            ax.invert_yaxis()
            plt.tight_layout()
            filepath = os.path.join(self.output_dir, f"sensitivity_run{self.run_id}.png")
            plt.savefig(filepath, dpi=150)
            plt.close()
            print(f"Saved chart: {filepath}")
