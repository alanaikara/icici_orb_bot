import os
import logging
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import seaborn as sns
from database_manager import PortfolioDatabase

logger = logging.getLogger("ICICI_ORB_Bot")

class PortfolioTracker:
    """Integration class between ORB Trading Bot and the portfolio database"""
    
    def __init__(self, db_path="data/portfolio.db", initial_capital=None):
        """Initialize the portfolio tracker"""
        self.db = PortfolioDatabase(db_path)
        
        # Set initial capital if provided
        if initial_capital is not None:
            current_capital = self.db.get_current_capital()
            if current_capital == 0:  # Only add if no capital history exists
                self.db.add_capital(initial_capital, notes="Initial capital")
    
    def record_entry(self, stock_code, exchange_code, action, entry_price, 
                    quantity, position_type, product_type, order_id=None, 
                    stop_loss=None, target=None, strategy="ORB", notes=None):
        """Record a trade entry"""
        return self.db.record_trade_entry(
            stock_code, exchange_code, action, entry_price, 
            quantity, position_type, product_type, order_id, 
            stop_loss, target, strategy, notes
        )
    
    def record_exit(self, trade_id, exit_price, exit_time=None, 
                   brokerage=0.0, other_charges=0.0, notes=None):
        """Record a trade exit"""
        return self.db.record_trade_exit(
            trade_id, exit_price, exit_time, 
            brokerage, other_charges, notes
        )
    
    def update_portfolio_prices(self, stock_data):
        """Update portfolio with current market prices"""
        return self.db.update_portfolio_prices(stock_data)
    
    def calculate_daily_metrics(self):
        """Calculate and store performance metrics for the day"""
        today = datetime.now().date()
        return self.db.calculate_performance_metrics(today, "daily")
    
    def generate_daily_report(self, date=None):
        """Generate a daily trading report"""
        report_date = date or datetime.now().date()
        
        # Get daily summary
        daily_data = self.db.get_daily_summary(report_date)
        if not daily_data:
            logger.info(f"No trading data found for {report_date}")
            return None
        
        daily_summary = daily_data[0]
        
        # Get today's trades
        trades = self.db.get_trades_by_date(report_date)
        
        # Get portfolio
        portfolio = self.db.get_portfolio()
        portfolio_summary = self.db.get_portfolio_summary()
        
        # Get current capital
        current_capital = self.db.get_current_capital()
        
        # Generate performance metrics if not already calculated
        metrics = self.db.get_performance_metrics("daily", report_date, report_date)
        if not metrics:
            metrics = [self.db.calculate_performance_metrics(report_date, "daily")]
        
        # Create report data
        report = {
            'date': report_date,
            'summary': daily_summary,
            'trades': trades,
            'portfolio': portfolio,
            'portfolio_summary': portfolio_summary,
            'metrics': metrics[0] if metrics else None,
            'current_capital': current_capital
        }
        
        # Save report to CSV
        self._save_report_to_csv(report, report_date)
        
        return report
    
    def _save_report_to_csv(self, report, report_date):
        """Save report data to CSV files"""
        # Create reports directory
        reports_dir = f"reports/{report_date.strftime('%Y-%m-%d')}"
        os.makedirs(reports_dir, exist_ok=True)
        
        # Save trades
        if report['trades']:
            trades_df = pd.DataFrame(report['trades'])
            trades_df.to_csv(f"{reports_dir}/trades.csv", index=False)
        
        # Save portfolio
        if report['portfolio']:
            portfolio_df = pd.DataFrame(report['portfolio'])
            portfolio_df.to_csv(f"{reports_dir}/portfolio.csv", index=False)
        
        # Save summary
        if report['summary']:
            # Convert dictionary to DataFrame with a single row
            summary_df = pd.DataFrame([report['summary']])
            summary_df.to_csv(f"{reports_dir}/summary.csv", index=False)
        
        # Save metrics
        if report['metrics']:
            metrics_df = pd.DataFrame([report['metrics']])
            metrics_df.to_csv(f"{reports_dir}/metrics.csv", index=False)
    
    def generate_weekly_report(self, end_date=None):
        """Generate a weekly trading report"""
        if end_date is None:
            end_date = datetime.now().date()
        
        # Calculate start of week (Monday)
        start_date = end_date - timedelta(days=end_date.weekday())
        
        return self._generate_period_report(start_date, end_date, "weekly")
    
    def generate_monthly_report(self, year=None, month=None):
        """Generate a monthly trading report"""
        current_date = datetime.now().date()
        
        if year is None:
            year = current_date.year
        
        if month is None:
            month = current_date.month
        
        # Calculate start and end dates for the month
        start_date = datetime(year, month, 1).date()
        
        # Calculate last day of month
        if month == 12:
            end_date = datetime(year + 1, 1, 1).date() - timedelta(days=1)
        else:
            end_date = datetime(year, month + 1, 1).date() - timedelta(days=1)
        
        # If the end date is in the future, use today's date instead
        if end_date > current_date:
            end_date = current_date
        
        return self._generate_period_report(start_date, end_date, "monthly")
    
    def _generate_period_report(self, start_date, end_date, period):
        """Generate a report for a specific period"""
        # Get period summary
        period_summary = self.db.get_period_summary(start_date, end_date)
        
        # Get all trades for the period
        trades = self.db.get_trades_by_date(start_date, end_date)
        
        # Get performance metrics
        metrics = self.db.get_performance_metrics(period, start_date, end_date)
        if not metrics:
            # If metrics don't exist, calculate them
            metrics = [self.db.calculate_performance_metrics(end_date, period)]
        
        # Get capital change for the period
        capital_history = self.db.get_capital_history(start_date, end_date)
        
        # Create report data
        report = {
            'start_date': start_date,
            'end_date': end_date,
            'period': period,
            'summary': period_summary,
            'trades': trades,
            'metrics': metrics[0] if metrics else None,
            'capital_history': capital_history
        }
        
        # Save report to CSV
        self._save_period_report_to_csv(report)
        
        return report
    
    def _save_period_report_to_csv(self, report):
        """Save period report data to CSV files"""
        # Create reports directory
        period_name = report['period']
        start_str = report['start_date'].strftime('%Y-%m-%d')
        end_str = report['end_date'].strftime('%Y-%m-%d')
        
        reports_dir = f"reports/{period_name}_{start_str}_to_{end_str}"
        os.makedirs(reports_dir, exist_ok=True)
        
        # Save trades
        if report['trades']:
            trades_df = pd.DataFrame(report['trades'])
            trades_df.to_csv(f"{reports_dir}/trades.csv", index=False)
        
        # Save summary
        if report['summary']:
            # Convert dictionary to DataFrame with a single row
            summary_df = pd.DataFrame([report['summary']])
            summary_df.to_csv(f"{reports_dir}/summary.csv", index=False)
        
        # Save metrics
        if report['metrics']:
            metrics_df = pd.DataFrame([report['metrics']])
            metrics_df.to_csv(f"{reports_dir}/metrics.csv", index=False)
        
        # Save capital history
        if report['capital_history']:
            capital_df = pd.DataFrame(report['capital_history'])
            capital_df.to_csv(f"{reports_dir}/capital_history.csv", index=False)
    
    def visualize_portfolio(self, save_path=None):
        """Create visualization of the current portfolio"""
        try:
            portfolio = self.db.get_portfolio()
            
            if not portfolio:
                logger.warning("No portfolio data available for visualization")
                return None
            
            # Convert to DataFrame
            df = pd.DataFrame(portfolio)
            
            # Create pie chart for portfolio allocation
            plt.figure(figsize=(12, 8))
            
            # Plot 1: Portfolio Allocation by Value
            plt.subplot(2, 2, 1)
            portfolio_values = df.groupby('stock_code')['current_value'].sum().abs()
            portfolio_values.plot(kind='pie', autopct='%1.1f%%', title='Portfolio Allocation by Value')
            
            # Plot 2: Unrealized P&L by Stock
            plt.subplot(2, 2, 2)
            df.set_index('stock_code')['unrealized_pnl'].plot(
                kind='bar', color=df['unrealized_pnl'].apply(lambda x: 'g' if x > 0 else 'r'),
                title='Unrealized P&L by Stock'
            )
            plt.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
            plt.xticks(rotation=45)
            
            # Plot 3: Long vs Short Positions
            plt.subplot(2, 2, 3)
            position_types = df.apply(
                lambda x: 'Long' if x['quantity'] > 0 else 'Short', axis=1
            ).value_counts()
            position_types.plot(kind='pie', autopct='%1.1f%%', title='Long vs Short Positions')
            
            # Plot 4: Current Value vs Cost Basis
            plt.subplot(2, 2, 4)
            x = range(len(df))
            width = 0.35
            plt.bar(x, df['current_value'], width, label='Current Value')
            plt.bar([i + width for i in x], df['average_price'] * df['quantity'], width, label='Cost Basis')
            plt.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
            plt.xticks([i + width/2 for i in x], df['stock_code'], rotation=45)
            plt.title('Current Value vs Cost Basis')
            plt.legend()
            
            plt.tight_layout()
            
            if save_path:
                plt.savefig(save_path)
                logger.info(f"Portfolio visualization saved to {save_path}")
                plt.close()
                return save_path
            else:
                plt.show()
                plt.close()
                return True
                
        except Exception as e:
            logger.error(f"Error visualizing portfolio: {e}")
            return None
    
    def visualize_performance(self, period_type="monthly", save_path=None):
        """Create visualization of trading performance"""
        try:
            current_date = datetime.now().date()
            
            if period_type == "weekly":
                # Get last 12 weeks
                end_date = current_date
                start_date = end_date - timedelta(weeks=12)
            elif period_type == "monthly":
                # Get last 12 months
                end_date = current_date
                start_date = datetime(current_date.year - 1, current_date.month, 1).date()
            else:  # daily
                # Get last 30 days
                end_date = current_date
                start_date = end_date - timedelta(days=30)
            
            # Get daily summaries
            daily_data = self.db.get_daily_summary(start_date, end_date)
            
            if not daily_data:
                logger.warning(f"No performance data available for {period_type} visualization")
                return None
            
            # Convert to DataFrame
            df = pd.DataFrame(daily_data)
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
            
            # Period grouping
            if period_type == "weekly":
                df_grouped = df.resample('W').sum()
            elif period_type == "monthly":
                df_grouped = df.resample('M').sum()
            else:  # daily
                df_grouped = df
            
            # Calculate win rate
            df_grouped['win_rate'] = df_grouped['winning_trades'] / df_grouped['total_trades']
            
            # Create figure
            plt.figure(figsize=(15, 10))
            
            # Plot 1: Net P&L Over Time
            plt.subplot(2, 2, 1)
            df_grouped['net_pnl'].cumsum().plot(
                marker='o', linestyle='-', title=f'Cumulative Net P&L ({period_type.capitalize()})'
            )
            plt.axhline(y=0, color='red', linestyle='--', linewidth=0.5)
            plt.grid(True, linestyle='--', alpha=0.7)
            
            # Plot 2: Win Rate Over Time
            plt.subplot(2, 2, 2)
            df_grouped['win_rate'].plot(
                marker='o', linestyle='-', title=f'Win Rate ({period_type.capitalize()})'
            )
            plt.axhline(y=0.5, color='red', linestyle='--', linewidth=0.5)
            plt.grid(True, linestyle='--', alpha=0.7)
            plt.ylim(0, 1.0)
            
            # Plot 3: Trading Volume
            plt.subplot(2, 2, 3)
            df_grouped['total_trades'].plot(
                kind='bar', title=f'Number of Trades ({period_type.capitalize()})'
            )
            plt.grid(True, linestyle='--', alpha=0.7, axis='y')
            
            # Plot 4: Winning vs Losing Trades
            plt.subplot(2, 2, 4)
            df_grouped[['winning_trades', 'losing_trades']].plot(
                kind='bar', stacked=True, title=f'Winning vs Losing Trades ({period_type.capitalize()})'
            )
            plt.grid(True, linestyle='--', alpha=0.7, axis='y')
            
            plt.tight_layout()
            
            if save_path:
                plt.savefig(save_path)
                logger.info(f"Performance visualization saved to {save_path}")
                plt.close()
                return save_path
            else:
                plt.show()
                plt.close()
                return True
                
        except Exception as e:
            logger.error(f"Error visualizing performance: {e}")
            return None
    
    def visualize_trade_distribution(self, start_date=None, end_date=None, save_path=None):
        """Create visualization of trade distribution"""
        try:
            if start_date is None:
                # Default to last 90 days
                end_date = datetime.now().date()
                start_date = end_date - timedelta(days=90)
            elif end_date is None:
                end_date = datetime.now().date()
            
            # Get trades for the period
            trades = self.db.get_trades_by_date(start_date, end_date)
            
            if not trades:
                logger.warning(f"No trade data available for distribution visualization")
                return None
            
            # Convert to DataFrame
            df = pd.DataFrame(trades)
            
            # Filter to only include closed trades with P&L
            df = df[df['status'] == 'closed']
            df = df[df['pnl'].notna()]
            
            if df.empty:
                logger.warning(f"No closed trades with P&L data for visualization")
                return None
            
            # Create figure
            plt.figure(figsize=(15, 12))
            
            # Plot 1: P&L Distribution
            plt.subplot(2, 2, 1)
            sns.histplot(df['pnl'], kde=True)
            plt.axvline(x=0, color='red', linestyle='--', linewidth=0.5)
            plt.title('P&L Distribution')
            plt.xlabel('P&L')
            plt.ylabel('Frequency')
            
            # Plot 2: P&L by Stock
            plt.subplot(2, 2, 2)
            stock_pnl = df.groupby('stock_code')['pnl'].sum().sort_values()
            colors = ['g' if x > 0 else 'r' for x in stock_pnl]
            stock_pnl.plot(kind='barh', color=colors, title='P&L by Stock')
            plt.axvline(x=0, color='black', linestyle='-', linewidth=0.5)
            plt.grid(True, linestyle='--', alpha=0.7, axis='x')
            
            # Plot 3: P&L by Day of Week
            plt.subplot(2, 2, 3)
            df['entry_day'] = pd.to_datetime(df['entry_time']).dt.day_name()
            day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
            day_pnl = df.groupby('entry_day')['pnl'].sum()
            day_pnl = day_pnl.reindex(day_order)
            colors = ['g' if x > 0 else 'r' for x in day_pnl]
            day_pnl.plot(kind='bar', color=colors, title='P&L by Day of Week')
            plt.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
            plt.grid(True, linestyle='--', alpha=0.7, axis='y')
            
            # Plot 4: P&L by Strategy
            plt.subplot(2, 2, 4)
            strategy_pnl = df.groupby('strategy')['pnl'].sum().sort_values()
            colors = ['g' if x > 0 else 'r' for x in strategy_pnl]
            strategy_pnl.plot(kind='barh', color=colors, title='P&L by Strategy')
            plt.axvline(x=0, color='black', linestyle='-', linewidth=0.5)
            plt.grid(True, linestyle='--', alpha=0.7, axis='x')
            
            plt.tight_layout()
            
            if save_path:
                plt.savefig(save_path)
                logger.info(f"Trade distribution visualization saved to {save_path}")
                plt.close()
                return save_path
            else:
                plt.show()
                plt.close()
                return True
                
        except Exception as e:
            logger.error(f"Error visualizing trade distribution: {e}")
            return None
    
    def export_all_data(self, output_dir="exports"):
        """Export all database tables to CSV files"""
        try:
            os.makedirs(output_dir, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            tables = [
                'trades', 'daily_summary', 'portfolio', 
                'capital_history', 'performance_metrics'
            ]
            
            exported_files = {}
            for table in tables:
                output_file = f"{output_dir}/{table}_{timestamp}.csv"
                result = self.db.export_to_csv(table, output_file)
                exported_files[table] = result
            
            return exported_files
            
        except Exception as e:
            logger.error(f"Error exporting all data: {e}")
            raise