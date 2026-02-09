#!/usr/bin/env python3
"""
Portfolio Reporting Tool - Generate reports and visualizations from trading data
"""

import os
import sys
import argparse
import logging
from datetime import datetime, timedelta, date
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import database and tracker
from portfolio_tracker import PortfolioTracker

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('reports/reporting.log')
    ]
)
logger = logging.getLogger('PortfolioReporting')

def parse_date(date_str):
    """Parse date string in YYYY-MM-DD format"""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        logger.error(f"Invalid date format: {date_str}. Use YYYY-MM-DD format.")
        return None

def generate_daily_report(tracker, report_date=None, output_dir=None):
    """Generate a daily trading report"""
    if report_date is None:
        report_date = datetime.now().date()
    elif isinstance(report_date, str):
        report_date = parse_date(report_date)
        if report_date is None:
            return False
    
    # Create output directory
    if output_dir is None:
        output_dir = f"reports/daily/{report_date.strftime('%Y-%m-%d')}"
    
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate report
    report = tracker.generate_daily_report(report_date)
    
    if report is None:
        logger.warning(f"No data available for daily report on {report_date}")
        return False
    
    # Create visualizations
    tracker.visualize_portfolio(f"{output_dir}/portfolio.png")
    tracker.visualize_performance("daily", f"{output_dir}/daily_performance.png")
    
    logger.info(f"Daily report for {report_date} generated in {output_dir}")
    return True

def generate_weekly_report(tracker, end_date=None, output_dir=None):
    """Generate a weekly trading report"""
    if end_date is None:
        end_date = datetime.now().date()
    elif isinstance(end_date, str):
        end_date = parse_date(end_date)
        if end_date is None:
            return False
    
    # Calculate start of week (Monday)
    start_date = end_date - timedelta(days=end_date.weekday())
    
    # Create output directory
    if output_dir is None:
        output_dir = f"reports/weekly/{start_date.strftime('%Y-%m-%d')}_to_{end_date.strftime('%Y-%m-%d')}"
    
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate report
    report = tracker._generate_period_report(start_date, end_date, "weekly")
    
    if report is None:
        logger.warning(f"No data available for weekly report from {start_date} to {end_date}")
        return False
    
    # Create visualizations
    tracker.visualize_portfolio(f"{output_dir}/portfolio.png")
    tracker.visualize_performance("weekly", f"{output_dir}/weekly_performance.png")
    tracker.visualize_trade_distribution(start_date, end_date, f"{output_dir}/trade_distribution.png")
    
    # Create additional weekly-specific visualizations
    create_weekly_analysis(tracker, start_date, end_date, output_dir)
    
    logger.info(f"Weekly report from {start_date} to {end_date} generated in {output_dir}")
    return True

def generate_monthly_report(tracker, year=None, month=None, output_dir=None):
    """Generate a monthly trading report"""
    current_date = datetime.now().date()
    
    if year is None:
        year = current_date.year
    
    if month is None:
        month = current_date.month
    
    # Calculate start and end dates for the month
    start_date = date(year, month, 1)
    
    # Calculate last day of month
    if month == 12:
        end_date = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        end_date = date(year, month + 1, 1) - timedelta(days=1)
    
    # If the end date is in the future, use today's date instead
    if end_date > current_date:
        end_date = current_date
    
    # Create output directory
    if output_dir is None:
        output_dir = f"reports/monthly/{year}_{month:02d}"
    
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate report
    report = tracker._generate_period_report(start_date, end_date, "monthly")
    
    if report is None:
        logger.warning(f"No data available for monthly report for {year}-{month:02d}")
        return False
    
    # Create visualizations
    tracker.visualize_portfolio(f"{output_dir}/portfolio.png")
    tracker.visualize_performance("monthly", f"{output_dir}/monthly_performance.png")
    tracker.visualize_trade_distribution(start_date, end_date, f"{output_dir}/trade_distribution.png")
    
    # Create additional monthly-specific visualizations
    create_monthly_analysis(tracker, year, month, output_dir)
    
    logger.info(f"Monthly report for {year}-{month:02d} generated in {output_dir}")
    return True

def generate_yearly_report(tracker, year=None, output_dir=None):
    """Generate a yearly trading report"""
    current_date = datetime.now().date()
    
    if year is None:
        year = current_date.year
    
    # Calculate start and end dates for the year
    start_date = date(year, 1, 1)
    end_date = date(year, 12, 31)
    
    # If the end date is in the future, use today's date instead
    if end_date > current_date:
        end_date = current_date
    
    # Create output directory
    if output_dir is None:
        output_dir = f"reports/yearly/{year}"
    
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate report using the period report function
    report = tracker._generate_period_report(start_date, end_date, "yearly")
    
    if report is None:
        logger.warning(f"No data available for yearly report for {year}")
        return False
    
    # Create visualizations
    tracker.visualize_portfolio(f"{output_dir}/portfolio.png")
    tracker.visualize_performance("monthly", f"{output_dir}/yearly_performance.png")
    tracker.visualize_trade_distribution(start_date, end_date, f"{output_dir}/trade_distribution.png")
    
    # Create additional yearly-specific visualizations
    create_yearly_analysis(tracker, year, output_dir)
    
    logger.info(f"Yearly report for {year} generated in {output_dir}")
    return True

def create_weekly_analysis(tracker, start_date, end_date, output_dir):
    """Create additional weekly analysis visualizations"""
    try:
        with tracker.db as db:
            # Get trades for the week
            trades = db.get_trades_by_date(start_date, end_date)
            
            if not trades:
                return
            
            # Convert to DataFrame
            df = pd.DataFrame(trades)
            df['entry_time'] = pd.to_datetime(df['entry_time'])
            df['exit_time'] = pd.to_datetime(df['exit_time'])
            
            # Create hourly analysis
            plt.figure(figsize=(12, 8))
            
            # Plot: Trades by hour of day
            df['entry_hour'] = df['entry_time'].dt.hour
            hourly_counts = df['entry_hour'].value_counts().sort_index()
            
            # Define market hours (9:15 AM to 3:30 PM)
            market_hours = list(range(9, 16))
            
            # Ensure all market hours are represented
            for hour in market_hours:
                if hour not in hourly_counts.index:
                    hourly_counts[hour] = 0
            
            hourly_counts = hourly_counts.sort_index()
            
            plt.bar(hourly_counts.index, hourly_counts.values)
            plt.title('Trades by Hour of Day')
            plt.xlabel('Hour')
            plt.ylabel('Number of Trades')
            plt.xticks(range(min(market_hours), max(market_hours) + 1))
            plt.grid(True, linestyle='--', alpha=0.7, axis='y')
            
            plt.tight_layout()
            plt.savefig(f"{output_dir}/trades_by_hour.png")
            plt.close()
            
            # Create trade duration analysis
            plt.figure(figsize=(12, 8))
            
            # Add trade duration in minutes
            closed_trades = df[df['exit_time'].notna()].copy()
            
            if not closed_trades.empty:
                closed_trades['duration_minutes'] = (
                    closed_trades['exit_time'] - closed_trades['entry_time']
                ).dt.total_seconds() / 60
                
                # Plot: Trade duration histogram
                plt.hist(closed_trades['duration_minutes'], bins=20, alpha=0.75)
                plt.title('Trade Duration Distribution')
                plt.xlabel('Duration (minutes)')
                plt.ylabel('Number of Trades')
                plt.grid(True, linestyle='--', alpha=0.7)
                
                plt.tight_layout()
                plt.savefig(f"{output_dir}/trade_duration.png")
                plt.close()
            
    except Exception as e:
        logger.error(f"Error creating weekly analysis: {e}")

def create_monthly_analysis(tracker, year, month, output_dir):
    """Create additional monthly analysis visualizations"""
    try:
        with tracker.db as db:
            # Calculate start and end dates
            start_date = date(year, month, 1)
            if month == 12:
                end_date = date(year + 1, 1, 1) - timedelta(days=1)
            else:
                end_date = date(year, month + 1, 1) - timedelta(days=1)
            
            # Get daily summaries for the month
            daily_data = db.get_daily_summary(start_date, end_date)
            
            if not daily_data:
                return
            
            # Convert to DataFrame
            df_daily = pd.DataFrame(daily_data)
            df_daily['date'] = pd.to_datetime(df_daily['date'])
            
            # Get trades for the month
            trades = db.get_trades_by_date(start_date, end_date)
            
            if trades:
                df_trades = pd.DataFrame(trades)
                df_trades['entry_time'] = pd.to_datetime(df_trades['entry_time'])
                
                # Create figure: P&L calendar heatmap
                plt.figure(figsize=(14, 8))
                
                # Prepare data for heatmap
                df_daily['day'] = df_daily['date'].dt.day
                df_daily['dayofweek'] = df_daily['date'].dt.dayofweek
                
                # Create a pivot table with days as columns and weeks as rows
                calendar_data = df_daily.pivot_table(
                    index='dayofweek', 
                    columns='day',
                    values='net_pnl',
                    aggfunc='sum'
                )
                
                # Create the heatmap
                sns.heatmap(
                    calendar_data, 
                    cmap='RdYlGn', 
                    center=0,
                    annot=True, 
                    fmt=".0f",
                    linewidths=.5
                )
                
                plt.title(f'Daily P&L Calendar - {year}-{month:02d}')
                plt.xlabel('Day of Month')
                plt.ylabel('Day of Week')
                
                # Replace y-axis labels with day names
                day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
                plt.yticks(
                    ticks=np.arange(0.5, len(day_names)), 
                    labels=day_names
                )
                
                plt.tight_layout()
                plt.savefig(f"{output_dir}/pnl_calendar.png")
                plt.close()
                
                # Create stock performance analysis
                plt.figure(figsize=(14, 8))
                
                # Group by stock and calculate metrics
                stock_metrics = df_trades.groupby('stock_code').agg({
                    'pnl': ['sum', 'mean', 'count', lambda x: (x > 0).sum() / len(x) if len(x) > 0 else 0]
                })
                
                # Flatten the column hierarchy
                stock_metrics.columns = ['Total P&L', 'Avg P&L', 'Trade Count', 'Win Rate']
                
                # Sort by total P&L
                stock_metrics = stock_metrics.sort_values('Total P&L', ascending=False)
                
                # Create subplot grid
                fig, axes = plt.subplots(2, 2, figsize=(16, 12))
                
                # Plot 1: Total P&L by stock
                stock_metrics['Total P&L'].plot(
                    kind='bar', 
                    ax=axes[0, 0], 
                    color=stock_metrics['Total P&L'].apply(lambda x: 'g' if x > 0 else 'r')
                )
                axes[0, 0].set_title('Total P&L by Stock')
                axes[0, 0].set_ylabel('P&L')
                axes[0, 0].grid(True, linestyle='--', alpha=0.7, axis='y')
                
                # Plot 2: Average P&L by stock
                stock_metrics['Avg P&L'].plot(
                    kind='bar', 
                    ax=axes[0, 1], 
                    color=stock_metrics['Avg P&L'].apply(lambda x: 'g' if x > 0 else 'r')
                )
                axes[0, 1].set_title('Average P&L by Stock')
                axes[0, 1].set_ylabel('Average P&L')
                axes[0, 1].grid(True, linestyle='--', alpha=0.7, axis='y')
                
                # Plot 3: Trade count by stock
                stock_metrics['Trade Count'].plot(
                    kind='bar', 
                    ax=axes[1, 0]
                )
                axes[1, 0].set_title('Number of Trades by Stock')
                axes[1, 0].set_ylabel('Count')
                axes[1, 0].grid(True, linestyle='--', alpha=0.7, axis='y')
                
                # Plot 4: Win rate by stock
                stock_metrics['Win Rate'].plot(
                    kind='bar', 
                    ax=axes[1, 1],
                    color=stock_metrics['Win Rate'].apply(lambda x: 'g' if x > 0.5 else 'r' if x < 0.5 else 'y')
                )
                axes[1, 1].set_title('Win Rate by Stock')
                axes[1, 1].set_ylabel('Win Rate')
                axes[1, 1].axhline(y=0.5, color='black', linestyle='--', linewidth=0.5)
                axes[1, 1].grid(True, linestyle='--', alpha=0.7, axis='y')
                
                plt.tight_layout()
                plt.savefig(f"{output_dir}/stock_performance.png")
                plt.close()
                
    except Exception as e:
        logger.error(f"Error creating monthly analysis: {e}")

def create_yearly_analysis(tracker, year, output_dir):
    """Create additional yearly analysis visualizations"""
    try:
        with tracker.db as db:
            # Calculate start and end dates
            start_date = date(year, 1, 1)
            end_date = date(year, 12, 31)
            
            # If end date is in the future, use today's date
            current_date = datetime.now().date()
            if end_date > current_date:
                end_date = current_date
            
            # Get daily summaries for the year
            daily_data = db.get_daily_summary(start_date, end_date)
            
            if not daily_data:
                return
            
            # Convert to DataFrame
            df_daily = pd.DataFrame(daily_data)
            df_daily['date'] = pd.to_datetime(df_daily['date'])
            
            # Add month column for grouping
            df_daily['month'] = df_daily['date'].dt.month
            
            # Group by month
            monthly_summary = df_daily.groupby('month').agg({
                'gross_pnl': 'sum',
                'net_pnl': 'sum',
                'total_trades': 'sum',
                'winning_trades': 'sum',
                'losing_trades': 'sum',
                'brokerage_total': 'sum',
                'other_charges_total': 'sum'
            }).reset_index()
            
            # Calculate win rate
            monthly_summary['win_rate'] = monthly_summary['winning_trades'] / monthly_summary['total_trades']
            
            # Calculate cumulative P&L
            monthly_summary['cumulative_pnl'] = monthly_summary['net_pnl'].cumsum()
            
            # Create monthly P&L chart
            plt.figure(figsize=(14, 8))
            
            # Plot bars for monthly P&L
            bars = plt.bar(
                monthly_summary['month'],
                monthly_summary['net_pnl'],
                color=monthly_summary['net_pnl'].apply(lambda x: 'g' if x > 0 else 'r')
            )
            
            # Plot line for cumulative P&L
            ax2 = plt.twinx()
            ax2.plot(
                monthly_summary['month'],
                monthly_summary['cumulative_pnl'],
                'b-',
                marker='o',
                linewidth=2
            )
            
            # Add labels and grid
            plt.title(f'Monthly P&L for {year}')
            plt.xlabel('Month')
            plt.ylabel('Monthly P&L')
            ax2.set_ylabel('Cumulative P&L', color='b')
            
            # Set x-ticks to month names
            month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
            plt.xticks(
                range(1, 13),
                [month_names[i-1] for i in range(1, 13)]
            )
            
            plt.grid(True, linestyle='--', alpha=0.7, axis='y')
            
            # Add value labels on bars
            for bar in bars:
                height = bar.get_height()
                plt.text(
                    bar.get_x() + bar.get_width()/2.,
                    height if height > 0 else height - 500,
                    f'{int(height)}',
                    ha='center',
                    va='bottom' if height > 0 else 'top'
                )
            
            plt.tight_layout()
            plt.savefig(f"{output_dir}/monthly_pnl.png")
            plt.close()
            
            # Create monthly metrics chart
            plt.figure(figsize=(14, 10))
            
            # Create subplot grid
            fig, axes = plt.subplots(2, 2, figsize=(16, 12))
            
            # Plot 1: Monthly trade count
            monthly_summary['total_trades'].plot(
                kind='bar',
                ax=axes[0, 0]
            )
            axes[0, 0].set_title('Monthly Trade Count')
            axes[0, 0].set_xlabel('Month')
            axes[0, 0].set_ylabel('Number of Trades')
            axes[0, 0].set_xticks(
                range(len(monthly_summary)),
                [month_names[i-1] for i in monthly_summary['month']]
            )
            axes[0, 0].grid(True, linestyle='--', alpha=0.7, axis='y')
            
            # Plot 2: Monthly win rate
            monthly_summary['win_rate'].plot(
                kind='bar',
                ax=axes[0, 1],
                color=monthly_summary['win_rate'].apply(lambda x: 'g' if x > 0.5 else 'r' if x < 0.5 else 'y')
            )
            axes[0, 1].set_title('Monthly Win Rate')
            axes[0, 1].set_xlabel('Month')
            axes[0, 1].set_ylabel('Win Rate')
            axes[0, 1].axhline(y=0.5, color='black', linestyle='--', linewidth=0.5)
            axes[0, 1].set_xticks(
                range(len(monthly_summary)),
                [month_names[i-1] for i in monthly_summary['month']]
            )
            axes[0, 1].grid(True, linestyle='--', alpha=0.7, axis='y')
            
            # Plot 3: Monthly winning vs losing trades
            monthly_summary[['winning_trades', 'losing_trades']].plot(
                kind='bar',
                stacked=True,
                ax=axes[1, 0]
            )
            axes[1, 0].set_title('Monthly Winning vs Losing Trades')
            axes[1, 0].set_xlabel('Month')
            axes[1, 0].set_ylabel('Number of Trades')
            axes[1, 0].set_xticks(
                range(len(monthly_summary)),
                [month_names[i-1] for i in monthly_summary['month']]
            )
            axes[1, 0].grid(True, linestyle='--', alpha=0.7, axis='y')
            
            # Plot 4: Monthly costs (brokerage + charges)
            monthly_summary[['brokerage_total', 'other_charges_total']].plot(
                kind='bar',
                stacked=True,
                ax=axes[1, 1]
            )
            axes[1, 1].set_title('Monthly Trading Costs')
            axes[1, 1].set_xlabel('Month')
            axes[1, 1].set_ylabel('Costs')
            axes[1, 1].set_xticks(
                range(len(monthly_summary)),
                [month_names[i-1] for i in monthly_summary['month']]
            )
            axes[1, 1].grid(True, linestyle='--', alpha=0.7, axis='y')
            
            plt.tight_layout()
            plt.savefig(f"{output_dir}/monthly_metrics.png")
            plt.close()
            
            # Get trades for deeper analysis
            trades = db.get_trades_by_date(start_date, end_date)
            
            if trades:
                df_trades = pd.DataFrame(trades)
                df_trades['entry_time'] = pd.to_datetime(df_trades['entry_time'])
                
                # Analysis by strategy
                if 'strategy' in df_trades.columns:
                    strategy_metrics = df_trades.groupby('strategy').agg({
                        'pnl': ['sum', 'mean', 'count', lambda x: (x > 0).sum() / len(x) if len(x) > 0 else 0]
                    })
                    
                    # Flatten the column hierarchy
                    strategy_metrics.columns = ['Total P&L', 'Avg P&L', 'Trade Count', 'Win Rate']
                    
                    # Create strategy performance chart
                    plt.figure(figsize=(14, 10))
                    
                    # Create subplot grid
                    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
                    
                    # Plot 1: Total P&L by strategy
                    strategy_metrics['Total P&L'].plot(
                        kind='bar', 
                        ax=axes[0, 0], 
                        color=strategy_metrics['Total P&L'].apply(lambda x: 'g' if x > 0 else 'r')
                    )
                    axes[0, 0].set_title('Total P&L by Strategy')
                    axes[0, 0].set_ylabel('P&L')
                    axes[0, 0].grid(True, linestyle='--', alpha=0.7, axis='y')
                    
                    # Plot 2: Average P&L by strategy
                    strategy_metrics['Avg P&L'].plot(
                        kind='bar', 
                        ax=axes[0, 1], 
                        color=strategy_metrics['Avg P&L'].apply(lambda x: 'g' if x > 0 else 'r')
                    )
                    axes[0, 1].set_title('Average P&L by Strategy')
                    axes[0, 1].set_ylabel('Average P&L')
                    axes[0, 1].grid(True, linestyle='--', alpha=0.7, axis='y')
                    
                    # Plot 3: Trade count by strategy
                    strategy_metrics['Trade Count'].plot(
                        kind='bar', 
                        ax=axes[1, 0]
                    )
                    axes[1, 0].set_title('Number of Trades by Strategy')
                    axes[1, 0].set_ylabel('Count')
                    axes[1, 0].grid(True, linestyle='--', alpha=0.7, axis='y')
                    
                    # Plot 4: Win rate by strategy
                    strategy_metrics['Win Rate'].plot(
                        kind='bar', 
                        ax=axes[1, 1],
                        color=strategy_metrics['Win Rate'].apply(
                            lambda x: 'g' if x > 0.5 else 'r' if x < 0.5 else 'y'
                        )
                    )
                    axes[1, 1].set_title('Win Rate by Strategy')
                    axes[1, 1].set_ylabel('Win Rate')
                    axes[1, 1].axhline(y=0.5, color='black', linestyle='--', linewidth=0.5)
                    axes[1, 1].grid(True, linestyle='--', alpha=0.7, axis='y')
                    
                    plt.tight_layout()
                    plt.savefig(f"{output_dir}/strategy_performance.png")
                    plt.close()
                
    except Exception as e:
        logger.error(f"Error creating yearly analysis: {e}")

def main():
    """Main entry point for the reporting tool"""
    # Create parser
    parser = argparse.ArgumentParser(description='Generate trading reports and visualizations')
    
    # Add arguments
    parser.add_argument('--db', default='data/portfolio.db', help='Path to the portfolio database')
    
    # Add report type arguments
    report_group = parser.add_mutually_exclusive_group(required=True)
    report_group.add_argument('--daily', action='store_true', help='Generate daily report')
    report_group.add_argument('--weekly', action='store_true', help='Generate weekly report')
    report_group.add_argument('--monthly', action='store_true', help='Generate monthly report')
    report_group.add_argument('--yearly', action='store_true', help='Generate yearly report')
    report_group.add_argument('--custom', action='store_true', help='Generate custom date range report')
    
    # Add date arguments
    parser.add_argument('--date', help='Specific date for daily report (YYYY-MM-DD)')
    parser.add_argument('--year', type=int, help='Year for monthly or yearly report')
    parser.add_argument('--month', type=int, help='Month for monthly report (1-12)')
    parser.add_argument('--start', help='Start date for custom report (YYYY-MM-DD)')
    parser.add_argument('--end', help='End date for custom report (YYYY-MM-DD)')
    
    # Add output directory argument
    parser.add_argument('--output', help='Output directory for reports')
    
    # Parse arguments
    args = parser.parse_args()
    
    # Create portfolio tracker
    tracker = PortfolioTracker(db_path=args.db)
    
    # Generate report based on type
    if args.daily:
        generate_daily_report(tracker, args.date, args.output)
    elif args.weekly:
        end_date = args.date if args.date else None
        generate_weekly_report(tracker, end_date, args.output)
    elif args.monthly:
        if args.month and (args.month < 1 or args.month > 12):
            logger.error("Month must be between 1 and 12")
            return 1
        generate_monthly_report(tracker, args.year, args.month, args.output)
    elif args.yearly:
        generate_yearly_report(tracker, args.year, args.output)
    elif args.custom:
        if not args.start or not args.end:
            logger.error("Custom reports require both --start and --end dates")
            return 1
        
        start_date = parse_date(args.start)
        end_date = parse_date(args.end)
        
        if not start_date or not end_date:
            return 1
        
        # Determine report type based on date range
        delta = end_date - start_date
        
        if delta.days <= 7:
            # Use weekly report format for short ranges
            output_dir = args.output or f"reports/custom/{start_date.strftime('%Y-%m-%d')}_to_{end_date.strftime('%Y-%m-%d')}"
            tracker._generate_period_report(start_date, end_date, "custom")
            tracker.visualize_portfolio(f"{output_dir}/portfolio.png")
            tracker.visualize_trade_distribution(start_date, end_date, f"{output_dir}/trade_distribution.png")
        else:
            # Use monthly/yearly format for longer ranges
            output_dir = args.output or f"reports/custom/{start_date.strftime('%Y-%m-%d')}_to_{end_date.strftime('%Y-%m-%d')}"
            tracker._generate_period_report(start_date, end_date, "custom")
            tracker.visualize_portfolio(f"{output_dir}/portfolio.png")
            tracker.visualize_performance("custom", f"{output_dir}/performance.png")
            tracker.visualize_trade_distribution(start_date, end_date, f"{output_dir}/trade_distribution.png")
    
    return 0

if __name__ == "__main__":
    import numpy as np  # Required for some calculations
    sys.exit(main())