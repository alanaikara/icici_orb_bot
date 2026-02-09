import os
import sys
import logging
from datetime import datetime
from Portfolio_tracker import PortfolioTracker

# Add src directory to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from core.bot import ORBTradingBot
from utils.logger import setup_logger

# Set up logger
logger = setup_logger(name="ICICI_ORB_Bot", log_file="logs/icici_orb_bot.log")

class ORBTradingBotWithTracking(ORBTradingBot):
    """Extended ORB Trading Bot with portfolio tracking capabilities"""
    
    def __init__(self, app_key, secret_key, api_session, config_path="config/config.json"):
        """Initialize the extended bot with portfolio tracking"""
        super().__init__(app_key, secret_key, api_session, config_path)
        
        # Initialize portfolio tracker with initial capital from config
        self.tracker = PortfolioTracker(
            db_path="data/portfolio.db", 
            initial_capital=self.config.get("capital", 100000)
        )
        
        logger.info("Portfolio tracking initialized")
    
    def place_entry_order(self, stock_code, action, quantity, price):
        """Override to record trade entry in the portfolio database"""
        super().place_entry_order(stock_code, action, quantity, price)
        
        # Get the stock data
        stock_data = self.stocks_data[stock_code]
        
        # Record the trade in our database
        trade_id = self.tracker.record_entry(
            stock_code=stock_code,
            exchange_code=self.config["exchange_code"],
            action=action,
            entry_price=price,
            quantity=quantity,
            position_type=stock_data["position"],
            product_type=self.config["product_type"],
            order_id=stock_data["order_id"],
            stop_loss=stock_data["stop_loss"],
            strategy="ORB",
            notes=f"Opening range: High={stock_data['opening_range_high']}, Low={stock_data['opening_range_low']}"
        )
        
        # Store the trade_id for later reference
        stock_data["trade_id"] = trade_id
        
        logger.info(f"Trade entry recorded in portfolio database with ID: {trade_id}")
    
    def place_exit_order(self, stock_code):
        """Override to record trade exit in the portfolio database"""
        # Get current data before exiting
        stock_data = self.stocks_data[stock_code]
        trade_id = stock_data.get("trade_id")
        
        # Call the original method
        super().place_exit_order(stock_code)
        
        # If we have a trade_id and this was a real trade with an entry price
        if trade_id and stock_data["entry_price"] is not None:
            # Get current price for exit
            exit_price = None
            
            try:
                # Try to get current price from exchange
                quotes_response = self.api.get_quotes(stock_code, self.config["exchange_code"])
                if 'Success' in quotes_response and quotes_response['Success']:
                    exit_price = float(quotes_response['Success'][0]['ltp'])
            except Exception as e:
                logger.error(f"Error getting exit price from exchange: {e}")
            
            # If we couldn't get a price, use the last known price or entry price
            if exit_price is None:
                exit_price = stock_data.get("last_price", stock_data["entry_price"])
            
            # Calculate costs
            quantity = stock_data["quantity"]
            entry_price = stock_data["entry_price"]
            
            # Estimate brokerage
            brokerage_rate = self.config.get("brokerage_rate", 0.0001)
            brokerage = (entry_price + exit_price) * quantity * brokerage_rate
            
            # Estimate other charges
            stt_rate = self.config.get("stt_rate", 0.00025)
            other_charges = exit_price * quantity * stt_rate  # STT on sell side only
            
            # Record the exit
            self.tracker.record_exit(
                trade_id=trade_id,
                exit_price=exit_price,
                brokerage=brokerage,
                other_charges=other_charges,
                notes=f"Exit triggered at {datetime.now().strftime('%H:%M:%S')}"
            )
            
            logger.info(f"Trade exit recorded in portfolio database for ID: {trade_id}")
    
    def update_portfolio_prices(self):
        """Update portfolio with current market prices"""
        stock_data = {}
        
        # Get current prices for all stocks in our watchlist
        for stock_code in self.config["stocks"]:
            try:
                quotes_response = self.api.get_quotes(stock_code, self.config["exchange_code"])
                if 'Success' in quotes_response and quotes_response['Success']:
                    last_price = float(quotes_response['Success'][0]['ltp'])
                    stock_data[stock_code] = {"last_price": last_price}
                    
                    # Also update our internal tracking
                    if stock_code in self.stocks_data:
                        self.stocks_data[stock_code]["last_price"] = last_price
            except Exception as e:
                logger.error(f"Error getting price for {stock_code}: {e}")
        
        # Update database
        if stock_data:
            self.tracker.update_portfolio_prices(stock_data)
    
    def generate_end_of_day_report(self):
        """Generate end of day trading report"""
        # Update portfolio prices one last time
        self.update_portfolio_prices()
        
        # Calculate daily metrics
        self.tracker.calculate_daily_metrics()
        
        # Generate daily report
        report = self.tracker.generate_daily_report()
        
        # Create visualizations
        date_str = datetime.now().strftime("%Y-%m-%d")
        reports_dir = f"reports/{date_str}"
        os.makedirs(reports_dir, exist_ok=True)
        
        # Portfolio visualization
        self.tracker.visualize_portfolio(f"{reports_dir}/portfolio.png")
        
        # Performance visualization
        self.tracker.visualize_performance("daily", f"{reports_dir}/daily_performance.png")
        
        logger.info(f"End of day report generated in {reports_dir}")
        return report
    
    def run_trading_cycle(self):
        """Override to add portfolio price updates during trading cycle"""
        # Call the original method
        super().run_trading_cycle()
        
        # Update portfolio prices every 5 cycles (configurable)
        if hasattr(self, 'update_cycle_count'):
            self.update_cycle_count += 1
        else:
            self.update_cycle_count = 1
        
        # Update every 5 cycles (approximately every 5 minutes)
        if self.update_cycle_count % 5 == 0:
            self.update_portfolio_prices()
    
    def stop(self):
        """Override to add end of day reporting"""
        # Generate end of day report
        if self.trading_active:
            self.generate_end_of_day_report()
        
        # Call the original method
        super().stop()


def main():
    """Main entry point with portfolio tracking"""
    import argparse
    from dotenv import load_dotenv
    
    # Set up command line arguments
    parser = argparse.ArgumentParser(description='ICICI Direct ORB Trading Bot with Portfolio Tracking')
    parser.add_argument('--config', default='config/config.json', help='Path to configuration file')
    parser.add_argument('--paper', action='store_true', help='Enable paper trading mode')
    parser.add_argument('--live', action='store_true', help='Enable live trading mode')
    parser.add_argument('--report-only', action='store_true', help='Generate reports without trading')
    args = parser.parse_args()
    
    # Load environment variables
    load_dotenv()
    
    # Get API credentials
    app_key = os.environ.get('ICICI_APP_KEY')
    secret_key = os.environ.get('ICICI_SECRET_KEY')
    api_session = os.environ.get('ICICI_API_SESSION')
    
    if not all([app_key, secret_key, api_session]):
        logger.error("Missing API credentials. Please set ICICI_APP_KEY, ICICI_SECRET_KEY, and ICICI_API_SESSION in your .env file.")
        return
    
    try:
        # Create the bot with tracking
        bot = ORBTradingBotWithTracking(app_key, secret_key, api_session, config_path=args.config)
        
        # Set trading mode
        if args.paper:
            bot.update_config({"paper_trading": True})
            logger.info("Paper trading mode enabled")
        elif args.live:
            bot.update_config({"paper_trading": False})
            logger.info("Live trading mode enabled")
        
        # Report only mode
        if args.report_only:
            logger.info("Report-only mode: generating reports without trading")
            
            # Portfolio tracker
            tracker = PortfolioTracker(db_path="data/portfolio.db")
            
            # Generate reports
            daily_report = tracker.generate_daily_report()
            weekly_report = tracker.generate_weekly_report()
            monthly_report = tracker.generate_monthly_report()
            
            # Create visualizations
            date_str = datetime.now().strftime("%Y-%m-%d")
            reports_dir = f"reports/{date_str}"
            os.makedirs(reports_dir, exist_ok=True)
            
            tracker.visualize_portfolio(f"{reports_dir}/portfolio.png")
            tracker.visualize_performance("daily", f"{reports_dir}/daily_performance.png")
            tracker.visualize_performance("weekly", f"{reports_dir}/weekly_performance.png")
            tracker.visualize_performance("monthly", f"{reports_dir}/monthly_performance.png")
            tracker.visualize_trade_distribution(save_path=f"{reports_dir}/trade_distribution.png")
            
            logger.info(f"Reports generated in {reports_dir}")
            return
        
        # Start the bot
        bot.start()
        
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Error running bot: {e}")
        raise

if __name__ == "__main__":
    main()