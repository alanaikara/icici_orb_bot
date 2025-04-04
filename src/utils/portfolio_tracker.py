import logging
import time
import pandas as pd
from datetime import datetime
from tabulate import tabulate

class PortfolioTracker:
    """
    Portfolio Tracker to monitor positions, holdings, margin, funds and calculate live PnL
    for the ICICI Direct ORB Trading Bot.
    """
    
    def __init__(self, api_client, config=None):
        """
        Initialize the Portfolio Tracker
        
        Args:
            api_client: ICICIDirectAPI instance
            config: Configuration dictionary (optional)
        """
        self.logger = logging.getLogger("ICICI_ORB_Bot.PortfolioTracker")
        self.api = api_client
        self.config = config or {}
        
        # Portfolio data
        self.positions = {}
        self.holdings = {}
        self.margin_data = {}
        self.funds_data = {}
        
        # PnL tracking
        self.daily_pnl = 0
        self.realized_pnl = 0
        self.unrealized_pnl = 0
        self.start_capital = 0
        self.current_capital = 0
        
        # Timestamp of last update
        self.last_update = None
        
        self.logger.info("Portfolio Tracker initialized")
    
    def update_all(self):
        """Update all portfolio data and calculate live PnL"""
        try:
            # Update all components
            self.update_positions()
            self.update_holdings()
            self.update_margin()
            self.update_funds()
            
            # Calculate PnL
            self.calculate_pnl()
            
            # Update timestamp
            self.last_update = datetime.now()
            
            # Log summary
            self.log_portfolio_summary()
            
            return True
        except Exception as e:
            self.logger.error(f"Error updating portfolio data: {e}")
            return False
    
    def update_positions(self):
        """Update current portfolio positions"""
        try:
            response = self.api.get_portfolio_positions()
            
            if 'Success' in response and response['Success']:
                self.positions = {}
                
                # Process positions data
                for position in response['Success']:
                    stock_code = position.get('stock_code', '')
                    if not stock_code:
                        # Sometimes the API returns positions with no stock code
                        # Try to get it from the underlying field
                        stock_code = position.get('underlying', '')
                        if not stock_code:
                            continue
                    
                    # Create a structured position record
                    self.positions[stock_code] = {
                        'segment': position.get('segment', ''),
                        'product_type': position.get('product_type', ''),
                        'exchange_code': position.get('exchange_code', ''),
                        'expiry_date': position.get('expiry_date', ''),
                        'right': position.get('right', ''),
                        'strike_price': float(position.get('strike_price', 0)),
                        'action': position.get('action', ''),  # BUY/SELL
                        'quantity': int(position.get('quantity', 0)),
                        'price': float(position.get('price', 0)),
                        'ltp': float(position.get('ltp', 0)),
                        'mtm': 0.0,  # Mark to Market - will calculate
                    }
                    
                    # Calculate Mark to Market
                    position_obj = self.positions[stock_code]
                    entry_price = position_obj['price']
                    current_price = position_obj['ltp']
                    quantity = position_obj['quantity']
                    
                    if position_obj['action'].upper() == 'BUY':
                        position_obj['mtm'] = (current_price - entry_price) * quantity
                    elif position_obj['action'].upper() == 'SELL':
                        position_obj['mtm'] = (entry_price - current_price) * quantity
                
                self.logger.info(f"Updated positions data: {len(self.positions)} active positions")
                return True
            else:
                error_msg = response.get('Error', 'Unknown error')
                self.logger.error(f"Error fetching positions: {error_msg}")
                return False
        
        except Exception as e:
            self.logger.error(f"Exception in update_positions: {e}")
            return False
    
    def update_holdings(self):
        """Update current demat holdings"""
        try:
            response = self.api.get_demat_holdings()
            
            if 'Success' in response and response['Success']:
                self.holdings = {}
                
                # Process holdings data
                for holding in response['Success']:
                    stock_code = holding.get('stock_code', '')
                    if not stock_code:
                        continue
                    
                    # Create a structured holding record
                    self.holdings[stock_code] = {
                        'isin': holding.get('stock_ISIN', ''),
                        'quantity': int(holding.get('quantity', 0)),
                        'available_quantity': int(holding.get('demat_avail_quantity', 0)),
                        'blocked_quantity': int(holding.get('blocked_quantity', 0)),
                        'purchase_price': 0.0,  # Not provided by the API directly
                        'current_price': 0.0,   # Will fetch separately
                        'current_value': 0.0,   # Will calculate
                        'profit_loss': 0.0      # Will calculate
                    }
                
                # Fetch current prices for holdings
                self._update_holdings_prices()
                
                self.logger.info(f"Updated holdings data: {len(self.holdings)} holdings")
                return True
            else:
                error_msg = response.get('Error', 'Unknown error')
                self.logger.error(f"Error fetching holdings: {error_msg}")
                return False
        
        except Exception as e:
            self.logger.error(f"Exception in update_holdings: {e}")
            return False
    
    def _update_holdings_prices(self):
        """Update current prices for all holdings"""
        for stock_code in self.holdings:
            try:
                # Get current price from quotes API
                quotes_response = self.api.get_quotes(
                    stock_code=stock_code,
                    exchange_code=self.config.get("exchange_code", "NSE")
                )
                
                if 'Success' in quotes_response and quotes_response['Success']:
                    current_price = float(quotes_response['Success'][0]['ltp'])
                    self.holdings[stock_code]['current_price'] = current_price
                    
                    # Calculate current value
                    quantity = self.holdings[stock_code]['quantity']
                    self.holdings[stock_code]['current_value'] = current_price * quantity
                    
                # Add a small delay to avoid hitting rate limits
                time.sleep(0.05)
                
            except Exception as e:
                self.logger.warning(f"Error updating price for {stock_code}: {e}")
    
    def update_margin(self):
        """Update margin information"""
        try:
            exchange_code = self.config.get("exchange_code", "NSE")
            response = self.api.get_margin(exchange_code=exchange_code)
            
            if 'Success' in response and response['Success']:
                self.margin_data = response['Success']
                self.logger.info("Updated margin data successfully")
                return True
            else:
                error_msg = response.get('Error', 'Unknown error')
                self.logger.error(f"Error fetching margin: {error_msg}")
                return False
        
        except Exception as e:
            self.logger.error(f"Exception in update_margin: {e}")
            return False
    
    def update_funds(self):
        """Update funds information"""
        try:
            response = self.api.get_funds()
            
            if 'Success' in response and response['Success']:
                self.funds_data = response['Success']
                
                # Initialize start capital if not already set
                if self.start_capital == 0:
                    self.start_capital = float(self.funds_data.get('total_bank_balance', 0))
                
                # Update current capital
                self.current_capital = float(self.funds_data.get('total_bank_balance', 0))
                
                self.logger.info("Updated funds data successfully")
                return True
            else:
                error_msg = response.get('Error', 'Unknown error')
                self.logger.error(f"Error fetching funds: {error_msg}")
                return False
        
        except Exception as e:
            self.logger.error(f"Exception in update_funds: {e}")
            return False
    
    def calculate_pnl(self):
        """Calculate realized and unrealized PnL"""
        try:
            # Calculate unrealized PnL from positions
            position_pnl = sum(position['mtm'] for position in self.positions.values())
            
            # Calculate unrealized PnL from holdings
            # Note: This requires purchase price which isn't directly provided by the API
            # We would need to track this separately or estimate it
            
            # Set PnL values
            self.unrealized_pnl = position_pnl
            
            # Calculate daily PnL (change in capital + unrealized PnL)
            if self.start_capital > 0:
                capital_change = self.current_capital - self.start_capital
                self.daily_pnl = capital_change + self.unrealized_pnl
            
            self.logger.info(f"PnL calculated - Daily: ₹{self.daily_pnl:.2f}, Unrealized: ₹{self.unrealized_pnl:.2f}")
            return True
        
        except Exception as e:
            self.logger.error(f"Exception in calculate_pnl: {e}")
            return False
    
    def log_portfolio_summary(self):
        """Log a summary of the portfolio"""
        try:
            # Create summary tables
            positions_table = []
            for stock, data in self.positions.items():
                positions_table.append([
                    stock, 
                    data['action'], 
                    data['quantity'], 
                    f"₹{data['price']:.2f}", 
                    f"₹{data['ltp']:.2f}", 
                    f"₹{data['mtm']:.2f}"
                ])
            
            # Log positions
            if positions_table:
                self.logger.info("\n=== OPEN POSITIONS ===\n" + 
                    tabulate(positions_table, 
                            headers=["Stock", "Side", "Qty", "Entry", "Current", "P&L"], 
                            tablefmt="grid"))
            else:
                self.logger.info("No open positions")
            
            # Log PnL summary
            self.logger.info(f"\n=== PnL SUMMARY ===\n" +
                            f"Daily P&L: ₹{self.daily_pnl:.2f}\n" +
                            f"Unrealized P&L: ₹{self.unrealized_pnl:.2f}\n" +
                            f"Realized P&L: ₹{self.realized_pnl:.2f}\n" +
                            f"Current Capital: ₹{self.current_capital:.2f}")
            
            return True
                            
        except Exception as e:
            self.logger.error(f"Exception in log_portfolio_summary: {e}")
            return False
    
    def get_portfolio_dataframe(self):
        """Get portfolio positions as a pandas DataFrame"""
        data = []
        for stock, position in self.positions.items():
            data.append({
                'Stock': stock,
                'Side': position['action'],
                'Quantity': position['quantity'],
                'Entry Price': position['price'],
                'Current Price': position['ltp'],
                'P&L': position['mtm'],
                'Exchange': position['exchange_code'],
                'Product': position['product_type'],
                'Segment': position['segment']
            })
        
        if not data:
            return pd.DataFrame()
        
        df = pd.DataFrame(data)
        return df
    
    def get_pnl_summary(self):
        """Get a summary of PnL information"""
        return {
            'daily_pnl': self.daily_pnl,
            'unrealized_pnl': self.unrealized_pnl,
            'realized_pnl': self.realized_pnl,
            'start_capital': self.start_capital,
            'current_capital': self.current_capital,
            'net_change': self.current_capital - self.start_capital,
            'net_change_percent': ((self.current_capital - self.start_capital) / self.start_capital * 100) if self.start_capital else 0,
            'last_update': self.last_update.strftime('%Y-%m-%d %H:%M:%S') if self.last_update else None
        }


# Example usage in the ORB Bot
def integrate_with_orb_bot():
    """Example of how to integrate the PortfolioTracker with your ORB bot"""
    from api.icici_api import ICICIDirectAPI
    
    # Initialize API client
    app_key = "your_app_key"
    secret_key = "your_secret_key"
    api_session = "your_api_session"
    
    api_client = ICICIDirectAPI(app_key, secret_key)
    api_client.get_customer_details(api_session, app_key)
    
    # Initialize portfolio tracker
    config = {
        "exchange_code": "NSE",
        "update_interval": 5 * 60  # Update every 5 minutes
    }
    
    tracker = PortfolioTracker(api_client, config)
    
    # Initial update
    tracker.update_all()
    
    # Schedule regular updates (example)
    import threading
    
    def schedule_updates():
        while True:
            tracker.update_all()
            time.sleep(config["update_interval"])
    
    # Start update thread
    update_thread = threading.Thread(target=schedule_updates)
    update_thread.daemon = True
    update_thread.start()
    
    # Now you can access tracker.get_pnl_summary() or tracker.get_portfolio_dataframe() anywhere in your bot


if __name__ == "__main__":
    # Example standalone usage
    from api.icici_api import ICICIDirectAPI
    import os
    from dotenv import load_dotenv
    
    # Load environment variables
    load_dotenv()
    
    # Set up logger
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Get API credentials
    app_key = os.environ.get('ICICI_APP_KEY')
    secret_key = os.environ.get('ICICI_SECRET_KEY')
    api_session = os.environ.get('ICICI_API_SESSION')
    
    if not all([app_key, secret_key, api_session]):
        logging.error("Missing API credentials. Please set ICICI_APP_KEY, ICICI_SECRET_KEY, and ICICI_API_SESSION in your .env file.")
        exit(1)
    
    # Initialize API client
    api_client = ICICIDirectAPI(app_key, secret_key)
    api_client.get_customer_details(api_session, app_key)
    
    # Initialize and use tracker
    tracker = PortfolioTracker(api_client)
    tracker.update_all()
    
    # Print summary
    print("\nPortfolio Summary:")
    print(f"Daily P&L: ₹{tracker.daily_pnl:.2f}")
    print(f"Unrealized P&L: ₹{tracker.unrealized_pnl:.2f}")
    print(f"Current Capital: ₹{tracker.current_capital:.2f}")
    
    # Print positions as DataFrame
    df = tracker.get_portfolio_dataframe()
    if not df.empty:
        print("\nOpen Positions:")
        print(df.to_string())
    else:
        print("\nNo open positions")