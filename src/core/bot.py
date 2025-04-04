import time
import json
import threading
import os
import schedule
import pandas as pd
from queue import Queue
from datetime import datetime, timedelta
import logging
from utils.portfolio_tracker import PortfolioTracker



# Fix imports to be relative to the project structure
from api.icici_api import ICICIDirectAPI
from core.risk_manager import RiskManager

logger = logging.getLogger("ICICI_ORB_Bot")

class ORBTradingBot:
    def __init__(self, app_key, secret_key, api_session, config_path="config/config.json"):
        """Initialize the ORB Trading Bot"""
        self.api = ICICIDirectAPI(app_key, secret_key)
        
        # Get session token from customer details
        customer_details = self.api.get_customer_details(api_session, app_key)
        if not self.api.session_token:
            logger.error("Failed to initialize bot: Could not get session token")
            raise Exception("Failed to get session token")
            
        logger.info(f"Session token obtained successfully: {self.api.session_token[:10]}...")
        
        self.config_path = config_path
        self.load_config()

        self.portfolio_tracker = PortfolioTracker(self.api, self.config)
        
        # Trading state variables
        self.stocks_data = {}  # Store stock data, opening ranges, positions, etc.
        self.trading_active = False
        self.market_open = False
        self.last_update_time = None
        
        # Create a queue for order processing
        self.order_queue = Queue()
        
        # Start order processing thread
        self.order_thread = threading.Thread(target=self._process_orders)
        self.order_thread.daemon = True
        self.order_thread.start()
        
        # Initialize risk manager
        self.risk_manager = RiskManager(self.config, self.stocks_data)
        
        logger.info("ICICI Direct ORB Trading Bot initialized")
    
            
    def load_config(self):
        """Load bot configuration from file"""
        try:
            if os.path.exists(self.config_path):
                with open(self.config_path, 'r') as f:
                    self.config = json.load(f)
                logger.info(f"Configuration loaded from {self.config_path}")
            else:
                # Default configuration
                self.config = {
                    "stocks": ["RELIANCE", "HDFCBANK", "TCS", "INFY", "ICICIBANK"],
                    "capital": 100000,
                    "max_risk_per_trade": 1000,
                    "opening_range_minutes": 30,
                    "max_opening_range_percent": 1.0,
                    "trade_exit_time": "15:14:00",
                    "market_open_time": "09:15:00",
                    "market_close_time": "15:30:00",
                    "exchange_code": "NSE",
                    "product_type": "cash",
                    "brokerage_rate": 0.0001,
                    "stt_rate": 0.00025,
                    "disable_weekend_trading": True,
                    "paper_trading": True,
                    "order_validity": "day",
                    "test_mode": False,  # Default to false for normal operation
                }
                self.save_config()
                logger.info("Default configuration created")
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            raise
    
    def save_config(self):
        """Save bot configuration to file"""
        try:
            os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
            with open(self.config_path, 'w') as f:
                json.dump(self.config, f, indent=4)
            logger.info(f"Configuration saved to {self.config_path}")
        except Exception as e:
            logger.error(f"Error saving configuration: {e}")
    
    def update_config(self, new_config):
        """Update bot configuration"""
        self.config.update(new_config)
        self.save_config()
        logger.info("Configuration updated")
    
    def initialize_trading_day(self):
        """Initialize data for the trading day"""
        # Check for test mode - skip time/day checks if in test mode
        if self.config.get("test_mode", False):
            logger.info("Test mode active: ignoring market hours and weekend restrictions")
            # Reset trading state
            self.stocks_data = {}
            self.trading_active = True
            self.market_open = True  # Force market to be considered open
            
            # Initialize data for each stock
            self._initialize_stock_data()
            
            # For test mode, simulate opening ranges with random values
            self._simulate_opening_ranges()
            
            logger.info(f"Test data initialized for {len(self.config['stocks'])} stocks")
            return True
            
        # Normal operation checks
        # Skip if weekend and configured to disable weekend trading
        current_day = datetime.now().weekday()
        if self.config["disable_weekend_trading"] and current_day >= 5:  # 5, 6 = Saturday, Sunday
            logger.info("Weekend trading disabled. Bot will not trade today.")
            return False
        
        # Reset trading state
        self.stocks_data = {}
        self.trading_active = True
        self.market_open = False
        
        # Initialize data for each stock
        self._initialize_stock_data()
        
        logger.info(f"Trading day initialized for {len(self.config['stocks'])} stocks")
        return True
    
    def _initialize_stock_data(self):
        """Initialize data structure for each stock"""
        for stock in self.config["stocks"]:
            self.stocks_data[stock] = {
                "opening_range_high": None,
                "opening_range_low": None,
                "opening_range_percent": None,
                "position": None,
                "entry_price": None,
                "stop_loss": None,
                "quantity": 0,
                "order_id": None,
                "stop_loss_order_id": None,
                "opening_range_calculated": False
            }
    
    def _simulate_opening_ranges(self):
        """Simulate opening ranges for test mode"""
        import random
        
        for stock in self.config["stocks"]:
            # Generate random base price between 100 and 1000
            base_price = random.uniform(100, 1000)
            
            # Generate random high and low with a reasonable range
            range_percent = random.uniform(0.5, 1.5)  # Between 0.5% and 1.5%
            range_amount = base_price * (range_percent / 100)
            
            low = base_price - (range_amount / 2)
            high = base_price + (range_amount / 2)
            
            # Store the simulated opening range
            self.stocks_data[stock]["opening_range_high"] = high
            self.stocks_data[stock]["opening_range_low"] = low
            self.stocks_data[stock]["opening_range_percent"] = range_percent
            self.stocks_data[stock]["opening_range_calculated"] = True
            
            logger.info(f"Test mode: Simulated opening range for {stock}: High={high:.2f}, Low={low:.2f}, Range={range_percent:.2f}%")
    
    def calculate_opening_range(self, stock_code):
        """Calculate opening range for a stock"""
        # In test mode, opening ranges are already simulated
        if self.config.get("test_mode", False):
            return True
            
        try:
            # Get current date string
            today = datetime.now().strftime("%Y-%m-%d")
            
            # Define time range for opening range calculation
            from_date = f"{today}T09:15:00.000Z"  # Market open
            opening_range_end_time = (datetime.strptime(f"{today}T09:15:00.000Z", "%Y-%m-%dT%H:%M:%S.000Z") + 
                                    timedelta(minutes=self.config["opening_range_minutes"]))
            to_date = opening_range_end_time.strftime("%Y-%m-%dT%H:%M:%S.000Z")
            
            # Fetch historical data for opening range period
            params = {
                "interval": "1minute",
                "from_date": from_date,
                "to_date": to_date,
                "stock_code": stock_code,
                "exchange_code": self.config["exchange_code"],
                "product_type": self.config["product_type"]
            }
            
            data = self.api.get_historical_data(params)
            
            if 'Success' in data and data['Success']:
                # Convert to DataFrame
                df = pd.DataFrame(data['Success'])
                
                # Convert string values to float
                for col in ['open', 'high', 'low', 'close', 'volume']:
                    if col in df.columns:
                        df[col] = df[col].astype(float)
                
                # Calculate high and low of opening range
                opening_range_high = df['high'].max()
                opening_range_low = df['low'].min()
                
                # Calculate average price during the opening range for percentage calculation
                opening_range_avg_price = (opening_range_high + opening_range_low) / 2
                
                # Calculate range as a percentage of the average price
                opening_range_percent = ((opening_range_high - opening_range_low) / opening_range_avg_price) * 100
                
                # Store opening range data
                self.stocks_data[stock_code]["opening_range_high"] = opening_range_high
                self.stocks_data[stock_code]["opening_range_low"] = opening_range_low
                self.stocks_data[stock_code]["opening_range_percent"] = opening_range_percent
                self.stocks_data[stock_code]["opening_range_calculated"] = True
                
                logger.info(f"Opening range calculated for {stock_code}: High={opening_range_high}, Low={opening_range_low}, Range={opening_range_percent:.2f}%")
                return True
            else:
                error_msg = data.get('Error', 'Unknown error')
                logger.error(f"Error calculating opening range for {stock_code}: {error_msg}")
                return False
                
        except Exception as e:
            logger.error(f"Exception when calculating opening range for {stock_code}: {e}")
            return False
    
    def check_entry_conditions(self, stock_code):
        """Check if entry conditions are met for a stock"""
        stock_data = self.stocks_data[stock_code]
        
        # Skip if opening range is not calculated or already in a position
        if not stock_data["opening_range_calculated"] or stock_data["position"] is not None:
            return False
        
        # Skip if opening range is too wide
        if stock_data["opening_range_percent"] > self.config["max_opening_range_percent"]:
            logger.info(f"{stock_code} - Opening range {stock_data['opening_range_percent']:.2f}% too wide (>{self.config['max_opening_range_percent']}%). No trade.")
            return False
        
        # In test mode, simulate market conditions for entry
        if self.config.get("test_mode", False):
            import random
            
            # Randomly decide whether to enter a position
            if random.random() < 0.3:  # 30% chance of entry
                # Randomly decide long or short
                is_long = random.random() < 0.5
                
                high = stock_data["opening_range_high"]
                low = stock_data["opening_range_low"]
                
                if is_long:
                    current_price = high * 1.01  # Price slightly above high
                    stop_loss = low
                    risk_per_share = current_price - stop_loss
                    
                    max_risk = self.config["max_risk_per_trade"]
                    quantity = min(int(max_risk / risk_per_share), int(self.config["capital"] / current_price))
                    
                    if quantity > 0:
                        # Set up long position
                        stock_data["position"] = "LONG"
                        stock_data["entry_price"] = current_price
                        stock_data["stop_loss"] = stop_loss
                        stock_data["quantity"] = quantity
                        
                        # Place the order
                        self.place_entry_order(stock_code, "buy", quantity, current_price)
                        
                        logger.info(f"TEST MODE: {stock_code} - LONG Entry at {current_price:.2f}, SL: {stop_loss:.2f}, Qty: {quantity}, Risk: ₹{risk_per_share * quantity:.2f}")
                        return True
                else:
                    current_price = low * 0.99  # Price slightly below low
                    stop_loss = high
                    risk_per_share = stop_loss - current_price
                    
                    max_risk = self.config["max_risk_per_trade"]
                    quantity = min(int(max_risk / risk_per_share), int(self.config["capital"] / current_price))
                    
                    if quantity > 0:
                        # Set up short position
                        stock_data["position"] = "SHORT"
                        stock_data["entry_price"] = current_price
                        stock_data["stop_loss"] = stop_loss
                        stock_data["quantity"] = quantity
                        
                        # Place the order
                        self.place_entry_order(stock_code, "sell", quantity, current_price)
                        
                        logger.info(f"TEST MODE: {stock_code} - SHORT Entry at {current_price:.2f}, SL: {stop_loss:.2f}, Qty: {quantity}, Risk: ₹{risk_per_share * quantity:.2f}")
                        return True
            return False
        
        # Normal operation - Check current market data
        quotes_response = self.api.get_quotes(stock_code, self.config["exchange_code"])
        
        if 'Success' in quotes_response and quotes_response['Success']:
            # Get the current price from the quote
            current_price = float(quotes_response['Success'][0]['ltp'])
            
            # Check for long entry - price above opening range high
            if current_price > stock_data["opening_range_high"]:
                # Calculate position size
                stop_loss = stock_data["opening_range_low"]
                risk_per_share = current_price - stop_loss
                
                if risk_per_share <= 0:
                    logger.warning(f"{stock_code} - No LONG entry: Invalid risk calculation")
                    return False
                
                max_risk = self.config["max_risk_per_trade"]
                quantity = min(int(max_risk / risk_per_share), int(self.config["capital"] / current_price))
                
                if quantity > 0:
                    # Set up long position
                    stock_data["position"] = "LONG"
                    stock_data["entry_price"] = current_price
                    stock_data["stop_loss"] = stop_loss
                    stock_data["quantity"] = quantity
                    
                    # Place the order
                    self.place_entry_order(stock_code, "buy", quantity, current_price)
                    
                    logger.info(f"{stock_code} - LONG Entry at {current_price}, SL: {stop_loss:.2f}, Qty: {quantity}, Risk: ₹{risk_per_share * quantity:.2f}")
                    return True
            
            # Check for short entry - price below opening range low
            elif current_price < stock_data["opening_range_low"]:
                # Calculate position size
                stop_loss = stock_data["opening_range_high"]
                risk_per_share = stop_loss - current_price
                
                if risk_per_share <= 0:
                    logger.warning(f"{stock_code} - No SHORT entry: Invalid risk calculation")
                    return False
                
                max_risk = self.config["max_risk_per_trade"]
                quantity = min(int(max_risk / risk_per_share), int(self.config["capital"] / current_price))
                
                if quantity > 0:
                    # Set up short position
                    stock_data["position"] = "SHORT"
                    stock_data["entry_price"] = current_price
                    stock_data["stop_loss"] = stop_loss
                    stock_data["quantity"] = quantity
                    
                    # Place the order
                    self.place_entry_order(stock_code, "sell", quantity, current_price)
                    
                    logger.info(f"{stock_code} - SHORT Entry at {current_price}, SL: {stop_loss:.2f}, Qty: {quantity}, Risk: ₹{risk_per_share * quantity:.2f}")
                    return True
        
        return False
    
    def place_entry_order(self, stock_code, action, quantity, price):
        """Place entry order for a stock"""
        # Prepare order details
        order_details = {
            "stock_code": stock_code,
            "exchange_code": self.config["exchange_code"],
            "product": self.config["product_type"],
            "action": action,
            "order_type": "market",
            "quantity": str(quantity),
            "price": str(price),
            "validity": self.config["order_validity"]
        }
        
        # Add order to queue
        self.order_queue.put(("ENTRY", stock_code, order_details))
    
    def place_stop_loss_order(self, stock_code):
        """Place stop loss order for a stock"""
        stock_data = self.stocks_data[stock_code]
        
        # Only place stop loss if in a position
        if stock_data["position"] is None:
            return False
        
        # Prepare order details
        action = "sell" if stock_data["position"] == "LONG" else "buy"
        
        order_details = {
            "stock_code": stock_code,
            "exchange_code": self.config["exchange_code"],
            "product": self.config["product_type"],
            "action": action,
            "order_type": "stoploss",
            "quantity": str(stock_data["quantity"]),
            "price": str(stock_data["stop_loss"]),
            "stoploss": str(stock_data["stop_loss"]),
            "validity": self.config["order_validity"]
        }
        
        # Add order to queue
        self.order_queue.put(("STOP_LOSS", stock_code, order_details))
    
    def place_exit_order(self, stock_code):
        """Place exit order for a stock"""
        stock_data = self.stocks_data[stock_code]
        
        # Only exit if in a position
        if stock_data["position"] is None:
            return False
        
        # Prepare order details
        action = "sell" if stock_data["position"] == "LONG" else "buy"
        
        order_details = {
            "stock_code": stock_code,
            "exchange_code": self.config["exchange_code"],
            "product": self.config["product_type"],
            "action": action,
            "order_type": "market",
            "quantity": str(stock_data["quantity"]),
            "price": "0",  # Market order
            "validity": self.config["order_validity"]
        }
        
        # Add order to queue
        self.order_queue.put(("EXIT", stock_code, order_details))
    
    def _process_orders(self):
        """Process orders from the queue"""
        while True:
            try:
                # Get order from queue
                order_type, stock_code, order_details = self.order_queue.get()
                
                # Process based on trading mode
                if self.config["paper_trading"] or self.config.get("test_mode", False):
                    # Simulate order in paper trading mode
                    order_id = f"paper_{order_type}_{stock_code}_{int(time.time())}"
                    mode = "TEST MODE" if self.config.get("test_mode", False) else "PAPER TRADING"
                    logger.info(f"{mode} - {order_type} order for {stock_code}: {order_details}")
                    
                    # Update stock data with simulated order ID
                    if order_type == "ENTRY":
                        self.stocks_data[stock_code]["order_id"] = order_id
                    elif order_type == "STOP_LOSS":
                        self.stocks_data[stock_code]["stop_loss_order_id"] = order_id
                    
                    # Simulate order success
                    time.sleep(0.5)  # Simulate API delay
                    
                else:
                    # Real trading mode - place actual order
                    response = self.api.place_order(order_details)
                    
                    if 'Success' in response and response['Success']:
                        order_id = response['Success'].get('order_id')
                        logger.info(f"Order placed successfully for {stock_code}: {order_type}, ID: {order_id}")
                        
                        # Update stock data with real order ID
                        if order_type == "ENTRY":
                            self.stocks_data[stock_code]["order_id"] = order_id
                        elif order_type == "STOP_LOSS":
                            self.stocks_data[stock_code]["stop_loss_order_id"] = order_id
                    else:
                        error_msg = response.get('Error', 'Unknown error')
                        logger.error(f"Error placing {order_type} order for {stock_code}: {error_msg}")
                
                # Mark as done
                self.order_queue.task_done()
                
            except Exception as e:
                logger.error(f"Error processing order: {e}")
                # Still mark as done to prevent queue blocking
                self.order_queue.task_done()
    
    def check_positions(self):
        """Check and manage open positions"""
        # For test mode, simulate random price movements and occasionally trigger stop loss
        if self.config.get("test_mode", False):
            import random
            
            for stock_code, stock_data in self.stocks_data.items():
                if stock_data["position"] is not None:
                    # Randomly decide if stop loss is hit (5% chance)
                    if random.random() < 0.05:
                        logger.info(f"TEST MODE: {stock_code} - Stop loss triggered at {stock_data['stop_loss']}")
                        self.place_exit_order(stock_code)
                        
                        # Reset position data
                        stock_data["position"] = None
                        stock_data["entry_price"] = None
                        stock_data["stop_loss"] = None
                        stock_data["quantity"] = 0
                        stock_data["order_id"] = None
                        stock_data["stop_loss_order_id"] = None
                        continue
                        
                    # Check if it's time for the time-based exit
                    current_time = datetime.now().strftime("%H:%M:%S")
                    exit_time = self.config["trade_exit_time"]
                    
                    if current_time >= exit_time:
                        logger.info(f"TEST MODE: {stock_code} - Time-based exit at {current_time}")
                        self.place_exit_order(stock_code)
                        
                        # Reset position data
                        stock_data["position"] = None
                        stock_data["entry_price"] = None
                        stock_data["stop_loss"] = None
                        stock_data["quantity"] = 0
                        stock_data["order_id"] = None
                        stock_data["stop_loss_order_id"] = None
            
            return
            
        # Normal operation - get current position data from the broker
        if not self.config["paper_trading"]:
            positions_response = self.api.get_portfolio_positions()
            
            # Process real positions data if available
            if 'Success' in positions_response and positions_response['Success']:
                for position in positions_response['Success']:
                    stock_code = position.get('stock_code')
                    if stock_code in self.stocks_data and self.stocks_data[stock_code]["position"] is not None:
                        # Update position data if needed
                        pass
        
        # For each stock in a position, check if exit conditions are met
        for stock_code, stock_data in self.stocks_data.items():
            if stock_data["position"] is not None:
                # Check if it's time for the time-based exit
                current_time = datetime.now().strftime("%H:%M:%S")
                exit_time = self.config["trade_exit_time"]
                
                if current_time >= exit_time:
                    logger.info(f"{stock_code} - Time-based exit at {current_time}")
                    self.place_exit_order(stock_code)
                    
                    # Cancel stop loss order if it exists
                    if stock_data["stop_loss_order_id"] is not None and not self.config["paper_trading"]:
                        self.api.cancel_order(stock_data["stop_loss_order_id"], self.config["exchange_code"])
                    
                    # Reset position data
                    stock_data["position"] = None
                    stock_data["entry_price"] = None
                    stock_data["stop_loss"] = None
                    stock_data["quantity"] = 0
                    stock_data["order_id"] = None
                    stock_data["stop_loss_order_id"] = None
    
    def update_market_status(self):
        """Update market open/close status"""
        # In test mode, market is always considered open
        if self.config.get("test_mode", False):
            if not self.market_open:
                self.market_open = True
                logger.info("TEST MODE: Market is now considered open")
            return
            
        # Normal operation - check actual market hours
        current_time = datetime.now().strftime("%H:%M:%S")
        market_open_time = self.config["market_open_time"]
        market_close_time = self.config["market_close_time"]
        
        if current_time >= market_open_time and current_time < market_close_time:
            if not self.market_open:
                self.market_open = True
                logger.info("Market is now open")
                
                # Calculate opening ranges after market opens
                opening_range_end_time = (datetime.strptime(market_open_time, "%H:%M:%S") + 
                                         timedelta(minutes=self.config["opening_range_minutes"]))
                opening_range_end_time_str = opening_range_end_time.strftime("%H:%M:%S")
                
                logger.info(f"Will calculate opening ranges at {opening_range_end_time_str}")
                
                # Schedule opening range calculations
                for stock_code in self.config["stocks"]:
                    current_time_dt = datetime.strptime(current_time, "%H:%M:%S")
                    if current_time_dt >= opening_range_end_time:
                        # If it's already past opening range end time, calculate now
                        self.calculate_opening_range(stock_code)
                    else:
                        # Otherwise, schedule for later
                        wait_seconds = (opening_range_end_time - current_time_dt).total_seconds()
                        threading.Timer(wait_seconds, self.calculate_opening_range, args=[stock_code]).start()
        
        elif current_time >= market_close_time:
            if self.market_open:
                self.market_open = False
                self.trading_active = False
                logger.info("Market is now closed")
                
                # Exit any remaining positions
                for stock_code, stock_data in self.stocks_data.items():
                    if stock_data["position"] is not None:
                        logger.info(f"{stock_code} - EOD exit")
                        self.place_exit_order(stock_code)
    
    def run_trading_cycle(self):
        """Run a single trading cycle"""
        if not self.trading_active:
            logger.info("Trading cycle skipped: trading not active")
            return
        
        self.update_market_status()
        
        if not self.market_open and not self.config.get("test_mode", False):
            logger.info("Trading cycle skipped: market not open")
            return
        
        logger.info("Running trading cycle")
        
        # Check for entry conditions for each stock
        for stock_code in self.config["stocks"]:
            stock_data = self.stocks_data[stock_code]
            
            # Skip if opening range calculation is not complete
            if not stock_data["opening_range_calculated"]:
                continue
            
            # Check for entry if not in a position
            if stock_data["position"] is None:
                self.check_entry_conditions(stock_code)
        
        # Check and manage open positions
        self.check_positions()

        # Update portfolio data
        self.portfolio_tracker.update_all()
        
        # You can log or use the PnL data
        pnl_summary = self.portfolio_tracker.get_pnl_summary()
        logger.info(f"Current P&L: ₹{pnl_summary['daily_pnl']:.2f}")
        
        # Update last cycle time
        self.last_update_time = datetime.now()
    
    def start(self):
        """Start the trading bot"""
        logger.info("Starting ICICI Direct ORB Trading Bot")
        
        # Initialize for trading day
        if not self.initialize_trading_day():
            logger.info("Trading day initialization failed. Bot will not trade today.")
            return
        
        # For test mode, add a shorter run time to avoid hanging
        if self.config.get("test_mode", False):
            max_run_minutes = 5  # Run for 5 minutes in test mode
            logger.info(f"Test mode: Bot will run for {max_run_minutes} minutes maximum")
        else:
            # Schedule the trading cycle to run every minute
            schedule.every(1).minutes.do(self.run_trading_cycle)
        
        # Run the first cycle immediately
        logger.info("Running first trading cycle")
        self.run_trading_cycle()
        
        # Start time for test mode timeout
        start_time = time.time()
        
        try:
            # Keep running until end of day or manual stop
            logger.info("Entering main trading loop")
            while self.trading_active:
                schedule.run_pending()
                
                # In test mode, run a cycle every 30 seconds instead of waiting for schedule
                if self.config.get("test_mode", False):
                    if (time.time() - start_time) % 30 < 1:  # Every ~30 seconds
                        logger.info("Test mode: Running additional trading cycle")
                        self.run_trading_cycle()
                    
                    # Exit after max_run_minutes in test mode to avoid hanging
                    if time.time() - start_time > max_run_minutes * 60:
                        logger.info(f"Test mode: Maximum run time of {max_run_minutes} minutes reached")
                        self.trading_active = False
                        break
                
                time.sleep(1)
        
        except KeyboardInterrupt:
            logger.info("Bot stopped manually")
        
        except Exception as e:
            logger.error(f"Error in main bot loop: {e}")
        
        finally:
            # Clean up and exit any open positions
            self.stop()

    def stop(self):
        """Stop the trading bot and clean up"""
        logger.info("Stopping ICICI Direct ORB Trading Bot")
        self.trading_active = False
        
        # Exit any open positions
        for stock_code, stock_data in self.stocks_data.items():
            if stock_data["position"] is not None:
                logger.info(f"Exiting position for {stock_code}")
                self.place_exit_order(stock_code)
        
        # Wait for order queue to be processed
        self.order_queue.join()
        
        logger.info("ICICI Direct ORB Trading Bot stopped")
    
    def get_status(self):
        """Get current bot status"""
        return {
            "trading_active": self.trading_active,
            "market_open": self.market_open,
            "paper_trading": self.config["paper_trading"],
            "test_mode": self.config.get("test_mode", False),
            "last_update": self.last_update_time.strftime("%Y-%m-%d %H:%M:%S") if self.last_update_time else None,
            "monitored_stocks": len(self.config["stocks"]),
            "open_positions": sum(1 for stock_data in self.stocks_data.values() if stock_data["position"] is not None),
            "stocks_data": self.stocks_data
        }