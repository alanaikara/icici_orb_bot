import time
import json
import threading
import os
import pandas as pd
from queue import Queue
from datetime import datetime, timedelta
import logging

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
            error_msg = "Failed to initialize bot: Could not get session token"
            logger.error(error_msg)
            if customer_details and 'Error' in customer_details:
                logger.error(f"API Error: {customer_details['Error']}")
            raise Exception(error_msg)
            
        logger.info(f"Session token obtained successfully: {self.api.session_token[:10]}...")
        
        self.config_path = config_path
        self.load_config()
        
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
    
    def calculate_opening_range(self, stock_code):
        """Calculate opening range for a stock"""
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
        
        # Check current market data
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
                    # Check with risk manager if position size is acceptable
                    if not self.risk_manager.check_position_sizing(stock_code, quantity, current_price, stop_loss):
                        logger.warning(f"{stock_code} - Risk check failed for LONG entry")
                        return False
                    
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
                    # Check with risk manager if position size is acceptable
                    if not self.risk_manager.check_position_sizing(stock_code, quantity, current_price, stop_loss):
                        logger.warning(f"{stock_code} - Risk check failed for SHORT entry")
                        return False
                    
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
                if self.config["paper_trading"]:
                    # Simulate order in paper trading mode
                    order_id = f"paper_{order_type}_{stock_code}_{int(time.time())}"
                    logger.info(f"PAPER TRADING - {order_type} order for {stock_code}: {order_details}")
                    
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
                    
                    # Calculate PnL before resetting position data
                    if not self.config["paper_trading"]:
                        # For live trading, get current price from the broker
                        quotes_response = self.api.get_quotes(stock_code, self.config["exchange_code"])
                        if 'Success' in quotes_response and quotes_response['Success']:
                            exit_price = float(quotes_response['Success'][0]['ltp'])
                            self.risk_manager.update_pnl(stock_code, exit_price)
                    
                    # Reset position data
                    stock_data["position"] = None
                    stock_data["entry_price"] = None
                    stock_data["stop_loss"] = None
                    stock_data["quantity"] = 0
                    stock_data["order_id"] = None
                    stock_data["stop_loss_order_id"] = None
    
    def update_market_status(self):
        """Update market open/close status"""
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
            return
        
        self.update_market_status()
        
        if not self.market_open:
            return
        
        logger.info("Running trading cycle")
        
        # Check if we've hit any daily risk limits
        if not self.risk_manager.check_daily_risk_limits():
            logger.warning("Daily risk limits hit. Stopping trading for today.")
            self.trading_active = False
            return
        
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
        
        # Update last cycle time
        self.last_update_time = datetime.now()
    
    def start(self):
        """Start the trading bot"""
        logger.info("Starting ICICI Direct ORB Trading Bot")
        
        # Initialize for trading day
        if not self.initialize_trading_day():
            logger.info("Trading day initialization failed. Bot will not trade today.")
            return
        
        # Set up a timer to run the trading cycle every minute
        trading_cycle_interval = 60  # seconds
        
        # Run the first cycle immediately
        logger.info("Running first trading cycle")
        self.run_trading_cycle()
        
        try:
            # Keep running until end of day or manual stop
            logger.info("Entering main trading loop")
            while self.trading_active:
                time.sleep(trading_cycle_interval)
                self.run_trading_cycle()
        
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
            "last_update": self.last_update_time.strftime("%Y-%m-%d %H:%M:%S") if self.last_update_time else None,
            "monitored_stocks": len(self.config["stocks"]),
            "open_positions": sum(1 for stock_data in self.stocks_data.values() if stock_data["position"] is not None),
            "stocks_data": self.stocks_data
        }