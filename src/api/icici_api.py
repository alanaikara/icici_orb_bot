import logging
from datetime import datetime, timedelta
from breeze_connect import BreezeConnect

logger = logging.getLogger("ICICI_ORB_Bot")

class ICICIDirectAPI:
    def __init__(self, app_key, secret_key, session_token=None):
        """
        Initialize the ICICI Direct API client using Breeze Connect library
        """
        self.app_key = app_key
        self.secret_key = secret_key
        self.session_token = session_token
        self.breeze = BreezeConnect(api_key=app_key)
        self.is_connected = False
    
    def get_customer_details(self, api_session, app_key):
        """Generate session using Breeze Connect and get customer details"""
        try:
            # Generate session
            self.breeze.generate_session(api_secret=self.secret_key, session_token=api_session)
            
            # Set the session token for future API calls
            self.session_token = api_session
            self.is_connected = True
            
            # Get customer details from Breeze
            customer_details = {
                'Success': {
                    'session_token': api_session,
                    'idirect_user_name': 'User', # This would be populated with actual user details in real response
                    'idirect_userid': 'User',
                    'exg_status': self.breeze.get_funds().get('Success', {}).get('segments_allowed', {})
                },
                'Status': 200,
                'Error': None
            }
            return customer_details
        except Exception as e:
            logger.error(f"Failed to generate session: {e}")
            return {'Success': None, 'Status': 500, 'Error': str(e)}
    
    def connect_websocket(self):
        """Connect to Breeze websocket for real-time data"""
        try:
            if self.is_connected:
                self.breeze.ws_connect()
                logger.info("Connected to Breeze websocket")
                return True
            else:
                logger.error("Cannot connect to websocket: Not authenticated")
                return False
        except Exception as e:
            logger.error(f"Failed to connect to websocket: {e}")
            return False
            
    def disconnect_websocket(self):
        """Disconnect from Breeze websocket"""
        try:
            if self.is_connected:
                self.breeze.ws_disconnect()
                logger.info("Disconnected from Breeze websocket")
                return True
            return False
        except Exception as e:
            logger.error(f"Error disconnecting from websocket: {e}")
            return False
    
    def get_historical_data(self, params):
        """Get historical data for a specific security using Breeze API"""
        try:
            if not self.is_connected:
                return {'Success': None, 'Status': 401, 'Error': 'Not authenticated'}
                
            # Convert params to Breeze format
            interval = params.get('interval', '1minute')
            from_date = params.get('from_date', '')
            to_date = params.get('to_date', '')
            stock_code = params.get('stock_code', '')
            exchange_code = params.get('exchange_code', '')
            product_type = params.get('product_type', 'cash').lower()
            
            # Convert ISO8601 format to Breeze format if needed
            if from_date and to_date:
                # Format is "2025-02-03T09:20:00.000Z" - convert to Breeze format
                from_date = from_date
                to_date = to_date
            
            # Handle expiry date, right, strike price for derivatives
            expiry_date = params.get('expiry_date', '')
            right = params.get('right', 'others')
            strike_price = params.get('strike_price', '0')
            
            # Call the appropriate Breeze API
            response = self.breeze.get_historical_data(
                interval=interval,
                from_date=from_date,
                to_date=to_date,
                stock_code=stock_code,
                exchange_code=exchange_code,
                product_type=product_type,
                expiry_date=expiry_date,
                right=right,
                strike_price=strike_price
            )
            
            return response
        except Exception as e:
            logger.error(f"Error fetching historical data: {e}")
            return {'Success': None, 'Status': 500, 'Error': str(e)}
    
    def get_quotes(self, stock_code, exchange_code, expiry_date=None, product_type=None, right=None, strike_price=None):
        """Get current market quotes for a stock using Breeze API"""
        try:
            if not self.is_connected:
                return {'Success': None, 'Status': 401, 'Error': 'Not authenticated'}
                
            # Set default values for options
            if not product_type:
                product_type = "cash"
            if not right:
                right = "others"
            if not strike_price:
                strike_price = "0"
                
            # Call Breeze API
            response = self.breeze.get_quotes(
                stock_code=stock_code,
                exchange_code=exchange_code,
                expiry_date=expiry_date or "",
                product_type=product_type,
                right=right,
                strike_price=strike_price
            )
            
            return response
        except Exception as e:
            logger.error(f"Error fetching quotes: {e}")
            return {'Success': None, 'Status': 500, 'Error': str(e)}
    
    def place_order(self, order_details):
        """Place a new order using Breeze API"""
        try:
            if not self.is_connected:
                return {'Success': None, 'Status': 401, 'Error': 'Not authenticated'}
                
            # Extract order parameters
            stock_code = order_details.get('stock_code', '')
            exchange_code = order_details.get('exchange_code', '')
            product = order_details.get('product', 'cash')
            action = order_details.get('action', '')
            order_type = order_details.get('order_type', 'limit')
            quantity = order_details.get('quantity', '1')
            price = order_details.get('price', '0')
            validity = order_details.get('validity', 'day')
            stoploss = order_details.get('stoploss', '')
            disclosed_quantity = order_details.get('disclosed_quantity', '0')
            expiry_date = order_details.get('expiry_date', '')
            right = order_details.get('right', '')
            strike_price = order_details.get('strike_price', '')
            
            # Call Breeze API
            response = self.breeze.place_order(
                stock_code=stock_code,
                exchange_code=exchange_code,
                product=product,
                action=action,
                order_type=order_type,
                quantity=quantity,
                price=price,
                validity=validity,
                stoploss=stoploss,
                disclosed_quantity=disclosed_quantity,
                expiry_date=expiry_date,
                right=right,
                strike_price=strike_price
            )
            
            return response
        except Exception as e:
            logger.error(f"Error placing order: {e}")
            return {'Success': None, 'Status': 500, 'Error': str(e)}
    
    def cancel_order(self, order_id, exchange_code):
        """Cancel an existing order using Breeze API"""
        try:
            if not self.is_connected:
                return {'Success': None, 'Status': 401, 'Error': 'Not authenticated'}
                
            # Call Breeze API
            response = self.breeze.cancel_order(
                order_id=order_id,
                exchange_code=exchange_code
            )
            
            return response
        except Exception as e:
            logger.error(f"Error cancelling order: {e}")
            return {'Success': None, 'Status': 500, 'Error': str(e)}
    
    def get_order_detail(self, order_id, exchange_code):
        """Get details of a specific order using Breeze API"""
        try:
            if not self.is_connected:
                return {'Success': None, 'Status': 401, 'Error': 'Not authenticated'}
                
            # Call Breeze API
            response = self.breeze.get_order_detail(
                exchange_code=exchange_code,
                order_id=order_id
            )
            
            return response
        except Exception as e:
            logger.error(f"Error fetching order details: {e}")
            return {'Success': None, 'Status': 500, 'Error': str(e)}
    
    def get_order_list(self, exchange_code, from_date=None, to_date=None):
        """Get list of orders using Breeze API"""
        try:
            if not self.is_connected:
                return {'Success': None, 'Status': 401, 'Error': 'Not authenticated'}
            
            # Use current date if not provided
            if not from_date or not to_date:
                current_date = datetime.now()
                from_date = from_date or current_date.strftime("%Y-%m-%dT00:00:00.000Z")
                to_date = to_date or current_date.strftime("%Y-%m-%dT23:59:59.000Z")
                
            # Call Breeze API
            response = self.breeze.get_order_list(
                exchange_code=exchange_code,
                from_date=from_date,
                to_date=to_date
            )
            
            return response
        except Exception as e:
            logger.error(f"Error fetching order list: {e}")
            return {'Success': None, 'Status': 500, 'Error': str(e)}
    
    def get_portfolio_holdings(self):
        """Get portfolio holdings using Breeze API"""
        try:
            if not self.is_connected:
                return {'Success': None, 'Status': 401, 'Error': 'Not authenticated'}
                
            # Call Breeze API
            response = self.breeze.get_demat_holdings()
            
            return response
        except Exception as e:
            logger.error(f"Error fetching portfolio holdings: {e}")
            return {'Success': None, 'Status': 500, 'Error': str(e)}
    
    def get_portfolio_positions(self):
        """Get portfolio positions using Breeze API"""
        try:
            if not self.is_connected:
                return {'Success': None, 'Status': 401, 'Error': 'Not authenticated'}
                
            # Call Breeze API
            response = self.breeze.get_portfolio_positions()
            
            return response
        except Exception as e:
            logger.error(f"Error fetching portfolio positions: {e}")
            return {'Success': None, 'Status': 500, 'Error': str(e)}
    
    def get_funds(self):
        """Get funds information using Breeze API"""
        try:
            if not self.is_connected:
                return {'Success': None, 'Status': 401, 'Error': 'Not authenticated'}
                
            # Call Breeze API
            response = self.breeze.get_funds()
            
            return response
        except Exception as e:
            logger.error(f"Error fetching funds: {e}")
            return {'Success': None, 'Status': 500, 'Error': str(e)}