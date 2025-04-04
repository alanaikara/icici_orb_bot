import http.client
import json
import hashlib
from datetime import datetime
import logging

logger = logging.getLogger("ICICI_ORB_Bot")

class ICICIDirectAPI:
    def __init__(self, app_key, secret_key, session_token=None):
        """
        Initialize the ICICI Direct API client
        """
        self.app_key = app_key
        self.secret_key = secret_key
        self.session_token = session_token
        self.base_url = "api.icicidirect.com"
    
    def _generate_headers(self, payload=""):
        """Generate headers with proper authentication for API requests"""
        time_stamp = datetime.utcnow().isoformat()[:19] + '.000Z'
        checksum = hashlib.sha256((time_stamp + payload + self.secret_key).encode("utf-8")).hexdigest()
        
        headers = {
            'Content-Type': 'application/json',
            'X-Checksum': 'token ' + checksum,
            'X-Timestamp': time_stamp,
            'X-AppKey': self.app_key
        }
        
        if self.session_token:
            headers['X-SessionToken'] = self.session_token
            
        return headers
    
    def make_request(self, method, endpoint, payload=None):
        """Make a request to the ICICI Direct API"""
        conn = http.client.HTTPSConnection(self.base_url)
        
        payload_str = json.dumps(payload) if payload else ""
        headers = self._generate_headers(payload_str)
        
        conn.request(method, endpoint, payload_str, headers)
        
        response = conn.getresponse()
        data = response.read().decode("utf-8")
        
        conn.close()
        
        try:
            return json.loads(data)
        except json.JSONDecodeError:
            return {"error": "Invalid JSON response", "raw_response": data}
    
    def get_customer_details(self, api_session, app_key):
        """Get customer details including session token"""
        payload = json.dumps({
            "SessionToken": api_session,
            "AppKey": app_key
        })
        
        headers = {
            "Content-Type": "application/json"
        }
        
        conn = http.client.HTTPSConnection(self.base_url)
        conn.request("GET", "/breezeapi/api/v1/customerdetails", payload, headers)
        res = conn.getresponse()
        data = res.read().decode("utf-8")
        conn.close()
        
        try:
            response = json.loads(data)
            if 'Success' in response and 'session_token' in response['Success']:
                self.session_token = response['Success']['session_token']
                return response
            else:
                logger.error(f"Failed to get session token: {response}")
                return response
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON response from customer details API: {data}")
            return {"error": "Invalid JSON response", "raw_response": data}
    
    def get_historical_data(self, params):
        """Get historical data for a specific security"""
        payload = json.dumps(params)
        return self.make_request("GET", "/breezeapi/api/v1/historicalcharts", payload)
    
    def get_quotes(self, stock_code, exchange_code, expiry_date=None, product_type=None, right=None, strike_price=None):
        """Get current market quotes for a stock"""
        payload = {
            "stock_code": stock_code,
            "exchange_code": exchange_code
        }
        
        if expiry_date:
            payload["expiry_date"] = expiry_date
        if product_type:
            payload["product_type"] = product_type
        if right:
            payload["right"] = right
        if strike_price:
            payload["strike_price"] = strike_price
            
        return self.make_request("GET", "/breezeapi/api/v1/quotes", json.dumps(payload))
    
    def place_order(self, order_details):
        """Place a new order"""
        return self.make_request("POST", "/breezeapi/api/v1/order", order_details)
    
    def cancel_order(self, order_id, exchange_code):
        """Cancel an existing order"""
        payload = {
            "order_id": order_id,
            "exchange_code": exchange_code
        }
        return self.make_request("DELETE", "/breezeapi/api/v1/order", payload)