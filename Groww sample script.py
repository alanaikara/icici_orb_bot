# =====================
# This is a Sample script that places a Buy order, waits 10 seconds, and then places a Sell order.
# =====================

import time
from growwapi import GrowwAPI

# =====================
# STEP 1: Setup
# =====================

# === Setup Groww API ===
# Please use the "Generate API key" option on the API keys page to obtain the API key and Secret.

user_api_key  = "x"
user_secret = "x"


access_token = GrowwAPI.get_access_token(api_key = user_api_key, secret = user_secret) 
# Use access_token to initiate GrowwAPi
groww = GrowwAPI(access_token)
print("✅ Ready to Groww")

# =====================
# STEP 2: Place BUY Order
# =====================

trading_symbol = "IDEA"  #Vodafone Idea Ltd
quantity = 1    #Set the quantity you want to buy 
 

#Ensure you have sufficient funds in your Groww account for this order

try:
    # Place a MARKET BUY order
    print(f"Placing MARKET BUY order for {trading_symbol}")
    buy_order_id = groww.place_order(
        trading_symbol=trading_symbol, 
        quantity=quantity, 
        validity=groww.VALIDITY_DAY,
        exchange=groww.EXCHANGE_NSE, 
        segment=groww.SEGMENT_CASH,
        product=groww.PRODUCT_MIS,
        order_type=groww.ORDER_TYPE_MARKET,
        transaction_type=groww.TRANSACTION_TYPE_BUY
    )

    print(f"✅ BUY order placed for {trading_symbol}. Order ID: {buy_order_id['groww_order_id']}") 
    # This will print the order ID of the placed order

except Exception as e: 
    print(f"❌ Failed to place BUY order: {e}")
    exit(1)

# =====================
# STEP 3: Wait 10 seconds
# =====================
print("⏳ Waiting for 10 secs before placing SELL order...")
time.sleep(10)

# =====================
# STEP 4: Place SELL Order
# =====================

#Place a MARKET SELL order
try:
    print(f"Placing MARKET SELL order for {trading_symbol}")

    sell_order_id = groww.place_order(
        trading_symbol=trading_symbol,
        quantity=quantity,
        validity=groww.VALIDITY_DAY,
        exchange=groww.EXCHANGE_NSE,
        segment=groww.SEGMENT_CASH,
        product=groww.PRODUCT_MIS,
        order_type=groww.ORDER_TYPE_MARKET,
        transaction_type=groww.TRANSACTION_TYPE_SELL
    )

    print(f"✅ SELL order placed for {trading_symbol}. Order ID: {sell_order_id['groww_order_id']}")  
    # This will print the order ID of the placed order

except Exception as e:
    print(f"❌ Failed to place SELL order: {e}")