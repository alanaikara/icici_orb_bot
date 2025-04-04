from datetime import datetime
import logging

logger = logging.getLogger("ICICI_ORB_Bot")

class RiskManager:
    def __init__(self, config, stocks_data):
        self.config = config
        self.stocks_data = stocks_data
        self.daily_trade_stats = {
            "trades_taken": 0,
            "daily_pnl": 0,
            "max_drawdown": 0,
            "initial_capital": config["capital"],
            "start_time": datetime.now()
        }
    
    def check_position_sizing(self, stock_code, quantity, entry_price, stop_loss):
        """Check if position size is within risk parameters"""
        # Calculate risk per trade
        risk_per_share = abs(entry_price - stop_loss)
        risk_amount = risk_per_share * quantity
        
        # Check against max risk per trade
        if risk_amount > self.config.get("max_risk_per_trade", 1000):
            logger.warning(f"Risk of ₹{risk_amount:.2f} exceeds max risk per trade of ₹{self.config['max_risk_per_trade']}")
            return False
            
        # Check against max position size as percentage of capital
        position_value = entry_price * quantity
        max_position_value = self.config.get("max_position_size_percent", 10) * self.config["capital"] / 100
        if position_value > max_position_value:
            logger.warning(f"Position value of ₹{position_value:.2f} exceeds max position size of ₹{max_position_value:.2f}")
            return False
            
        return True
    
    def update_pnl(self, stock_code, exit_price):
        """Update P&L after a trade"""
        stock_data = self.stocks_data[stock_code]
        
        if stock_data["position"] is None or stock_data["entry_price"] is None:
            return 0
            
        # Calculate P&L
        entry_price = stock_data["entry_price"]
        quantity = stock_data["quantity"]
        
        if stock_data["position"] == "LONG":
            pnl = (exit_price - entry_price) * quantity
        else:  # SHORT
            pnl = (entry_price - exit_price) * quantity
            
        # Subtract trading costs
        brokerage = entry_price * quantity * self.config.get("brokerage_rate", 0.0001) * 2  # Entry and exit
        stt = exit_price * quantity * self.config.get("stt_rate", 0.00025)  # STT charged on sell side only
        
        net_pnl = pnl - brokerage - stt
        
        # Update stats
        self.daily_trade_stats["trades_taken"] += 1
        self.daily_trade_stats["daily_pnl"] += net_pnl
        
        # Update max drawdown if needed
        if net_pnl < 0 and abs(net_pnl) > self.daily_trade_stats["max_drawdown"]:
            self.daily_trade_stats["max_drawdown"] = abs(net_pnl)
            
        logger.info(f"{stock_code} trade closed - P&L: ₹{net_pnl:.2f}")
        return net_pnl
    
    def check_daily_risk_limits(self):
        """Check if we've hit any daily risk limits that should stop trading"""
        # Check max daily loss
        max_daily_loss = self.config.get("max_daily_loss", 5000)
        if self.daily_trade_stats["daily_pnl"] < -max_daily_loss:
            logger.warning(f"Daily loss limit of ₹{max_daily_loss} reached. Daily P&L: ₹{self.daily_trade_stats['daily_pnl']:.2f}")
            return False
            
        # Check max trades per day
        max_trades = self.config.get("max_trades_per_day", 10)
        if self.daily_trade_stats["trades_taken"] >= max_trades:
            logger.warning(f"Max trades per day ({max_trades}) reached.")
            return False
            
        return True