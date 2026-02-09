import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
import os
import logging

logger = logging.getLogger("ICICI_ORB_Bot")

class PortfolioDatabase:
    def __init__(self, db_path="data/portfolio.db"):
        """Initialize database connection"""
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        self.db_path = db_path
        self.conn = None
        self.cur = None
        self.initialize_database()
    
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()
    
    def connect(self):
        """Connect to the SQLite database"""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row  # Return results as dictionaries
        self.cur = self.conn.cursor()
        return self.conn
    
    def close(self):
        """Close the database connection"""
        if self.conn:
            self.conn.close()
            self.conn = None
            self.cur = None
    
    def execute(self, query, params=None):
        """Execute SQL query with parameters"""
        if not self.conn:
            self.connect()
        
        try:
            if params:
                self.cur.execute(query, params)
            else:
                self.cur.execute(query)
            return self.cur
        except sqlite3.Error as e:
            logger.error(f"Database error: {e}")
            raise
    
    def commit(self):
        """Commit changes to the database"""
        if self.conn:
            self.conn.commit()
    
    def initialize_database(self):
        """Create database tables if they don't exist"""
        try:
            self.connect()
            
            # Read schema from file
            schema_path = os.path.join(os.path.dirname(__file__), 'schema.sql')
            if os.path.exists(schema_path):
                with open(schema_path, 'r') as f:
                    schema_script = f.read()
                self.conn.executescript(schema_script)
            else:
                # Fallback schema creation if file doesn't exist
                self.create_tables()
            
            self.commit()
            logger.info("Database initialized successfully")
        except sqlite3.Error as e:
            logger.error(f"Error initializing database: {e}")
            raise
        finally:
            self.close()
    
    def create_tables(self):
        """Create database tables"""
        # Create trades table
        self.execute('''
        CREATE TABLE IF NOT EXISTS trades (
            trade_id INTEGER PRIMARY KEY AUTOINCREMENT,
            stock_code TEXT NOT NULL,
            exchange_code TEXT NOT NULL,
            action TEXT NOT NULL,
            entry_time TIMESTAMP NOT NULL,
            exit_time TIMESTAMP,
            entry_price REAL NOT NULL,
            exit_price REAL,
            quantity INTEGER NOT NULL,
            position_type TEXT NOT NULL,
            product_type TEXT NOT NULL,
            order_id TEXT,
            stop_loss REAL,
            target REAL,
            status TEXT NOT NULL,
            strategy TEXT NOT NULL,
            brokerage REAL,
            other_charges REAL,
            pnl REAL,
            notes TEXT
        )
        ''')
        
        # Create daily_summary table
        self.execute('''
        CREATE TABLE IF NOT EXISTS daily_summary (
            summary_id INTEGER PRIMARY KEY AUTOINCREMENT,
            date DATE UNIQUE NOT NULL,
            gross_pnl REAL NOT NULL,
            net_pnl REAL NOT NULL,
            total_trades INTEGER NOT NULL,
            winning_trades INTEGER NOT NULL,
            losing_trades INTEGER NOT NULL,
            brokerage_total REAL NOT NULL,
            other_charges_total REAL NOT NULL,
            max_profit_trade REAL,
            max_loss_trade REAL,
            capital_used REAL,
            notes TEXT
        )
        ''')
        
        # Create portfolio table
        self.execute('''
        CREATE TABLE IF NOT EXISTS portfolio (
            portfolio_id INTEGER PRIMARY KEY AUTOINCREMENT,
            stock_code TEXT NOT NULL,
            exchange_code TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            average_price REAL NOT NULL,
            current_price REAL,
            current_value REAL,
            unrealized_pnl REAL,
            realized_pnl REAL,
            last_updated TIMESTAMP NOT NULL,
            product_type TEXT NOT NULL
        )
        ''')
        
        # Create capital_history table
        self.execute('''
        CREATE TABLE IF NOT EXISTS capital_history (
            capital_id INTEGER PRIMARY KEY AUTOINCREMENT,
            date DATE NOT NULL,
            amount REAL NOT NULL,
            transaction_type TEXT NOT NULL,
            notes TEXT,
            balance_after REAL NOT NULL
        )
        ''')
        
        # Create performance_metrics table
        self.execute('''
        CREATE TABLE IF NOT EXISTS performance_metrics (
            metric_id INTEGER PRIMARY KEY AUTOINCREMENT,
            date DATE NOT NULL,
            strategy TEXT NOT NULL,
            win_rate REAL,
            profit_factor REAL,
            avg_profit_per_trade REAL,
            max_drawdown REAL,
            sharpe_ratio REAL,
            sortino_ratio REAL,
            total_trades INTEGER,
            period TEXT NOT NULL
        )
        ''')
        
        # Create indexes
        self.execute('CREATE INDEX IF NOT EXISTS idx_trades_date ON trades(entry_time)')
        self.execute('CREATE INDEX IF NOT EXISTS idx_trades_stock ON trades(stock_code)')
        self.execute('CREATE INDEX IF NOT EXISTS idx_portfolio_stock ON portfolio(stock_code)')
        self.execute('CREATE INDEX IF NOT EXISTS idx_daily_summary_date ON daily_summary(date)')
    
    #================ Trade Operations ================
    
    def record_trade_entry(self, stock_code, exchange_code, action, entry_price, 
                          quantity, position_type, product_type, order_id=None, 
                          stop_loss=None, target=None, strategy="ORB", notes=None):
        """Record a new trade entry in the database"""
        try:
            with self:
                entry_time = datetime.now()
                
                query = '''
                INSERT INTO trades (
                    stock_code, exchange_code, action, entry_time, entry_price, 
                    quantity, position_type, product_type, order_id, stop_loss, 
                    target, status, strategy, notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                '''
                
                params = (
                    stock_code, exchange_code, action, entry_time, entry_price,
                    quantity, position_type, product_type, order_id, stop_loss,
                    target, 'open', strategy, notes
                )
                
                self.execute(query, params)
                self.commit()
                
                # Get the trade_id of the inserted trade
                trade_id = self.cur.lastrowid
                
                # Update the portfolio
                self._update_portfolio_on_entry(stock_code, exchange_code, action, 
                                              entry_price, quantity, product_type)
                
                logger.info(f"Trade entry recorded for {stock_code}, ID: {trade_id}")
                return trade_id
                
        except Exception as e:
            logger.error(f"Error recording trade entry: {e}")
            raise
    
    def record_trade_exit(self, trade_id, exit_price, exit_time=None, 
                         brokerage=0.0, other_charges=0.0, notes=None):
        """Record the exit for an existing trade"""
        try:
            with self:
                # Get trade details
                trade = self.get_trade(trade_id)
                if not trade:
                    raise ValueError(f"Trade with ID {trade_id} not found")
                
                if trade['status'] != 'open':
                    raise ValueError(f"Trade with ID {trade_id} is already {trade['status']}")
                
                exit_time = exit_time or datetime.now()
                
                # Calculate P&L
                if trade['position_type'] == 'LONG':
                    pnl = (exit_price - trade['entry_price']) * trade['quantity']
                else:  # SHORT
                    pnl = (trade['entry_price'] - exit_price) * trade['quantity']
                
                # Subtract costs
                net_pnl = pnl - brokerage - other_charges
                
                # Update trade record
                query = '''
                UPDATE trades 
                SET exit_time = ?, exit_price = ?, status = ?, 
                    brokerage = ?, other_charges = ?, pnl = ?, notes = ?
                WHERE trade_id = ?
                '''
                
                params = (
                    exit_time, exit_price, 'closed', 
                    brokerage, other_charges, net_pnl, 
                    notes, trade_id
                )
                
                self.execute(query, params)
                self.commit()
                
                # Update the portfolio
                self._update_portfolio_on_exit(trade['stock_code'], trade['exchange_code'], 
                                             trade['position_type'], trade['quantity'], 
                                             exit_price, net_pnl, trade['product_type'])
                
                # Update daily summary
                self._update_daily_summary(exit_time.date(), pnl, net_pnl, 
                                         brokerage, other_charges)
                
                logger.info(f"Trade exit recorded for ID: {trade_id}, P&L: {net_pnl}")
                return net_pnl
                
        except Exception as e:
            logger.error(f"Error recording trade exit: {e}")
            raise
    
    def get_trade(self, trade_id):
        """Get a specific trade by ID"""
        try:
            with self:
                query = "SELECT * FROM trades WHERE trade_id = ?"
                self.execute(query, (trade_id,))
                row = self.cur.fetchone()
                
                if row:
                    return dict(row)
                return None
                
        except Exception as e:
            logger.error(f"Error getting trade: {e}")
            raise
    
    def get_open_trades(self):
        """Get all open trades"""
        try:
            with self:
                query = "SELECT * FROM trades WHERE status = 'open'"
                self.execute(query)
                rows = self.cur.fetchall()
                
                return [dict(row) for row in rows]
                
        except Exception as e:
            logger.error(f"Error getting open trades: {e}")
            raise
    
    def get_trades_by_date(self, start_date, end_date=None):
        """Get trades between specified dates"""
        try:
            with self:
                if end_date is None:
                    end_date = start_date
                
                # Convert to datetime objects if strings are provided
                if isinstance(start_date, str):
                    start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
                if isinstance(end_date, str):
                    end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
                
                # Add one day to end_date to include trades on the end date
                end_date = end_date + timedelta(days=1)
                
                query = """
                SELECT * FROM trades 
                WHERE entry_time >= ? AND entry_time < ?
                ORDER BY entry_time DESC
                """
                
                self.execute(query, (start_date, end_date))
                rows = self.cur.fetchall()
                
                return [dict(row) for row in rows]
                
        except Exception as e:
            logger.error(f"Error getting trades by date: {e}")
            raise
    
    def get_trades_by_stock(self, stock_code):
        """Get all trades for a specific stock"""
        try:
            with self:
                query = "SELECT * FROM trades WHERE stock_code = ? ORDER BY entry_time DESC"
                self.execute(query, (stock_code,))
                rows = self.cur.fetchall()
                
                return [dict(row) for row in rows]
                
        except Exception as e:
            logger.error(f"Error getting trades by stock: {e}")
            raise
    
    #================ Portfolio Operations ================
    
    def _update_portfolio_on_entry(self, stock_code, exchange_code, action, 
                                  price, quantity, product_type):
        """Update portfolio when a new trade is entered"""
        try:
            # Check if stock already exists in portfolio
            query = """
            SELECT * FROM portfolio 
            WHERE stock_code = ? AND exchange_code = ? AND product_type = ?
            """
            
            self.execute(query, (stock_code, exchange_code, product_type))
            existing_position = self.cur.fetchone()
            
            now = datetime.now()
            
            if existing_position:
                existing = dict(existing_position)
                new_quantity = existing['quantity']
                new_average = existing['average_price']
                
                if action.lower() == 'buy':
                    # Calculate new average price
                    total_value = existing['average_price'] * existing['quantity']
                    new_value = price * quantity
                    new_quantity = existing['quantity'] + quantity
                    new_average = (total_value + new_value) / new_quantity
                else:  # sell
                    # For short positions or reducing long positions
                    new_quantity = existing['quantity'] - quantity
                
                # Update portfolio
                if new_quantity != 0:
                    update_query = """
                    UPDATE portfolio
                    SET quantity = ?, average_price = ?, current_price = ?, 
                        current_value = ?, last_updated = ?
                    WHERE portfolio_id = ?
                    """
                    
                    current_value = new_quantity * price
                    
                    self.execute(update_query, (
                        new_quantity, new_average, price, 
                        current_value, now, existing['portfolio_id']
                    ))
                else:
                    # If quantity becomes zero, remove from portfolio
                    self.execute(
                        "DELETE FROM portfolio WHERE portfolio_id = ?", 
                        (existing['portfolio_id'],)
                    )
            else:
                # New position
                if action.lower() == 'buy':
                    # Long position
                    insert_query = """
                    INSERT INTO portfolio (
                        stock_code, exchange_code, quantity, average_price,
                        current_price, current_value, unrealized_pnl,
                        realized_pnl, last_updated, product_type
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                    
                    self.execute(insert_query, (
                        stock_code, exchange_code, quantity, price,
                        price, price * quantity, 0.0,
                        0.0, now, product_type
                    ))
                else:
                    # Short position - negative quantity
                    insert_query = """
                    INSERT INTO portfolio (
                        stock_code, exchange_code, quantity, average_price,
                        current_price, current_value, unrealized_pnl,
                        realized_pnl, last_updated, product_type
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                    
                    self.execute(insert_query, (
                        stock_code, exchange_code, -quantity, price,
                        price, price * quantity, 0.0,
                        0.0, now, product_type
                    ))
            
            self.commit()
                
        except Exception as e:
            logger.error(f"Error updating portfolio on entry: {e}")
            raise
    
    def _update_portfolio_on_exit(self, stock_code, exchange_code, position_type, 
                                quantity, exit_price, realized_pnl, product_type):
        """Update portfolio when a trade is exited"""
        try:
            # Get current portfolio position
            query = """
            SELECT * FROM portfolio 
            WHERE stock_code = ? AND exchange_code = ? AND product_type = ?
            """
            
            self.execute(query, (stock_code, exchange_code, product_type))
            position = self.cur.fetchone()
            
            if not position:
                logger.warning(f"No portfolio position found for {stock_code} while recording exit")
                return
            
            position = dict(position)
            now = datetime.now()
            
            # Calculate new quantity
            if position_type == 'LONG':
                new_quantity = position['quantity'] - quantity
            else:  # SHORT
                new_quantity = position['quantity'] + quantity
            
            # Update realized P&L
            new_realized_pnl = position['realized_pnl'] + realized_pnl
            
            if new_quantity == 0:
                # Position closed, remove from portfolio
                self.execute(
                    "DELETE FROM portfolio WHERE portfolio_id = ?", 
                    (position['portfolio_id'],)
                )
            else:
                # Update position
                update_query = """
                UPDATE portfolio
                SET quantity = ?, current_price = ?, current_value = ?,
                    realized_pnl = ?, last_updated = ?
                WHERE portfolio_id = ?
                """
                
                current_value = new_quantity * exit_price
                
                self.execute(update_query, (
                    new_quantity, exit_price, current_value,
                    new_realized_pnl, now, position['portfolio_id']
                ))
            
            self.commit()
                
        except Exception as e:
            logger.error(f"Error updating portfolio on exit: {e}")
            raise
    
    def update_portfolio_prices(self, stock_data):
        """Update current prices and values in the portfolio"""
        try:
            with self:
                for stock_code, price_data in stock_data.items():
                    current_price = price_data.get('last_price', 0)
                    
                    # Skip if price is invalid
                    if not current_price or current_price <= 0:
                        continue
                    
                    # Update portfolio for this stock
                    query = """
                    UPDATE portfolio
                    SET current_price = ?,
                        current_value = quantity * ?,
                        unrealized_pnl = quantity * (? - average_price),
                        last_updated = ?
                    WHERE stock_code = ?
                    """
                    
                    now = datetime.now()
                    
                    self.execute(query, (
                        current_price, current_price, 
                        current_price, now, stock_code
                    ))
                
                self.commit()
                logger.info(f"Updated portfolio prices for {len(stock_data)} stocks")
                
        except Exception as e:
            logger.error(f"Error updating portfolio prices: {e}")
            raise
    
    def get_portfolio(self):
        """Get the current portfolio holdings"""
        try:
            with self:
                query = "SELECT * FROM portfolio"
                self.execute(query)
                rows = self.cur.fetchall()
                
                return [dict(row) for row in rows]
                
        except Exception as e:
            logger.error(f"Error getting portfolio: {e}")
            raise
    
    def get_portfolio_summary(self):
        """Get a summary of the portfolio with total values"""
        try:
            with self:
                portfolio = self.get_portfolio()
                
                if not portfolio:
                    return {
                        'total_positions': 0,
                        'total_value': 0,
                        'unrealized_pnl': 0,
                        'realized_pnl': 0,
                        'total_pnl': 0
                    }
                
                total_value = sum(p['current_value'] for p in portfolio)
                unrealized_pnl = sum(p['unrealized_pnl'] for p in portfolio)
                realized_pnl = sum(p['realized_pnl'] for p in portfolio)
                
                return {
                    'total_positions': len(portfolio),
                    'total_value': total_value,
                    'unrealized_pnl': unrealized_pnl,
                    'realized_pnl': realized_pnl,
                    'total_pnl': unrealized_pnl + realized_pnl
                }
                
        except Exception as e:
            logger.error(f"Error getting portfolio summary: {e}")
            raise
    
    #================ Daily Summary Operations ================
    
    def _update_daily_summary(self, summary_date, gross_pnl, net_pnl, 
                            brokerage, other_charges):
        """Update the daily summary with new trade information"""
        try:
            # Convert to date object if string is provided
            if isinstance(summary_date, str):
                summary_date = datetime.strptime(summary_date, "%Y-%m-%d").date()
            
            # Check if a summary exists for this date
            query = "SELECT * FROM daily_summary WHERE date = ?"
            self.execute(query, (summary_date,))
            existing = self.cur.fetchone()
            
            if existing:
                # Update existing summary
                existing = dict(existing)
                
                winning = 1 if gross_pnl > 0 else 0
                losing = 1 if gross_pnl < 0 else 0
                
                update_query = """
                UPDATE daily_summary
                SET gross_pnl = gross_pnl + ?,
                    net_pnl = net_pnl + ?,
                    total_trades = total_trades + 1,
                    winning_trades = winning_trades + ?,
                    losing_trades = losing_trades + ?,
                    brokerage_total = brokerage_total + ?,
                    other_charges_total = other_charges_total + ?,
                    max_profit_trade = CASE 
                                       WHEN ? > 0 AND ? > max_profit_trade THEN ?
                                       ELSE max_profit_trade
                                       END,
                    max_loss_trade = CASE
                                    WHEN ? < 0 AND ? < max_loss_trade THEN ?
                                    ELSE max_loss_trade
                                    END
                WHERE summary_id = ?
                """
                
                self.execute(update_query, (
                    gross_pnl, net_pnl, winning, losing, brokerage, other_charges,
                    gross_pnl, gross_pnl, gross_pnl,
                    gross_pnl, gross_pnl, gross_pnl,
                    existing['summary_id']
                ))
            else:
                # Create new summary
                insert_query = """
                INSERT INTO daily_summary (
                    date, gross_pnl, net_pnl, total_trades, 
                    winning_trades, losing_trades, brokerage_total,
                    other_charges_total, max_profit_trade, max_loss_trade
                ) VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?)
                """
                
                winning = 1 if gross_pnl > 0 else 0
                losing = 1 if gross_pnl < 0 else 0
                max_profit = gross_pnl if gross_pnl > 0 else 0
                max_loss = gross_pnl if gross_pnl < 0 else 0
                
                self.execute(insert_query, (
                    summary_date, gross_pnl, net_pnl, winning, losing,
                    brokerage, other_charges, max_profit, max_loss
                ))
            
            self.commit()
                
        except Exception as e:
            logger.error(f"Error updating daily summary: {e}")
            raise
    
    def get_daily_summary(self, start_date, end_date=None):
        """Get daily summary data between specified dates"""
        try:
            with self:
                if end_date is None:
                    end_date = start_date
                
                # Convert to date objects if strings are provided
                if isinstance(start_date, str):
                    start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
                if isinstance(end_date, str):
                    end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
                
                query = """
                SELECT * FROM daily_summary 
                WHERE date >= ? AND date <= ?
                ORDER BY date
                """
                
                self.execute(query, (start_date, end_date))
                rows = self.cur.fetchall()
                
                return [dict(row) for row in rows]
                
        except Exception as e:
            logger.error(f"Error getting daily summary: {e}")
            raise
    
    def get_period_summary(self, start_date, end_date=None):
        """Get a summary of performance over a specified period"""
        try:
            with self:
                daily_summaries = self.get_daily_summary(start_date, end_date)
                
                if not daily_summaries:
                    return {
                        'period_start': start_date,
                        'period_end': end_date or start_date,
                        'total_trades': 0,
                        'total_gross_pnl': 0,
                        'total_net_pnl': 0,
                        'total_winning_trades': 0,
                        'total_losing_trades': 0,
                        'win_rate': 0,
                        'average_win': 0,
                        'average_loss': 0,
                        'profit_factor': 0,
                        'max_profit_trade': 0,
                        'max_loss_trade': 0,
                        'total_brokerage': 0,
                        'total_charges': 0
                    }
                
                total_trades = sum(day['total_trades'] for day in daily_summaries)
                total_gross_pnl = sum(day['gross_pnl'] for day in daily_summaries)
                total_net_pnl = sum(day['net_pnl'] for day in daily_summaries)
                total_winning = sum(day['winning_trades'] for day in daily_summaries)
                total_losing = sum(day['losing_trades'] for day in daily_summaries)
                total_brokerage = sum(day['brokerage_total'] for day in daily_summaries)
                total_charges = sum(day['other_charges_total'] for day in daily_summaries)
                
                # Find max values
                max_profit = max([day['max_profit_trade'] for day in daily_summaries if day['max_profit_trade'] is not None], default=0)
                max_loss = min([day['max_loss_trade'] for day in daily_summaries if day['max_loss_trade'] is not None], default=0)
                
                # Calculate performance metrics
                win_rate = total_winning / total_trades if total_trades > 0 else 0
                
                # Get average win and loss amounts
                average_win = 0
                average_loss = 0
                profit_factor = 0
                
                # Calculate these metrics from trade data
                trade_data = self.get_trades_by_date(start_date, end_date)
                
                if trade_data:
                    winning_trades = [t for t in trade_data if t['pnl'] is not None and t['pnl'] > 0]
                    losing_trades = [t for t in trade_data if t['pnl'] is not None and t['pnl'] < 0]
                    
                    average_win = sum(t['pnl'] for t in winning_trades) / len(winning_trades) if winning_trades else 0
                    average_loss = sum(t['pnl'] for t in losing_trades) / len(losing_trades) if losing_trades else 0
                    
                    total_wins = sum(t['pnl'] for t in winning_trades) if winning_trades else 0
                    total_losses = abs(sum(t['pnl'] for t in losing_trades)) if losing_trades else 0
                    
                    profit_factor = total_wins / total_losses if total_losses > 0 else float('inf') if total_wins > 0 else 0
                
                return {
                    'period_start': start_date,
                    'period_end': end_date or start_date,
                    'total_trades': total_trades,
                    'total_gross_pnl': total_gross_pnl,
                    'total_net_pnl': total_net_pnl,
                    'total_winning_trades': total_winning,
                    'total_losing_trades': total_losing,
                    'win_rate': win_rate,
                    'average_win': average_win,
                    'average_loss': average_loss,
                    'profit_factor': profit_factor,
                    'max_profit_trade': max_profit,
                    'max_loss_trade': max_loss,
                    'total_brokerage': total_brokerage,
                    'total_charges': total_charges
                }
                
        except Exception as e:
            logger.error(f"Error getting period summary: {e}")
            raise
    
    #================ Capital Operations ================
    
    def add_capital(self, amount, date=None, notes=None):
        """Add capital to the account"""
        try:
            with self:
                date = date or datetime.now().date()
                
                # Get current balance
                query = """
                SELECT balance_after FROM capital_history 
                ORDER BY capital_id DESC LIMIT 1
                """
                
                self.execute(query)
                last_record = self.cur.fetchone()
                
                current_balance = last_record['balance_after'] if last_record else 0
                new_balance = current_balance + amount
                
                # Insert new capital record
                insert_query = """
                INSERT INTO capital_history (
                    date, amount, transaction_type, notes, balance_after
                ) VALUES (?, ?, ?, ?, ?)
                """
                
                self.execute(insert_query, (
                    date, amount, 'deposit', notes, new_balance
                ))
                
                self.commit()
                logger.info(f"Capital added: {amount}, New balance: {new_balance}")
                
                return new_balance
                
        except Exception as e:
            logger.error(f"Error adding capital: {e}")
            raise
    
    def withdraw_capital(self, amount, date=None, notes=None):
        """Withdraw capital from the account"""
        try:
            with self:
                date = date or datetime.now().date()
                
                # Get current balance
                query = """
                SELECT balance_after FROM capital_history 
                ORDER BY capital_id DESC LIMIT 1
                """
                
                self.execute(query)
                last_record = self.cur.fetchone()
                
                if not last_record:
                    raise ValueError("No capital record found to withdraw from")
                
                current_balance = last_record['balance_after']
                
                if amount > current_balance:
                    raise ValueError(f"Withdrawal amount {amount} exceeds available balance {current_balance}")
                
                new_balance = current_balance - amount
                
                # Insert new capital record
                insert_query = """
                INSERT INTO capital_history (
                    date, amount, transaction_type, notes, balance_after
                ) VALUES (?, ?, ?, ?, ?)
                """
                
                self.execute(insert_query, (
                    date, -amount, 'withdrawal', notes, new_balance
                ))
                
                self.commit()
                logger.info(f"Capital withdrawn: {amount}, New balance: {new_balance}")
                
                return new_balance
                
        except Exception as e:
            logger.error(f"Error withdrawing capital: {e}")
            raise
    
    def get_current_capital(self):
        """Get the current capital balance"""
        try:
            with self:
                query = """
                SELECT balance_after FROM capital_history 
                ORDER BY capital_id DESC LIMIT 1
                """
                
                self.execute(query)
                last_record = self.cur.fetchone()
                
                return last_record['balance_after'] if last_record else 0
                
        except Exception as e:
            logger.error(f"Error getting current capital: {e}")
            raise
    
    def get_capital_history(self, start_date=None, end_date=None):
        """Get capital history between specified dates"""
        try:
            with self:
                if start_date:
                    if end_date is None:
                        end_date = datetime.now().date()
                    
                    # Convert to date objects if strings are provided
                    if isinstance(start_date, str):
                        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
                    if isinstance(end_date, str):
                        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
                    
                    query = """
                    SELECT * FROM capital_history 
                    WHERE date >= ? AND date <= ?
                    ORDER BY date
                    """
                    
                    self.execute(query, (start_date, end_date))
                else:
                    query = "SELECT * FROM capital_history ORDER BY date"
                    self.execute(query)
                
                rows = self.cur.fetchall()
                
                return [dict(row) for row in rows]
                
        except Exception as e:
            logger.error(f"Error getting capital history: {e}")
            raise
    
    #================ Performance Metrics Operations ================
    
    def calculate_performance_metrics(self, date=None, period="daily"):
        """Calculate and store performance metrics"""
        try:
            with self:
                current_date = date or datetime.now().date()
                
                # Define date ranges based on period
                if period == "daily":
                    start_date = current_date
                    end_date = current_date
                elif period == "weekly":
                    # Get start of week (Monday)
                    start_date = current_date - timedelta(days=current_date.weekday())
                    end_date = current_date
                elif period == "monthly":
                    # Get start of month
                    start_date = current_date.replace(day=1)
                    end_date = current_date
                elif period == "yearly":
                    # Get start of year
                    start_date = current_date.replace(month=1, day=1)
                    end_date = current_date
                else:
                    raise ValueError(f"Invalid period: {period}")
                
                # Get trade data for the period
                trades = self.get_trades_by_date(start_date, end_date)
                
                # Get daily summaries for the period
                daily_summaries = self.get_daily_summary(start_date, end_date)
                
                if not trades or not daily_summaries:
                    logger.info(f"No trade data for period {period} on {current_date}. Skipping metrics calculation.")
                    return None
                
                # Calculate metrics
                total_trades = sum(day['total_trades'] for day in daily_summaries)
                winning_trades = sum(day['winning_trades'] for day in daily_summaries)
                
                # Win rate
                win_rate = winning_trades / total_trades if total_trades > 0 else 0
                
                # Profit factor and average trade
                closed_trades = [t for t in trades if t['status'] == 'closed' and t['pnl'] is not None]
                
                if closed_trades:
                    winning_trades_list = [t for t in closed_trades if t['pnl'] > 0]
                    losing_trades_list = [t for t in closed_trades if t['pnl'] < 0]
                    
                    total_profit = sum(t['pnl'] for t in winning_trades_list) if winning_trades_list else 0
                    total_loss = abs(sum(t['pnl'] for t in losing_trades_list)) if losing_trades_list else 0
                    
                    profit_factor = total_profit / total_loss if total_loss > 0 else float('inf') if total_profit > 0 else 0
                    avg_profit_per_trade = sum(t['pnl'] for t in closed_trades) / len(closed_trades)
                else:
                    profit_factor = 0
                    avg_profit_per_trade = 0
                
                # Calculate drawdown
                # This would require storing equity curve data, which is beyond the scope here
                # Instead, use a simple approximation based on daily summary data
                daily_pnls = [day['net_pnl'] for day in daily_summaries]
                max_drawdown = self._calculate_max_drawdown(daily_pnls)
                
                # Sharpe and Sortino ratios require more data (like daily returns)
                # For simplicity, we'll skip these for now
                sharpe_ratio = None
                sortino_ratio = None
                
                # Store the metrics
                query = """
                INSERT INTO performance_metrics (
                    date, strategy, win_rate, profit_factor, 
                    avg_profit_per_trade, max_drawdown, sharpe_ratio,
                    sortino_ratio, total_trades, period
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                
                # Use 'All' as strategy for now, could be more specific later
                self.execute(query, (
                    current_date, 'All', win_rate, profit_factor,
                    avg_profit_per_trade, max_drawdown, sharpe_ratio,
                    sortino_ratio, total_trades, period
                ))
                
                self.commit()
                
                # Return the calculated metrics
                return {
                    'date': current_date,
                    'period': period,
                    'win_rate': win_rate,
                    'profit_factor': profit_factor,
                    'avg_profit_per_trade': avg_profit_per_trade,
                    'max_drawdown': max_drawdown,
                    'sharpe_ratio': sharpe_ratio,
                    'sortino_ratio': sortino_ratio,
                    'total_trades': total_trades
                }
                
        except Exception as e:
            logger.error(f"Error calculating performance metrics: {e}")
            raise
    
    def _calculate_max_drawdown(self, values):
        """Calculate maximum drawdown from a list of values"""
        peak = values[0] if values else 0
        max_dd = 0
        
        for value in values:
            if value > peak:
                peak = value
            dd = (peak - value) / peak if peak != 0 else 0
            if dd > max_dd:
                max_dd = dd
        
        return max_dd
    
    def get_performance_metrics(self, period="daily", start_date=None, end_date=None):
        """Get performance metrics for a specific period"""
        try:
            with self:
                if start_date:
                    if end_date is None:
                        end_date = datetime.now().date()
                    
                    # Convert to date objects if strings are provided
                    if isinstance(start_date, str):
                        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
                    if isinstance(end_date, str):
                        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
                    
                    query = """
                    SELECT * FROM performance_metrics 
                    WHERE period = ? AND date >= ? AND date <= ?
                    ORDER BY date
                    """
                    
                    self.execute(query, (period, start_date, end_date))
                else:
                    query = """
                    SELECT * FROM performance_metrics 
                    WHERE period = ?
                    ORDER BY date DESC
                    """
                    
                    self.execute(query, (period,))
                
                rows = self.cur.fetchall()
                
                return [dict(row) for row in rows]
                
        except Exception as e:
            logger.error(f"Error getting performance metrics: {e}")
            raise
    
    #================ Export/Import Operations ================
    
    def export_to_csv(self, table_name, output_file=None):
        """Export a table to CSV"""
        try:
            with self:
                if table_name not in ['trades', 'daily_summary', 'portfolio', 
                                    'capital_history', 'performance_metrics']:
                    raise ValueError(f"Invalid table name: {table_name}")
                
                query = f"SELECT * FROM {table_name}"
                self.execute(query)
                rows = self.cur.fetchall()
                
                if not rows:
                    logger.warning(f"No data found in table {table_name}")
                    return False
                
                # Convert to DataFrame
                df = pd.DataFrame([dict(row) for row in rows])
                
                # Generate output filename if not provided
                if not output_file:
                    output_dir = "exports"
                    os.makedirs(output_dir, exist_ok=True)
                    
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    output_file = f"{output_dir}/{table_name}_{timestamp}.csv"
                
                # Export to CSV
                df.to_csv(output_file, index=False)
                logger.info(f"Table {table_name} exported to {output_file}")
                
                return output_file
                
        except Exception as e:
            logger.error(f"Error exporting to CSV: {e}")
            raise
    
    def import_from_csv(self, table_name, input_file):
        """Import data from CSV to a table"""
        try:
            with self:
                if table_name not in ['trades', 'daily_summary', 'portfolio', 
                                    'capital_history', 'performance_metrics']:
                    raise ValueError(f"Invalid table name: {table_name}")
                
                if not os.path.exists(input_file):
                    raise FileNotFoundError(f"File not found: {input_file}")
                
                # Read CSV
                df = pd.read_csv(input_file)
                
                # Get table columns
                self.execute(f"PRAGMA table_info({table_name})")
                table_columns = [col[1] for col in self.cur.fetchall()]
                
                # Filter data to include only valid columns
                valid_columns = [col for col in df.columns if col in table_columns]
                df = df[valid_columns]
                
                # Convert to list of dictionaries
                records = df.to_dict('records')
                
                if not records:
                    logger.warning(f"No valid data found in {input_file}")
                    return 0
                
                # Insert data
                column_names = ', '.join(valid_columns)
                placeholders = ', '.join(['?' for _ in valid_columns])
                
                query = f"""
                INSERT INTO {table_name} ({column_names})
                VALUES ({placeholders})
                """
                
                count = 0
                for record in records:
                    values = [record[col] for col in valid_columns]
                    self.execute(query, values)
                    count += 1
                
                self.commit()
                logger.info(f"Imported {count} records into {table_name}")
                
                return count
                
        except Exception as e:
            logger.error(f"Error importing from CSV: {e}")
            raise