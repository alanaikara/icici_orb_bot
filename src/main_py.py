import os
import argparse
from dotenv import load_dotenv

from utils.logger import setup_logger
from core.bot import ORBTradingBot

def main():
    """Main entry point for the ICICI ORB Trading Bot"""
    
    # Set up command line arguments
    parser = argparse.ArgumentParser(description='ICICI Direct ORB Trading Bot')
    parser.add_argument('--config', default='config/config.json', help='Path to configuration file')
    parser.add_argument('--paper', action='store_true', help='Enable paper trading mode')
    parser.add_argument('--live', action='store_true', help='Enable live trading mode')
    args = parser.parse_args()
    
    # Set up logger
    logger = setup_logger(name="ICICI_ORB_Bot", log_file="logs/icici_orb_bot.log")
    
    # Load environment variables from .env file
    load_dotenv()
    
    # Get API credentials from environment variables
    app_key = os.environ.get('ICICI_APP_KEY')
    secret_key = os.environ.get('ICICI_SECRET_KEY')
    api_session = os.environ.get('ICICI_API_SESSION')
    
    if not all([app_key, secret_key, api_session]):
        logger.error("Missing API credentials. Please set ICICI_APP_KEY, ICICI_SECRET_KEY, and ICICI_API_SESSION in your .env file.")
        return
    
    try:
        # Create and initialize the bot
        bot = ORBTradingBot(app_key, secret_key, api_session, config_path=args.config)
        
        # Set trading mode based on command line arguments
        if args.paper:
            bot.update_config({"paper_trading": True})
            logger.info("Paper trading mode enabled")
        elif args.live:
            bot.update_config({"paper_trading": False})
            logger.info("Live trading mode enabled")
        
        # Start the bot
        bot.start()
        
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Error running bot: {e}")
        raise

if __name__ == "__main__":
    main()