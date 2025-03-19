# ICICI Direct Opening Range Breakout (ORB) Trading Bot

A Python-based automated trading bot that implements the Opening Range Breakout (ORB) strategy for stocks on ICICI Direct.

## Features

- Automated trading based on ORB strategy
- Paper trading mode for strategy testing
- Risk management with position sizing and stop-loss orders
- Real-time market data integration with ICICI Direct API
- Configurable parameters for trading strategy
- Comprehensive logging

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/icici-orb-bot.git
cd icici-orb-bot
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows, use: venv\Scripts\activate
```

3. Install the required dependencies:
```bash
pip install -r requirements.txt
```

4. Set up environment variables by copying the example file:
```bash
cp .env.example .env
```

5. Edit the `.env` file with your ICICI Direct API credentials:
```
ICICI_APP_KEY="your_app_key_here"
ICICI_SECRET_KEY="your_secret_key_here"
ICICI_API_SESSION="your_api_session_here"
```

## Configuration

The bot's configuration is stored in `config/config.json`. You can modify these settings to adjust the bot's behavior:

- `stocks`: List of stock symbols to monitor
- `capital`: Total capital available for trading
- `max_risk_per_trade`: Maximum risk amount per trade
- `opening_range_minutes`: Duration of opening range in minutes
- `max_opening_range_percent`: Maximum opening range as percentage
- `trade_exit_time`: Time to exit all trades
- `market_open_time`: Market opening time
- `market_close_time`: Market closing time
- `exchange_code`: Exchange code (e.g., "NSE")
- `product_type`: Product type (e.g., "cash")
- `paper_trading`: Enable/disable paper trading mode

## Usage

To start the bot in paper trading mode:
```bash
python -m src.main --paper
```

To start the bot in live trading mode:
```bash
python -m src.main --live
```

To use a custom configuration file:
```bash
python -m src.main --config path/to/your/config.json
```

## Project Structure

```
icici_orb_bot/
├── config/               # Configuration files
│   └── config.json       # Bot configuration
├── logs/                 # Log files directory
├── src/                  # Source code
│   ├── api/              # API integration
│   │   └── icici_api.py  # ICICI Direct API client
│   ├── core/             # Core bot functionality
│   │   ├── bot.py        # ORB Trading Bot implementation
│   │   └── risk_manager.py # Risk management component
│   ├── utils/            # Utility functions
│   │   └── logger.py     # Logging configuration
│   └── main.py           # Entry point for the bot
├── .env                  # Environment variables (not tracked by git)
├── .env.example          # Example environment variables
├── README.md             # Project documentation
└── requirements.txt      # Python dependencies
```

## Disclaimer

This bot is provided for educational and research purposes only. Trading in financial markets involves risk. Use this bot at your own risk. The authors and contributors are not responsible for any financial losses incurred from using this software.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
