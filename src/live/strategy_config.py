"""
Live strategy configuration.

Defines which stocks to trade and their individual parameters
(direction, target R, etc.) derived from the backtest results.
"""

from dataclasses import dataclass, field


@dataclass
class StockConfig:
    stock_code: str
    direction: str          # 'long_only' | 'short_only' | 'both'
    target_r: float = 1.5   # Risk:Reward target (1.5R best from backtest)
    active: bool = True


@dataclass
class StrategyConfig:
    # Opening range
    or_minutes: int = 30

    # Fibonacci
    fib_entry_pct: float = 0.618        # Entry retracement level
    fib_stop_pct: float = 0.786         # Stop loss reference level
    sl_buffer_pct: float = 0.001        # 0.1% buffer beyond 78.6%

    # Swing detection
    swing_confirm_pct: float = 0.003    # 0.3% retrace confirms swing

    # MACD (5-min bars)
    macd_fast: int = 12
    macd_slow: int = 26
    macd_signal: int = 9
    macd_condition: str = "macd_cross"  # macd_cross | histogram_positive | none

    # Trade management
    exit_time: str = "15:14"            # Force-exit all positions
    max_risk_per_trade: float = 1000.0  # ₹ max risk per trade
    capital: float = 100_000.0         # Capital per trade for sizing

    # Max bars to wait for fib touch after swing confirmed
    max_wait_bars: int = 60

    # Stocks to trade (populated from backtest results)
    stocks: list[StockConfig] = field(default_factory=list)


# ── Best 34-stock portfolio from Run 7 (Sharpe > 2.5) ────────────────────────
# Direction and target_r per stock from backtest optimisation

FIB_MACD_PORTFOLIO = StrategyConfig(
    stocks=[
        StockConfig("LTINFO",  "long_only",  1.5),
        StockConfig("TATMOT",  "short_only", 1.5),
        StockConfig("SHRTRA",  "short_only", 1.5),
        StockConfig("ADAENT",  "short_only", 1.5),
        StockConfig("MAHMAH",  "both",       1.5),
        StockConfig("ONGC",    "both",       1.5),
        StockConfig("NTPC",    "long_only",  1.5),
        StockConfig("MARUTI",  "short_only", 1.5),
        StockConfig("EICMOT",  "short_only", 1.5),
        StockConfig("TECMAH",  "long_only",  1.5),
        StockConfig("INDBA",   "short_only", 1.5),
        StockConfig("HINDAL",  "both",       1.5),
        StockConfig("BAJFI",   "short_only", 1.5),
        StockConfig("HCLTEC",  "short_only", 1.5),
        StockConfig("HDFSTA",  "both",       1.5),
        StockConfig("POWGRI",  "long_only",  1.5),
        StockConfig("BAFINS",  "short_only", 1.5),
        StockConfig("JSWSTE",  "both",       1.5),
        StockConfig("SUNPHA",  "long_only",  1.5),
        StockConfig("TITIND",  "long_only",  1.5),
        StockConfig("SBILIF",  "long_only",  1.5),
        StockConfig("GRASIM",  "short_only", 1.5),
        StockConfig("HERHON",  "short_only", 1.5),
        StockConfig("ADAPOR",  "both",       1.5),
        StockConfig("AXIBAN",  "short_only", 1.5),
        StockConfig("COALIN",  "short_only", 1.5),
        StockConfig("BHAAIR",  "long_only",  1.5),
        StockConfig("LARTOU",  "short_only", 1.5),
        StockConfig("BHAPET",  "both",       1.5),
        StockConfig("CIPLA",   "long_only",  1.5),
        StockConfig("TATGLO",  "short_only", 1.5),
        StockConfig("TATSTE",  "short_only", 1.5),
        StockConfig("KOTMAH",  "long_only",  1.5),
        StockConfig("STABAN",  "both",       1.5),
    ]
)
