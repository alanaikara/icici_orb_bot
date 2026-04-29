"""
Abstract broker interface.

All broker implementations must subclass BrokerBase.
The live strategy engine only calls methods on this interface —
swap in any broker without touching strategy logic.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional


@dataclass
class OrderResult:
    success: bool
    order_id: str
    message: str


@dataclass
class Position:
    stock_code: str
    direction: str          # 'LONG' or 'SHORT'
    quantity: int
    entry_price: float
    current_price: float
    pnl: float


class BrokerBase(ABC):

    # ── Connection ────────────────────────────────────────────────────────────

    @abstractmethod
    def connect(self) -> bool:
        """Authenticate and connect. Returns True on success."""
        ...

    @abstractmethod
    def is_connected(self) -> bool:
        """Return True if session is active."""
        ...

    # ── Market data ───────────────────────────────────────────────────────────

    @abstractmethod
    def get_ltp(self, stock_code: str) -> Optional[float]:
        """Return last traded price for a stock."""
        ...

    @abstractmethod
    def get_candles(
        self,
        stock_code: str,
        interval: str,          # '1minute', '5minute'
        from_datetime: str,     # 'YYYY-MM-DD HH:MM:SS'
        to_datetime: str,
    ) -> list[dict]:
        """
        Fetch historical/intraday OHLCV candles.
        Each dict must have: datetime, open, high, low, close, volume
        """
        ...

    # ── Orders ────────────────────────────────────────────────────────────────

    @abstractmethod
    def place_market_order(
        self,
        stock_code: str,
        action: str,        # 'buy' or 'sell'
        quantity: int,
        product_type: str,  # 'intraday' or 'delivery'
    ) -> OrderResult:
        ...

    @abstractmethod
    def place_limit_order(
        self,
        stock_code: str,
        action: str,
        quantity: int,
        price: float,
        product_type: str,
    ) -> OrderResult:
        ...

    @abstractmethod
    def place_stoploss_order(
        self,
        stock_code: str,
        action: str,
        quantity: int,
        trigger_price: float,
        limit_price: float,
        product_type: str,
    ) -> OrderResult:
        ...

    @abstractmethod
    def cancel_order(self, order_id: str) -> bool:
        ...

    @abstractmethod
    def get_order_status(self, order_id: str) -> dict:
        """Return dict with at least: status, filled_qty, avg_price"""
        ...

    # ── Positions / funds ─────────────────────────────────────────────────────

    @abstractmethod
    def get_positions(self) -> list[Position]:
        """Return all open intraday positions."""
        ...

    @abstractmethod
    def get_funds(self) -> dict:
        """Return dict with at least: available_cash, used_margin"""
        ...
