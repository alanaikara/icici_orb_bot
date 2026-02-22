"""
Parameter grid definition and generation for ORB backtesting.

Defines the strategy parameter space (OR duration, SL type, targets, etc.)
and generates all combinations for grid search optimization.
"""

import hashlib
import json
from enum import Enum
from dataclasses import dataclass, field, asdict
from itertools import product
from typing import Optional


class StopLossType(str, Enum):
    """Stop loss calculation method."""
    FIXED = "fixed"           # SL at opposite OR edge
    TRAILING = "trailing"     # Trailing % from peak/trough
    ATR_BASED = "atr_based"   # ATR-multiple from entry price


class TradeDirection(str, Enum):
    """Allowed trade direction."""
    LONG_ONLY = "long_only"
    SHORT_ONLY = "short_only"
    BOTH = "both"


class EntryConfirmation(str, Enum):
    """Entry signal confirmation method."""
    IMMEDIATE = "immediate"        # Enter on breakout (high > OR_high)
    CANDLE_CLOSE = "candle_close"  # Enter only if candle closes outside OR
    VOLUME_CONFIRM = "volume"      # Candle close + volume > 1.5x OR avg


# Default parameter values for grid search
DEFAULT_OR_MINUTES = [5, 10, 15, 20, 30, 45, 60]
DEFAULT_TARGET_MULTIPLIERS = [0, 1.0, 1.5, 2.0, 2.5, 3.0]
DEFAULT_SL_TYPES = [StopLossType.FIXED, StopLossType.TRAILING, StopLossType.ATR_BASED]
DEFAULT_DIRECTIONS = [TradeDirection.LONG_ONLY, TradeDirection.SHORT_ONLY, TradeDirection.BOTH]
DEFAULT_EXIT_TIMES = ["12:30", "14:00", "14:30", "15:00", "15:14"]
DEFAULT_OR_FILTERS = [0.5, 1.0, 1.5, 2.0, 0]  # 0 = no filter
DEFAULT_ENTRY_CONFIRMATIONS = [
    EntryConfirmation.IMMEDIATE,
    EntryConfirmation.CANDLE_CLOSE,
    EntryConfirmation.VOLUME_CONFIRM,
]

# Quick mode: reduced grid for fast validation
QUICK_OR_MINUTES = [15, 30]
QUICK_TARGET_MULTIPLIERS = [0, 2.0]
QUICK_SL_TYPES = [StopLossType.FIXED]
QUICK_DIRECTIONS = [TradeDirection.BOTH]
QUICK_EXIT_TIMES = ["15:14"]
QUICK_OR_FILTERS = [0]
QUICK_ENTRY_CONFIRMATIONS = [EntryConfirmation.IMMEDIATE]


@dataclass(frozen=True)
class StrategyParams:
    """
    Immutable strategy parameter set for one backtest combination.

    Frozen (immutable + hashable) so it can be used as dict keys
    and is safe for multiprocessing.
    """
    or_minutes: int                         # Opening range duration
    target_multiplier: float                # 0 = no target (time exit only)
    stop_loss_type: StopLossType
    trade_direction: TradeDirection
    exit_time: str                          # "HH:MM" format
    max_or_filter_pct: float                # 0 = no filter
    entry_confirmation: EntryConfirmation

    # Strategy constants (not swept in Phase 1)
    trailing_stop_pct: float = 0.5          # For trailing SL type
    atr_multiplier: float = 1.5             # For ATR-based SL
    atr_period: int = 14                    # ATR lookback period

    def param_id(self) -> str:
        """
        Generate a deterministic short hash ID for this parameter set.
        Used as primary key in results database.
        """
        key_str = (
            f"{self.or_minutes}|{self.target_multiplier}|{self.stop_loss_type.value}|"
            f"{self.trade_direction.value}|{self.exit_time}|{self.max_or_filter_pct}|"
            f"{self.entry_confirmation.value}|{self.trailing_stop_pct}|"
            f"{self.atr_multiplier}|{self.atr_period}"
        )
        return hashlib.md5(key_str.encode()).hexdigest()[:12]

    def to_dict(self) -> dict:
        """Serialize to dictionary for JSON/DB storage."""
        return {
            "or_minutes": self.or_minutes,
            "target_multiplier": self.target_multiplier,
            "stop_loss_type": self.stop_loss_type.value,
            "trade_direction": self.trade_direction.value,
            "exit_time": self.exit_time,
            "max_or_filter_pct": self.max_or_filter_pct,
            "entry_confirmation": self.entry_confirmation.value,
            "trailing_stop_pct": self.trailing_stop_pct,
            "atr_multiplier": self.atr_multiplier,
            "atr_period": self.atr_period,
        }

    def to_json(self) -> str:
        """Serialize to JSON string."""
        return json.dumps(self.to_dict(), sort_keys=True)

    def short_description(self) -> str:
        """Human-readable one-line summary."""
        target_str = f"{self.target_multiplier}R" if self.target_multiplier > 0 else "NoTarget"
        filter_str = f"OR<{self.max_or_filter_pct}%" if self.max_or_filter_pct > 0 else "NoFilter"
        return (
            f"OR{self.or_minutes}m | {self.stop_loss_type.value} SL | "
            f"{target_str} | {self.trade_direction.value} | "
            f"Exit@{self.exit_time} | {filter_str} | {self.entry_confirmation.value}"
        )


class ParameterGrid:
    """
    Generates strategy parameter combinations for grid search.

    Supports full grid, quick validation grid, and filtered grid
    with specific parameters pinned.
    """

    def __init__(self, config: dict = None):
        """
        Initialize with optional config overrides.

        Args:
            config: dict with optional 'backtest_sweep' key containing
                    trailing_stop_pct, atr_multiplier, atr_period overrides.
        """
        self.config = config or {}
        sweep_config = self.config.get("backtest_sweep", {})
        self.trailing_stop_pct = sweep_config.get("trailing_stop_pct", 0.5)
        self.atr_multiplier = sweep_config.get("atr_multiplier", 1.5)
        self.atr_period = sweep_config.get("atr_period", 14)

    def generate_all(self) -> list[StrategyParams]:
        """Generate all parameter combinations (full grid)."""
        return self._generate(
            or_minutes=DEFAULT_OR_MINUTES,
            targets=DEFAULT_TARGET_MULTIPLIERS,
            sl_types=DEFAULT_SL_TYPES,
            directions=DEFAULT_DIRECTIONS,
            exit_times=DEFAULT_EXIT_TIMES,
            or_filters=DEFAULT_OR_FILTERS,
            entry_confirmations=DEFAULT_ENTRY_CONFIRMATIONS,
        )

    def generate_quick(self) -> list[StrategyParams]:
        """
        Generate reduced grid for fast validation.
        ~4 combos: 2 OR x 2 targets x 1 SL x 1 dir x 1 exit x 1 filter x 1 entry
        """
        return self._generate(
            or_minutes=QUICK_OR_MINUTES,
            targets=QUICK_TARGET_MULTIPLIERS,
            sl_types=QUICK_SL_TYPES,
            directions=QUICK_DIRECTIONS,
            exit_times=QUICK_EXIT_TIMES,
            or_filters=QUICK_OR_FILTERS,
            entry_confirmations=QUICK_ENTRY_CONFIRMATIONS,
        )

    def generate_filtered(
        self,
        or_minutes: list[int] = None,
        targets: list[float] = None,
        sl_types: list[str] = None,
        directions: list[str] = None,
        exit_times: list[str] = None,
        or_filters: list[float] = None,
        entry_confirmations: list[str] = None,
    ) -> list[StrategyParams]:
        """
        Generate grid with specific parameters pinned.
        Pass None to use full default range for that parameter.
        String values are converted to enums automatically.
        """
        # Convert string lists to enums where needed
        parsed_sl = (
            [StopLossType(s) for s in sl_types]
            if sl_types else DEFAULT_SL_TYPES
        )
        parsed_dirs = (
            [TradeDirection(d) for d in directions]
            if directions else DEFAULT_DIRECTIONS
        )
        parsed_entry = (
            [EntryConfirmation(e) for e in entry_confirmations]
            if entry_confirmations else DEFAULT_ENTRY_CONFIRMATIONS
        )

        return self._generate(
            or_minutes=or_minutes or DEFAULT_OR_MINUTES,
            targets=targets or DEFAULT_TARGET_MULTIPLIERS,
            sl_types=parsed_sl,
            directions=parsed_dirs,
            exit_times=exit_times or DEFAULT_EXIT_TIMES,
            or_filters=or_filters or DEFAULT_OR_FILTERS,
            entry_confirmations=parsed_entry,
        )

    def _generate(
        self,
        or_minutes: list[int],
        targets: list[float],
        sl_types: list[StopLossType],
        directions: list[TradeDirection],
        exit_times: list[str],
        or_filters: list[float],
        entry_confirmations: list[EntryConfirmation],
    ) -> list[StrategyParams]:
        """Core grid generation using itertools.product."""
        params_list = []
        for om, tgt, sl, dir_, et, orf, ec in product(
            or_minutes, targets, sl_types, directions,
            exit_times, or_filters, entry_confirmations
        ):
            params_list.append(StrategyParams(
                or_minutes=om,
                target_multiplier=tgt,
                stop_loss_type=sl,
                trade_direction=dir_,
                exit_time=et,
                max_or_filter_pct=orf,
                entry_confirmation=ec,
                trailing_stop_pct=self.trailing_stop_pct,
                atr_multiplier=self.atr_multiplier,
                atr_period=self.atr_period,
            ))
        return params_list

    def count(self) -> int:
        """Total combinations in the full grid (without generating them)."""
        return (
            len(DEFAULT_OR_MINUTES)
            * len(DEFAULT_TARGET_MULTIPLIERS)
            * len(DEFAULT_SL_TYPES)
            * len(DEFAULT_DIRECTIONS)
            * len(DEFAULT_EXIT_TIMES)
            * len(DEFAULT_OR_FILTERS)
            * len(DEFAULT_ENTRY_CONFIRMATIONS)
        )

    @staticmethod
    def group_by_or_minutes(
        params_list: list[StrategyParams],
    ) -> dict[int, list[StrategyParams]]:
        """
        Group parameter combos by OR duration.
        All combos sharing the same or_minutes reuse the same precomputed OR data.
        """
        groups = {}
        for p in params_list:
            groups.setdefault(p.or_minutes, []).append(p)
        return groups

    @staticmethod
    def get_unique_or_minutes(params_list: list[StrategyParams]) -> list[int]:
        """Get sorted list of unique OR durations needed."""
        return sorted(set(p.or_minutes for p in params_list))
