"""Thin pipeline wiring for the walk-forward evaluation command."""

from __future__ import annotations

from pathlib import Path

from tw_quant.backtest.walkforward import run_walkforward
from tw_quant.config import load_backtest_settings
from tw_quant.core.models import WalkForwardResult


def execute_walkforward(config_path: str | Path) -> WalkForwardResult:
    """Load config and run the walk-forward local-data evaluation flow."""

    config = load_backtest_settings(config_path)
    return run_walkforward(config)
