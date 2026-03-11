"""Thin pipeline wiring for the scaffold backtest command."""

from __future__ import annotations

from pathlib import Path

from tw_quant.backtest.run import run_backtest
from tw_quant.config import load_settings
from tw_quant.core.models import BacktestResult
from tw_quant.reporting.report import build_report


def execute_backtest(config_path: str | Path) -> BacktestResult:
    """Load config, run the scaffold backtest flow, and write a report."""

    config = load_settings(config_path)
    result = run_backtest(config)
    build_report(result)
    return result
