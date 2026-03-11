"""Thin pipeline wiring for the diagnostics command."""

from __future__ import annotations

from pathlib import Path

from tw_quant.config import load_backtest_settings
from tw_quant.core.models import DiagnosticsResult
from tw_quant.diagnostics import run_diagnostics


def execute_diagnostics(config_path: str | Path) -> DiagnosticsResult:
    """Load config and analyze existing local artifacts for the project."""

    config = load_backtest_settings(config_path)
    return run_diagnostics(config)
