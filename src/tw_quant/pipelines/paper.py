"""Thin pipeline wiring for the paper-trading update command."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from tw_quant.config import load_settings
from tw_quant.core.models import PaperTradingResult
from tw_quant.execution import update_paper_trading


def execute_paper_update(
    config_path: str | Path,
    as_of_date: date | None = None,
) -> PaperTradingResult:
    """Load config and roll the paper-trading ledger forward."""

    app_config = load_settings(config_path)
    return update_paper_trading(app_config, as_of_date=as_of_date)
