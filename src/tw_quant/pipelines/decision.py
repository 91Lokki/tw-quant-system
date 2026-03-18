"""Thin pipeline wiring for the daily decision command."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from tw_quant.config import load_settings
from tw_quant.core.models import DailyDecisionResult
from tw_quant.execution import generate_daily_decision


def execute_daily_decision(
    config_path: str | Path,
    as_of_date: date | None = None,
) -> DailyDecisionResult:
    """Load config and write the latest practical-mainline decision snapshot."""

    app_config = load_settings(config_path)
    return generate_daily_decision(app_config, as_of_date=as_of_date)
