"""Signal-generation placeholder for future strategy logic."""

from __future__ import annotations

from tw_quant.core.models import BacktestConfig


def generate_signals(config: BacktestConfig) -> dict[str, object]:
    """Return a lightweight placeholder payload for the signal stage."""

    return {
        "model_name": "placeholder_daily_signal",
        "frequency": "daily",
        "universe": config.universe,
        "records": 0,
        "notes": (
            "Signal generation is not implemented yet.",
            "Future work belongs in this module, not in notebooks alone.",
        ),
    }
