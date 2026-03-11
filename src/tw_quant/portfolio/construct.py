"""Portfolio construction placeholder for future target-weight logic."""

from __future__ import annotations

from tw_quant.core.models import BacktestConfig


def build_target_weights(
    config: BacktestConfig,
    signal_payload: dict[str, object],
) -> dict[str, object]:
    """Return a lightweight placeholder payload for portfolio construction."""

    return {
        "allocator_name": "equal_weight_placeholder",
        "universe": config.universe,
        "target_count": signal_payload["records"],
        "notes": (
            "Portfolio construction is not implemented yet.",
            "Future work will translate signals into target weights and constraints.",
        ),
    }
