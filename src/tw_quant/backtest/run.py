"""Backtest orchestration for the v1 scaffold."""

from __future__ import annotations

from tw_quant.core.models import BacktestConfig, BacktestResult
from tw_quant.data.io import prepare_data_paths
from tw_quant.portfolio.construct import build_target_weights
from tw_quant.signals.generate import generate_signals


def run_backtest(config: BacktestConfig) -> BacktestResult:
    """Run the scaffold backtest flow and return a summary object."""

    prepare_data_paths(config.data_paths)
    signal_payload = generate_signals(config)
    target_payload = build_target_weights(config, signal_payload)
    report_path = config.data_paths.reports_dir / f"{config.project_name}_backtest_summary.md"

    notes = (
        f"Prepared local directories under {config.data_paths.project_root}.",
        f"Signal stage reserved for {signal_payload['model_name']}.",
        f"Portfolio stage reserved for {target_payload['allocator_name']}.",
        "Execution remains a placeholder in v1.",
    )

    return BacktestResult(
        project_name=config.project_name,
        market=config.market,
        universe=config.universe,
        benchmark=config.benchmark,
        start_date=config.start_date,
        end_date=config.end_date,
        report_path=report_path,
        status="scaffold backtest completed",
        notes=notes,
    )
