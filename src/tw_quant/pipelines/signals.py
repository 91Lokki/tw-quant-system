"""Thin pipeline wiring for the signals command."""

from __future__ import annotations

from pathlib import Path

from tw_quant.config import load_settings
from tw_quant.core.models import SignalResult
from tw_quant.data import load_market_dataset, prepare_data_paths
from tw_quant.signals import build_signal_rows, write_signal_rows


def execute_signals(config_path: str | Path) -> SignalResult:
    """Load normalized bars, compute signals, and persist the combined panel."""

    app_config = load_settings(config_path)
    prepare_data_paths(app_config.data_paths)

    signal_config = app_config.signals
    dataset = load_market_dataset(
        normalized_dir=signal_config.input_dir,
        symbols=signal_config.requested_symbols(),
        start_date=app_config.start_date,
        end_date=app_config.end_date,
        align_by_date=signal_config.align_by_date,
    )
    signal_rows = build_signal_rows(dataset, signal_config)
    output_path = write_signal_rows(
        signal_config.output_dir / signal_config.output_file,
        signal_rows,
    )

    notes = list(dataset.notes)
    notes.append(
        "signal_score 目前定義為趨勢訊號與動能訊號的平均值，並以 volatility_filter 做風險過濾。"
    )

    return SignalResult(
        start_date=app_config.start_date,
        end_date=app_config.end_date,
        symbols=dataset.symbols,
        aligned_dates=dataset.aligned_dates,
        row_count=len(signal_rows),
        output_path=output_path,
        notes=tuple(notes),
    )
