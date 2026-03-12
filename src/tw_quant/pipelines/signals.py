"""Thin pipeline wiring for the signals command."""

from __future__ import annotations

from pathlib import Path

from tw_quant.config import load_settings
from tw_quant.core.models import AppConfig, SignalResult
from tw_quant.data import load_market_dataset, prepare_data_paths
from tw_quant.signals import (
    build_cross_sectional_signal_rows,
    build_signal_rows,
    write_cross_sectional_signal_rows,
    write_signal_rows,
)
from tw_quant.universe import (
    build_top_liquidity_membership,
    load_stock_metadata,
    validate_artifact_freshness,
    validate_membership_coverage,
    write_universe_membership,
)


def execute_signals(config_path: str | Path) -> SignalResult:
    """Load normalized bars, compute signals, and persist the combined panel."""

    app_config = load_settings(config_path)
    prepare_data_paths(app_config.data_paths)
    if app_config.signals.mode == "cross_sectional_vol_adj_momentum":
        return _execute_cross_sectional_signals(app_config)

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
        mode=signal_config.mode,
    )


def _execute_cross_sectional_signals(app_config: AppConfig) -> SignalResult:
    signal_config = app_config.signals
    metadata_rows = load_stock_metadata(app_config.universe_config.usable_metadata_path)
    candidate_symbols = tuple(row["stock_id"] for row in metadata_rows)
    if len(candidate_symbols) < app_config.universe_config.top_n:
        raise ValueError(
            "Cross-sectional signals cannot run because the effective candidate pool is too small: "
            f"found {len(candidate_symbols)} usable symbols, expected at least {app_config.universe_config.top_n}. "
            "Please rerun ingest and inspect twse_price_availability.csv."
        )
    membership_rows, reconstitution_dates, participating_symbols, membership_notes = (
        build_top_liquidity_membership(
            normalized_dir=signal_config.input_dir,
            candidate_symbols=candidate_symbols,
            benchmark_symbol=signal_config.benchmark,
            start_date=app_config.start_date,
            end_date=app_config.end_date,
            liquidity_lookback_days=app_config.universe_config.liquidity_lookback_days,
            top_n=app_config.universe_config.top_n,
        )
    )
    if not membership_rows:
        raise ValueError("No top-liquidity membership rows were produced for the requested date range.")
    validate_membership_coverage(
        membership_rows,
        expected_top_n=app_config.universe_config.top_n,
    )

    membership_path = write_universe_membership(
        app_config.universe_config.membership_path,
        membership_rows,
    )
    signal_rows = build_cross_sectional_signal_rows(
        normalized_dir=signal_config.input_dir,
        membership_rows=membership_rows,
        momentum_window=signal_config.momentum_window,
        volatility_window=signal_config.volatility_window,
    )
    membership_dates = tuple(sorted({row.date for row in membership_rows}))
    output_path = write_cross_sectional_signal_rows(
        signal_config.output_dir / signal_config.output_file,
        signal_rows,
    )
    validate_artifact_freshness(
        app_config.universe_config.usable_metadata_path,
        (membership_path, output_path),
    )

    notes = list(membership_notes)
    notes.append(
        "Universe 規則為每月第一個 TAIEX 交易日，以最近 60 個有效交易日平均 Trading_money 選出 TWSE Top-50 liquidity members。"
    )
    notes.append(
        "signal_score 定義為 momentum_126 / volatility_20，其中 volatility_20 使用最近 20 個交易日 close-to-close returns 的原始標準差。"
    )
    notes.append("本分支只會使用 ingest 階段確認有可用價格歷史的候選股，避免把 metadata 中無價格資料的股票帶進 effective universe。")

    return SignalResult(
        start_date=app_config.start_date,
        end_date=app_config.end_date,
        symbols=participating_symbols,
        aligned_dates=membership_dates if membership_dates else reconstitution_dates,
        row_count=len(signal_rows),
        output_path=output_path,
        notes=tuple(notes),
        mode=signal_config.mode,
        membership_output_path=membership_path,
    )
