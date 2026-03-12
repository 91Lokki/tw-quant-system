"""Walk-forward evaluation utilities for the local v1 backtest workflow."""

from __future__ import annotations

import csv
from datetime import date
from pathlib import Path

from tw_quant.backtest.metrics import compute_metrics
from tw_quant.backtest.cross_sectional import (
    build_cross_sectional_variant_configs,
    CrossSectionalBacktestInputs,
    compute_cross_sectional_backtest_data,
    load_cross_sectional_backtest_inputs,
    slice_cross_sectional_backtest_inputs,
)
from tw_quant.backtest.run import compute_backtest_data, load_backtest_inputs
from tw_quant.core.models import (
    BacktestConfig,
    MarketDataset,
    NavRow,
    NormalizedBar,
    SignalRow,
    WalkForwardResult,
    WalkForwardWindow,
    WalkForwardWindowResult,
)
from tw_quant.reporting.report import build_walkforward_report


def build_walkforward_windows(
    trading_dates: tuple[date, ...],
    config: BacktestConfig,
) -> tuple[WalkForwardWindow, ...]:
    """Split aligned trading dates into repeated train/test walk-forward windows."""

    walkforward_config = config.walkforward
    if not trading_dates:
        raise ValueError("Walk-forward requires at least one aligned trading date.")

    first_test_index = max(
        walkforward_config.train_window_days,
        walkforward_config.minimum_history_days,
    )
    if first_test_index >= len(trading_dates):
        raise ValueError(
            "Not enough aligned trading dates to create a walk-forward test window "
            f"with train_window_days={walkforward_config.train_window_days} and "
            f"minimum_history_days={walkforward_config.minimum_history_days}."
        )

    windows: list[WalkForwardWindow] = []
    window_id = 1
    test_start_index = first_test_index

    while test_start_index < len(trading_dates):
        test_end_index = min(
            test_start_index + walkforward_config.test_window_days,
            len(trading_dates),
        ) - 1
        train_end_index = test_start_index - 1
        if walkforward_config.window_type == "expanding":
            train_start_index = 0
        elif walkforward_config.window_type == "rolling":
            train_start_index = max(0, test_start_index - walkforward_config.train_window_days)
        else:  # pragma: no cover - validated earlier in config loading
            raise ValueError(f"Unsupported walk-forward window type: {walkforward_config.window_type}")

        windows.append(
            WalkForwardWindow(
                window_id=window_id,
                train_start=trading_dates[train_start_index],
                train_end=trading_dates[train_end_index],
                test_start=trading_dates[test_start_index],
                test_end=trading_dates[test_end_index],
                train_size=train_end_index - train_start_index + 1,
                test_size=test_end_index - test_start_index + 1,
            )
        )
        window_id += 1
        test_start_index += walkforward_config.test_window_days

    if not windows:
        raise ValueError("Walk-forward could not create any out-of-sample windows.")

    return tuple(windows)


def run_walkforward(config: BacktestConfig) -> WalkForwardResult:
    """Run repeated out-of-sample walk-forward evaluation on local data artifacts."""

    if not config.walkforward.enabled:
        raise ValueError("walkforward.enabled must be true to run the walk-forward workflow.")

    comparison_path: Path | None = None
    if config.research_branch == "tw_top50_liquidity_cross_sectional":
        inputs = load_cross_sectional_backtest_inputs(config)
        windows = build_walkforward_windows(inputs.master_dates, config)
        combined_nav_rows, window_results, tradable_symbols, base_notes = _run_cross_sectional_walkforward_variant(
            config,
            inputs,
        )
        comparison_path = _write_cross_sectional_walkforward_comparison(
            path=config.backtest.output_dir / config.project_name / "walkforward" / "risk_comparison.csv",
            variants=build_cross_sectional_variant_configs(config),
            inputs=inputs,
            config=config,
            primary_nav_rows=combined_nav_rows,
            primary_window_results=window_results,
        )
    else:
        combined_nav_rows = []
        window_results = []
        combined_nav = config.backtest.initial_nav
        combined_benchmark_nav = config.backtest.initial_nav
        market_dataset, signal_rows = load_backtest_inputs(config)
        windows = build_walkforward_windows(market_dataset.aligned_dates, config)
        tradable_symbols = config.portfolio.tradable_symbols
        base_notes = market_dataset.notes

        for window in windows:
            window_dataset = _slice_market_dataset(market_dataset, window.test_start, window.test_end)
            window_signal_rows = _slice_signal_rows(signal_rows, window_dataset.aligned_dates)
            computation = compute_backtest_data(
                config=config,
                market_dataset=window_dataset,
                signal_rows=window_signal_rows,
            )
            scaled_window_rows = _scale_window_nav_rows(
                list(computation.nav_rows),
                nav_scale=combined_nav,
                benchmark_scale=combined_benchmark_nav,
            )
            combined_nav_rows.extend(scaled_window_rows)
            combined_nav = scaled_window_rows[-1].nav
            combined_benchmark_nav = scaled_window_rows[-1].benchmark_nav

            segment_initial_nav = config.backtest.initial_nav
            segment_benchmark_initial_nav = config.backtest.initial_nav
            window_results.append(
                WalkForwardWindowResult(
                    window_id=window.window_id,
                    train_start=window.train_start,
                    train_end=window.train_end,
                    test_start=window.test_start,
                    test_end=window.test_end,
                    train_size=window.train_size,
                    test_size=window.test_size,
                    final_nav=computation.nav_rows[-1].nav if computation.nav_rows else segment_initial_nav,
                    benchmark_final_nav=(
                        computation.nav_rows[-1].benchmark_nav
                        if computation.nav_rows
                        else segment_benchmark_initial_nav
                    ),
                    metrics=computation.metrics,
                )
            )

    nav_path = _write_walkforward_nav(
        config.backtest.output_dir / config.project_name / "walkforward" / "walkforward_nav.csv",
        combined_nav_rows,
    )
    window_summary_path = _write_window_summary(
        config.backtest.output_dir / config.project_name / "walkforward" / "window_summary.csv",
        window_results,
    )
    report_path = config.data_paths.reports_dir / config.project_name / "walkforward" / "walkforward_summary.md"

    combined_metrics = compute_metrics(combined_nav_rows, config.backtest.initial_nav)
    notes = _build_walkforward_notes(config, windows, base_notes)
    result = WalkForwardResult(
        project_name=config.project_name,
        market=config.market,
        universe=config.universe,
        benchmark=config.benchmark,
        tradable_symbols=tradable_symbols,
        rebalance_frequency=config.portfolio.rebalance_frequency,
        rebalance_cadence_months=config.risk_controls.rebalance_cadence_months,
        trading_costs=config.trading_costs,
        hold_cash_when_inactive=config.portfolio.hold_cash_when_inactive,
        benchmark_filter_enabled=config.risk_controls.benchmark_filter_enabled,
        benchmark_ma_window=config.risk_controls.benchmark_ma_window,
        defensive_mode=config.risk_controls.defensive_mode,
        window_type=config.walkforward.window_type,
        train_window_days=config.walkforward.train_window_days,
        test_window_days=config.walkforward.test_window_days,
        minimum_history_days=config.walkforward.minimum_history_days,
        start_date=combined_nav_rows[0].date,
        end_date=combined_nav_rows[-1].date,
        nav_path=nav_path,
        window_summary_path=window_summary_path,
        report_path=report_path,
        comparison_path=comparison_path,
        metrics=combined_metrics,
        final_nav=combined_nav_rows[-1].nav,
        benchmark_final_nav=combined_nav_rows[-1].benchmark_nav,
        window_count=len(window_results),
        status="walk-forward out-of-sample evaluation completed",
        notes=notes,
        windows=tuple(window_results),
    )
    build_walkforward_report(result)
    return result


def _run_cross_sectional_walkforward_variant(
    config: BacktestConfig,
    inputs: CrossSectionalBacktestInputs,
) -> tuple[list[NavRow], list[WalkForwardWindowResult], tuple[str, ...], tuple[str, ...]]:
    combined_nav_rows: list[NavRow] = []
    window_results: list[WalkForwardWindowResult] = []
    combined_nav = config.backtest.initial_nav
    combined_benchmark_nav = config.backtest.initial_nav
    windows = build_walkforward_windows(inputs.master_dates, config)

    for window in windows:
        window_inputs = slice_cross_sectional_backtest_inputs(
            inputs,
            window.test_start,
            window.test_end,
        )
        computation = compute_cross_sectional_backtest_data(config, window_inputs)
        scaled_window_rows = _scale_window_nav_rows(
            list(computation.nav_rows),
            nav_scale=combined_nav,
            benchmark_scale=combined_benchmark_nav,
        )
        combined_nav_rows.extend(scaled_window_rows)
        combined_nav = scaled_window_rows[-1].nav
        combined_benchmark_nav = scaled_window_rows[-1].benchmark_nav

        window_results.append(
            WalkForwardWindowResult(
                window_id=window.window_id,
                train_start=window.train_start,
                train_end=window.train_end,
                test_start=window.test_start,
                test_end=window.test_end,
                train_size=window.train_size,
                test_size=window.test_size,
                final_nav=computation.nav_rows[-1].nav,
                benchmark_final_nav=computation.nav_rows[-1].benchmark_nav,
                metrics=computation.metrics,
            )
        )

    return combined_nav_rows, window_results, inputs.participating_symbols, inputs.notes


def _write_cross_sectional_walkforward_comparison(
    path: Path,
    variants: tuple[tuple[str, BacktestConfig], ...],
    inputs: CrossSectionalBacktestInputs,
    config: BacktestConfig,
    primary_nav_rows: list[NavRow],
    primary_window_results: list[WalkForwardWindowResult],
) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    primary_signature = (
        config.risk_controls.benchmark_filter_enabled,
        config.risk_controls.benchmark_ma_window,
        config.risk_controls.defensive_mode,
        config.risk_controls.rebalance_cadence_months,
    )

    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "label",
                "benchmark_filter_enabled",
                "benchmark_ma_window",
                "defensive_mode",
                "rebalance_cadence_months",
                "window_count",
                "final_nav",
                "benchmark_final_nav",
                "cumulative_return",
                "annualized_return",
                "annualized_volatility",
                "max_drawdown",
                "sharpe_ratio",
                "turnover",
            ],
        )
        writer.writeheader()
        for label, variant_config in variants:
            variant_signature = (
                variant_config.risk_controls.benchmark_filter_enabled,
                variant_config.risk_controls.benchmark_ma_window,
                variant_config.risk_controls.defensive_mode,
                variant_config.risk_controls.rebalance_cadence_months,
            )
            if variant_signature == primary_signature:
                nav_rows = primary_nav_rows
                window_results = primary_window_results
            else:
                nav_rows, window_results, _, _ = _run_cross_sectional_walkforward_variant(
                    variant_config,
                    inputs,
                )
            metrics = compute_metrics(nav_rows, config.backtest.initial_nav)
            writer.writerow(
                {
                    "label": label,
                    "benchmark_filter_enabled": str(
                        variant_config.risk_controls.benchmark_filter_enabled
                    ).lower(),
                    "benchmark_ma_window": str(variant_config.risk_controls.benchmark_ma_window),
                    "defensive_mode": variant_config.risk_controls.defensive_mode,
                    "rebalance_cadence_months": str(
                        variant_config.risk_controls.rebalance_cadence_months
                    ),
                    "window_count": str(len(window_results)),
                    "final_nav": f"{nav_rows[-1].nav}",
                    "benchmark_final_nav": f"{nav_rows[-1].benchmark_nav}",
                    "cumulative_return": f"{metrics.cumulative_return}",
                    "annualized_return": f"{metrics.annualized_return}",
                    "annualized_volatility": f"{metrics.annualized_volatility}",
                    "max_drawdown": f"{metrics.max_drawdown}",
                    "sharpe_ratio": f"{metrics.sharpe_ratio}",
                    "turnover": f"{metrics.turnover}",
                }
            )
    return path


def _slice_market_dataset(
    market_dataset: MarketDataset,
    start_date: date,
    end_date: date,
) -> MarketDataset:
    aligned_dates = tuple(
        trading_date
        for trading_date in market_dataset.aligned_dates
        if start_date <= trading_date <= end_date
    )
    if not aligned_dates:
        raise ValueError("Walk-forward test window produced no aligned trading dates.")

    aligned_date_set = set(aligned_dates)
    bars_by_symbol: dict[str, tuple[NormalizedBar, ...]] = {}
    for symbol, rows in market_dataset.bars_by_symbol.items():
        filtered_rows = tuple(row for row in rows if row.date in aligned_date_set)
        if not filtered_rows:
            raise ValueError(f"Walk-forward test window produced no bars for symbol {symbol}.")
        bars_by_symbol[symbol] = filtered_rows

    return MarketDataset(
        symbols=market_dataset.symbols,
        start_date=start_date,
        end_date=end_date,
        bars_by_symbol=bars_by_symbol,
        aligned_dates=aligned_dates,
        notes=market_dataset.notes,
    )


def _slice_signal_rows(
    signal_rows: list[SignalRow],
    aligned_dates: tuple[date, ...],
) -> list[SignalRow]:
    aligned_date_set = set(aligned_dates)
    return [row for row in signal_rows if row.date in aligned_date_set]


def _scale_window_nav_rows(
    rows: list[NavRow],
    nav_scale: float,
    benchmark_scale: float,
) -> list[NavRow]:
    return [
        NavRow(
            date=row.date,
            nav=row.nav * nav_scale,
            daily_return=row.daily_return,
            gross_return=row.gross_return,
            benchmark_nav=row.benchmark_nav * benchmark_scale,
            benchmark_return=row.benchmark_return,
            turnover=row.turnover,
            transaction_cost=row.transaction_cost * nav_scale,
            cash_weight=row.cash_weight,
        )
        for row in rows
    ]


def _write_walkforward_nav(path: Path, rows: list[NavRow]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "date",
                "nav",
                "daily_return",
                "gross_return",
                "benchmark_nav",
                "benchmark_return",
                "turnover",
                "transaction_cost",
                "cash_weight",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row.to_csv_row())
    return path


def _write_window_summary(path: Path, rows: list[WalkForwardWindowResult]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "window_id",
                "train_start",
                "train_end",
                "test_start",
                "test_end",
                "train_size",
                "test_size",
                "final_nav",
                "benchmark_final_nav",
                "cumulative_return",
                "annualized_return",
                "annualized_volatility",
                "max_drawdown",
                "sharpe_ratio",
                "turnover",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row.to_csv_row())
    return path


def _build_walkforward_notes(
    config: BacktestConfig,
    windows: tuple[WalkForwardWindow, ...],
    base_notes: tuple[str, ...],
) -> tuple[str, ...]:
    notes = list(base_notes)
    notes.append(
        "目前的 walk-forward 評估使用既有固定規則策略，因此訓練窗主要扮演 in-sample 保留與樣本外切分的角色，而不是重新估參。"
    )
    notes.append(
        f"v1 預設採用 {config.walkforward.window_type} window；每個測試窗為 {config.walkforward.test_window_days} 個對齊交易日。"
    )
    notes.append(
        f"總共評估 {len(windows)} 個樣本外視窗，且每個視窗都只以對應 OOS 區間的報酬參與最終聚合。"
    )
    if windows[-1].test_size < config.walkforward.test_window_days:
        notes.append("最後一個測試窗若剩餘交易日不足完整長度，會以較短的 OOS 視窗納入聚合。")
    notes.append("樣本外聚合 NAV 會把各視窗的日報酬串接起來，不會把 in-sample 期間計入最終績效。")
    if config.risk_controls.benchmark_filter_enabled:
        notes.append(
            f"Phase D 風控在 OOS 也保持一致：當 {config.benchmark} 未站上 {config.risk_controls.benchmark_ma_window} 日均線時，"
            "防守模式會回到現金。"
        )
    if config.risk_controls.rebalance_cadence_months > 1:
        notes.append(
            f"Phase D cadence 敏感度：此 run 每 {config.risk_controls.rebalance_cadence_months} 個月才更新一次投組持倉。"
        )
    return tuple(notes)
