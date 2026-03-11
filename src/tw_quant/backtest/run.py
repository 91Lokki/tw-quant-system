"""Local-data daily backtest engine for the v1 research workflow."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from tw_quant.backtest.metrics import compute_metrics
from tw_quant.core.models import (
    BacktestConfig,
    BacktestResult,
    MarketDataset,
    NavRow,
    PerformanceMetrics,
    PortfolioWeightRow,
    SignalRow,
)
from tw_quant.data import load_market_dataset, prepare_data_paths
from tw_quant.portfolio.construct import build_target_weights, determine_rebalance_dates, expand_daily_weights
from tw_quant.reporting.report import build_report
from tw_quant.signals import load_signal_rows


@dataclass(frozen=True, slots=True)
class BacktestComputation:
    market_dataset: MarketDataset
    signal_rows: tuple[SignalRow, ...]
    nav_rows: tuple[NavRow, ...]
    weight_rows: tuple[PortfolioWeightRow, ...]
    metrics: PerformanceMetrics
    benchmark_final_nav: float
    notes: tuple[str, ...]


def run_backtest(config: BacktestConfig) -> BacktestResult:
    """Run the local-data backtest flow and persist daily artifacts."""

    market_dataset, signal_rows = load_backtest_inputs(config)
    computation = compute_backtest_data(
        config=config,
        market_dataset=market_dataset,
        signal_rows=signal_rows,
    )

    output_dir = config.backtest.output_dir / config.project_name
    nav_path = _write_nav_rows(output_dir / config.backtest.nav_file, list(computation.nav_rows))
    weights_path = _write_weight_rows(output_dir / config.backtest.weights_file, list(computation.weight_rows))
    report_dir = config.data_paths.reports_dir / config.project_name
    report_path = report_dir / "backtest_summary.md"
    equity_curve_path = report_dir / "equity_curve.svg"
    drawdown_path = report_dir / "drawdown.svg"

    result = BacktestResult(
        project_name=config.project_name,
        market=config.market,
        universe=config.universe,
        benchmark=config.benchmark,
        tradable_symbols=config.portfolio.tradable_symbols,
        rebalance_frequency=config.portfolio.rebalance_frequency,
        trading_costs=config.trading_costs,
        hold_cash_when_inactive=config.portfolio.hold_cash_when_inactive,
        start_date=config.start_date,
        end_date=config.end_date,
        report_path=report_path,
        nav_path=nav_path,
        weights_path=weights_path,
        equity_curve_path=equity_curve_path,
        drawdown_path=drawdown_path,
        metrics=computation.metrics,
        final_nav=computation.nav_rows[-1].nav if computation.nav_rows else config.backtest.initial_nav,
        benchmark_final_nav=computation.benchmark_final_nav,
        status="local-data backtest completed",
        notes=computation.notes,
    )
    build_report(result)
    return result


def load_backtest_inputs(config: BacktestConfig) -> tuple[MarketDataset, list[SignalRow]]:
    """Load aligned market bars and signal rows for a backtest configuration."""

    prepare_data_paths(config.data_paths)

    market_dataset = load_market_dataset(
        normalized_dir=config.backtest.bar_input_dir,
        symbols=config.portfolio.requested_symbols(),
        start_date=config.start_date,
        end_date=config.end_date,
        align_by_date=True,
    )
    signal_rows = load_signal_rows(
        path=config.backtest.signal_input_path,
        symbols=config.portfolio.tradable_symbols,
        start_date=config.start_date,
        end_date=config.end_date,
        aligned_dates=market_dataset.aligned_dates,
    )
    return market_dataset, signal_rows


def compute_backtest_data(
    config: BacktestConfig,
    market_dataset: MarketDataset,
    signal_rows: list[SignalRow],
) -> BacktestComputation:
    """Run the core portfolio/backtest calculation from already loaded local inputs."""

    rebalance_dates = determine_rebalance_dates(
        market_dataset.aligned_dates,
        config.portfolio.rebalance_frequency,
    )
    target_weights_by_date, signal_scores_by_date = build_target_weights(
        signal_rows,
        config.portfolio,
        rebalance_dates,
    )
    applied_weights_by_date, weight_rows = expand_daily_weights(
        market_dataset.aligned_dates,
        config.portfolio.tradable_symbols,
        target_weights_by_date,
        signal_scores_by_date,
    )
    nav_rows, benchmark_final_nav = _simulate_nav(
        config=config,
        market_dataset=market_dataset,
        applied_weights_by_date=applied_weights_by_date,
        target_weights_by_date=target_weights_by_date,
    )
    metrics = compute_metrics(nav_rows, config.backtest.initial_nav)
    notes = _build_backtest_notes(config, market_dataset)
    return BacktestComputation(
        market_dataset=market_dataset,
        signal_rows=tuple(signal_rows),
        nav_rows=tuple(nav_rows),
        weight_rows=tuple(weight_rows),
        metrics=metrics,
        benchmark_final_nav=benchmark_final_nav,
        notes=notes,
    )


def _build_backtest_notes(config: BacktestConfig, market_dataset: MarketDataset) -> tuple[str, ...]:
    notes = list(market_dataset.notes)
    notes.append(
        f"再平衡頻率使用 {config.portfolio.rebalance_frequency}，且以每個週期的第一個共同交易日作為換倉訊號日。"
    )
    notes.append("換倉在當日收盤後生效，下一個交易日才開始承擔新權重的報酬，以避免 lookahead bias。")
    if config.portfolio.hold_cash_when_inactive:
        notes.append("當沒有標的通過訊號門檻時，投資組合會保留現金部位。")
    return tuple(notes)


def _simulate_nav(
    config: BacktestConfig,
    market_dataset: MarketDataset,
    applied_weights_by_date: dict[date, dict[str, float]],
    target_weights_by_date: dict[date, dict[str, float]],
) -> tuple[list[NavRow], float]:
    aligned_dates = market_dataset.aligned_dates
    tradable_symbols = config.portfolio.tradable_symbols
    benchmark_symbol = config.portfolio.benchmark

    close_lookup = {
        symbol: {row.date: row.close for row in rows}
        for symbol, rows in market_dataset.bars_by_symbol.items()
    }

    nav_rows: list[NavRow] = []
    nav = config.backtest.initial_nav
    benchmark_nav = config.backtest.initial_nav
    last_index = len(aligned_dates) - 1

    for index, trading_date in enumerate(aligned_dates):
        starting_nav = nav
        applied_weights = applied_weights_by_date[trading_date]
        cash_weight = max(0.0, 1.0 - sum(applied_weights.values()))

        asset_return = 0.0
        benchmark_return = 0.0
        if index > 0:
            previous_date = aligned_dates[index - 1]
            asset_return = sum(
                applied_weights[symbol]
                * _close_to_close_return(close_lookup[symbol][previous_date], close_lookup[symbol][trading_date])
                for symbol in tradable_symbols
            )
            benchmark_return = _close_to_close_return(
                close_lookup[benchmark_symbol][previous_date],
                close_lookup[benchmark_symbol][trading_date],
            )

        nav_after_return = starting_nav * (1.0 + asset_return)
        benchmark_nav *= 1.0 + benchmark_return

        turnover = 0.0
        transaction_cost = 0.0
        if trading_date in target_weights_by_date and index < last_index:
            target_weights = target_weights_by_date[trading_date]
            turnover, cost_rate = _compute_turnover_and_cost_rate(
                current_weights=applied_weights,
                target_weights=target_weights,
                config=config,
            )
            transaction_cost = nav_after_return * cost_rate
            nav = nav_after_return - transaction_cost
        else:
            nav = nav_after_return

        daily_return = (nav / starting_nav) - 1.0 if starting_nav != 0 else 0.0
        nav_rows.append(
            NavRow(
                date=trading_date,
                nav=nav,
                daily_return=daily_return,
                gross_return=asset_return,
                benchmark_nav=benchmark_nav,
                benchmark_return=benchmark_return,
                turnover=turnover,
                transaction_cost=transaction_cost,
                cash_weight=cash_weight,
            )
        )

    return nav_rows, benchmark_nav


def _compute_turnover_and_cost_rate(
    current_weights: dict[str, float],
    target_weights: dict[str, float],
    config: BacktestConfig,
) -> tuple[float, float]:
    total_delta = 0.0
    sell_notional = 0.0

    for symbol in config.portfolio.tradable_symbols:
        delta = target_weights[symbol] - current_weights[symbol]
        total_delta += abs(delta)
        if delta < 0:
            sell_notional += -delta

    turnover = total_delta / 2.0
    commission_and_slippage_rate = (
        config.trading_costs.commission_bps + config.trading_costs.slippage_bps
    ) / 10_000.0
    tax_rate = config.trading_costs.tax_bps / 10_000.0
    total_cost_rate = (commission_and_slippage_rate * total_delta) + (tax_rate * sell_notional)
    return turnover, total_cost_rate


def _close_to_close_return(previous_close: float, current_close: float) -> float:
    if previous_close == 0:
        return 0.0
    return (current_close / previous_close) - 1.0


def _write_nav_rows(path: Path, rows: list[NavRow]) -> Path:
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


def _write_weight_rows(path: Path, rows: list[PortfolioWeightRow]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["date", "symbol", "weight", "signal_score"])
        writer.writeheader()
        for row in rows:
            writer.writerow(row.to_csv_row())
    return path
