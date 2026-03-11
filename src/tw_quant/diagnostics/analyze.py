"""Diagnostics and failure analysis for existing local research artifacts."""

from __future__ import annotations

import csv
from collections import defaultdict
from datetime import date
from math import prod
from pathlib import Path
from statistics import fmean, median, pstdev

from tw_quant.backtest.metrics import compute_metrics
from tw_quant.core.models import (
    BacktestConfig,
    DiagnosticsResult,
    NavRow,
    PerformanceMetrics,
    PortfolioWeightRow,
    SignalRow,
)
from tw_quant.signals import load_signal_rows


EPSILON = 1e-12


def run_diagnostics(config: BacktestConfig) -> DiagnosticsResult:
    """Run diagnostics on persisted local artifacts without changing research logic."""

    backtest_dir = config.backtest.output_dir / config.project_name
    diagnostics_dir = backtest_dir / "diagnostics"
    diagnostics_dir.mkdir(parents=True, exist_ok=True)

    report_dir = config.data_paths.reports_dir / config.project_name / "diagnostics"
    report_dir.mkdir(parents=True, exist_ok=True)

    nav_rows = load_nav_rows(backtest_dir / config.backtest.nav_file)
    weight_rows = load_weight_rows(backtest_dir / config.backtest.weights_file)
    signal_rows = load_signal_rows(
        path=config.backtest.signal_input_path,
        symbols=config.portfolio.tradable_symbols,
        start_date=config.start_date,
        end_date=config.end_date,
        aligned_dates=None,
    )
    walkforward_nav_rows = load_nav_rows(backtest_dir / "walkforward" / "walkforward_nav.csv")
    walkforward_window_rows = load_walkforward_window_rows(
        backtest_dir / "walkforward" / "window_summary.csv"
    )

    yearly_rows, yearly_summary = build_yearly_return_rows(nav_rows)
    walkforward_rows, walkforward_summary = build_walkforward_window_diagnostics(
        walkforward_window_rows
    )
    symbol_rows, exposure_summary = build_symbol_exposure_summary(
        weight_rows=weight_rows,
        nav_rows=nav_rows,
        tradable_symbols=config.portfolio.tradable_symbols,
    )
    signal_diagnostic_rows, signal_summary = build_signal_diagnostics(
        signal_rows=signal_rows,
        tradable_symbols=config.portfolio.tradable_symbols,
    )

    yearly_table_path = _write_csv(
        diagnostics_dir / "yearly_return_table.csv",
        yearly_rows,
        fieldnames=(
            "year",
            "strategy_return",
            "benchmark_return",
            "active_return",
            "strategy_max_drawdown",
            "end_nav",
            "benchmark_end_nav",
        ),
    )
    walkforward_table_path = _write_csv(
        diagnostics_dir / "walkforward_window_diagnostics.csv",
        walkforward_rows,
        fieldnames=(
            "window_id",
            "train_start",
            "train_end",
            "test_start",
            "test_end",
            "test_size",
            "cumulative_return",
            "benchmark_cumulative_return",
            "relative_return",
            "annualized_return",
            "annualized_volatility",
            "max_drawdown",
            "sharpe_ratio",
            "turnover",
            "outcome_label",
        ),
    )
    symbol_exposure_path = _write_csv(
        diagnostics_dir / "symbol_exposure_summary.csv",
        symbol_rows,
        fieldnames=(
            "symbol",
            "held_days",
            "held_ratio",
            "average_weight",
            "average_weight_when_held",
            "max_weight",
            "latest_weight",
            "average_signal_score_when_held",
        ),
    )
    signal_diagnostics_path = _write_csv(
        diagnostics_dir / "signal_diagnostics.csv",
        signal_diagnostic_rows,
        fieldnames=(
            "symbol",
            "observation_days",
            "positive_days",
            "negative_days",
            "inactive_days",
            "positive_ratio",
            "negative_ratio",
            "inactive_ratio",
            "average_signal_score",
            "average_abs_signal_score",
            "volatility_filter_pass_ratio",
            "volatility_suppressed_days",
            "volatility_suppressed_ratio",
        ),
    )

    backtest_metrics = compute_metrics(nav_rows, initial_nav=config.backtest.initial_nav)
    walkforward_metrics = compute_metrics(
        walkforward_nav_rows,
        initial_nav=config.backtest.initial_nav,
    )
    key_findings = build_key_findings(
        yearly_summary=yearly_summary,
        walkforward_summary=walkforward_summary,
        exposure_summary=exposure_summary,
        signal_summary=signal_summary,
        backtest_metrics=backtest_metrics,
        walkforward_metrics=walkforward_metrics,
    )

    report_path = report_dir / "diagnostics_summary.md"
    _write_diagnostics_report(
        path=report_path,
        config=config,
        backtest_metrics=backtest_metrics,
        backtest_final_nav=nav_rows[-1].nav,
        backtest_benchmark_final_nav=nav_rows[-1].benchmark_nav,
        walkforward_metrics=walkforward_metrics,
        walkforward_final_nav=walkforward_nav_rows[-1].nav,
        walkforward_benchmark_final_nav=walkforward_nav_rows[-1].benchmark_nav,
        walkforward_start_date=walkforward_nav_rows[0].date,
        walkforward_end_date=walkforward_nav_rows[-1].date,
        yearly_rows=yearly_rows,
        yearly_summary=yearly_summary,
        walkforward_rows=walkforward_rows,
        walkforward_summary=walkforward_summary,
        symbol_rows=symbol_rows,
        exposure_summary=exposure_summary,
        signal_rows=signal_diagnostic_rows,
        signal_summary=signal_summary,
        diagnostics_dir=diagnostics_dir,
        key_findings=key_findings,
    )

    return DiagnosticsResult(
        project_name=config.project_name,
        start_date=config.start_date,
        end_date=config.end_date,
        report_path=report_path,
        yearly_table_path=yearly_table_path,
        walkforward_table_path=walkforward_table_path,
        symbol_exposure_path=symbol_exposure_path,
        signal_diagnostics_path=signal_diagnostics_path,
        key_findings=tuple(key_findings),
    )


def load_nav_rows(path: Path) -> list[NavRow]:
    if not path.exists():
        raise FileNotFoundError(f"NAV artifact not found: {path}")

    rows: list[NavRow] = []
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for raw_row in reader:
            rows.append(
                NavRow(
                    date=date.fromisoformat(raw_row["date"]),
                    nav=float(raw_row["nav"]),
                    daily_return=float(raw_row["daily_return"]),
                    gross_return=float(raw_row["gross_return"]),
                    benchmark_nav=float(raw_row["benchmark_nav"]),
                    benchmark_return=float(raw_row["benchmark_return"]),
                    turnover=float(raw_row["turnover"]),
                    transaction_cost=float(raw_row["transaction_cost"]),
                    cash_weight=float(raw_row["cash_weight"]),
                )
            )
    if not rows:
        raise ValueError(f"NAV artifact is empty: {path}")
    return rows


def load_weight_rows(path: Path) -> list[PortfolioWeightRow]:
    if not path.exists():
        raise FileNotFoundError(f"Weights artifact not found: {path}")

    rows: list[PortfolioWeightRow] = []
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for raw_row in reader:
            rows.append(
                PortfolioWeightRow(
                    date=date.fromisoformat(raw_row["date"]),
                    symbol=raw_row["symbol"],
                    weight=float(raw_row["weight"]),
                    signal_score=(
                        None if raw_row["signal_score"] == "" else float(raw_row["signal_score"])
                    ),
                )
            )
    if not rows:
        raise ValueError(f"Weights artifact is empty: {path}")
    return rows


def load_walkforward_window_rows(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        raise FileNotFoundError(f"Walk-forward window artifact not found: {path}")

    rows: list[dict[str, object]] = []
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for raw_row in reader:
            rows.append(
                {
                    "window_id": int(raw_row["window_id"]),
                    "train_start": date.fromisoformat(raw_row["train_start"]),
                    "train_end": date.fromisoformat(raw_row["train_end"]),
                    "test_start": date.fromisoformat(raw_row["test_start"]),
                    "test_end": date.fromisoformat(raw_row["test_end"]),
                    "train_size": int(raw_row["train_size"]),
                    "test_size": int(raw_row["test_size"]),
                    "final_nav": float(raw_row["final_nav"]),
                    "benchmark_final_nav": float(raw_row["benchmark_final_nav"]),
                    "cumulative_return": float(raw_row["cumulative_return"]),
                    "annualized_return": float(raw_row["annualized_return"]),
                    "annualized_volatility": float(raw_row["annualized_volatility"]),
                    "max_drawdown": float(raw_row["max_drawdown"]),
                    "sharpe_ratio": float(raw_row["sharpe_ratio"]),
                    "turnover": float(raw_row["turnover"]),
                }
            )
    if not rows:
        raise ValueError(f"Walk-forward window artifact is empty: {path}")
    return rows


def build_yearly_return_rows(nav_rows: list[NavRow]) -> tuple[list[dict[str, str]], dict[str, object]]:
    grouped: dict[int, list[NavRow]] = defaultdict(list)
    for row in nav_rows:
        grouped[row.date.year].append(row)

    rows: list[dict[str, str]] = []
    metrics_by_year: list[dict[str, float | int]] = []
    for year in sorted(grouped):
        year_rows = grouped[year]
        strategy_return = _compound_returns([row.daily_return for row in year_rows])
        benchmark_return = _compound_returns([row.benchmark_return for row in year_rows])
        active_return = strategy_return - benchmark_return
        year_drawdown = _max_drawdown_from_nav([row.nav for row in year_rows])
        row = {
            "year": str(year),
            "strategy_return": f"{strategy_return}",
            "benchmark_return": f"{benchmark_return}",
            "active_return": f"{active_return}",
            "strategy_max_drawdown": f"{year_drawdown}",
            "end_nav": f"{year_rows[-1].nav}",
            "benchmark_end_nav": f"{year_rows[-1].benchmark_nav}",
        }
        rows.append(row)
        metrics_by_year.append(
            {
                "year": year,
                "strategy_return": strategy_return,
                "benchmark_return": benchmark_return,
                "active_return": active_return,
                "strategy_max_drawdown": year_drawdown,
            }
        )

    best_year = max(metrics_by_year, key=lambda item: item["strategy_return"])
    worst_year = min(metrics_by_year, key=lambda item: item["strategy_return"])
    benchmark_underperformance_years = sum(
        1 for item in metrics_by_year if item["active_return"] < -EPSILON
    )
    summary = {
        "year_count": len(metrics_by_year),
        "best_year": best_year,
        "worst_year": worst_year,
        "benchmark_underperformance_years": benchmark_underperformance_years,
        "benchmark_underperformance_ratio": (
            benchmark_underperformance_years / len(metrics_by_year) if metrics_by_year else 0.0
        ),
    }
    return rows, summary


def build_walkforward_window_diagnostics(
    walkforward_rows: list[dict[str, object]],
) -> tuple[list[dict[str, str]], dict[str, object]]:
    rows: list[dict[str, str]] = []
    returns: list[float] = []
    relative_returns: list[float] = []
    positive_count = 0
    flat_count = 0
    negative_count = 0

    for row in walkforward_rows:
        cumulative_return = float(row["cumulative_return"])
        benchmark_cumulative_return = float(row["benchmark_final_nav"]) - 1.0
        relative_return = cumulative_return - benchmark_cumulative_return
        outcome_label = _outcome_label(cumulative_return)
        if outcome_label == "positive":
            positive_count += 1
        elif outcome_label == "flat":
            flat_count += 1
        else:
            negative_count += 1

        returns.append(cumulative_return)
        relative_returns.append(relative_return)
        rows.append(
            {
                "window_id": str(row["window_id"]),
                "train_start": _isoformat(row["train_start"]),
                "train_end": _isoformat(row["train_end"]),
                "test_start": _isoformat(row["test_start"]),
                "test_end": _isoformat(row["test_end"]),
                "test_size": str(row["test_size"]),
                "cumulative_return": f"{cumulative_return}",
                "benchmark_cumulative_return": f"{benchmark_cumulative_return}",
                "relative_return": f"{relative_return}",
                "annualized_return": f"{row['annualized_return']}",
                "annualized_volatility": f"{row['annualized_volatility']}",
                "max_drawdown": f"{row['max_drawdown']}",
                "sharpe_ratio": f"{row['sharpe_ratio']}",
                "turnover": f"{row['turnover']}",
                "outcome_label": outcome_label,
            }
        )

    best_window = max(walkforward_rows, key=lambda item: float(item["cumulative_return"]))
    worst_window = min(walkforward_rows, key=lambda item: float(item["cumulative_return"]))
    summary = {
        "window_count": len(walkforward_rows),
        "positive_count": positive_count,
        "flat_count": flat_count,
        "negative_count": negative_count,
        "positive_ratio": positive_count / len(walkforward_rows),
        "mean_return": fmean(returns),
        "median_return": median(returns),
        "return_std": float(pstdev(returns)) if len(returns) > 1 else 0.0,
        "mean_relative_return": fmean(relative_returns),
        "best_window": best_window,
        "worst_window": worst_window,
    }
    return rows, summary


def build_symbol_exposure_summary(
    weight_rows: list[PortfolioWeightRow],
    nav_rows: list[NavRow],
    tradable_symbols: tuple[str, ...],
) -> tuple[list[dict[str, str]], dict[str, object]]:
    by_symbol: dict[str, list[PortfolioWeightRow]] = defaultdict(list)
    by_date: dict[date, list[PortfolioWeightRow]] = defaultdict(list)
    for row in weight_rows:
        by_symbol[row.symbol].append(row)
        by_date[row.date].append(row)

    total_days = len(by_date)
    rows: list[dict[str, str]] = []
    for symbol in tradable_symbols:
        symbol_rows = by_symbol[symbol]
        held_rows = [row for row in symbol_rows if row.weight > EPSILON]
        avg_signal_score_when_held = (
            fmean(row.signal_score for row in held_rows if row.signal_score is not None)
            if held_rows and any(row.signal_score is not None for row in held_rows)
            else 0.0
        )
        rows.append(
            {
                "symbol": symbol,
                "held_days": f"{len(held_rows)}",
                "held_ratio": f"{len(held_rows) / total_days if total_days else 0.0}",
                "average_weight": f"{fmean(row.weight for row in symbol_rows) if symbol_rows else 0.0}",
                "average_weight_when_held": f"{fmean(row.weight for row in held_rows) if held_rows else 0.0}",
                "max_weight": f"{max((row.weight for row in symbol_rows), default=0.0)}",
                "latest_weight": f"{symbol_rows[-1].weight if symbol_rows else 0.0}",
                "average_signal_score_when_held": f"{avg_signal_score_when_held}",
            }
        )

    active_position_counts: list[int] = []
    single_name_days = 0
    inactive_days = 0
    effective_positions: list[float] = []
    for day_rows in by_date.values():
        active_weights = [row.weight for row in day_rows if row.weight > EPSILON]
        active_count = len(active_weights)
        active_position_counts.append(active_count)
        if active_count == 0:
            inactive_days += 1
            effective_positions.append(0.0)
        else:
            if active_count == 1:
                single_name_days += 1
            effective_positions.append(_effective_positions(active_weights))

    cash_weights = [row.cash_weight for row in nav_rows]
    high_cash_days = sum(1 for value in cash_weights if value >= 0.95)
    summary = {
        "total_days": total_days,
        "average_active_positions": fmean(active_position_counts) if active_position_counts else 0.0,
        "single_name_days_ratio": single_name_days / total_days if total_days else 0.0,
        "inactive_days_ratio": inactive_days / total_days if total_days else 0.0,
        "average_cash_weight": fmean(cash_weights) if cash_weights else 0.0,
        "high_cash_days_ratio": high_cash_days / len(cash_weights) if cash_weights else 0.0,
        "average_effective_positions": fmean(effective_positions) if effective_positions else 0.0,
        "dominant_symbol": max(rows, key=lambda item: float(item["held_ratio"])),
    }
    return rows, summary


def build_signal_diagnostics(
    signal_rows: list[SignalRow],
    tradable_symbols: tuple[str, ...],
) -> tuple[list[dict[str, str]], dict[str, object]]:
    by_symbol: dict[str, list[SignalRow]] = defaultdict(list)
    for row in signal_rows:
        by_symbol[row.symbol].append(row)

    rows: list[dict[str, str]] = []
    symbol_summaries: list[dict[str, float | str]] = []
    for symbol in tradable_symbols:
        symbol_rows = by_symbol[symbol]
        observation_days = len(symbol_rows)
        positive_days = sum(1 for row in symbol_rows if row.signal_score > EPSILON)
        negative_days = sum(1 for row in symbol_rows if row.signal_score < -EPSILON)
        inactive_days = observation_days - positive_days - negative_days
        suppressed_days = sum(
            1
            for row in symbol_rows
            if ((row.trend_signal + row.momentum_signal) / 2.0) != 0.0 and row.volatility_filter == 0
        )
        volatility_filter_pass_days = sum(1 for row in symbol_rows if row.volatility_filter == 1)
        average_signal_score = fmean(row.signal_score for row in symbol_rows) if symbol_rows else 0.0
        average_abs_signal_score = (
            fmean(abs(row.signal_score) for row in symbol_rows) if symbol_rows else 0.0
        )
        rows.append(
            {
                "symbol": symbol,
                "observation_days": f"{observation_days}",
                "positive_days": f"{positive_days}",
                "negative_days": f"{negative_days}",
                "inactive_days": f"{inactive_days}",
                "positive_ratio": f"{positive_days / observation_days if observation_days else 0.0}",
                "negative_ratio": f"{negative_days / observation_days if observation_days else 0.0}",
                "inactive_ratio": f"{inactive_days / observation_days if observation_days else 0.0}",
                "average_signal_score": f"{average_signal_score}",
                "average_abs_signal_score": f"{average_abs_signal_score}",
                "volatility_filter_pass_ratio": f"{volatility_filter_pass_days / observation_days if observation_days else 0.0}",
                "volatility_suppressed_days": f"{suppressed_days}",
                "volatility_suppressed_ratio": f"{suppressed_days / observation_days if observation_days else 0.0}",
            }
        )
        symbol_summaries.append(
            {
                "symbol": symbol,
                "positive_ratio": positive_days / observation_days if observation_days else 0.0,
                "inactive_ratio": inactive_days / observation_days if observation_days else 0.0,
                "average_signal_score": average_signal_score,
                "volatility_suppressed_ratio": (
                    suppressed_days / observation_days if observation_days else 0.0
                ),
            }
        )

    dominant_symbol = max(symbol_summaries, key=lambda item: item["average_signal_score"])
    summary = {
        "symbol_count": len(symbol_summaries),
        "dominant_symbol": dominant_symbol,
        "average_positive_ratio": fmean(
            float(row["positive_ratio"]) for row in rows
        ) if rows else 0.0,
        "average_inactive_ratio": fmean(
            float(row["inactive_ratio"]) for row in rows
        ) if rows else 0.0,
        "average_volatility_suppressed_ratio": fmean(
            float(row["volatility_suppressed_ratio"]) for row in rows
        ) if rows else 0.0,
    }
    return rows, summary


def build_key_findings(
    yearly_summary: dict[str, object],
    walkforward_summary: dict[str, object],
    exposure_summary: dict[str, object],
    signal_summary: dict[str, object],
    backtest_metrics: PerformanceMetrics,
    walkforward_metrics: PerformanceMetrics,
) -> list[str]:
    findings: list[str] = []
    if backtest_metrics.max_drawdown <= -0.5 and backtest_metrics.annualized_return < 0.05:
        findings.append(
            "策略在長歷史下呈現低報酬但極深回撤，風險報酬結構明顯失衡。"
        )
    if float(walkforward_summary["positive_ratio"]) < 0.5 and float(
        yearly_summary["benchmark_underperformance_ratio"]
    ) > 0.5:
        findings.append(
            "弱勢看起來比較像結構性問題，而不是只集中在單一市場 regime。"
        )
    if float(exposure_summary["high_cash_days_ratio"]) > 0.3:
        findings.append(
            f"策略有 {float(exposure_summary['high_cash_days_ratio']):.1%} 的交易日接近高現金狀態，資金使用率偏低。"
        )
    dominant_symbol = exposure_summary["dominant_symbol"]
    findings.append(
        f"{dominant_symbol['symbol']} 是持有天數最高的標的，持有比率約為 {float(dominant_symbol['held_ratio']):.1%}。"
    )
    if float(signal_summary["average_volatility_suppressed_ratio"]) > 0.2:
        findings.append(
            f"volatility filter 平均抑制了約 {float(signal_summary['average_volatility_suppressed_ratio']):.1%} 的方向性訊號。"
        )
    best_year = yearly_summary["best_year"]
    worst_year = yearly_summary["worst_year"]
    findings.append(
        f"最佳年度是 {best_year['year']} 年，最差年度是 {worst_year['year']} 年。"
    )
    if walkforward_metrics.cumulative_return < 0.15:
        findings.append("walk-forward 樣本外累積報酬偏弱，顯示目前規則在 OOS 也缺乏穩定優勢。")
    return findings


def _write_diagnostics_report(
    path: Path,
    config: BacktestConfig,
    backtest_metrics: PerformanceMetrics,
    backtest_final_nav: float,
    backtest_benchmark_final_nav: float,
    walkforward_metrics: PerformanceMetrics,
    walkforward_final_nav: float,
    walkforward_benchmark_final_nav: float,
    walkforward_start_date: date,
    walkforward_end_date: date,
    yearly_rows: list[dict[str, str]],
    yearly_summary: dict[str, object],
    walkforward_rows: list[dict[str, str]],
    walkforward_summary: dict[str, object],
    symbol_rows: list[dict[str, str]],
    exposure_summary: dict[str, object],
    signal_rows: list[dict[str, str]],
    signal_summary: dict[str, object],
    diagnostics_dir: Path,
    key_findings: list[str],
) -> None:
    best_year = yearly_summary["best_year"]
    worst_year = yearly_summary["worst_year"]
    best_window = walkforward_summary["best_window"]
    worst_window = walkforward_summary["worst_window"]
    dominant_signal_symbol = signal_summary["dominant_symbol"]
    dominant_held_symbol = exposure_summary["dominant_symbol"]

    content = "\n".join(
        [
            f"# {config.project_name} Diagnostics Summary",
            "",
            "## Diagnostic Scope",
            "",
            "- This report analyzes existing local artifacts only. It does not change the strategy, signals, portfolio rules, backtest engine, or walk-forward design.",
            "- Inputs used: backtest NAV, daily weights, walk-forward window summary, and the persisted signal panel.",
            f"- Analysis Period: {config.start_date.isoformat()} to {config.end_date.isoformat()}",
            "",
            "## Headline Context",
            "",
            f"- Backtest Final NAV: {backtest_final_nav:.6f}",
            f"- Backtest Benchmark Final NAV: {backtest_benchmark_final_nav:.6f}",
            f"- Backtest Cumulative Return: {backtest_metrics.cumulative_return:.4%}",
            f"- Backtest Annualized Return: {backtest_metrics.annualized_return:.4%}",
            f"- Backtest Annualized Volatility: {backtest_metrics.annualized_volatility:.4%}",
            f"- Backtest Sharpe Ratio: {backtest_metrics.sharpe_ratio:.4f}",
            f"- Backtest Max Drawdown: {backtest_metrics.max_drawdown:.4%}",
            f"- Walk-Forward OOS Period: {walkforward_start_date.isoformat()} to {walkforward_end_date.isoformat()}",
            f"- Walk-Forward Final NAV: {walkforward_final_nav:.6f}",
            f"- Walk-Forward Benchmark Final NAV: {walkforward_benchmark_final_nav:.6f}",
            f"- Walk-Forward Cumulative Return: {walkforward_metrics.cumulative_return:.4%}",
            f"- Walk-Forward Annualized Return: {walkforward_metrics.annualized_return:.4%}",
            f"- Walk-Forward Annualized Volatility: {walkforward_metrics.annualized_volatility:.4%}",
            f"- Walk-Forward Sharpe Ratio: {walkforward_metrics.sharpe_ratio:.4f}",
            f"- Walk-Forward Max Drawdown: {walkforward_metrics.max_drawdown:.4%}",
            "",
            "## Major Findings",
            "",
            *(f"- {finding}" for finding in key_findings),
            "",
            "## Walk-Forward Diagnostics",
            "",
            f"- Windows Evaluated: {walkforward_summary['window_count']}",
            f"- Positive Windows: {walkforward_summary['positive_count']} ({float(walkforward_summary['positive_ratio']):.1%})",
            f"- Flat Windows: {walkforward_summary['flat_count']}",
            f"- Negative Windows: {walkforward_summary['negative_count']}",
            f"- Mean Window Return: {float(walkforward_summary['mean_return']):.4%}",
            f"- Median Window Return: {float(walkforward_summary['median_return']):.4%}",
            f"- Mean Relative Return vs Benchmark: {float(walkforward_summary['mean_relative_return']):.4%}",
            f"- Best Window: #{best_window['window_id']} ({best_window['test_start'].isoformat()} to {best_window['test_end'].isoformat()}) -> {float(best_window['cumulative_return']):.4%}",
            f"- Worst Window: #{worst_window['window_id']} ({worst_window['test_start'].isoformat()} to {worst_window['test_end'].isoformat()}) -> {float(worst_window['cumulative_return']):.4%}",
            "",
            "## Time-Based Performance Breakdown",
            "",
            f"- Best Year: {best_year['year']} -> {float(best_year['strategy_return']):.4%}",
            f"- Worst Year: {worst_year['year']} -> {float(worst_year['strategy_return']):.4%}",
            f"- Benchmark Underperformance Years: {yearly_summary['benchmark_underperformance_years']} / {yearly_summary['year_count']}",
            "",
            _markdown_table(
                title="Top 3 Years by Strategy Return",
                headers=("Year", "Strategy Return", "Benchmark Return", "Active Return"),
                rows=_top_n_year_rows(yearly_rows, 3, reverse=True),
            ),
            "",
            _markdown_table(
                title="Bottom 3 Years by Strategy Return",
                headers=("Year", "Strategy Return", "Benchmark Return", "Active Return"),
                rows=_top_n_year_rows(yearly_rows, 3, reverse=False),
            ),
            "",
            "## Symbol / Exposure Diagnostics",
            "",
            f"- Dominant Held Symbol: {dominant_held_symbol['symbol']} (held on {float(dominant_held_symbol['held_ratio']):.1%} of days)",
            f"- Average Active Positions: {float(exposure_summary['average_active_positions']):.2f}",
            f"- Average Effective Positions: {float(exposure_summary['average_effective_positions']):.2f}",
            f"- Single-Name Days Ratio: {float(exposure_summary['single_name_days_ratio']):.1%}",
            f"- Inactive Days Ratio: {float(exposure_summary['inactive_days_ratio']):.1%}",
            f"- Average Cash Weight: {float(exposure_summary['average_cash_weight']):.1%}",
            f"- High Cash Days Ratio (cash >= 95%): {float(exposure_summary['high_cash_days_ratio']):.1%}",
            "",
            _markdown_table(
                title="Per-Symbol Exposure Summary",
                headers=("Symbol", "Held Ratio", "Avg Weight", "Avg Weight When Held", "Max Weight"),
                rows=[
                    (
                        row["symbol"],
                        _as_percent(row["held_ratio"]),
                        _as_percent(row["average_weight"]),
                        _as_percent(row["average_weight_when_held"]),
                        _as_percent(row["max_weight"]),
                    )
                    for row in symbol_rows
                ],
            ),
            "",
            "## Signal Diagnostics",
            "",
            f"- Dominant Signal Symbol: {dominant_signal_symbol['symbol']} (average signal score {float(dominant_signal_symbol['average_signal_score']):.3f})",
            f"- Average Positive Signal Ratio: {float(signal_summary['average_positive_ratio']):.1%}",
            f"- Average Inactive Signal Ratio: {float(signal_summary['average_inactive_ratio']):.1%}",
            f"- Average Volatility-Suppressed Ratio: {float(signal_summary['average_volatility_suppressed_ratio']):.1%}",
            "",
            _markdown_table(
                title="Per-Symbol Signal Summary",
                headers=(
                    "Symbol",
                    "Positive Ratio",
                    "Inactive Ratio",
                    "Avg Signal Score",
                    "Vol Suppressed Ratio",
                ),
                rows=[
                    (
                        row["symbol"],
                        _as_percent(row["positive_ratio"]),
                        _as_percent(row["inactive_ratio"]),
                        f"{float(row['average_signal_score']):.3f}",
                        _as_percent(row["volatility_suppressed_ratio"]),
                    )
                    for row in signal_rows
                ],
            ),
            "",
            "## Interpretation",
            "",
            _interpretation_text(
                yearly_summary=yearly_summary,
                walkforward_summary=walkforward_summary,
                exposure_summary=exposure_summary,
                signal_summary=signal_summary,
            ),
            "",
            "## Output Artifacts",
            "",
            f"- Yearly Return Table: {diagnostics_dir / 'yearly_return_table.csv'}",
            f"- Walk-Forward Window Diagnostics: {diagnostics_dir / 'walkforward_window_diagnostics.csv'}",
            f"- Symbol Exposure Summary: {diagnostics_dir / 'symbol_exposure_summary.csv'}",
            f"- Signal Diagnostics: {diagnostics_dir / 'signal_diagnostics.csv'}",
            f"- Report: {path}",
            "",
            "## Diagnostic Limitations",
            "",
            "- This layer explains the behavior of the current strategy outputs, but it does not test alternative rules or parameter sets.",
            "- The analysis is based on persisted daily artifacts, so it inherits any simplifications in the current transaction-cost and benchmark workflow.",
            "- The report can identify likely failure modes, but not prove causal market microstructure mechanisms on its own.",
            "",
        ]
    )
    path.write_text(content, encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, str]], fieldnames: tuple[str, ...]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fieldnames))
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return path


def _compound_returns(returns: list[float]) -> float:
    return prod(1.0 + value for value in returns) - 1.0 if returns else 0.0


def _max_drawdown_from_nav(nav_values: list[float]) -> float:
    if not nav_values:
        return 0.0
    running_max = nav_values[0]
    max_drawdown = 0.0
    for nav in nav_values:
        running_max = max(running_max, nav)
        max_drawdown = min(max_drawdown, (nav / running_max) - 1.0)
    return max_drawdown


def _effective_positions(weights: list[float]) -> float:
    if not weights:
        return 0.0
    denominator = sum(weight * weight for weight in weights)
    if denominator <= EPSILON:
        return 0.0
    numerator = sum(weights) ** 2
    return numerator / denominator


def _outcome_label(value: float) -> str:
    if value > EPSILON:
        return "positive"
    if value < -EPSILON:
        return "negative"
    return "flat"


def _top_n_year_rows(
    yearly_rows: list[dict[str, str]],
    count: int,
    reverse: bool,
) -> list[tuple[str, str, str, str]]:
    ordered = sorted(
        yearly_rows,
        key=lambda row: float(row["strategy_return"]),
        reverse=reverse,
    )[:count]
    return [
        (
            row["year"],
            _as_percent(row["strategy_return"]),
            _as_percent(row["benchmark_return"]),
            _as_percent(row["active_return"]),
        )
        for row in ordered
    ]


def _markdown_table(
    title: str,
    headers: tuple[str, ...],
    rows: list[tuple[str, ...]],
) -> str:
    lines = [f"### {title}", "", "| " + " | ".join(headers) + " |", "| " + " | ".join("---" for _ in headers) + " |"]
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines)


def _interpretation_text(
    yearly_summary: dict[str, object],
    walkforward_summary: dict[str, object],
    exposure_summary: dict[str, object],
    signal_summary: dict[str, object],
) -> str:
    structural_weakness = (
        float(walkforward_summary["positive_ratio"]) < 0.5
        and float(yearly_summary["benchmark_underperformance_ratio"]) > 0.5
    )
    inactivity = float(exposure_summary["high_cash_days_ratio"]) > 0.3
    suppression = float(signal_summary["average_volatility_suppressed_ratio"]) > 0.2

    parts = []
    if structural_weakness:
        parts.append(
            "The weakness appears more structural than regime-specific: too many yearly outcomes trail the benchmark and fewer than half of OOS windows are positive."
        )
    else:
        parts.append(
            "The weakness looks at least partly regime-specific: some windows and years remain positive, although the overall profile is still weak."
        )
    if inactivity:
        parts.append(
            "A large share of high-cash or inactive days suggests the strategy often fails to maintain productive exposure."
        )
    if suppression:
        parts.append(
            "The volatility filter materially reduces directional signal participation, which may be limiting trade frequency and compounding."
        )
    parts.append(
        "Even when the strategy does participate, the resulting return path still exhibits very deep drawdowns, so the current issue is not only inactivity."
    )
    return " ".join(parts)


def _isoformat(value: object) -> str:
    return value.isoformat() if isinstance(value, date) else str(value)


def _as_percent(raw_value: str) -> str:
    return f"{float(raw_value):.2%}"
