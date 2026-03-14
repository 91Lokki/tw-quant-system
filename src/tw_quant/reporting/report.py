"""Generate markdown reports for backtest runs."""

from __future__ import annotations

from tw_quant.core.models import BacktestResult, WalkForwardResult
from tw_quant.reporting.charts import write_backtest_charts


def build_report(result: BacktestResult) -> str:
    """Write a markdown summary of the backtest run and return its path."""

    result.report_path.parent.mkdir(parents=True, exist_ok=True)
    write_backtest_charts(
        nav_path=result.nav_path,
        equity_curve_path=result.equity_curve_path,
        drawdown_path=result.drawdown_path,
    )

    notes = "\n".join(f"- {note}" for note in result.notes) or "- No notes recorded."
    tradable_symbols = _format_symbol_preview(result.tradable_symbols)
    strategy_notes = "\n".join(_strategy_logic_lines(result))
    limitation_notes = "\n".join(_limitation_lines())
    output_artifact_lines = [
        f"- NAV CSV: {result.nav_path}",
        f"- Weights CSV: {result.weights_path}",
        f"- Report: {result.report_path}",
        f"- Equity Curve Chart: {result.equity_curve_path}",
        f"- Drawdown Chart: {result.drawdown_path}",
    ]
    if result.comparison_path is not None:
        output_artifact_lines.append(f"- Risk Comparison CSV: {result.comparison_path}")
    comparison_focus_section = []
    if result.comparison_path is not None:
        comparison_focus_section = [
            "## Comparison Focus",
            "",
            "- Comparison Focus: `original_monthly` is the pure-alpha benchmark line.",
            "- Current Practical Candidate: `risk_controlled_3m_half_exposure_exp60` is the preferred practical line.",
            "- Phase G Practical Robustness Checks: `risk_controlled_3m_half_exposure_exp60_delay1`, `risk_controlled_3m_half_exposure_exp60_delay3`, and `risk_controlled_3m_half_exposure_exp60_w08` stress implementation timing and concentration sensitivity.",
            "",
        ]
    content = "\n".join(
        [
            f"# {result.project_name} Backtest Summary",
            "",
            "## Run Overview",
            "",
            f"- Project Name: {result.project_name}",
            f"- Market: {result.market}",
            f"- Universe: {result.universe}",
            f"- Tradable Symbols: {tradable_symbols}",
            f"- Benchmark: {result.benchmark}",
            f"- Run Period: {result.start_date.isoformat()} to {result.end_date.isoformat()}",
            f"- Rebalance Frequency: {result.rebalance_frequency}",
            f"- Rebalance Cadence: every {result.rebalance_cadence_months} month(s)",
            f"- Benchmark Regime Filter: {'enabled' if result.benchmark_filter_enabled else 'disabled'}",
            f"- Benchmark MA Window: {result.benchmark_ma_window} trading days",
            f"- Defensive Mode: {result.defensive_mode}",
            f"- Defensive Gross Exposure: {result.defensive_gross_exposure:.0%}",
            f"- Execution Delay Days: {result.execution_delay_days}",
            f"- Portfolio Max Weight: {result.portfolio_max_weight:.0%}",
            f"- Transaction Costs: commission {result.trading_costs.commission_bps:.2f} bps, tax {result.trading_costs.tax_bps:.2f} bps, slippage {result.trading_costs.slippage_bps:.2f} bps",
            f"- Status: {result.status}",
            f"- Final NAV: {result.final_nav:.6f}",
            f"- Benchmark Final NAV: {result.benchmark_final_nav:.6f}",
            "",
            *comparison_focus_section,
            "## Strategy Logic",
            "",
            strategy_notes,
            "",
            "## Performance Metrics",
            "",
            f"- Cumulative Return: {result.metrics.cumulative_return:.4%}",
            f"- Annualized Return: {result.metrics.annualized_return:.4%}",
            f"- Annualized Volatility: {result.metrics.annualized_volatility:.4%}",
            f"- Max Drawdown: {result.metrics.max_drawdown:.4%}",
            f"- Sharpe Ratio: {result.metrics.sharpe_ratio:.4f}",
            f"- Turnover: {result.metrics.turnover:.6f}",
            "",
            "## Output Artifacts",
            "",
            *output_artifact_lines,
            "",
            "## Charts",
            "",
            "### Equity Curve",
            "",
            f"![Equity Curve]({result.equity_curve_path.name})",
            "",
            "### Drawdown",
            "",
            f"![Drawdown]({result.drawdown_path.name})",
            "",
            "## Notes",
            "",
            notes,
            "",
            "## Known Limitations",
            "",
            limitation_notes,
            "",
        ]
    )
    result.report_path.write_text(content, encoding="utf-8")
    return str(result.report_path)


def _strategy_logic_lines(result: BacktestResult) -> tuple[str, ...]:
    cash_behavior = (
        "holds cash when no tradable symbol clears the signal threshold."
        if result.hold_cash_when_inactive
        else "keeps exposure in the configured tradable universe even when signals are weak."
    )
    lines = [
        "- Uses the locally generated signal panel as the portfolio input for a long-only daily backtest.",
        f"- Rebalances on the first aligned trading day of each {result.rebalance_frequency} period and equal-weights selected symbols.",
        f"- Applies new target weights after the signal-day close with an extra {result.execution_delay_days} trading-day execution delay; the default `0` means the next trading day remains the first active day.",
        "- Treats the benchmark as a comparison series, not as a directly held portfolio asset.",
        f"- Caps single-name weights at {result.portfolio_max_weight:.0%}.",
        f"- Current cash behavior: {cash_behavior}",
    ]
    if result.benchmark_filter_enabled:
        lines.append(
            f"- Adds a benchmark regime gate: only holds risk positions when {result.benchmark} closes above its {result.benchmark_ma_window}-day moving average."
        )
        lines.append(
            "- Defensive behavior when the regime filter is OFF: "
            f"{_describe_defensive_mode(result.defensive_mode, result.defensive_gross_exposure)}."
        )
    else:
        lines.append("- Benchmark regime gating is disabled in this run.")
    if result.rebalance_cadence_months > 1:
        lines.append(
            f"- Monthly universe membership is still computed, but the portfolio only refreshes every {result.rebalance_cadence_months} month(s)."
        )
    return tuple(lines)


def _limitation_lines() -> tuple[str, ...]:
    return (
        "- The v1 transaction cost model is bps-based only and does not model board-lot sizing, partial fills, or exchange microstructure.",
        "- The current portfolio construction rule is intentionally simple: long-only, signal-driven, and equal-weight.",
        "- The benchmark workflow relies on the normalized TAIEX proxy series and therefore does not include full benchmark OHLCV fields.",
        "- The project is a local research workflow; it does not include paper execution monitoring or broker connectivity yet.",
    )


def build_walkforward_report(result: WalkForwardResult) -> str:
    """Write a markdown summary for the walk-forward evaluation workflow."""

    result.report_path.parent.mkdir(parents=True, exist_ok=True)

    tradable_symbols = _format_symbol_preview(result.tradable_symbols)
    notes = "\n".join(f"- {note}" for note in result.notes) or "- No notes recorded."
    window_lines = _walkforward_window_table_lines(result)
    limitation_notes = "\n".join(_walkforward_limitation_lines())
    output_artifact_lines = [
        f"- Walk-Forward NAV CSV: {result.nav_path}",
        f"- Window Summary CSV: {result.window_summary_path}",
        f"- Report: {result.report_path}",
    ]
    if result.comparison_path is not None:
        output_artifact_lines.append(f"- Risk Comparison CSV: {result.comparison_path}")
    comparison_focus_section = []
    if result.comparison_path is not None:
        comparison_focus_section = [
            "## Comparison Focus",
            "",
            "- Comparison Focus: `original_monthly` remains the pure-alpha benchmark line.",
            "- Current Practical Candidate: `risk_controlled_3m_half_exposure_exp60` is the main practical line to inspect in OOS.",
            "- Phase G Practical Robustness Checks: `risk_controlled_3m_half_exposure_exp60_delay1`, `risk_controlled_3m_half_exposure_exp60_delay3`, and `risk_controlled_3m_half_exposure_exp60_w08` are compact implementation-realism checks.",
            "",
        ]
    content = "\n".join(
        [
            f"# {result.project_name} Walk-Forward Summary",
            "",
            "## Evaluation Design",
            "",
            f"- Project Name: {result.project_name}",
            f"- Market: {result.market}",
            f"- Universe: {result.universe}",
            f"- Tradable Symbols: {tradable_symbols}",
            f"- Benchmark: {result.benchmark}",
            f"- Window Type: {result.window_type}",
            f"- Train Window Length: {result.train_window_days} aligned trading days",
            f"- Test Window Length: {result.test_window_days} aligned trading days",
            f"- Minimum History Length: {result.minimum_history_days} aligned trading days",
            f"- Windows Evaluated: {result.window_count}",
            f"- Combined OOS Period: {result.start_date.isoformat()} to {result.end_date.isoformat()}",
            f"- Rebalance Frequency: {result.rebalance_frequency}",
            f"- Rebalance Cadence: every {result.rebalance_cadence_months} month(s)",
            f"- Benchmark Regime Filter: {'enabled' if result.benchmark_filter_enabled else 'disabled'}",
            f"- Benchmark MA Window: {result.benchmark_ma_window} trading days",
            f"- Defensive Mode: {result.defensive_mode}",
            f"- Defensive Gross Exposure: {result.defensive_gross_exposure:.0%}",
            f"- Execution Delay Days: {result.execution_delay_days}",
            f"- Portfolio Max Weight: {result.portfolio_max_weight:.0%}",
            f"- Transaction Costs: commission {result.trading_costs.commission_bps:.2f} bps, tax {result.trading_costs.tax_bps:.2f} bps, slippage {result.trading_costs.slippage_bps:.2f} bps",
            f"- Status: {result.status}",
            "",
            *comparison_focus_section,
            "## Combined Out-of-Sample Performance",
            "",
            f"- Final NAV: {result.final_nav:.6f}",
            f"- Benchmark Final NAV: {result.benchmark_final_nav:.6f}",
            f"- Cumulative Return: {result.metrics.cumulative_return:.4%}",
            f"- Annualized Return: {result.metrics.annualized_return:.4%}",
            f"- Annualized Volatility: {result.metrics.annualized_volatility:.4%}",
            f"- Max Drawdown: {result.metrics.max_drawdown:.4%}",
            f"- Sharpe Ratio: {result.metrics.sharpe_ratio:.4f}",
            f"- Turnover: {result.metrics.turnover:.6f}",
            "",
            "## Window Schedule",
            "",
            *window_lines,
            "",
            "## Output Artifacts",
            "",
            *output_artifact_lines,
            "",
            "## Notes",
            "",
            notes,
            "",
            "## Known Limitations",
            "",
            limitation_notes,
            "",
        ]
    )
    result.report_path.write_text(content, encoding="utf-8")
    return str(result.report_path)


def _walkforward_window_table_lines(result: WalkForwardResult) -> tuple[str, ...]:
    header = (
        "| Window | Train Range | Test Range | OOS Return | Annualized Return | "
        "Max Drawdown | Turnover |"
    )
    divider = "| --- | --- | --- | --- | --- | --- | --- |"
    rows = [header, divider]
    for window in result.windows:
        rows.append(
            "| "
            f"{window.window_id} | "
            f"{window.train_start.isoformat()} to {window.train_end.isoformat()} | "
            f"{window.test_start.isoformat()} to {window.test_end.isoformat()} | "
            f"{window.metrics.cumulative_return:.2%} | "
            f"{window.metrics.annualized_return:.2%} | "
            f"{window.metrics.max_drawdown:.2%} | "
            f"{window.metrics.turnover:.4f} |"
        )
    return tuple(rows)


def _walkforward_limitation_lines() -> tuple[str, ...]:
    return (
        "- The current walk-forward workflow evaluates a fixed-rule strategy; it does not refit model parameters inside each train window.",
        "- The OOS aggregation is portfolio-return based and intentionally simple, which keeps the workflow explainable but not yet feature-complete.",
        "- The benchmark view still relies on the normalized TAIEX proxy series rather than a full benchmark OHLCV history.",
        "- The project remains a local research system and does not include live execution or broker connectivity.",
    )


def _describe_defensive_mode(defensive_mode: str, gross_exposure: float) -> str:
    if defensive_mode == "cash":
        return "move fully to cash"
    if defensive_mode == "half_exposure":
        return f"keep the same ranking logic but reduce gross exposure to {gross_exposure:.0%}"
    if defensive_mode == "top5":
        return f"concentrate into the top-5 ranked names with {gross_exposure:.0%} gross exposure"
    return defensive_mode


def _format_symbol_preview(symbols: tuple[str, ...], limit: int = 12) -> str:
    if len(symbols) <= limit:
        return ", ".join(symbols)
    preview = ", ".join(symbols[:limit])
    return f"{preview}, ... (total {len(symbols)})"
