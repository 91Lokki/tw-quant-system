"""Generate markdown reports for backtest runs."""

from __future__ import annotations

from tw_quant.core.models import BacktestResult
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
    tradable_symbols = ", ".join(result.tradable_symbols)
    strategy_notes = "\n".join(_strategy_logic_lines(result))
    limitation_notes = "\n".join(_limitation_lines())
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
            f"- Transaction Costs: commission {result.trading_costs.commission_bps:.2f} bps, tax {result.trading_costs.tax_bps:.2f} bps, slippage {result.trading_costs.slippage_bps:.2f} bps",
            f"- Status: {result.status}",
            f"- Final NAV: {result.final_nav:.6f}",
            f"- Benchmark Final NAV: {result.benchmark_final_nav:.6f}",
            "",
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
            f"- NAV CSV: {result.nav_path}",
            f"- Weights CSV: {result.weights_path}",
            f"- Report: {result.report_path}",
            f"- Equity Curve Chart: {result.equity_curve_path}",
            f"- Drawdown Chart: {result.drawdown_path}",
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
    return (
        "- Uses the locally generated signal panel as the portfolio input for a long-only daily backtest.",
        f"- Rebalances on the first aligned trading day of each {result.rebalance_frequency} period and equal-weights selected symbols.",
        "- Applies new target weights on the next trading day to avoid lookahead bias.",
        "- Treats the benchmark as a comparison series, not as a directly held portfolio asset.",
        f"- Current cash behavior: {cash_behavior}",
    )


def _limitation_lines() -> tuple[str, ...]:
    return (
        "- The v1 transaction cost model is bps-based only and does not model board-lot sizing, partial fills, or exchange microstructure.",
        "- The current portfolio construction rule is intentionally simple: long-only, signal-driven, and equal-weight.",
        "- The benchmark workflow relies on the normalized TAIEX proxy series and therefore does not include full benchmark OHLCV fields.",
        "- The project is a local research workflow; it does not include paper execution monitoring or broker connectivity yet.",
    )
