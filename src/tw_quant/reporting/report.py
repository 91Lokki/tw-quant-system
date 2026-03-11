"""Generate markdown reports for backtest runs."""

from __future__ import annotations

from tw_quant.core.models import BacktestResult


def build_report(result: BacktestResult) -> str:
    """Write a markdown summary of the backtest run and return its path."""

    result.report_path.parent.mkdir(parents=True, exist_ok=True)
    notes = "\n".join(f"- {note}" for note in result.notes) or "- No notes recorded."
    content = "\n".join(
        [
            f"# {result.project_name} Backtest Summary",
            "",
            "## Run Scope",
            "",
            f"- Market: {result.market}",
            f"- Universe: {result.universe}",
            f"- Benchmark: {result.benchmark}",
            f"- Date range: {result.start_date.isoformat()} to {result.end_date.isoformat()}",
            f"- Status: {result.status}",
            f"- Final NAV: {result.final_nav:.6f}",
            f"- Benchmark Final NAV: {result.benchmark_final_nav:.6f}",
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
            "",
            "## Notes",
            "",
            notes,
            "",
        ]
    )
    result.report_path.write_text(content, encoding="utf-8")
    return str(result.report_path)
