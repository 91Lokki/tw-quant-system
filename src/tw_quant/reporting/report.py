"""Generate lightweight report artifacts for scaffold runs."""

from __future__ import annotations

from tw_quant.core.models import BacktestResult


def build_report(result: BacktestResult) -> str:
    """Write a markdown summary of the scaffold run and return its path."""

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
            "",
            "## Notes",
            "",
            notes,
            "",
            "## Next Step",
            "",
            "Replace the scaffold placeholders with real data ingestion, signal generation, portfolio construction, and PnL accounting.",
            "",
        ]
    )
    result.report_path.write_text(content, encoding="utf-8")
    return str(result.report_path)
