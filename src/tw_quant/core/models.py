"""Small shared contracts for the v1 scaffold."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path


@dataclass(frozen=True, slots=True)
class DataPaths:
    project_root: Path
    raw_dir: Path
    processed_dir: Path
    reports_dir: Path


@dataclass(frozen=True, slots=True)
class TradingCosts:
    commission_bps: float
    tax_bps: float
    slippage_bps: float


@dataclass(frozen=True, slots=True)
class BacktestConfig:
    project_name: str
    market: str
    universe: str
    benchmark: str
    start_date: date
    end_date: date
    data_paths: DataPaths
    trading_costs: TradingCosts

    def date_range_label(self) -> str:
        return f"{self.start_date.isoformat()} to {self.end_date.isoformat()}"


@dataclass(frozen=True, slots=True)
class BacktestResult:
    project_name: str
    market: str
    universe: str
    benchmark: str
    start_date: date
    end_date: date
    report_path: Path
    status: str
    notes: tuple[str, ...]

    def summary_text(self) -> str:
        lines = [
            f"Project: {self.project_name}",
            f"Market: {self.market}",
            f"Universe: {self.universe}",
            f"Benchmark: {self.benchmark}",
            f"Date range: {self.start_date.isoformat()} to {self.end_date.isoformat()}",
            f"Status: {self.status}",
            f"Report: {self.report_path}",
        ]
        if self.notes:
            lines.append("Notes:")
            lines.extend(f"- {note}" for note in self.notes)
        return "\n".join(lines)
