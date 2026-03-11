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
class IngestConfig:
    provider: str
    symbols: tuple[str, ...]
    benchmark: str
    refresh: bool
    storage_format: str
    raw_cache_dir: Path
    normalized_dir: Path
    token_env_var: str | None

    def requested_symbols(self) -> tuple[str, ...]:
        ordered: list[str] = []
        for symbol in (*self.symbols, self.benchmark):
            if symbol and symbol not in ordered:
                ordered.append(symbol)
        return tuple(ordered)


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
class AppConfig:
    project_name: str
    market: str
    universe: str
    benchmark: str
    start_date: date
    end_date: date
    data_paths: DataPaths
    trading_costs: TradingCosts
    ingest: IngestConfig

    def to_backtest_config(self) -> BacktestConfig:
        return BacktestConfig(
            project_name=self.project_name,
            market=self.market,
            universe=self.universe,
            benchmark=self.benchmark,
            start_date=self.start_date,
            end_date=self.end_date,
            data_paths=self.data_paths,
            trading_costs=self.trading_costs,
        )


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


@dataclass(frozen=True, slots=True)
class NormalizedBar:
    date: date
    symbol: str
    open: float
    high: float
    low: float
    close: float
    volume: int | None

    def to_csv_row(self) -> dict[str, str]:
        return {
            "date": self.date.isoformat(),
            "symbol": self.symbol,
            "open": f"{self.open}",
            "high": f"{self.high}",
            "low": f"{self.low}",
            "close": f"{self.close}",
            "volume": "" if self.volume is None else str(self.volume),
        }


@dataclass(frozen=True, slots=True)
class IngestedDataset:
    symbol: str
    dataset: str
    rows: int
    path: Path
    raw_path: Path | None
    from_cache: bool


@dataclass(frozen=True, slots=True)
class IngestResult:
    provider: str
    start_date: date
    end_date: date
    storage_format: str
    normalized_dir: Path
    datasets: tuple[IngestedDataset, ...]
    notes: tuple[str, ...]

    def summary_text_zh(self) -> str:
        cached = sum(1 for dataset in self.datasets if dataset.from_cache)
        fetched = len(self.datasets) - cached
        lines = [
            "資料擷取完成",
            f"提供者: {self.provider}",
            f"期間: {self.start_date.isoformat()} 至 {self.end_date.isoformat()}",
            f"輸出格式: {self.storage_format}",
            f"輸出目錄: {self.normalized_dir}",
            f"資料集數量: {len(self.datasets)}",
            f"快取命中: {cached}",
            f"重新抓取: {fetched}",
        ]
        for dataset in self.datasets:
            source_label = "快取" if dataset.from_cache else dataset.dataset
            lines.append(
                f"- {dataset.symbol}: {dataset.rows} 筆, {source_label}, {dataset.path.name}"
            )
        if self.notes:
            lines.append("說明:")
            lines.extend(f"- {note}" for note in self.notes)
        return "\n".join(lines)
