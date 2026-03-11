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
class SignalConfig:
    enabled_symbols: tuple[str, ...]
    benchmark: str
    ma_fast_window: int
    ma_slow_window: int
    momentum_window: int
    volatility_window: int
    volatility_cap: float
    align_by_date: bool
    input_dir: Path
    output_dir: Path
    output_file: str

    def requested_symbols(self) -> tuple[str, ...]:
        ordered: list[str] = []
        for symbol in (*self.enabled_symbols, self.benchmark):
            if symbol and symbol not in ordered:
                ordered.append(symbol)
        return tuple(ordered)


@dataclass(frozen=True, slots=True)
class PortfolioConfig:
    tradable_symbols: tuple[str, ...]
    benchmark: str
    rebalance_frequency: str
    weighting: str
    min_signal_score: float
    max_positions: int
    max_weight: float
    hold_cash_when_inactive: bool

    def requested_symbols(self) -> tuple[str, ...]:
        ordered: list[str] = []
        for symbol in (*self.tradable_symbols, self.benchmark):
            if symbol and symbol not in ordered:
                ordered.append(symbol)
        return tuple(ordered)


@dataclass(frozen=True, slots=True)
class BacktestEngineConfig:
    initial_nav: float
    bar_input_dir: Path
    signal_input_path: Path
    output_dir: Path
    nav_file: str
    weights_file: str


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
    portfolio: PortfolioConfig
    backtest: BacktestEngineConfig

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
    signals: SignalConfig
    portfolio: PortfolioConfig
    backtest: BacktestEngineConfig

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
            portfolio=self.portfolio,
            backtest=self.backtest,
        )


@dataclass(frozen=True, slots=True)
class PerformanceMetrics:
    cumulative_return: float
    annualized_return: float
    annualized_volatility: float
    max_drawdown: float
    sharpe_ratio: float
    turnover: float


@dataclass(frozen=True, slots=True)
class PortfolioWeightRow:
    date: date
    symbol: str
    weight: float
    signal_score: float | None

    def to_csv_row(self) -> dict[str, str]:
        return {
            "date": self.date.isoformat(),
            "symbol": self.symbol,
            "weight": f"{self.weight}",
            "signal_score": "" if self.signal_score is None else f"{self.signal_score}",
        }


@dataclass(frozen=True, slots=True)
class NavRow:
    date: date
    nav: float
    daily_return: float
    gross_return: float
    benchmark_nav: float
    benchmark_return: float
    turnover: float
    transaction_cost: float
    cash_weight: float

    def to_csv_row(self) -> dict[str, str]:
        return {
            "date": self.date.isoformat(),
            "nav": f"{self.nav}",
            "daily_return": f"{self.daily_return}",
            "gross_return": f"{self.gross_return}",
            "benchmark_nav": f"{self.benchmark_nav}",
            "benchmark_return": f"{self.benchmark_return}",
            "turnover": f"{self.turnover}",
            "transaction_cost": f"{self.transaction_cost}",
            "cash_weight": f"{self.cash_weight}",
        }


@dataclass(frozen=True, slots=True)
class BacktestResult:
    project_name: str
    market: str
    universe: str
    benchmark: str
    tradable_symbols: tuple[str, ...]
    rebalance_frequency: str
    trading_costs: TradingCosts
    hold_cash_when_inactive: bool
    start_date: date
    end_date: date
    report_path: Path
    nav_path: Path
    weights_path: Path
    equity_curve_path: Path
    drawdown_path: Path
    metrics: PerformanceMetrics
    final_nav: float
    benchmark_final_nav: float
    status: str
    notes: tuple[str, ...]

    def summary_text(self) -> str:
        lines = [
            f"Project: {self.project_name}",
            f"Market: {self.market}",
            f"Universe: {self.universe}",
            f"Benchmark: {self.benchmark}",
            f"Tradable Symbols: {', '.join(self.tradable_symbols)}",
            f"Rebalance Frequency: {self.rebalance_frequency}",
            f"Date range: {self.start_date.isoformat()} to {self.end_date.isoformat()}",
            f"Status: {self.status}",
            f"Final NAV: {self.final_nav:.6f}",
            f"Cumulative Return: {self.metrics.cumulative_return:.4%}",
            f"Report: {self.report_path}",
            f"NAV Path: {self.nav_path}",
            f"Weights Path: {self.weights_path}",
            f"Equity Curve: {self.equity_curve_path}",
            f"Drawdown Chart: {self.drawdown_path}",
        ]
        if self.notes:
            lines.append("Notes:")
            lines.extend(f"- {note}" for note in self.notes)
        return "\n".join(lines)

    def summary_text_zh(self) -> str:
        lines = [
            "回測完成",
            f"市場: {self.market}",
            f"投資範圍: {self.universe}",
            f"基準指標: {self.benchmark}",
            f"可交易標的: {', '.join(self.tradable_symbols)}",
            f"再平衡頻率: {self.rebalance_frequency}",
            f"期間: {self.start_date.isoformat()} 至 {self.end_date.isoformat()}",
            f"最終 NAV: {self.final_nav:.6f}",
            f"累積報酬: {self.metrics.cumulative_return:.2%}",
            f"年化報酬: {self.metrics.annualized_return:.2%}",
            f"年化波動: {self.metrics.annualized_volatility:.2%}",
            f"最大回撤: {self.metrics.max_drawdown:.2%}",
            f"Sharpe 比率: {self.metrics.sharpe_ratio:.3f}",
            f"累積換手: {self.metrics.turnover:.4f}",
            f"NAV 檔案: {self.nav_path}",
            f"權重檔案: {self.weights_path}",
            f"摘要報告: {self.report_path}",
            f"權益曲線圖: {self.equity_curve_path}",
            f"回撤圖: {self.drawdown_path}",
        ]
        if self.notes:
            lines.append("說明:")
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


@dataclass(frozen=True, slots=True)
class MarketDataset:
    symbols: tuple[str, ...]
    start_date: date
    end_date: date
    bars_by_symbol: dict[str, tuple[NormalizedBar, ...]]
    aligned_dates: tuple[date, ...]
    notes: tuple[str, ...]

    @property
    def row_count(self) -> int:
        return sum(len(rows) for rows in self.bars_by_symbol.values())


@dataclass(frozen=True, slots=True)
class SignalRow:
    date: date
    symbol: str
    close: float
    ma_fast: float | None
    ma_slow: float | None
    trend_signal: int
    momentum_n: float | None
    momentum_signal: int
    volatility_n: float | None
    volatility_filter: int
    signal_score: float

    def to_csv_row(self) -> dict[str, str]:
        return {
            "date": self.date.isoformat(),
            "symbol": self.symbol,
            "close": f"{self.close}",
            "ma_fast": "" if self.ma_fast is None else f"{self.ma_fast}",
            "ma_slow": "" if self.ma_slow is None else f"{self.ma_slow}",
            "trend_signal": str(self.trend_signal),
            "momentum_n": "" if self.momentum_n is None else f"{self.momentum_n}",
            "momentum_signal": str(self.momentum_signal),
            "volatility_n": "" if self.volatility_n is None else f"{self.volatility_n}",
            "volatility_filter": str(self.volatility_filter),
            "signal_score": f"{self.signal_score}",
        }


@dataclass(frozen=True, slots=True)
class SignalResult:
    start_date: date
    end_date: date
    symbols: tuple[str, ...]
    aligned_dates: tuple[date, ...]
    row_count: int
    output_path: Path
    notes: tuple[str, ...]

    def summary_text_zh(self) -> str:
        lines = [
            "訊號產生完成",
            f"期間: {self.start_date.isoformat()} 至 {self.end_date.isoformat()}",
            f"標的數量: {len(self.symbols)}",
            f"對齊交易日數: {len(self.aligned_dates)}",
            f"輸出筆數: {self.row_count}",
            f"輸出檔案: {self.output_path}",
        ]
        for symbol in self.symbols:
            lines.append(f"- {symbol}")
        if self.notes:
            lines.append("說明:")
            lines.extend(f"- {note}" for note in self.notes)
        return "\n".join(lines)
