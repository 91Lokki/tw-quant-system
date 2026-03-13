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
class UniverseConfig:
    candidate_market: str
    selection_rule: str
    liquidity_lookback_days: int
    top_n: int
    reconstitution_frequency: str
    metadata_output_dir: Path
    membership_output_dir: Path
    membership_file: str

    @property
    def metadata_path(self) -> Path:
        return self.metadata_output_dir / "twse_stock_info.csv"

    @property
    def usable_metadata_path(self) -> Path:
        return self.metadata_output_dir / "twse_usable_stock_info.csv"

    @property
    def availability_path(self) -> Path:
        return self.metadata_output_dir / "twse_price_availability.csv"

    @property
    def membership_path(self) -> Path:
        return self.membership_output_dir / self.membership_file


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
    mode: str
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
class RiskControlConfig:
    benchmark_filter_enabled: bool
    benchmark_ma_window: int
    defensive_mode: str
    defensive_gross_exposure: float
    rebalance_cadence_months: int


@dataclass(frozen=True, slots=True)
class BacktestEngineConfig:
    initial_nav: float
    bar_input_dir: Path
    signal_input_path: Path
    output_dir: Path
    nav_file: str
    weights_file: str


@dataclass(frozen=True, slots=True)
class WalkForwardConfig:
    enabled: bool
    window_type: str
    train_window_days: int
    test_window_days: int
    minimum_history_days: int


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
    risk_controls: RiskControlConfig
    backtest: BacktestEngineConfig
    walkforward: WalkForwardConfig
    research_branch: str = "baseline_failure_case"
    signal_mode: str = "time_series_baseline"
    universe_config: UniverseConfig | None = None

    def date_range_label(self) -> str:
        return f"{self.start_date.isoformat()} to {self.end_date.isoformat()}"


@dataclass(frozen=True, slots=True)
class AppConfig:
    project_name: str
    research_branch: str
    market: str
    universe: str
    benchmark: str
    start_date: date
    end_date: date
    data_paths: DataPaths
    trading_costs: TradingCosts
    ingest: IngestConfig
    universe_config: UniverseConfig
    signals: SignalConfig
    portfolio: PortfolioConfig
    risk_controls: RiskControlConfig
    backtest: BacktestEngineConfig
    walkforward: WalkForwardConfig

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
            risk_controls=self.risk_controls,
            backtest=self.backtest,
            walkforward=self.walkforward,
            research_branch=self.research_branch,
            signal_mode=self.signals.mode,
            universe_config=self.universe_config,
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
    rebalance_cadence_months: int
    trading_costs: TradingCosts
    hold_cash_when_inactive: bool
    benchmark_filter_enabled: bool
    benchmark_ma_window: int
    defensive_mode: str
    defensive_gross_exposure: float
    start_date: date
    end_date: date
    report_path: Path
    nav_path: Path
    weights_path: Path
    equity_curve_path: Path
    drawdown_path: Path
    comparison_path: Path | None
    metrics: PerformanceMetrics
    final_nav: float
    benchmark_final_nav: float
    status: str
    notes: tuple[str, ...]

    def summary_text(self) -> str:
        tradable_preview = _format_symbol_preview(self.tradable_symbols)
        lines = [
            f"Project: {self.project_name}",
            f"Market: {self.market}",
            f"Universe: {self.universe}",
            f"Benchmark: {self.benchmark}",
            f"Tradable Symbols: {tradable_preview}",
            f"Rebalance Frequency: {self.rebalance_frequency}",
            f"Rebalance Cadence (months): {self.rebalance_cadence_months}",
            f"Benchmark Filter Enabled: {self.benchmark_filter_enabled}",
            f"Benchmark MA Window: {self.benchmark_ma_window}",
            f"Defensive Mode: {self.defensive_mode}",
            f"Defensive Gross Exposure: {self.defensive_gross_exposure}",
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
        if self.comparison_path is not None:
            lines.append(f"Risk Comparison: {self.comparison_path}")
        if self.notes:
            lines.append("Notes:")
            lines.extend(f"- {note}" for note in self.notes)
        return "\n".join(lines)

    def summary_text_zh(self) -> str:
        tradable_preview = _format_symbol_preview(self.tradable_symbols)
        lines = [
            "回測完成",
            f"市場: {self.market}",
            f"投資範圍: {self.universe}",
            f"基準指標: {self.benchmark}",
            f"可交易標的: {tradable_preview}",
            f"再平衡頻率: {self.rebalance_frequency}",
            f"換倉 cadence: 每 {self.rebalance_cadence_months} 個月",
            f"Benchmark regime filter: {'開啟' if self.benchmark_filter_enabled else '關閉'}",
            f"Benchmark MA 視窗: {self.benchmark_ma_window}",
            f"防守模式: {self.defensive_mode}",
            f"防守總曝險: {self.defensive_gross_exposure:.0%}",
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
        if self.comparison_path is not None:
            lines.append(f"比較檔案: {self.comparison_path}")
        if self.notes:
            lines.append("說明:")
            lines.extend(f"- {note}" for note in self.notes)
        return "\n".join(lines)


@dataclass(frozen=True, slots=True)
class WalkForwardWindow:
    window_id: int
    train_start: date
    train_end: date
    test_start: date
    test_end: date
    train_size: int
    test_size: int


@dataclass(frozen=True, slots=True)
class WalkForwardWindowResult:
    window_id: int
    train_start: date
    train_end: date
    test_start: date
    test_end: date
    train_size: int
    test_size: int
    final_nav: float
    benchmark_final_nav: float
    metrics: PerformanceMetrics

    def to_csv_row(self) -> dict[str, str]:
        return {
            "window_id": str(self.window_id),
            "train_start": self.train_start.isoformat(),
            "train_end": self.train_end.isoformat(),
            "test_start": self.test_start.isoformat(),
            "test_end": self.test_end.isoformat(),
            "train_size": str(self.train_size),
            "test_size": str(self.test_size),
            "final_nav": f"{self.final_nav}",
            "benchmark_final_nav": f"{self.benchmark_final_nav}",
            "cumulative_return": f"{self.metrics.cumulative_return}",
            "annualized_return": f"{self.metrics.annualized_return}",
            "annualized_volatility": f"{self.metrics.annualized_volatility}",
            "max_drawdown": f"{self.metrics.max_drawdown}",
            "sharpe_ratio": f"{self.metrics.sharpe_ratio}",
            "turnover": f"{self.metrics.turnover}",
        }


@dataclass(frozen=True, slots=True)
class WalkForwardResult:
    project_name: str
    market: str
    universe: str
    benchmark: str
    tradable_symbols: tuple[str, ...]
    rebalance_frequency: str
    rebalance_cadence_months: int
    trading_costs: TradingCosts
    hold_cash_when_inactive: bool
    benchmark_filter_enabled: bool
    benchmark_ma_window: int
    defensive_mode: str
    defensive_gross_exposure: float
    window_type: str
    train_window_days: int
    test_window_days: int
    minimum_history_days: int
    start_date: date
    end_date: date
    nav_path: Path
    window_summary_path: Path
    report_path: Path
    comparison_path: Path | None
    metrics: PerformanceMetrics
    final_nav: float
    benchmark_final_nav: float
    window_count: int
    status: str
    notes: tuple[str, ...]
    windows: tuple[WalkForwardWindowResult, ...]

    def summary_text_zh(self) -> str:
        tradable_preview = _format_symbol_preview(self.tradable_symbols)
        lines = [
            "Walk-forward 評估完成",
            f"市場: {self.market}",
            f"投資範圍: {self.universe}",
            f"基準指標: {self.benchmark}",
            f"可交易標的: {tradable_preview}",
            f"換倉 cadence: 每 {self.rebalance_cadence_months} 個月",
            f"Benchmark regime filter: {'開啟' if self.benchmark_filter_enabled else '關閉'}",
            f"Benchmark MA 視窗: {self.benchmark_ma_window}",
            f"防守模式: {self.defensive_mode}",
            f"防守總曝險: {self.defensive_gross_exposure:.0%}",
            f"Walk-forward 設計: {self.window_type}",
            f"訓練窗長度: {self.train_window_days} 個交易日",
            f"測試窗長度: {self.test_window_days} 個交易日",
            f"最小歷史長度: {self.minimum_history_days} 個交易日",
            f"視窗數量: {self.window_count}",
            f"樣本外期間: {self.start_date.isoformat()} 至 {self.end_date.isoformat()}",
            f"最終 NAV: {self.final_nav:.6f}",
            f"累積報酬: {self.metrics.cumulative_return:.2%}",
            f"年化報酬: {self.metrics.annualized_return:.2%}",
            f"年化波動: {self.metrics.annualized_volatility:.2%}",
            f"最大回撤: {self.metrics.max_drawdown:.2%}",
            f"Sharpe 比率: {self.metrics.sharpe_ratio:.3f}",
            f"累積換手: {self.metrics.turnover:.4f}",
            f"樣本外 NAV 檔案: {self.nav_path}",
            f"視窗摘要檔案: {self.window_summary_path}",
            f"摘要報告: {self.report_path}",
        ]
        if self.comparison_path is not None:
            lines.append(f"比較檔案: {self.comparison_path}")
        if self.notes:
            lines.append("說明:")
            lines.extend(f"- {note}" for note in self.notes)
        return "\n".join(lines)


@dataclass(frozen=True, slots=True)
class DiagnosticsResult:
    project_name: str
    start_date: date
    end_date: date
    report_path: Path
    yearly_table_path: Path
    walkforward_table_path: Path
    symbol_exposure_path: Path
    signal_diagnostics_path: Path
    key_findings: tuple[str, ...]

    def summary_text_zh(self) -> str:
        lines = [
            "診斷分析完成",
            f"專案: {self.project_name}",
            f"期間: {self.start_date.isoformat()} 至 {self.end_date.isoformat()}",
            f"年度拆解: {self.yearly_table_path}",
            f"Walk-forward 診斷: {self.walkforward_table_path}",
            f"部位曝險摘要: {self.symbol_exposure_path}",
            f"訊號診斷摘要: {self.signal_diagnostics_path}",
            f"診斷報告: {self.report_path}",
        ]
        if self.key_findings:
            lines.append("重點發現:")
            lines.extend(f"- {finding}" for finding in self.key_findings)
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
    traded_value: float | None = None

    def to_csv_row(self) -> dict[str, str]:
        return {
            "date": self.date.isoformat(),
            "symbol": self.symbol,
            "open": f"{self.open}",
            "high": f"{self.high}",
            "low": f"{self.low}",
            "close": f"{self.close}",
            "volume": "" if self.volume is None else str(self.volume),
            "traded_value": "" if self.traded_value is None else f"{self.traded_value}",
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
    metadata_path: Path | None = None
    usable_metadata_path: Path | None = None
    availability_path: Path | None = None
    candidate_symbol_count: int | None = None
    usable_symbol_count: int | None = None
    skipped_symbol_count: int | None = None
    research_branch: str = "baseline_failure_case"

    def summary_text_zh(self) -> str:
        cached = sum(1 for dataset in self.datasets if dataset.from_cache)
        fetched = len(self.datasets) - cached
        lines = [
            "資料擷取完成",
            f"研究分支: {self.research_branch}",
            f"提供者: {self.provider}",
            f"期間: {self.start_date.isoformat()} 至 {self.end_date.isoformat()}",
            f"輸出格式: {self.storage_format}",
            f"輸出目錄: {self.normalized_dir}",
            f"資料集數量: {len(self.datasets)}",
            f"快取命中: {cached}",
            f"重新抓取: {fetched}",
        ]
        if self.candidate_symbol_count is not None:
            lines.append(f"候選股票數量: {self.candidate_symbol_count}")
        if self.metadata_path is not None:
            lines.append(f"Metadata 檔案: {self.metadata_path}")
        if self.usable_metadata_path is not None:
            lines.append(f"可用候選池檔案: {self.usable_metadata_path}")
        if self.availability_path is not None:
            lines.append(f"價格可用性檔案: {self.availability_path}")
        if self.usable_symbol_count is not None:
            lines.append(f"可用股票數量: {self.usable_symbol_count}")
        if self.skipped_symbol_count is not None:
            lines.append(f"跳過股票數量: {self.skipped_symbol_count}")
        visible_datasets = self.datasets[:20]
        for dataset in visible_datasets:
            source_label = "快取" if dataset.from_cache else dataset.dataset
            lines.append(
                f"- {dataset.symbol}: {dataset.rows} 筆, {source_label}, {dataset.path.name}"
            )
        if len(self.datasets) > len(visible_datasets):
            lines.append(f"- 其餘 {len(self.datasets) - len(visible_datasets)} 個資料集已省略")
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
class UniverseMembershipRow:
    date: date
    symbol: str
    liquidity_rank: int
    avg_traded_value_60d: float
    universe_name: str
    is_member: bool

    def to_csv_row(self) -> dict[str, str]:
        return {
            "date": self.date.isoformat(),
            "symbol": self.symbol,
            "liquidity_rank": str(self.liquidity_rank),
            "avg_traded_value_60d": f"{self.avg_traded_value_60d}",
            "universe_name": self.universe_name,
            "is_member": "1" if self.is_member else "0",
        }


@dataclass(frozen=True, slots=True)
class CrossSectionalSignalRow:
    date: date
    symbol: str
    close: float
    avg_traded_value_60d: float
    liquidity_rank: int
    momentum_126: float | None
    volatility_20: float | None
    signal_score: float | None
    factor_rank: int | None
    universe_name: str

    def to_csv_row(self) -> dict[str, str]:
        return {
            "date": self.date.isoformat(),
            "symbol": self.symbol,
            "close": f"{self.close}",
            "avg_traded_value_60d": f"{self.avg_traded_value_60d}",
            "liquidity_rank": str(self.liquidity_rank),
            "momentum_126": "" if self.momentum_126 is None else f"{self.momentum_126}",
            "volatility_20": "" if self.volatility_20 is None else f"{self.volatility_20}",
            "signal_score": "" if self.signal_score is None else f"{self.signal_score}",
            "factor_rank": "" if self.factor_rank is None else str(self.factor_rank),
            "universe_name": self.universe_name,
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
    mode: str = "time_series_baseline"
    membership_output_path: Path | None = None

    def summary_text_zh(self) -> str:
        lines = [
            "訊號產生完成",
            f"模式: {self.mode}",
            f"期間: {self.start_date.isoformat()} 至 {self.end_date.isoformat()}",
            f"標的數量: {len(self.symbols)}",
            f"對齊交易日數: {len(self.aligned_dates)}",
            f"輸出筆數: {self.row_count}",
            f"輸出檔案: {self.output_path}",
        ]
        if self.membership_output_path is not None:
            lines.append(f"Universe 檔案: {self.membership_output_path}")
        visible_symbols = self.symbols[:20]
        for symbol in visible_symbols:
            lines.append(f"- {symbol}")
        if len(self.symbols) > len(visible_symbols):
            lines.append(f"- 其餘 {len(self.symbols) - len(visible_symbols)} 檔標的已省略")
        if self.notes:
            lines.append("說明:")
            lines.extend(f"- {note}" for note in self.notes)
        return "\n".join(lines)


def _format_symbol_preview(symbols: tuple[str, ...], limit: int = 12) -> str:
    if len(symbols) <= limit:
        return ", ".join(symbols)
    preview = ", ".join(symbols[:limit])
    return f"{preview}, ... (total {len(symbols)})"
