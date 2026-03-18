"""Configuration loading for tw_quant."""

from __future__ import annotations

from datetime import date
from pathlib import Path
import tomllib

from tw_quant.core.models import (
    AppConfig,
    BacktestEngineConfig,
    BacktestConfig,
    DataPaths,
    IngestConfig,
    PaperTradingConfig,
    PortfolioConfig,
    RiskControlConfig,
    SignalConfig,
    TradingCosts,
    UniverseConfig,
    WalkForwardConfig,
)


def _resolve_path(raw_path: str, base_dir: Path) -> Path:
    candidate = Path(raw_path).expanduser()
    if candidate.is_absolute():
        return candidate
    return (base_dir / candidate).resolve()


def load_settings(path: str | Path) -> AppConfig:
    """Load a TOML settings file into a typed application configuration."""

    settings_path = Path(path).expanduser().resolve()
    with settings_path.open("rb") as handle:
        payload = tomllib.load(handle)

    project_root = _resolve_path(payload["paths"]["project_root"], settings_path.parent)
    research_payload = payload.get("research", {})
    research_branch = str(research_payload.get("branch", "baseline_failure_case")).lower()
    data_paths = DataPaths(
        project_root=project_root,
        raw_dir=_resolve_path(payload["paths"]["raw"], project_root),
        processed_dir=_resolve_path(payload["paths"]["processed"], project_root),
        reports_dir=_resolve_path(payload["paths"]["reports"], project_root),
    )
    trading_costs = TradingCosts(
        commission_bps=float(payload["costs"]["commission_bps"]),
        tax_bps=float(payload["costs"]["tax_bps"]),
        slippage_bps=float(payload["costs"]["slippage_bps"]),
    )
    ingest_payload = payload["ingest"]
    ingest_config = IngestConfig(
        provider=str(ingest_payload["provider"]).lower(),
        symbols=tuple(str(symbol) for symbol in ingest_payload.get("symbols", ())),
        benchmark=str(payload["benchmark"]),
        refresh=bool(ingest_payload.get("refresh", False)),
        storage_format=str(ingest_payload.get("storage_format", "csv")).lower(),
        raw_cache_dir=_resolve_path(
            str(ingest_payload.get("raw_cache_subdir", "finmind")),
            data_paths.raw_dir,
        ),
        normalized_dir=_resolve_path(
            str(ingest_payload.get("normalized_subdir", "market_data/daily")),
            data_paths.processed_dir,
        ),
        token_env_var=(
            str(ingest_payload["token_env_var"])
            if ingest_payload.get("token_env_var")
            else None
        ),
    )
    raw_universe_payload = payload.get("universe_selection", {})
    universe_payload = raw_universe_payload if isinstance(raw_universe_payload, dict) else {}
    universe_config = UniverseConfig(
        candidate_market=str(universe_payload.get("candidate_market", "twse")).lower(),
        selection_rule=str(universe_payload.get("selection_rule", "top_liquidity")).lower(),
        liquidity_lookback_days=int(universe_payload.get("liquidity_lookback_days", 60)),
        top_n=int(universe_payload.get("top_n", 50)),
        reconstitution_frequency=str(
            universe_payload.get("reconstitution_frequency", "monthly")
        ).lower(),
        metadata_output_dir=_resolve_path(
            str(universe_payload.get("metadata_output_subdir", "metadata")),
            data_paths.processed_dir,
        ),
        membership_output_dir=_resolve_path(
            str(universe_payload.get("membership_output_subdir", "universe")),
            data_paths.processed_dir,
        ),
        membership_file=str(
            universe_payload.get(
                "membership_file",
                "tw_top50_liquidity_membership.csv",
            )
        ),
    )
    signals_payload = payload["signals"]
    signals_config = SignalConfig(
        mode=str(signals_payload.get("mode", "time_series_baseline")).lower(),
        enabled_symbols=tuple(str(symbol) for symbol in signals_payload.get("enabled_symbols", ())),
        benchmark=str(signals_payload.get("benchmark", payload["benchmark"])),
        ma_fast_window=int(signals_payload["ma_fast_window"]),
        ma_slow_window=int(signals_payload["ma_slow_window"]),
        momentum_window=int(signals_payload["momentum_window"]),
        volatility_window=int(signals_payload["volatility_window"]),
        volatility_cap=float(signals_payload.get("volatility_cap", 0.35)),
        align_by_date=bool(signals_payload.get("align_by_date", True)),
        input_dir=_resolve_path(
            str(signals_payload.get("input_subdir", "market_data/daily")),
            data_paths.processed_dir,
        ),
        output_dir=_resolve_path(
            str(signals_payload.get("output_subdir", "signals/daily")),
            data_paths.processed_dir,
        ),
        output_file=str(signals_payload.get("output_file", "signal_panel.csv")),
    )
    portfolio_payload = payload["portfolio"]
    portfolio_config = PortfolioConfig(
        tradable_symbols=tuple(str(symbol) for symbol in portfolio_payload.get("tradable_symbols", ())),
        benchmark=str(portfolio_payload.get("benchmark", payload["benchmark"])),
        rebalance_frequency=str(portfolio_payload.get("rebalance_frequency", "monthly")).lower(),
        weighting=str(portfolio_payload.get("weighting", "equal")).lower(),
        min_signal_score=float(portfolio_payload.get("min_signal_score", 0.0)),
        max_positions=int(portfolio_payload.get("max_positions", max(len(signals_config.enabled_symbols), 1))),
        max_weight=float(portfolio_payload.get("max_weight", 1.0)),
        hold_cash_when_inactive=bool(portfolio_payload.get("hold_cash_when_inactive", True)),
    )
    risk_controls_payload = payload.get("risk_controls", {})
    risk_controls = RiskControlConfig(
        benchmark_filter_enabled=bool(
            risk_controls_payload.get("benchmark_filter_enabled", False)
        ),
        benchmark_ma_window=int(risk_controls_payload.get("benchmark_ma_window", 200)),
        defensive_mode=str(risk_controls_payload.get("defensive_mode", "cash")).lower(),
        defensive_gross_exposure=float(
            risk_controls_payload.get("defensive_gross_exposure", 0.5)
        ),
        execution_delay_days=int(risk_controls_payload.get("execution_delay_days", 0)),
        rebalance_cadence_months=int(
            risk_controls_payload.get("rebalance_cadence_months", 1)
        ),
    )
    backtest_payload = payload["backtest"]
    backtest_config = BacktestEngineConfig(
        initial_nav=float(backtest_payload.get("initial_nav", 1.0)),
        bar_input_dir=_resolve_path(
            str(backtest_payload.get("bar_input_subdir", "market_data/daily")),
            data_paths.processed_dir,
        ),
        signal_input_path=_resolve_path(
            str(backtest_payload.get("signal_input_subdir", "signals/daily")),
            data_paths.processed_dir,
        )
        / str(backtest_payload.get("signal_input_file", "signal_panel.csv")),
        output_dir=_resolve_path(
            str(backtest_payload.get("output_subdir", "backtests")),
            data_paths.processed_dir,
        ),
        nav_file=str(backtest_payload.get("nav_file", "daily_nav.csv")),
        weights_file=str(backtest_payload.get("weights_file", "daily_weights.csv")),
    )
    walkforward_payload = payload.get("walkforward", {})
    walkforward_config = WalkForwardConfig(
        enabled=bool(walkforward_payload.get("enabled", False)),
        window_type=str(walkforward_payload.get("window_type", "expanding")).lower(),
        train_window_days=int(walkforward_payload.get("train_window_days", 252)),
        test_window_days=int(walkforward_payload.get("test_window_days", 63)),
        minimum_history_days=int(
            walkforward_payload.get(
                "minimum_history_days",
                walkforward_payload.get("train_window_days", 252),
            )
        ),
    )
    paper_payload = payload.get("paper_trading", {})
    paper_trading_config = PaperTradingConfig(
        output_dir=_resolve_path(
            str(paper_payload.get("output_subdir", "paper_trading")),
            data_paths.processed_dir,
        ),
        initial_cash=float(paper_payload.get("initial_cash", 1_000_000.0)),
        execution_model=str(
            paper_payload.get("execution_model", "next_open_after_delay")
        ).lower(),
        execution_delay_days=int(paper_payload.get("execution_delay_days", 1)),
    )
    config = AppConfig(
        project_name=str(payload["project_name"]),
        research_branch=research_branch,
        market=str(payload["market"]),
        universe=str(payload["universe"]),
        benchmark=str(payload["benchmark"]),
        start_date=date.fromisoformat(str(payload["start_date"])),
        end_date=date.fromisoformat(str(payload["end_date"])),
        data_paths=data_paths,
        trading_costs=trading_costs,
        ingest=ingest_config,
        universe_config=universe_config,
        signals=signals_config,
        portfolio=portfolio_config,
        risk_controls=risk_controls,
        backtest=backtest_config,
        walkforward=walkforward_config,
        paper_trading=paper_trading_config,
    )

    if config.start_date > config.end_date:
        raise ValueError("start_date must be on or before end_date")
    if config.research_branch not in {"baseline_failure_case", "tw_top50_liquidity_cross_sectional"}:
        raise ValueError(
            "research.branch must be one of baseline_failure_case, tw_top50_liquidity_cross_sectional"
        )
    if config.signals.mode not in {"time_series_baseline", "cross_sectional_vol_adj_momentum"}:
        raise ValueError(
            "signals.mode must be one of time_series_baseline, cross_sectional_vol_adj_momentum"
        )
    if config.research_branch == "baseline_failure_case" and not config.ingest.symbols:
        raise ValueError("ingest.symbols must contain at least one symbol")
    if config.ingest.storage_format != "csv":
        raise ValueError("Only csv storage_format is supported in v1")
    if config.signals.ma_fast_window <= 0:
        raise ValueError("signals.ma_fast_window must be positive")
    if config.signals.ma_slow_window <= 0:
        raise ValueError("signals.ma_slow_window must be positive")
    if (
        config.signals.mode == "time_series_baseline"
        and config.signals.ma_fast_window >= config.signals.ma_slow_window
    ):
        raise ValueError("signals.ma_fast_window must be smaller than signals.ma_slow_window")
    if config.signals.momentum_window <= 0:
        raise ValueError("signals.momentum_window must be positive")
    if config.signals.volatility_window <= 0:
        raise ValueError("signals.volatility_window must be positive")
    if config.signals.volatility_cap <= 0:
        raise ValueError("signals.volatility_cap must be positive")
    if config.universe_config.candidate_market != "twse":
        raise ValueError("universe.candidate_market must be twse in Phase A")
    if config.universe_config.selection_rule != "top_liquidity":
        raise ValueError("universe.selection_rule must be top_liquidity in Phase A")
    if config.universe_config.liquidity_lookback_days <= 0:
        raise ValueError("universe.liquidity_lookback_days must be positive")
    if config.universe_config.top_n <= 0:
        raise ValueError("universe.top_n must be positive")
    if config.universe_config.reconstitution_frequency != "monthly":
        raise ValueError("universe.reconstitution_frequency must be monthly in Phase A")
    if config.portfolio.rebalance_frequency not in {"daily", "weekly", "monthly"}:
        raise ValueError("portfolio.rebalance_frequency must be one of daily, weekly, monthly")
    if config.portfolio.weighting != "equal":
        raise ValueError("Only equal portfolio weighting is supported in v1")
    if config.portfolio.max_positions <= 0:
        raise ValueError("portfolio.max_positions must be positive")
    if config.portfolio.max_weight <= 0 or config.portfolio.max_weight > 1:
        raise ValueError("portfolio.max_weight must be between 0 and 1")
    if config.risk_controls.benchmark_ma_window <= 0:
        raise ValueError("risk_controls.benchmark_ma_window must be positive")
    if config.risk_controls.defensive_mode not in {"cash", "half_exposure", "top5"}:
        raise ValueError(
            "risk_controls.defensive_mode must be one of cash, half_exposure, top5"
        )
    if not (0.0 < config.risk_controls.defensive_gross_exposure <= 1.0):
        raise ValueError("risk_controls.defensive_gross_exposure must be within (0, 1]")
    if config.risk_controls.execution_delay_days < 0:
        raise ValueError("risk_controls.execution_delay_days must be non-negative")
    if config.risk_controls.rebalance_cadence_months <= 0:
        raise ValueError("risk_controls.rebalance_cadence_months must be positive")
    if config.backtest.initial_nav <= 0:
        raise ValueError("backtest.initial_nav must be positive")
    if config.walkforward.window_type not in {"expanding", "rolling"}:
        raise ValueError("walkforward.window_type must be one of expanding, rolling")
    if config.walkforward.train_window_days <= 0:
        raise ValueError("walkforward.train_window_days must be positive")
    if config.walkforward.test_window_days <= 0:
        raise ValueError("walkforward.test_window_days must be positive")
    if config.walkforward.minimum_history_days <= 0:
        raise ValueError("walkforward.minimum_history_days must be positive")
    if config.walkforward.minimum_history_days < config.signals.ma_slow_window:
        raise ValueError(
            "walkforward.minimum_history_days must be at least as large as signals.ma_slow_window"
        )
    if config.paper_trading is not None:
        if config.paper_trading.initial_cash <= 0:
            raise ValueError("paper_trading.initial_cash must be positive")
        if config.paper_trading.execution_model != "next_open_after_delay":
            raise ValueError(
                "paper_trading.execution_model must be next_open_after_delay"
            )
        if config.paper_trading.execution_delay_days < 0:
            raise ValueError("paper_trading.execution_delay_days must be non-negative")
    if config.signals.mode == "time_series_baseline":
        if not config.signals.enabled_symbols:
            raise ValueError("signals.enabled_symbols must contain at least one symbol")
        if not config.portfolio.tradable_symbols:
            raise ValueError("portfolio.tradable_symbols must contain at least one symbol")
        if config.portfolio.benchmark in config.portfolio.tradable_symbols:
            raise ValueError("portfolio.benchmark must not appear in portfolio.tradable_symbols")
        missing_signal_symbols = set(config.portfolio.tradable_symbols) - set(config.signals.enabled_symbols)
        if missing_signal_symbols:
            missing_text = ", ".join(sorted(missing_signal_symbols))
            raise ValueError(
                f"portfolio.tradable_symbols must be covered by signals.enabled_symbols: {missing_text}"
            )

    return config


def load_backtest_settings(path: str | Path) -> BacktestConfig:
    """Load only the backtest-relevant configuration."""

    return load_settings(path).to_backtest_config()
