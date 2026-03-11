"""Configuration loading for tw_quant."""

from __future__ import annotations

from datetime import date
from pathlib import Path
import tomllib

from tw_quant.core.models import AppConfig, BacktestConfig, DataPaths, IngestConfig, TradingCosts


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
        symbols=tuple(str(symbol) for symbol in ingest_payload["symbols"]),
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
    config = AppConfig(
        project_name=str(payload["project_name"]),
        market=str(payload["market"]),
        universe=str(payload["universe"]),
        benchmark=str(payload["benchmark"]),
        start_date=date.fromisoformat(str(payload["start_date"])),
        end_date=date.fromisoformat(str(payload["end_date"])),
        data_paths=data_paths,
        trading_costs=trading_costs,
        ingest=ingest_config,
    )

    if config.start_date > config.end_date:
        raise ValueError("start_date must be on or before end_date")
    if not config.ingest.symbols:
        raise ValueError("ingest.symbols must contain at least one symbol")
    if config.ingest.storage_format != "csv":
        raise ValueError("Only csv storage_format is supported in v1")

    return config


def load_backtest_settings(path: str | Path) -> BacktestConfig:
    """Load only the backtest-relevant configuration."""

    return load_settings(path).to_backtest_config()
