"""Configuration loading for tw_quant."""

from __future__ import annotations

from datetime import date
from pathlib import Path
import tomllib

from tw_quant.core.models import BacktestConfig, DataPaths, TradingCosts


def _resolve_path(raw_path: str, base_dir: Path) -> Path:
    candidate = Path(raw_path).expanduser()
    if candidate.is_absolute():
        return candidate
    return (base_dir / candidate).resolve()


def load_settings(path: str | Path) -> BacktestConfig:
    """Load a TOML settings file into a typed backtest configuration."""

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
    config = BacktestConfig(
        project_name=str(payload["project_name"]),
        market=str(payload["market"]),
        universe=str(payload["universe"]),
        benchmark=str(payload["benchmark"]),
        start_date=date.fromisoformat(str(payload["start_date"])),
        end_date=date.fromisoformat(str(payload["end_date"])),
        data_paths=data_paths,
        trading_costs=trading_costs,
    )

    if config.start_date > config.end_date:
        raise ValueError("start_date must be on or before end_date")

    return config
