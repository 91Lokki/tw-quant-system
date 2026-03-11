"""Thin pipeline wiring for the ingestion command."""

from __future__ import annotations

from dataclasses import replace
from datetime import date
from pathlib import Path

from tw_quant.config import load_settings
from tw_quant.core.models import IngestConfig, IngestResult, IngestedDataset
from tw_quant.data import (
    build_provider,
    cache_covers_range,
    normalize_benchmark_daily,
    normalize_security_daily,
    prepare_data_paths,
    read_normalized_csv,
    write_normalized_csv,
    write_raw_payload,
)
from tw_quant.data.providers import DailyDataProvider


def execute_ingest(config_path: str | Path, force_refresh: bool = False) -> IngestResult:
    """Load config, fetch market data, normalize it, and cache it locally."""

    app_config = load_settings(config_path)
    prepare_data_paths(app_config.data_paths)
    ingest_config = _resolve_refresh(app_config.ingest, force_refresh)
    ingest_config.raw_cache_dir.mkdir(parents=True, exist_ok=True)
    ingest_config.normalized_dir.mkdir(parents=True, exist_ok=True)

    provider = build_provider(ingest_config.provider, ingest_config.token_env_var)
    datasets: list[IngestedDataset] = []
    notes: list[str] = []

    for symbol in ingest_config.requested_symbols():
        if symbol == ingest_config.benchmark:
            datasets.append(
                _ingest_benchmark_symbol(
                    provider=provider,
                    ingest_config=ingest_config,
                    symbol=symbol,
                    start_date=app_config.start_date,
                    end_date=app_config.end_date,
                )
            )
            notes.append(
                f"{symbol} 目前使用 FinMind 的 TaiwanStockTotalReturnIndex；該資料集不是完整 OHLCV，"
                "因此 normalized benchmark 會以單一 price 映射到 open/high/low/close，volume 留空。"
            )
            continue

        datasets.append(
            _ingest_security_symbol(
                provider=provider,
                ingest_config=ingest_config,
                symbol=symbol,
                start_date=app_config.start_date,
                end_date=app_config.end_date,
            )
        )

    return IngestResult(
        provider=ingest_config.provider,
        start_date=app_config.start_date,
        end_date=app_config.end_date,
        storage_format=ingest_config.storage_format,
        normalized_dir=ingest_config.normalized_dir,
        datasets=tuple(datasets),
        notes=tuple(notes),
    )


def _resolve_refresh(ingest_config: IngestConfig, force_refresh: bool) -> IngestConfig:
    if not force_refresh:
        return ingest_config
    return replace(ingest_config, refresh=True)


def _ingest_security_symbol(
    provider: DailyDataProvider,
    ingest_config: IngestConfig,
    symbol: str,
    start_date: date,
    end_date: date,
) -> IngestedDataset:
    target_path = ingest_config.normalized_dir / f"{symbol}.csv"
    if not ingest_config.refresh and cache_covers_range(target_path, start_date, end_date):
        rows = read_normalized_csv(target_path)
        return IngestedDataset(
            symbol=symbol,
            dataset="cached_csv",
            rows=len(rows),
            path=target_path,
            raw_path=None,
            from_cache=True,
        )

    payload = provider.fetch_security_daily(symbol, start_date, end_date)
    normalized_rows = normalize_security_daily(payload)
    if not normalized_rows:
        raise RuntimeError(f"No rows returned for symbol {symbol}.")
    raw_path = write_raw_payload(
        ingest_config.raw_cache_dir,
        payload.dataset,
        symbol,
        start_date,
        end_date,
        payload.raw_payload,
    )
    write_normalized_csv(target_path, normalized_rows)
    return IngestedDataset(
        symbol=symbol,
        dataset=payload.dataset,
        rows=len(normalized_rows),
        path=target_path,
        raw_path=raw_path,
        from_cache=False,
    )


def _ingest_benchmark_symbol(
    provider: DailyDataProvider,
    ingest_config: IngestConfig,
    symbol: str,
    start_date: date,
    end_date: date,
) -> IngestedDataset:
    target_path = ingest_config.normalized_dir / f"{symbol}.csv"
    if not ingest_config.refresh and cache_covers_range(target_path, start_date, end_date):
        rows = read_normalized_csv(target_path)
        return IngestedDataset(
            symbol=symbol,
            dataset="cached_csv",
            rows=len(rows),
            path=target_path,
            raw_path=None,
            from_cache=True,
        )

    payload = provider.fetch_benchmark_daily(symbol, start_date, end_date)
    normalized_rows = normalize_benchmark_daily(payload)
    if not normalized_rows:
        raise RuntimeError(f"No benchmark rows returned for symbol {symbol}.")
    raw_path = write_raw_payload(
        ingest_config.raw_cache_dir,
        payload.dataset,
        symbol,
        start_date,
        end_date,
        payload.raw_payload,
    )
    write_normalized_csv(target_path, normalized_rows)
    return IngestedDataset(
        symbol=symbol,
        dataset=payload.dataset,
        rows=len(normalized_rows),
        path=target_path,
        raw_path=raw_path,
        from_cache=False,
    )
