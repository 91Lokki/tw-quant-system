"""Thin pipeline wiring for the ingestion command."""

from __future__ import annotations

from dataclasses import replace
from datetime import date
from pathlib import Path

from tw_quant.config import load_settings
from tw_quant.core.models import AppConfig, IngestConfig, IngestResult, IngestedDataset, NormalizedBar
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
from tw_quant.universe import (
    filter_twse_common_stocks,
    is_twse_common_stock_candidate,
    load_stock_availability,
    load_stock_metadata,
    write_stock_availability,
    write_stock_metadata,
)


def execute_ingest(config_path: str | Path, force_refresh: bool = False) -> IngestResult:
    """Load config, fetch market data, normalize it, and cache it locally."""

    app_config = load_settings(config_path)
    prepare_data_paths(app_config.data_paths)
    ingest_config = _resolve_refresh(app_config.ingest, force_refresh)
    ingest_config.raw_cache_dir.mkdir(parents=True, exist_ok=True)
    ingest_config.normalized_dir.mkdir(parents=True, exist_ok=True)

    provider = build_provider(ingest_config.provider, ingest_config.token_env_var)
    if app_config.research_branch == "tw_top50_liquidity_cross_sectional":
        return _execute_cross_sectional_ingest(
            app_config=app_config,
            ingest_config=ingest_config,
            provider=provider,
        )

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
        research_branch=app_config.research_branch,
    )


def _resolve_refresh(ingest_config: IngestConfig, force_refresh: bool) -> IngestConfig:
    if not force_refresh:
        return ingest_config
    return replace(ingest_config, refresh=True)


def _execute_cross_sectional_ingest(
    app_config: AppConfig,
    ingest_config: IngestConfig,
    provider: DailyDataProvider,
) -> IngestResult:
    if ingest_config.provider != "twse":
        raise ValueError(
            "The TWSE top-50 liquidity cross-sectional branch requires ingest.provider = \"twse\" in Phase C."
        )

    cached_result = _load_cross_sectional_cached_ingest_result(app_config, ingest_config)
    if cached_result is not None:
        return cached_result

    _clear_cross_sectional_reference_artifacts(app_config)

    benchmark_dataset, benchmark_rows = _ingest_twse_benchmark_history(
        provider=provider,
        ingest_config=ingest_config,
        symbol=ingest_config.benchmark,
        start_date=app_config.start_date,
        end_date=app_config.end_date,
    )
    trading_dates = tuple(row.date for row in benchmark_rows)
    if not trading_dates:
        raise RuntimeError("TWSE benchmark history did not produce any trading dates for the requested range.")

    market_datasets, metadata_rows, usable_metadata_rows, availability_rows = _ingest_twse_daily_market(
        provider=provider,
        ingest_config=ingest_config,
        trading_dates=trading_dates,
    )

    metadata_path = write_stock_metadata(app_config.universe_config.metadata_path, metadata_rows)
    usable_metadata_path = write_stock_metadata(
        app_config.universe_config.usable_metadata_path,
        usable_metadata_rows,
    )
    availability_path = write_stock_availability(
        app_config.universe_config.availability_path,
        availability_rows,
    )

    skipped_symbols = [row["stock_id"] for row in availability_rows if row["has_usable_price_data"] != "1"]
    datasets = [benchmark_dataset, *market_datasets]
    notes = [
        "Cross-sectional branch 現在使用 TWSE 官方每日全市場收盤資料，避免逐檔 FinMind 請求導致的 request-limit 問題。",
        "Benchmark 與 master trading calendar 使用 TWSE 官方 TAIEX 歷史資料；它作為基準指標，不是可交易資產。",
        "TWSE 候選池主檔是從官方每日市場快照觀測到的證券列推導而來，不追求完整 securities master，而優先追求可重現與可執行性。",
        (
            f"本次觀測到的 TWSE common-stock-like 候選池共有 {len(metadata_rows)} 檔股票，"
            f"在設定區間內具有可用價格歷史的股票共有 {len(usable_metadata_rows)} 檔。"
        ),
    ]
    if skipped_symbols:
        notes.append(
            f"{len(skipped_symbols)} 檔觀測到的候選股在設定期間內沒有可用日線資料，已從有效候選池排除。"
        )
    if len(usable_metadata_rows) < app_config.universe_config.top_n:
        raise ValueError(
            "Cross-sectional ingest completed but the effective candidate pool is too small to support the "
            f"configured top-{app_config.universe_config.top_n} universe. Only {len(usable_metadata_rows)} symbols have usable price history. "
            "Please inspect twse_price_availability.csv before running downstream commands."
        )

    return IngestResult(
        provider=ingest_config.provider,
        start_date=app_config.start_date,
        end_date=app_config.end_date,
        storage_format=ingest_config.storage_format,
        normalized_dir=ingest_config.normalized_dir,
        datasets=tuple(datasets),
        notes=tuple(notes),
        metadata_path=metadata_path,
        usable_metadata_path=usable_metadata_path,
        availability_path=availability_path,
        candidate_symbol_count=len(metadata_rows),
        usable_symbol_count=len(usable_metadata_rows),
        skipped_symbol_count=len(skipped_symbols),
        research_branch=app_config.research_branch,
    )


def _load_cross_sectional_cached_ingest_result(
    app_config: AppConfig,
    ingest_config: IngestConfig,
) -> IngestResult | None:
    metadata_path = app_config.universe_config.metadata_path
    usable_metadata_path = app_config.universe_config.usable_metadata_path
    availability_path = app_config.universe_config.availability_path
    benchmark_path = ingest_config.normalized_dir / f"{ingest_config.benchmark}.csv"

    if not (
        metadata_path.exists()
        and usable_metadata_path.exists()
        and availability_path.exists()
        and cache_covers_range(benchmark_path, app_config.start_date, app_config.end_date)
    ):
        return None

    metadata_rows = load_stock_metadata(metadata_path)
    usable_metadata_rows = load_stock_metadata(usable_metadata_path)
    availability_rows = load_stock_availability(availability_path)
    if len(usable_metadata_rows) < app_config.universe_config.top_n:
        raise ValueError(
            "Cached cross-sectional artifacts are incomplete for the configured top-liquidity branch. "
            f"Only {len(usable_metadata_rows)} usable symbols are available, expected at least "
            f"{app_config.universe_config.top_n}. Please rerun ingest with --refresh."
        )
    for row in usable_metadata_rows:
        symbol_path = ingest_config.normalized_dir / f"{row['stock_id']}.csv"
        if not cache_covers_range(symbol_path, app_config.start_date, app_config.end_date):
            return None

    datasets = [
        _build_cached_dataset(benchmark_path, ingest_config.benchmark),
        *[
            _build_cached_dataset(ingest_config.normalized_dir / f"{row['stock_id']}.csv", row["stock_id"])
            for row in usable_metadata_rows
        ],
    ]
    skipped_symbols = [row["stock_id"] for row in availability_rows if row["has_usable_price_data"] != "1"]
    return IngestResult(
        provider=ingest_config.provider,
        start_date=app_config.start_date,
        end_date=app_config.end_date,
        storage_format=ingest_config.storage_format,
        normalized_dir=ingest_config.normalized_dir,
        datasets=tuple(datasets),
        notes=(
            "Cross-sectional branch 直接重用本地 TWSE normalized bars 與 metadata artifacts；未重新發送官方資料請求。",
        ),
        metadata_path=metadata_path,
        usable_metadata_path=usable_metadata_path,
        availability_path=availability_path,
        candidate_symbol_count=len(metadata_rows),
        usable_symbol_count=len(usable_metadata_rows),
        skipped_symbol_count=len(skipped_symbols),
        research_branch=app_config.research_branch,
    )


def _build_cached_dataset(path: Path, symbol: str) -> IngestedDataset:
    rows = read_normalized_csv(path)
    return IngestedDataset(
        symbol=symbol,
        dataset="cached_csv",
        rows=len(rows),
        path=path,
        raw_path=None,
        from_cache=True,
    )


def _clear_cross_sectional_reference_artifacts(app_config: AppConfig) -> None:
    for artifact_path in (
        app_config.universe_config.metadata_path,
        app_config.universe_config.usable_metadata_path,
        app_config.universe_config.availability_path,
    ):
        if artifact_path.exists():
            artifact_path.unlink()


def _ingest_twse_benchmark_history(
    provider: DailyDataProvider,
    ingest_config: IngestConfig,
    symbol: str,
    start_date: date,
    end_date: date,
) -> tuple[IngestedDataset, list[NormalizedBar]]:
    benchmark_rows_by_date: dict[date, NormalizedBar] = {}
    for month_anchor in _iter_month_anchors(start_date, end_date):
        payload = provider.fetch_benchmark_month(symbol, month_anchor)
        write_raw_payload(
            ingest_config.raw_cache_dir,
            payload.dataset,
            f"{symbol}_{month_anchor.strftime('%Y%m')}",
            month_anchor,
            month_anchor,
            payload.raw_payload,
        )
        for row in normalize_benchmark_daily(payload):
            if start_date <= row.date <= end_date:
                benchmark_rows_by_date[row.date] = row

    normalized_rows = [benchmark_rows_by_date[row_date] for row_date in sorted(benchmark_rows_by_date)]
    if not normalized_rows:
        raise RuntimeError(f"No benchmark rows returned for symbol {symbol}.")

    target_path = ingest_config.normalized_dir / f"{symbol}.csv"
    write_normalized_csv(target_path, normalized_rows)
    dataset = IngestedDataset(
        symbol=symbol,
        dataset="TWSE_TAIEX_HISTORY",
        rows=len(normalized_rows),
        path=target_path,
        raw_path=None,
        from_cache=False,
    )
    return dataset, normalized_rows


def _ingest_twse_daily_market(
    provider: DailyDataProvider,
    ingest_config: IngestConfig,
    trading_dates: tuple[date, ...],
) -> tuple[list[IngestedDataset], list[dict[str, str]], list[dict[str, str]], list[dict[str, str]]]:
    datasets: list[IngestedDataset] = []
    metadata_by_symbol: dict[str, dict[str, str]] = {}
    bars_by_symbol: dict[str, list[NormalizedBar]] = {}

    for trading_date in trading_dates:
        payload = provider.fetch_market_snapshot(trading_date)
        write_raw_payload(
            ingest_config.raw_cache_dir,
            payload.dataset,
            trading_date.isoformat(),
            trading_date,
            trading_date,
            payload.raw_payload,
        )
        candidate_symbols_for_day: set[str] = set()
        for row in payload.rows:
            stock_id = str(row.get("stock_id", "")).strip()
            stock_name = str(row.get("stock_name", "")).strip()
            if not is_twse_common_stock_candidate(stock_id, stock_name):
                continue
            candidate_symbols_for_day.add(stock_id)
            metadata_by_symbol[stock_id] = {
                "stock_id": stock_id,
                "stock_name": stock_name,
                "type": "twse",
                "industry_category": "",
                "date": str(row.get("date", trading_date.isoformat())),
            }

        for normalized_bar in normalize_security_daily(payload):
            if normalized_bar.symbol in candidate_symbols_for_day:
                bars_by_symbol.setdefault(normalized_bar.symbol, []).append(normalized_bar)

    metadata_rows = filter_twse_common_stocks(list(metadata_by_symbol.values()))
    candidate_symbols = tuple(row["stock_id"] for row in metadata_rows)
    metadata_by_symbol = {row["stock_id"]: row for row in metadata_rows}
    availability_rows: list[dict[str, str]] = []
    usable_symbols: list[str] = []

    for symbol in candidate_symbols:
        normalized_rows = list(bars_by_symbol.get(symbol, ()))
        target_path = ingest_config.normalized_dir / f"{symbol}.csv"
        availability_row = _build_availability_row(symbol, normalized_rows, "twse_daily_market")
        availability_rows.append(availability_row)
        if normalized_rows:
            write_normalized_csv(target_path, normalized_rows)
            dataset = IngestedDataset(
                symbol=symbol,
                dataset="TWSE_MI_INDEX",
                rows=len(normalized_rows),
                path=target_path,
                raw_path=None,
                from_cache=False,
            )
            datasets.append(dataset)
            usable_symbols.append(symbol)
    usable_metadata_rows = [metadata_by_symbol[symbol] for symbol in usable_symbols]
    return datasets, metadata_rows, usable_metadata_rows, availability_rows


def _iter_month_anchors(start_date: date, end_date: date) -> tuple[date, ...]:
    anchors: list[date] = []
    current = date(start_date.year, start_date.month, 1)
    while current <= end_date:
        anchors.append(current)
        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)
    return tuple(anchors)


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


def _build_availability_row(
    symbol: str,
    rows: list[object],
    status: str,
) -> dict[str, str]:
    if not rows:
        return {
            "stock_id": symbol,
            "has_usable_price_data": "0",
            "status": status,
            "row_count": "0",
            "first_date": "",
            "last_date": "",
        }

    first_row = rows[0]
    last_row = rows[-1]
    return {
        "stock_id": symbol,
        "has_usable_price_data": "1",
        "status": status,
        "row_count": str(len(rows)),
        "first_date": first_row.date.isoformat(),
        "last_date": last_row.date.isoformat(),
    }
