"""Normalization helpers for Taiwan daily bar data."""

from __future__ import annotations

from datetime import date
from typing import Iterable

from tw_quant.core.models import NormalizedBar
from tw_quant.data.providers import ProviderPayload


NORMALIZED_BAR_COLUMNS = ("date", "symbol", "open", "high", "low", "close", "volume")


def normalize_security_daily(payload: ProviderPayload) -> list[NormalizedBar]:
    """Normalize FinMind TaiwanStockPrice rows into the project schema."""

    normalized: list[NormalizedBar] = []
    for row in payload.rows:
        normalized.append(
            NormalizedBar(
                date=date.fromisoformat(str(row["date"])),
                symbol=str(row.get("stock_id", payload.symbol)),
                open=float(row["open"]),
                high=float(row["max"]),
                low=float(row["min"]),
                close=float(row["close"]),
                volume=_coerce_volume(row["Trading_Volume"]),
            )
        )
    return _sort_bars(normalized)


def normalize_benchmark_daily(payload: ProviderPayload) -> list[NormalizedBar]:
    """Normalize FinMind TaiwanStockTotalReturnIndex rows into the project schema."""

    normalized: list[NormalizedBar] = []
    for row in payload.rows:
        price = float(row["price"])
        normalized.append(
            NormalizedBar(
                date=date.fromisoformat(str(row["date"])),
                symbol=str(row.get("stock_id", payload.symbol)),
                open=price,
                high=price,
                low=price,
                close=price,
                volume=None,
            )
        )
    return _sort_bars(normalized)


def filter_bars_by_date(
    bars: Iterable[NormalizedBar],
    start_date: date,
    end_date: date,
) -> list[NormalizedBar]:
    """Return only rows inside the requested date range."""

    return [
        bar
        for bar in bars
        if start_date <= bar.date <= end_date
    ]


def _coerce_volume(raw_value: object) -> int:
    if isinstance(raw_value, int):
        return raw_value
    return int(float(str(raw_value)))


def _sort_bars(rows: list[NormalizedBar]) -> list[NormalizedBar]:
    return sorted(rows, key=lambda row: (row.date, row.symbol))
