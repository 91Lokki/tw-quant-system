"""Normalization helpers for Taiwan daily bar data."""

from __future__ import annotations

from datetime import date
from typing import Iterable

from tw_quant.core.models import NormalizedBar
from tw_quant.data.providers import ProviderPayload


REQUIRED_NORMALIZED_BAR_COLUMNS = ("date", "symbol", "open", "high", "low", "close", "volume")
NORMALIZED_BAR_COLUMNS = REQUIRED_NORMALIZED_BAR_COLUMNS + ("traded_value",)


def normalize_security_daily(payload: ProviderPayload) -> list[NormalizedBar]:
    """Normalize provider-specific security daily rows into the project schema."""

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
                traded_value=_coerce_optional_float(row.get("Trading_money")),
            )
        )
    return _sort_bars(normalized)


def normalize_benchmark_daily(payload: ProviderPayload) -> list[NormalizedBar]:
    """Normalize provider-specific benchmark rows into the project schema."""

    normalized: list[NormalizedBar] = []
    for row in payload.rows:
        if {"open", "max", "min", "close"}.issubset(row):
            open_price = float(row["open"])
            high_price = float(row["max"])
            low_price = float(row["min"])
            close_price = float(row["close"])
        else:
            close_price = float(row["price"])
            open_price = close_price
            high_price = close_price
            low_price = close_price
        normalized.append(
            NormalizedBar(
                date=date.fromisoformat(str(row["date"])),
                symbol=str(row.get("stock_id", payload.symbol)),
                open=open_price,
                high=high_price,
                low=low_price,
                close=close_price,
                volume=None,
                traded_value=None,
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


def _coerce_optional_float(raw_value: object | None) -> float | None:
    if raw_value is None or raw_value == "":
        return None
    return float(str(raw_value))


def _sort_bars(rows: list[NormalizedBar]) -> list[NormalizedBar]:
    return sorted(rows, key=lambda row: (row.date, row.symbol))
