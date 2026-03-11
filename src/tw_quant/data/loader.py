"""Load normalized local market data for downstream research workflows."""

from __future__ import annotations

import csv
from datetime import date
from pathlib import Path

from tw_quant.core.models import MarketDataset, NormalizedBar
from tw_quant.data.normalize import NORMALIZED_BAR_COLUMNS


def load_market_dataset(
    normalized_dir: Path,
    symbols: tuple[str, ...],
    start_date: date,
    end_date: date,
    align_by_date: bool = True,
) -> MarketDataset:
    """Load normalized daily bars, validate schema, and optionally align by date."""

    if not symbols:
        raise ValueError("At least one symbol is required to load a market dataset.")

    bars_by_symbol: dict[str, tuple[NormalizedBar, ...]] = {}
    notes: list[str] = []

    for symbol in symbols:
        path = normalized_dir / f"{symbol}.csv"
        rows = _load_symbol_rows(path, symbol, start_date, end_date)
        if not rows:
            raise ValueError(f"No normalized rows found for symbol {symbol} in the requested date range.")
        bars_by_symbol[symbol] = tuple(rows)
        if any(row.volume is None for row in rows):
            notes.append(f"{symbol} 在部分日期缺少 volume；下游流程需要把它視為缺值而不是 0。")

    aligned_dates = _intersection_dates(bars_by_symbol)
    if not aligned_dates:
        raise ValueError("No shared trading dates were found across the requested symbols.")

    if align_by_date:
        aligned_set = set(aligned_dates)
        bars_by_symbol = {
            symbol: tuple(row for row in rows if row.date in aligned_set)
            for symbol, rows in bars_by_symbol.items()
        }

    return MarketDataset(
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        bars_by_symbol=bars_by_symbol,
        aligned_dates=aligned_dates,
        notes=tuple(dict.fromkeys(notes)),
    )


def _load_symbol_rows(
    path: Path,
    expected_symbol: str,
    start_date: date,
    end_date: date,
) -> list[NormalizedBar]:
    if not path.exists():
        raise FileNotFoundError(f"Normalized data file not found for symbol {expected_symbol}: {path}")

    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError(f"CSV header is missing in {path}.")
        missing_columns = [column for column in NORMALIZED_BAR_COLUMNS if column not in reader.fieldnames]
        if missing_columns:
            raise ValueError(f"Missing required columns in {path}: {', '.join(missing_columns)}")

        rows: list[NormalizedBar] = []
        seen_dates: set[date] = set()
        for raw_row in reader:
            row = _parse_row(raw_row, path)
            if row.symbol != expected_symbol:
                raise ValueError(
                    f"Unexpected symbol {row.symbol} found in {path}; expected only {expected_symbol}."
                )
            if row.date in seen_dates:
                raise ValueError(f"Duplicate date {row.date.isoformat()} found in {path}.")
            seen_dates.add(row.date)
            if start_date <= row.date <= end_date:
                rows.append(row)

    return rows


def _parse_row(raw_row: dict[str, str], path: Path) -> NormalizedBar:
    try:
        return NormalizedBar(
            date=date.fromisoformat(raw_row["date"]),
            symbol=raw_row["symbol"],
            open=float(raw_row["open"]),
            high=float(raw_row["high"]),
            low=float(raw_row["low"]),
            close=float(raw_row["close"]),
            volume=_parse_volume(raw_row["volume"]),
        )
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError(f"Failed to parse normalized row in {path}: {raw_row}") from error


def _parse_volume(raw_value: str) -> int | None:
    if raw_value == "":
        return None
    return int(float(raw_value))


def _intersection_dates(bars_by_symbol: dict[str, tuple[NormalizedBar, ...]]) -> tuple[date, ...]:
    date_sets = [
        {row.date for row in rows}
        for rows in bars_by_symbol.values()
    ]
    common_dates = set.intersection(*date_sets)
    return tuple(sorted(common_dates))
