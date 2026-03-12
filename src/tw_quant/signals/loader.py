"""Load persisted signal panels for downstream portfolio and backtest workflows."""

from __future__ import annotations

import csv
from datetime import date
from pathlib import Path

from tw_quant.core.models import CrossSectionalSignalRow, SignalRow
from tw_quant.signals.generate import CROSS_SECTIONAL_SIGNAL_COLUMNS, SIGNAL_COLUMNS


def load_signal_rows(
    path: Path,
    symbols: tuple[str, ...],
    start_date: date,
    end_date: date,
    aligned_dates: tuple[date, ...] | None = None,
) -> list[SignalRow]:
    """Load signal rows from the persisted combined signal panel."""

    if not path.exists():
        raise FileNotFoundError(f"Signal panel not found: {path}")

    requested_symbols = set(symbols)
    rows: list[SignalRow] = []
    seen_pairs: set[tuple[date, str]] = set()

    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError(f"CSV header is missing in {path}.")
        missing_columns = [column for column in SIGNAL_COLUMNS if column not in reader.fieldnames]
        if missing_columns:
            raise ValueError(f"Missing required signal columns in {path}: {', '.join(missing_columns)}")

        for raw_row in reader:
            row = _parse_row(raw_row, path)
            if row.symbol not in requested_symbols:
                continue
            if not (start_date <= row.date <= end_date):
                continue
            key = (row.date, row.symbol)
            if key in seen_pairs:
                raise ValueError(f"Duplicate signal row found for {row.symbol} on {row.date.isoformat()} in {path}")
            seen_pairs.add(key)
            rows.append(row)

    rows.sort(key=lambda row: (row.date, row.symbol))
    if aligned_dates is not None:
        _validate_aligned_coverage(rows, symbols, aligned_dates, path)
        aligned_set = set(aligned_dates)
        rows = [row for row in rows if row.date in aligned_set]

    return rows


def load_cross_sectional_signal_rows(
    path: Path,
    start_date: date,
    end_date: date,
) -> list[CrossSectionalSignalRow]:
    """Load monthly cross-sectional signal rows from the persisted Phase A artifact."""

    if not path.exists():
        raise FileNotFoundError(f"Cross-sectional signal panel not found: {path}")

    rows: list[CrossSectionalSignalRow] = []
    seen_pairs: set[tuple[date, str]] = set()
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError(f"CSV header is missing in {path}.")
        missing_columns = [
            column for column in CROSS_SECTIONAL_SIGNAL_COLUMNS if column not in reader.fieldnames
        ]
        if missing_columns:
            raise ValueError(
                f"Missing required cross-sectional signal columns in {path}: {', '.join(missing_columns)}"
            )
        for raw_row in reader:
            row = _parse_cross_sectional_row(raw_row, path)
            if not (start_date <= row.date <= end_date):
                continue
            key = (row.date, row.symbol)
            if key in seen_pairs:
                raise ValueError(
                    f"Duplicate cross-sectional signal row found for {row.symbol} on {row.date.isoformat()} in {path}"
                )
            seen_pairs.add(key)
            rows.append(row)

    rows.sort(key=lambda row: (row.date, row.symbol))
    return rows


def _parse_row(raw_row: dict[str, str], path: Path) -> SignalRow:
    try:
        return SignalRow(
            date=date.fromisoformat(raw_row["date"]),
            symbol=raw_row["symbol"],
            close=float(raw_row["close"]),
            ma_fast=_parse_optional_float(raw_row["ma_fast"]),
            ma_slow=_parse_optional_float(raw_row["ma_slow"]),
            trend_signal=int(raw_row["trend_signal"]),
            momentum_n=_parse_optional_float(raw_row["momentum_n"]),
            momentum_signal=int(raw_row["momentum_signal"]),
            volatility_n=_parse_optional_float(raw_row["volatility_n"]),
            volatility_filter=int(raw_row["volatility_filter"]),
            signal_score=float(raw_row["signal_score"]),
        )
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError(f"Failed to parse signal row in {path}: {raw_row}") from error


def _parse_optional_float(raw_value: str) -> float | None:
    if raw_value == "":
        return None
    return float(raw_value)


def _parse_optional_int(raw_value: str) -> int | None:
    if raw_value == "":
        return None
    return int(raw_value)


def _parse_cross_sectional_row(
    raw_row: dict[str, str],
    path: Path,
) -> CrossSectionalSignalRow:
    try:
        return CrossSectionalSignalRow(
            date=date.fromisoformat(raw_row["date"]),
            symbol=raw_row["symbol"],
            close=float(raw_row["close"]),
            avg_traded_value_60d=float(raw_row["avg_traded_value_60d"]),
            liquidity_rank=int(raw_row["liquidity_rank"]),
            momentum_126=_parse_optional_float(raw_row["momentum_126"]),
            volatility_20=_parse_optional_float(raw_row["volatility_20"]),
            signal_score=_parse_optional_float(raw_row["signal_score"]),
            factor_rank=_parse_optional_int(raw_row["factor_rank"]),
            universe_name=raw_row["universe_name"],
        )
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError(f"Failed to parse cross-sectional signal row in {path}: {raw_row}") from error


def _validate_aligned_coverage(
    rows: list[SignalRow],
    symbols: tuple[str, ...],
    aligned_dates: tuple[date, ...],
    path: Path,
) -> None:
    expected = {(row_date, symbol) for row_date in aligned_dates for symbol in symbols}
    actual = {(row.date, row.symbol) for row in rows}
    missing = sorted(expected - actual)
    if missing:
        first_missing_date, first_missing_symbol = missing[0]
        raise ValueError(
            f"Signal panel {path} is missing {first_missing_symbol} on {first_missing_date.isoformat()} "
            "for the requested aligned backtest dates."
        )
