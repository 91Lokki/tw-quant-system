"""Local storage helpers for raw payloads and normalized market data."""

from __future__ import annotations

import csv
from datetime import date
import json
from pathlib import Path
from typing import Any, Iterable

from tw_quant.core.models import NormalizedBar
from tw_quant.data.normalize import NORMALIZED_BAR_COLUMNS


def write_raw_payload(
    raw_cache_dir: Path,
    dataset: str,
    symbol: str,
    start_date: date,
    end_date: date,
    payload: dict[str, Any],
) -> Path:
    """Persist the raw provider payload for traceability."""

    dataset_dir = raw_cache_dir / dataset
    dataset_dir.mkdir(parents=True, exist_ok=True)
    output_path = dataset_dir / f"{symbol}_{start_date.isoformat()}_{end_date.isoformat()}.json"
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return output_path


def write_normalized_csv(path: Path, rows: Iterable[NormalizedBar]) -> Path:
    """Write normalized daily bars to CSV."""

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(NORMALIZED_BAR_COLUMNS))
        writer.writeheader()
        for row in rows:
            writer.writerow(row.to_csv_row())
    return path


def read_normalized_csv(path: Path) -> list[NormalizedBar]:
    """Read normalized bars from CSV."""

    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        rows: list[NormalizedBar] = []
        for row in reader:
            rows.append(
                NormalizedBar(
                    date=date.fromisoformat(row["date"]),
                    symbol=row["symbol"],
                    open=float(row["open"]),
                    high=float(row["high"]),
                    low=float(row["low"]),
                    close=float(row["close"]),
                    volume=_parse_volume(row["volume"]),
                )
            )
    return rows


def cache_covers_range(path: Path, start_date: date, end_date: date) -> bool:
    """Check whether a cached CSV covers the requested date window."""

    if not path.exists():
        return False
    rows = read_normalized_csv(path)
    if not rows:
        return False
    first_date = rows[0].date
    last_date = rows[-1].date
    return first_date <= start_date and last_date >= end_date


def _parse_volume(raw_value: str) -> int | None:
    if raw_value == "":
        return None
    return int(float(raw_value))
