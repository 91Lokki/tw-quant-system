"""Concrete TWSE top-liquidity universe helpers for the cross-sectional branch."""

from __future__ import annotations

import csv
from datetime import date
from pathlib import Path
from statistics import fmean
from typing import Any

from tw_quant.core.models import NormalizedBar, UniverseMembershipRow
from tw_quant.data.store import read_normalized_csv


METADATA_COLUMNS = ("stock_id", "stock_name", "type", "industry_category", "date")
AVAILABILITY_COLUMNS = (
    "stock_id",
    "has_usable_price_data",
    "status",
    "row_count",
    "first_date",
    "last_date",
)
UNIVERSE_NAME = "twse_top50_liquidity"
_EXCLUDED_KEYWORDS = (
    "etf",
    "etn",
    "index",
    "warrant",
    "preferred",
    "dr",
    "特別股",
    "權證",
    "指數",
    "基金",
    "槓桿",
    "反向",
)


def is_twse_common_stock_candidate(
    stock_id: str,
    stock_name: str,
    industry_category: str = "",
) -> bool:
    """Return whether a TWSE row looks like a practical common-stock candidate in v1."""

    normalized_symbol = stock_id.strip()
    if len(normalized_symbol) != 4 or not normalized_symbol.isdigit():
        return False
    if normalized_symbol.startswith("0"):
        return False
    combined_text = f"{stock_name} {industry_category}".lower()
    if any(keyword in combined_text for keyword in _EXCLUDED_KEYWORDS):
        return False
    return True


def filter_twse_common_stocks(rows: list[dict[str, Any]]) -> list[dict[str, str]]:
    """Return a reproducible TWSE common-stock candidate list from metadata-like rows."""

    latest_by_symbol: dict[str, dict[str, str]] = {}
    for row in rows:
        stock_id = str(row.get("stock_id", "")).strip()
        market_type = str(row.get("type", "")).strip().lower()
        stock_name = str(row.get("stock_name", "")).strip()
        industry_category = str(row.get("industry_category", "")).strip()
        row_date = str(row.get("date", "")).strip()
        if market_type != "twse":
            continue
        if not is_twse_common_stock_candidate(stock_id, stock_name, industry_category):
            continue
        latest_by_symbol[stock_id] = {
            "stock_id": stock_id,
            "stock_name": stock_name,
            "type": market_type,
            "industry_category": industry_category,
            "date": row_date,
        }

    return [latest_by_symbol[symbol] for symbol in sorted(latest_by_symbol)]


def write_stock_metadata(path: Path, rows: list[dict[str, str]]) -> Path:
    """Persist filtered TWSE stock metadata for reproducible universe construction."""

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(METADATA_COLUMNS))
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in METADATA_COLUMNS})
    return path


def write_stock_availability(path: Path, rows: list[dict[str, str]]) -> Path:
    """Persist per-symbol price availability for the configured research horizon."""

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(AVAILABILITY_COLUMNS))
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in AVAILABILITY_COLUMNS})
    return path


def load_stock_availability(path: Path) -> list[dict[str, str]]:
    """Load the persisted per-symbol price availability artifact."""

    if not path.exists():
        raise FileNotFoundError(f"Stock availability artifact not found: {path}")

    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError(f"CSV header is missing in {path}.")
        missing_columns = [column for column in AVAILABILITY_COLUMNS if column not in reader.fieldnames]
        if missing_columns:
            raise ValueError(f"Missing availability columns in {path}: {', '.join(missing_columns)}")
        return [{column: row.get(column, "") for column in AVAILABILITY_COLUMNS} for row in reader]


def load_stock_metadata(path: Path) -> list[dict[str, str]]:
    """Load previously saved TWSE stock metadata."""

    if not path.exists():
        raise FileNotFoundError(f"Stock metadata artifact not found: {path}")

    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError(f"CSV header is missing in {path}.")
        missing_columns = [column for column in METADATA_COLUMNS if column not in reader.fieldnames]
        if missing_columns:
            raise ValueError(f"Missing metadata columns in {path}: {', '.join(missing_columns)}")
        return [{column: row.get(column, "") for column in METADATA_COLUMNS} for row in reader]


def build_top_liquidity_membership(
    normalized_dir: Path,
    candidate_symbols: tuple[str, ...],
    benchmark_symbol: str,
    start_date: date,
    end_date: date,
    liquidity_lookback_days: int,
    top_n: int,
    universe_name: str = UNIVERSE_NAME,
) -> tuple[list[UniverseMembershipRow], tuple[date, ...], tuple[str, ...], tuple[str, ...]]:
    """Build monthly top-liquidity membership rows from local normalized bars."""

    benchmark_rows = _filter_by_date(
        read_normalized_csv(normalized_dir / f"{benchmark_symbol}.csv"),
        start_date,
        end_date,
    )
    reconstitution_dates = _first_monthly_dates(tuple(row.date for row in benchmark_rows))

    bars_by_symbol: dict[str, list[NormalizedBar]] = {}
    missing_symbols: list[str] = []
    for symbol in candidate_symbols:
        path = normalized_dir / f"{symbol}.csv"
        if not path.exists():
            missing_symbols.append(symbol)
            continue
        bars_by_symbol[symbol] = _filter_by_date(read_normalized_csv(path), start_date, end_date)

    membership_rows: list[UniverseMembershipRow] = []
    participating_symbols: set[str] = set()

    for reconstitution_date in reconstitution_dates:
        liquidity_rows: list[tuple[str, float]] = []
        for symbol, bars in bars_by_symbol.items():
            avg_traded_value = _average_recent_traded_value(
                bars,
                as_of_date=reconstitution_date,
                lookback_days=liquidity_lookback_days,
            )
            if avg_traded_value is None:
                continue
            liquidity_rows.append((symbol, avg_traded_value))

        liquidity_rows.sort(key=lambda item: (-item[1], item[0]))
        for rank, (symbol, avg_traded_value) in enumerate(liquidity_rows[:top_n], start=1):
            participating_symbols.add(symbol)
            membership_rows.append(
                UniverseMembershipRow(
                    date=reconstitution_date,
                    symbol=symbol,
                    liquidity_rank=rank,
                    avg_traded_value_60d=avg_traded_value,
                    universe_name=universe_name,
                    is_member=True,
                )
            )

    notes: list[str] = []
    if missing_symbols:
        notes.append(f"{len(missing_symbols)} 檔候選股缺少 normalized bars，已在 universe 建立時略過。")

    return (
        membership_rows,
        reconstitution_dates,
        tuple(sorted(participating_symbols)),
        tuple(notes),
    )


def write_universe_membership(path: Path, rows: list[UniverseMembershipRow]) -> Path:
    """Write the monthly top-liquidity membership artifact."""

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "date",
                "symbol",
                "liquidity_rank",
                "avg_traded_value_60d",
                "universe_name",
                "is_member",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row.to_csv_row())
    return path


def load_universe_membership(
    path: Path,
    start_date: date,
    end_date: date,
) -> list[UniverseMembershipRow]:
    """Load the persisted monthly top-liquidity membership artifact."""

    if not path.exists():
        raise FileNotFoundError(f"Universe membership artifact not found: {path}")

    rows: list[UniverseMembershipRow] = []
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError(f"CSV header is missing in {path}.")
        required_columns = (
            "date",
            "symbol",
            "liquidity_rank",
            "avg_traded_value_60d",
            "universe_name",
            "is_member",
        )
        missing_columns = [column for column in required_columns if column not in reader.fieldnames]
        if missing_columns:
            raise ValueError(
                f"Missing universe membership columns in {path}: {', '.join(missing_columns)}"
            )
        for raw_row in reader:
            row = UniverseMembershipRow(
                date=date.fromisoformat(raw_row["date"]),
                symbol=raw_row["symbol"],
                liquidity_rank=int(raw_row["liquidity_rank"]),
                avg_traded_value_60d=float(raw_row["avg_traded_value_60d"]),
                universe_name=raw_row["universe_name"],
                is_member=raw_row["is_member"] == "1",
            )
            if start_date <= row.date <= end_date:
                rows.append(row)

    rows.sort(key=lambda row: (row.date, row.liquidity_rank, row.symbol))
    return rows


def validate_membership_coverage(
    rows: list[UniverseMembershipRow],
    expected_top_n: int,
) -> None:
    """Ensure every membership date contains the expected number of universe members."""

    if not rows:
        raise ValueError("Universe membership artifact is empty.")

    counts_by_date: dict[date, int] = {}
    for row in rows:
        counts_by_date[row.date] = counts_by_date.get(row.date, 0) + 1

    insufficient_dates = [
        (row_date, count)
        for row_date, count in sorted(counts_by_date.items())
        if count < expected_top_n
    ]
    if insufficient_dates:
        first_date, count = insufficient_dates[0]
        raise ValueError(
            "Universe membership artifact is incomplete for the configured top-liquidity branch: "
            f"{first_date.isoformat()} only has {count} members, expected {expected_top_n}."
        )


def validate_artifact_freshness(reference_path: Path, dependent_paths: tuple[Path, ...]) -> None:
    """Ensure dependent artifacts are at least as new as the reference artifact."""

    if not reference_path.exists():
        raise FileNotFoundError(f"Required reference artifact not found: {reference_path}")

    reference_mtime = reference_path.stat().st_mtime
    for dependent_path in dependent_paths:
        if not dependent_path.exists():
            raise FileNotFoundError(f"Required downstream artifact not found: {dependent_path}")
        if dependent_path.stat().st_mtime + 1e-9 < reference_mtime:
            raise ValueError(
                "Detected stale downstream artifacts for the cross-sectional branch. "
                f"{dependent_path} is older than {reference_path}. "
                "Please rerun the pipeline in order: ingest -> signals -> backtest."
            )


def _filter_by_date(rows: list[NormalizedBar], start_date: date, end_date: date) -> list[NormalizedBar]:
    return [row for row in rows if start_date <= row.date <= end_date]


def _first_monthly_dates(trading_dates: tuple[date, ...]) -> tuple[date, ...]:
    selected_dates: list[date] = []
    current_key: tuple[int, int] | None = None
    for trading_date in trading_dates:
        next_key = (trading_date.year, trading_date.month)
        if next_key != current_key:
            selected_dates.append(trading_date)
            current_key = next_key
    return tuple(selected_dates)


def _average_recent_traded_value(
    bars: list[NormalizedBar],
    as_of_date: date,
    lookback_days: int,
) -> float | None:
    eligible = [bar.traded_value for bar in bars if bar.date <= as_of_date and bar.traded_value is not None]
    if len(eligible) < lookback_days:
        return None
    window = eligible[-lookback_days:]
    return float(fmean(window))
