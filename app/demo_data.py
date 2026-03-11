"""Helpers for loading local artifacts into the Streamlit demo app."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd


def get_project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def discover_projects(project_root: Path) -> tuple[str, ...]:
    candidates: dict[str, float] = {}
    for base_dir in (
        project_root / "data" / "processed" / "backtests",
        project_root / "data" / "processed" / "reports",
    ):
        if not base_dir.exists():
            continue
        for entry in base_dir.iterdir():
            if entry.is_dir():
                candidates[entry.name] = max(candidates.get(entry.name, 0.0), entry.stat().st_mtime)
    ordered = sorted(candidates.items(), key=lambda item: (-item[1], item[0]))
    return tuple(name for name, _ in ordered)


def build_project_paths(project_root: Path, project_name: str) -> dict[str, Path]:
    processed_dir = project_root / "data" / "processed"
    reports_dir = processed_dir / "reports" / project_name
    return {
        "normalized_dir": processed_dir / "market_data" / "daily",
        "signal_panel": processed_dir / "signals" / "daily" / "signal_panel.csv",
        "backtest_dir": processed_dir / "backtests" / project_name,
        "nav": processed_dir / "backtests" / project_name / "daily_nav.csv",
        "weights": processed_dir / "backtests" / project_name / "daily_weights.csv",
        "report_dir": reports_dir,
        "report": reports_dir / "backtest_summary.md",
        "equity_curve": reports_dir / "equity_curve.svg",
        "drawdown": reports_dir / "drawdown.svg",
    }


def build_artifact_status_rows(project_root: Path, project_name: str) -> list[dict[str, str]]:
    paths = build_project_paths(project_root, project_name)
    labels = {
        "normalized_dir": "Normalized Bars Directory",
        "signal_panel": "Signal Panel CSV",
        "nav": "Daily NAV CSV",
        "weights": "Daily Weights CSV",
        "report": "Backtest Summary Report",
        "equity_curve": "Equity Curve SVG",
        "drawdown": "Drawdown SVG",
    }
    rows: list[dict[str, str]] = []
    for key, label in labels.items():
        path = paths[key]
        rows.append(
            {
                "artifact": label,
                "status": "Present" if path.exists() else "Missing",
                "path": _relative_or_absolute(path, project_root),
                "modified_at": _format_modified_at(path),
                "size_kb": _format_size_kb(path),
            }
        )
    return rows


def summarize_artifact_status_rows(rows: list[dict[str, str]]) -> dict[str, str | int]:
    present_rows = [row for row in rows if row["status"] == "Present"]
    missing_rows = [row for row in rows if row["status"] != "Present"]
    latest_modified_at = max(
        (row["modified_at"] for row in present_rows if row["modified_at"] != "-"),
        default="-",
    )
    return {
        "present_count": len(present_rows),
        "missing_count": len(missing_rows),
        "total_count": len(rows),
        "latest_modified_at": latest_modified_at,
    }


def list_directory_files(directory: Path, project_root: Path, suffix: str | None = None) -> pd.DataFrame:
    import pandas as pd

    rows: list[dict[str, str]] = []
    if directory.exists():
        entries = sorted(directory.iterdir(), key=lambda entry: entry.name)
        for entry in entries:
            if not entry.is_file():
                continue
            if suffix is not None and entry.suffix.lower() != suffix.lower():
                continue
            rows.append(
                {
                    "name": entry.name,
                    "path": _relative_or_absolute(entry, project_root),
                    "modified_at": _format_modified_at(entry),
                    "size_kb": _format_size_kb(entry),
                }
            )
    return pd.DataFrame(rows)


def load_nav_frame(nav_path: Path) -> pd.DataFrame | None:
    import pandas as pd

    if not nav_path.exists():
        return None
    frame = pd.read_csv(nav_path, parse_dates=["date"])
    frame.sort_values("date", inplace=True)
    return frame


def load_weights_frame(weights_path: Path) -> pd.DataFrame | None:
    import pandas as pd

    if not weights_path.exists():
        return None
    frame = pd.read_csv(weights_path, parse_dates=["date"])
    frame.sort_values(["date", "symbol"], inplace=True)
    return frame


def load_summary_metrics(report_path: Path) -> dict[str, str]:
    if not report_path.exists():
        return {}
    summary: dict[str, str] = {}
    for raw_line in report_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line.startswith("- ") or ": " not in line:
            continue
        key, value = line[2:].split(": ", maxsplit=1)
        summary[key.strip()] = value.strip()
    return summary


def load_markdown(report_path: Path) -> str | None:
    if not report_path.exists():
        return None
    return report_path.read_text(encoding="utf-8")


def load_svg(svg_path: Path) -> str | None:
    if not svg_path.exists():
        return None
    return svg_path.read_text(encoding="utf-8")


def build_latest_portfolio_snapshot(
    weight_rows: list[dict[str, object]],
    nav_rows: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    if not weight_rows:
        return {
            "latest_date": None,
            "held_count": 0,
            "held_symbols": tuple(),
            "gross_exposure": 0.0,
            "cash_weight": None,
            "latest_rows": tuple(),
            "held_rows": tuple(),
        }

    normalized_weight_rows: list[dict[str, object]] = []
    for row in weight_rows:
        normalized_weight_rows.append(
            {
                "date": _normalize_date_value(row.get("date")),
                "symbol": str(row.get("symbol", "")),
                "weight": _to_float(row.get("weight")),
                "signal_score": _to_float_or_none(row.get("signal_score")),
            }
        )

    latest_date = max(str(row["date"]) for row in normalized_weight_rows)
    latest_rows = tuple(
        sorted(
            (row for row in normalized_weight_rows if row["date"] == latest_date),
            key=lambda row: str(row["symbol"]),
        )
    )
    held_rows = tuple(row for row in latest_rows if float(row["weight"]) > 0.0)
    held_symbols = tuple(str(row["symbol"]) for row in held_rows)
    gross_exposure = sum(float(row["weight"]) for row in latest_rows)

    cash_weight = None
    if nav_rows:
        normalized_nav_rows = [
            {
                "date": _normalize_date_value(row.get("date")),
                "cash_weight": _to_float_or_none(row.get("cash_weight")),
            }
            for row in nav_rows
        ]
        latest_nav_row = next(
            (row for row in reversed(normalized_nav_rows) if row["date"] == latest_date),
            normalized_nav_rows[-1] if normalized_nav_rows else None,
        )
        if latest_nav_row is not None:
            cash_weight = latest_nav_row["cash_weight"]

    return {
        "latest_date": latest_date,
        "held_count": len(held_rows),
        "held_symbols": held_symbols,
        "gross_exposure": gross_exposure,
        "cash_weight": cash_weight,
        "latest_rows": latest_rows,
        "held_rows": held_rows,
    }


def _relative_or_absolute(path: Path, project_root: Path) -> str:
    try:
        return str(path.relative_to(project_root))
    except ValueError:
        return str(path)


def _format_modified_at(path: Path) -> str:
    if not path.exists():
        return "-"
    return datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")


def _format_size_kb(path: Path) -> str:
    if not path.exists() or not path.is_file():
        return "-"
    return f"{path.stat().st_size / 1024.0:.1f}"


def _normalize_date_value(raw_value: object) -> str:
    if raw_value is None:
        return ""
    if hasattr(raw_value, "strftime"):
        return raw_value.strftime("%Y-%m-%d")
    return str(raw_value)


def _to_float(raw_value: object) -> float:
    try:
        if raw_value is None or raw_value == "":
            return 0.0
        return float(raw_value)
    except (TypeError, ValueError):
        return 0.0


def _to_float_or_none(raw_value: object) -> float | None:
    try:
        if raw_value is None or raw_value == "":
            return None
        return float(raw_value)
    except (TypeError, ValueError):
        return None
