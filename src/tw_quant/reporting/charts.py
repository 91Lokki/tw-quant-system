"""Generate simple SVG performance charts for backtest outputs."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from html import escape
from pathlib import Path


@dataclass(frozen=True, slots=True)
class NavPoint:
    date_label: str
    nav: float
    benchmark_nav: float


def write_backtest_charts(
    nav_path: Path,
    equity_curve_path: Path,
    drawdown_path: Path,
) -> tuple[Path, Path]:
    """Generate an equity curve chart and a drawdown chart from the NAV CSV."""

    points = _load_nav_points(nav_path)
    equity_curve_path.parent.mkdir(parents=True, exist_ok=True)
    drawdown_path.parent.mkdir(parents=True, exist_ok=True)
    equity_curve_path.write_text(_render_equity_curve(points), encoding="utf-8")
    drawdown_path.write_text(_render_drawdown_chart(points), encoding="utf-8")
    return equity_curve_path, drawdown_path


def _load_nav_points(nav_path: Path) -> list[NavPoint]:
    points: list[NavPoint] = []
    with nav_path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            points.append(
                NavPoint(
                    date_label=row["date"],
                    nav=float(row["nav"]),
                    benchmark_nav=float(row["benchmark_nav"]),
                )
            )
    return points


def _render_equity_curve(points: list[NavPoint]) -> str:
    return _render_line_chart(
        title="Equity Curve",
        dates=[point.date_label for point in points],
        series=(
            ("Portfolio NAV", [point.nav for point in points], "#0f766e"),
            ("Benchmark NAV", [point.benchmark_nav for point in points], "#64748b"),
        ),
        y_formatter=lambda value: f"{value:.2f}x",
    )


def _render_drawdown_chart(points: list[NavPoint]) -> str:
    drawdowns: list[float] = []
    peak = 0.0
    for point in points:
        peak = max(peak, point.nav)
        drawdown = 0.0 if peak == 0 else (point.nav / peak) - 1.0
        drawdowns.append(drawdown)

    return _render_area_chart(
        title="Drawdown",
        dates=[point.date_label for point in points],
        values=drawdowns,
        line_color="#dc2626",
        fill_color="#fecaca",
        y_formatter=lambda value: f"{value:.1%}",
    )


def _render_line_chart(
    title: str,
    dates: list[str],
    series: tuple[tuple[str, list[float], str], ...],
    y_formatter,
) -> str:
    if not dates:
        return _render_empty_chart(title)

    width = 960
    height = 420
    left = 78
    right = 24
    top = 54
    bottom = 52
    plot_width = width - left - right
    plot_height = height - top - bottom

    all_values = [value for _, values, _ in series for value in values]
    y_min, y_max = _normalized_bounds(min(all_values), max(all_values))
    x_positions = _x_positions(len(dates), left, plot_width)

    elements: list[str] = [_svg_shell_open(width, height), _chart_title(title, left, 30)]
    elements.extend(_grid_elements(left, top, plot_width, plot_height, y_min, y_max, y_formatter))
    elements.extend(_axis_elements(left, top, plot_width, plot_height))
    elements.extend(_x_label_elements(dates, x_positions, top + plot_height + 22))

    legend_x = left + plot_width - 160
    legend_y = top - 28
    for index, (label, values, color) in enumerate(series):
        points_text = _polyline_points(x_positions, values, top, plot_height, y_min, y_max)
        elements.append(
            f'<polyline points="{points_text}" fill="none" stroke="{color}" '
            'stroke-width="2.5" stroke-linejoin="round" stroke-linecap="round" />'
        )
        elements.append(
            f'<rect x="{legend_x}" y="{legend_y + (index * 18)}" width="12" height="3" fill="{color}" />'
        )
        elements.append(
            f'<text x="{legend_x + 18}" y="{legend_y + 4 + (index * 18)}" '
            'font-size="12" fill="#334155">'
            f"{escape(label)}</text>"
        )

    elements.append("</svg>")
    return "\n".join(elements)


def _render_area_chart(
    title: str,
    dates: list[str],
    values: list[float],
    line_color: str,
    fill_color: str,
    y_formatter,
) -> str:
    if not dates:
        return _render_empty_chart(title)

    width = 960
    height = 420
    left = 78
    right = 24
    top = 54
    bottom = 52
    plot_width = width - left - right
    plot_height = height - top - bottom

    min_value = min(values)
    max_value = max(0.0, max(values))
    y_min, y_max = _normalized_bounds(min_value, max_value, prefer_zero_top=True)
    x_positions = _x_positions(len(dates), left, plot_width)
    baseline_y = _scale_y(0.0, top, plot_height, y_min, y_max)
    line_points = _point_pairs(x_positions, values, top, plot_height, y_min, y_max)
    polygon_points = [(x_positions[0], baseline_y), *line_points, (x_positions[-1], baseline_y)]

    elements: list[str] = [_svg_shell_open(width, height), _chart_title(title, left, 30)]
    elements.extend(_grid_elements(left, top, plot_width, plot_height, y_min, y_max, y_formatter))
    elements.extend(_axis_elements(left, top, plot_width, plot_height))
    elements.extend(_x_label_elements(dates, x_positions, top + plot_height + 22))
    elements.append(
        f'<polygon points="{_format_points(polygon_points)}" fill="{fill_color}" opacity="0.9" />'
    )
    elements.append(
        f'<polyline points="{_format_points(line_points)}" fill="none" stroke="{line_color}" '
        'stroke-width="2.5" stroke-linejoin="round" stroke-linecap="round" />'
    )
    elements.append("</svg>")
    return "\n".join(elements)


def _svg_shell_open(width: int, height: int) -> str:
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" role="img" aria-label="Backtest chart">'
        '<rect width="100%" height="100%" fill="#ffffff" />'
    )


def _chart_title(title: str, x: int, y: int) -> str:
    return (
        f'<text x="{x}" y="{y}" font-size="20" font-weight="600" fill="#0f172a">'
        f"{escape(title)}</text>"
    )


def _render_empty_chart(title: str) -> str:
    return "\n".join(
        [
            _svg_shell_open(960, 420),
            _chart_title(title, 78, 30),
            '<text x="78" y="100" font-size="14" fill="#64748b">No data available.</text>',
            "</svg>",
        ]
    )


def _axis_elements(left: int, top: int, plot_width: int, plot_height: int) -> list[str]:
    return [
        (
            f'<line x1="{left}" y1="{top}" x2="{left}" y2="{top + plot_height}" '
            'stroke="#94a3b8" stroke-width="1" />'
        ),
        (
            f'<line x1="{left}" y1="{top + plot_height}" x2="{left + plot_width}" '
            f'y2="{top + plot_height}" stroke="#94a3b8" stroke-width="1" />'
        ),
    ]


def _grid_elements(
    left: int,
    top: int,
    plot_width: int,
    plot_height: int,
    y_min: float,
    y_max: float,
    y_formatter,
) -> list[str]:
    elements: list[str] = []
    tick_count = 5
    for tick in range(tick_count):
        fraction = tick / (tick_count - 1)
        value = y_max - ((y_max - y_min) * fraction)
        y = top + (plot_height * fraction)
        elements.append(
            f'<line x1="{left}" y1="{y:.2f}" x2="{left + plot_width}" y2="{y:.2f}" '
            'stroke="#e2e8f0" stroke-width="1" />'
        )
        elements.append(
            f'<text x="{left - 10}" y="{y + 4:.2f}" text-anchor="end" '
            f'font-size="12" fill="#475569">{escape(y_formatter(value))}</text>'
        )
    return elements


def _x_label_elements(dates: list[str], x_positions: list[float], y: float) -> list[str]:
    if not dates:
        return []
    label_indexes = sorted({0, len(dates) // 2, len(dates) - 1})
    elements: list[str] = []
    for index in label_indexes:
        elements.append(
            f'<text x="{x_positions[index]:.2f}" y="{y:.2f}" text-anchor="middle" '
            f'font-size="12" fill="#475569">{escape(dates[index])}</text>'
        )
    return elements


def _x_positions(count: int, left: int, plot_width: int) -> list[float]:
    if count <= 1:
        return [left + (plot_width / 2.0)]
    return [left + ((plot_width * index) / (count - 1)) for index in range(count)]


def _polyline_points(
    x_positions: list[float],
    values: list[float],
    top: int,
    plot_height: int,
    y_min: float,
    y_max: float,
) -> str:
    return _format_points(_point_pairs(x_positions, values, top, plot_height, y_min, y_max))


def _point_pairs(
    x_positions: list[float],
    values: list[float],
    top: int,
    plot_height: int,
    y_min: float,
    y_max: float,
) -> list[tuple[float, float]]:
    return [
        (x_position, _scale_y(value, top, plot_height, y_min, y_max))
        for x_position, value in zip(x_positions, values, strict=True)
    ]


def _format_points(points: list[tuple[float, float]]) -> str:
    return " ".join(f"{x:.2f},{y:.2f}" for x, y in points)


def _scale_y(value: float, top: int, plot_height: int, y_min: float, y_max: float) -> float:
    if y_max == y_min:
        return top + (plot_height / 2.0)
    fraction = (value - y_min) / (y_max - y_min)
    return top + ((1.0 - fraction) * plot_height)


def _normalized_bounds(
    y_min: float,
    y_max: float,
    prefer_zero_top: bool = False,
) -> tuple[float, float]:
    if prefer_zero_top:
        y_max = max(0.0, y_max)
    if y_min == y_max:
        padding = max(abs(y_min) * 0.05, 0.05)
        return y_min - padding, y_max + padding
    padding = (y_max - y_min) * 0.08
    upper = y_max + padding
    lower = y_min - padding
    if prefer_zero_top:
        upper = max(0.0, upper)
    return lower, upper
