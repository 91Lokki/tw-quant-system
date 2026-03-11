"""Portfolio construction utilities for the v1 daily backtest workflow."""

from __future__ import annotations

from collections import defaultdict
from datetime import date
from typing import Callable

from tw_quant.core.models import PortfolioConfig, PortfolioWeightRow, SignalRow


def determine_rebalance_dates(
    trading_dates: tuple[date, ...],
    frequency: str,
) -> tuple[date, ...]:
    """Return rebalance dates using the first aligned trading day of each period."""

    if not trading_dates:
        return ()

    if frequency == "daily":
        candidates = list(trading_dates)
    elif frequency == "weekly":
        candidates = _first_dates_by_period(trading_dates, lambda row_date: row_date.isocalendar()[:2])
    elif frequency == "monthly":
        candidates = _first_dates_by_period(trading_dates, lambda row_date: (row_date.year, row_date.month))
    else:
        raise ValueError(f"Unsupported rebalance frequency: {frequency}")

    if len(candidates) > 1:
        candidates = candidates[:-1]
    else:
        candidates = []
    return tuple(candidates)


def build_target_weights(
    signal_rows: list[SignalRow],
    portfolio_config: PortfolioConfig,
    rebalance_dates: tuple[date, ...],
) -> tuple[dict[date, dict[str, float]], dict[date, dict[str, float]]]:
    """Convert signal rows into target weights on rebalance dates."""

    signal_scores_by_date = _group_signal_scores(signal_rows, portfolio_config.tradable_symbols)
    target_weights_by_date: dict[date, dict[str, float]] = {}

    for rebalance_date in rebalance_dates:
        day_scores = signal_scores_by_date.get(rebalance_date)
        if day_scores is None:
            raise ValueError(
                f"Signal panel is missing tradable symbols for rebalance date {rebalance_date.isoformat()}."
            )

        positive_symbols = [
            symbol
            for symbol, score in sorted(day_scores.items(), key=lambda item: (-item[1], item[0]))
            if score > portfolio_config.min_signal_score
        ]
        selected_symbols = positive_symbols[: portfolio_config.max_positions]
        if not selected_symbols and not portfolio_config.hold_cash_when_inactive:
            selected_symbols = list(portfolio_config.tradable_symbols)

        target_weights_by_date[rebalance_date] = _equal_weight_allocation(
            selected_symbols=tuple(selected_symbols),
            tradable_symbols=portfolio_config.tradable_symbols,
            max_weight=portfolio_config.max_weight,
        )

    return target_weights_by_date, signal_scores_by_date


def expand_daily_weights(
    trading_dates: tuple[date, ...],
    tradable_symbols: tuple[str, ...],
    target_weights_by_date: dict[date, dict[str, float]],
    signal_scores_by_date: dict[date, dict[str, float]],
) -> tuple[dict[date, dict[str, float]], list[PortfolioWeightRow]]:
    """Expand rebalance targets into daily applied weights for the backtest."""

    current_weights = {symbol: 0.0 for symbol in tradable_symbols}
    current_signal_scores = {symbol: None for symbol in tradable_symbols}
    applied_weights_by_date: dict[date, dict[str, float]] = {}
    output_rows: list[PortfolioWeightRow] = []

    last_index = len(trading_dates) - 1
    for index, trading_date in enumerate(trading_dates):
        applied_weights_by_date[trading_date] = current_weights.copy()
        for symbol in tradable_symbols:
            output_rows.append(
                PortfolioWeightRow(
                    date=trading_date,
                    symbol=symbol,
                    weight=current_weights[symbol],
                    signal_score=current_signal_scores[symbol],
                )
            )

        if trading_date in target_weights_by_date and index < last_index:
            current_weights = target_weights_by_date[trading_date].copy()
            current_signal_scores = {
                symbol: signal_scores_by_date[trading_date][symbol]
                for symbol in tradable_symbols
            }

    return applied_weights_by_date, output_rows


def _group_signal_scores(
    signal_rows: list[SignalRow],
    tradable_symbols: tuple[str, ...],
) -> dict[date, dict[str, float]]:
    grouped: dict[date, dict[str, float]] = defaultdict(dict)
    allowed_symbols = set(tradable_symbols)
    for row in signal_rows:
        if row.symbol not in allowed_symbols:
            continue
        grouped[row.date][row.symbol] = row.signal_score

    for row_date, scores in grouped.items():
        missing = [symbol for symbol in tradable_symbols if symbol not in scores]
        if missing:
            missing_text = ", ".join(missing)
            raise ValueError(f"Signal panel is missing {missing_text} on {row_date.isoformat()}.")
    return dict(grouped)


def _equal_weight_allocation(
    selected_symbols: tuple[str, ...],
    tradable_symbols: tuple[str, ...],
    max_weight: float,
) -> dict[str, float]:
    weights = {symbol: 0.0 for symbol in tradable_symbols}
    if not selected_symbols:
        return weights

    uncapped_weight = 1.0 / len(selected_symbols)
    assigned_weight = min(max_weight, uncapped_weight)
    for symbol in selected_symbols:
        weights[symbol] = assigned_weight
    return weights


def _first_dates_by_period(
    trading_dates: tuple[date, ...],
    period_key: Callable[[date], object],
) -> list[date]:
    selected_dates: list[date] = []
    current_key: object | None = None
    for trading_date in trading_dates:
        next_key = period_key(trading_date)
        if next_key != current_key:
            selected_dates.append(trading_date)
            current_key = next_key
    return selected_dates
