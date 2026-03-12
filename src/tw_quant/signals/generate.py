"""Signal generation utilities for daily Taiwan equities data."""

from __future__ import annotations

import csv
from datetime import date
from math import sqrt
from pathlib import Path
from statistics import fmean, pstdev

from tw_quant.core.models import (
    BacktestConfig,
    CrossSectionalSignalRow,
    MarketDataset,
    NormalizedBar,
    SignalConfig,
    SignalRow,
    UniverseMembershipRow,
)
from tw_quant.data.store import read_normalized_csv


SIGNAL_COLUMNS = (
    "date",
    "symbol",
    "close",
    "ma_fast",
    "ma_slow",
    "trend_signal",
    "momentum_n",
    "momentum_signal",
    "volatility_n",
    "volatility_filter",
    "signal_score",
)

CROSS_SECTIONAL_SIGNAL_COLUMNS = (
    "date",
    "symbol",
    "close",
    "avg_traded_value_60d",
    "liquidity_rank",
    "momentum_126",
    "volatility_20",
    "signal_score",
    "factor_rank",
    "universe_name",
)


def generate_signals(config: BacktestConfig) -> dict[str, object]:
    """Return a lightweight placeholder payload for the scaffold backtest stage."""

    return {
        "model_name": "standalone_daily_signal_pipeline",
        "frequency": "daily",
        "universe": config.universe,
        "records": 0,
        "notes": (
            "Standalone signal generation is available through the signals pipeline.",
            "Backtest integration with saved signal outputs is the next planned step.",
        ),
    }


def build_signal_rows(dataset: MarketDataset, signal_config: SignalConfig) -> list[SignalRow]:
    """Build a simple, explicit daily signal panel from aligned market data."""

    rows: list[SignalRow] = []
    for symbol in dataset.symbols:
        bars = list(dataset.bars_by_symbol[symbol])
        closes = [bar.close for bar in bars]
        returns = _daily_returns(closes)

        for index, bar in enumerate(bars):
            ma_fast = _rolling_mean(closes, index, signal_config.ma_fast_window)
            ma_slow = _rolling_mean(closes, index, signal_config.ma_slow_window)
            trend_signal = _trend_signal(ma_fast, ma_slow)

            momentum_n = _momentum(closes, index, signal_config.momentum_window)
            momentum_signal = _sign(momentum_n)

            volatility_n = _rolling_volatility(returns, index, signal_config.volatility_window)
            volatility_filter = _volatility_filter(volatility_n, signal_config.volatility_cap)

            signal_score = 0.0
            if volatility_filter == 1 and ma_fast is not None and ma_slow is not None and momentum_n is not None:
                signal_score = (trend_signal + momentum_signal) / 2.0

            rows.append(
                SignalRow(
                    date=bar.date,
                    symbol=bar.symbol,
                    close=bar.close,
                    ma_fast=ma_fast,
                    ma_slow=ma_slow,
                    trend_signal=trend_signal,
                    momentum_n=momentum_n,
                    momentum_signal=momentum_signal,
                    volatility_n=volatility_n,
                    volatility_filter=volatility_filter,
                    signal_score=signal_score,
                )
            )

    return sorted(rows, key=lambda row: (row.date, row.symbol))


def write_signal_rows(path: Path, rows: list[SignalRow]) -> Path:
    """Write the combined signal panel to CSV."""

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(SIGNAL_COLUMNS))
        writer.writeheader()
        for row in rows:
            writer.writerow(row.to_csv_row())
    return path


def build_cross_sectional_signal_rows(
    normalized_dir: Path,
    membership_rows: list[UniverseMembershipRow],
    momentum_window: int,
    volatility_window: int,
) -> list[CrossSectionalSignalRow]:
    """Build a monthly volatility-adjusted momentum panel for top-liquidity members."""

    unique_symbols = sorted({row.symbol for row in membership_rows})
    bars_by_symbol = {symbol: read_normalized_csv(normalized_dir / f"{symbol}.csv") for symbol in unique_symbols}

    provisional_rows: list[CrossSectionalSignalRow] = []
    membership_by_date: dict[date, list[UniverseMembershipRow]] = {}
    for row in membership_rows:
        membership_by_date.setdefault(row.date, []).append(row)

    for rebalance_date in sorted(membership_by_date):
        day_rows: list[CrossSectionalSignalRow] = []
        for membership_row in sorted(
            membership_by_date[rebalance_date],
            key=lambda row: (row.liquidity_rank, row.symbol),
        ):
            signal_row = _build_cross_sectional_signal_row(
                membership_row=membership_row,
                bars=bars_by_symbol[membership_row.symbol],
                momentum_window=momentum_window,
                volatility_window=volatility_window,
            )
            if signal_row is not None:
                day_rows.append(signal_row)

        ranked_rows = _assign_factor_ranks(day_rows)
        provisional_rows.extend(ranked_rows)

    return provisional_rows


def write_cross_sectional_signal_rows(path: Path, rows: list[CrossSectionalSignalRow]) -> Path:
    """Write the monthly cross-sectional signal panel."""

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(CROSS_SECTIONAL_SIGNAL_COLUMNS))
        writer.writeheader()
        for row in rows:
            writer.writerow(row.to_csv_row())
    return path


def _rolling_mean(values: list[float], end_index: int, window: int) -> float | None:
    if end_index + 1 < window:
        return None
    start_index = end_index + 1 - window
    return float(fmean(values[start_index : end_index + 1]))


def _momentum(values: list[float], end_index: int, window: int) -> float | None:
    if end_index < window:
        return None
    reference = values[end_index - window]
    if reference == 0:
        return None
    return (values[end_index] / reference) - 1.0


def _daily_returns(closes: list[float]) -> list[float | None]:
    returns: list[float | None] = [None]
    for index in range(1, len(closes)):
        previous_close = closes[index - 1]
        if previous_close == 0:
            returns.append(None)
            continue
        returns.append((closes[index] / previous_close) - 1.0)
    return returns


def _rolling_volatility(
    returns: list[float | None],
    end_index: int,
    window: int,
) -> float | None:
    if end_index < window:
        return None
    window_slice = returns[end_index - window + 1 : end_index + 1]
    if any(value is None for value in window_slice):
        return None
    clean_values = [float(value) for value in window_slice if value is not None]
    return float(pstdev(clean_values) * sqrt(252))


def _trend_signal(ma_fast: float | None, ma_slow: float | None) -> int:
    if ma_fast is None or ma_slow is None:
        return 0
    if ma_fast > ma_slow:
        return 1
    if ma_fast < ma_slow:
        return -1
    return 0


def _sign(value: float | None) -> int:
    if value is None:
        return 0
    if value > 0:
        return 1
    if value < 0:
        return -1
    return 0


def _volatility_filter(volatility_n: float | None, volatility_cap: float) -> int:
    if volatility_n is None:
        return 0
    return 1 if volatility_n <= volatility_cap else 0


def _build_cross_sectional_signal_row(
    membership_row: UniverseMembershipRow,
    bars: list[NormalizedBar],
    momentum_window: int,
    volatility_window: int,
) -> CrossSectionalSignalRow | None:
    symbol_bars = list(bars)
    dates = [bar.date for bar in symbol_bars]
    try:
        end_index = dates.index(membership_row.date)
    except ValueError:
        return None

    closes = [bar.close for bar in symbol_bars]
    returns = _daily_returns(closes)
    momentum_126 = _momentum(closes, end_index, momentum_window)
    volatility_20 = _rolling_raw_volatility(returns, end_index, volatility_window)
    signal_score = None
    if momentum_126 is not None and volatility_20 is not None and volatility_20 > 0:
        signal_score = momentum_126 / volatility_20

    return CrossSectionalSignalRow(
        date=membership_row.date,
        symbol=membership_row.symbol,
        close=closes[end_index],
        avg_traded_value_60d=membership_row.avg_traded_value_60d,
        liquidity_rank=membership_row.liquidity_rank,
        momentum_126=momentum_126,
        volatility_20=volatility_20,
        signal_score=signal_score,
        factor_rank=None,
        universe_name=membership_row.universe_name,
    )


def _assign_factor_ranks(rows: list[CrossSectionalSignalRow]) -> list[CrossSectionalSignalRow]:
    valid_rows = sorted(
        [row for row in rows if row.signal_score is not None],
        key=lambda row: (-float(row.signal_score), row.symbol),
    )
    rank_lookup = {row.symbol: rank for rank, row in enumerate(valid_rows, start=1)}

    ranked_rows: list[CrossSectionalSignalRow] = []
    for row in rows:
        ranked_rows.append(
            CrossSectionalSignalRow(
                date=row.date,
                symbol=row.symbol,
                close=row.close,
                avg_traded_value_60d=row.avg_traded_value_60d,
                liquidity_rank=row.liquidity_rank,
                momentum_126=row.momentum_126,
                volatility_20=row.volatility_20,
                signal_score=row.signal_score,
                factor_rank=rank_lookup.get(row.symbol),
                universe_name=row.universe_name,
            )
        )
    return ranked_rows


def _rolling_raw_volatility(
    returns: list[float | None],
    end_index: int,
    window: int,
) -> float | None:
    if end_index < window:
        return None
    window_slice = returns[end_index - window + 1 : end_index + 1]
    if any(value is None for value in window_slice):
        return None
    clean_values = [float(value) for value in window_slice if value is not None]
    return float(pstdev(clean_values))
