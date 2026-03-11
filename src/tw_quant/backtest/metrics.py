"""Performance metric calculations for the v1 daily backtest engine."""

from __future__ import annotations

from math import sqrt
from statistics import fmean, pstdev

from tw_quant.core.models import NavRow, PerformanceMetrics


def compute_metrics(nav_rows: list[NavRow], initial_nav: float) -> PerformanceMetrics:
    """Compute core backtest metrics from the daily NAV series."""

    if not nav_rows:
        return PerformanceMetrics(
            cumulative_return=0.0,
            annualized_return=0.0,
            annualized_volatility=0.0,
            max_drawdown=0.0,
            sharpe_ratio=0.0,
            turnover=0.0,
        )

    daily_returns = [row.daily_return for row in nav_rows]
    nav_values = [row.nav for row in nav_rows]
    cumulative_return = (nav_values[-1] / initial_nav) - 1.0
    annualized_return = _annualized_return(nav_values[-1] / initial_nav, len(nav_rows))
    annualized_volatility = _annualized_volatility(daily_returns)
    max_drawdown = _max_drawdown(nav_values)
    sharpe_ratio = _sharpe_ratio(daily_returns, annualized_volatility)
    turnover = sum(row.turnover for row in nav_rows)

    return PerformanceMetrics(
        cumulative_return=cumulative_return,
        annualized_return=annualized_return,
        annualized_volatility=annualized_volatility,
        max_drawdown=max_drawdown,
        sharpe_ratio=sharpe_ratio,
        turnover=turnover,
    )


def _annualized_return(growth_multiple: float, trading_days: int) -> float:
    if trading_days <= 0 or growth_multiple <= 0:
        return 0.0
    return growth_multiple ** (252.0 / trading_days) - 1.0


def _annualized_volatility(daily_returns: list[float]) -> float:
    if len(daily_returns) <= 1:
        return 0.0
    return float(pstdev(daily_returns) * sqrt(252))


def _max_drawdown(nav_values: list[float]) -> float:
    running_max = nav_values[0]
    max_drawdown = 0.0
    for nav_value in nav_values:
        running_max = max(running_max, nav_value)
        drawdown = (nav_value / running_max) - 1.0
        max_drawdown = min(max_drawdown, drawdown)
    return max_drawdown


def _sharpe_ratio(daily_returns: list[float], annualized_volatility: float) -> float:
    if not daily_returns or annualized_volatility == 0:
        return 0.0
    mean_daily_return = fmean(daily_returns)
    return float((mean_daily_return * 252) / annualized_volatility)
