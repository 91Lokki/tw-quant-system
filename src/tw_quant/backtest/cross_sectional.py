"""Branch-specific backtest helpers for the TWSE top-liquidity cross-sectional workflow."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, replace
from datetime import date

from tw_quant.backtest.metrics import compute_metrics
from tw_quant.core.models import (
    BacktestConfig,
    CrossSectionalSignalRow,
    NavRow,
    NormalizedBar,
    PerformanceMetrics,
    PortfolioWeightRow,
    UniverseMembershipRow,
)
from tw_quant.data import prepare_data_paths
from tw_quant.data.store import read_normalized_csv
from tw_quant.signals import load_cross_sectional_signal_rows
from tw_quant.universe import (
    load_stock_metadata,
    load_universe_membership,
    validate_artifact_freshness,
    validate_membership_coverage,
)


@dataclass(frozen=True, slots=True)
class CrossSectionalBacktestInputs:
    master_dates: tuple[date, ...]
    participating_symbols: tuple[str, ...]
    benchmark_closes: dict[date, float]
    benchmark_history_dates: tuple[date, ...]
    benchmark_history_closes: dict[date, float]
    close_asof_by_symbol: dict[str, dict[date, float | None]]
    membership_rows: tuple[UniverseMembershipRow, ...]
    signal_rows: tuple[CrossSectionalSignalRow, ...]
    notes: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class CrossSectionalBacktestComputation:
    nav_rows: tuple[NavRow, ...]
    weight_rows: tuple[PortfolioWeightRow, ...]
    metrics: PerformanceMetrics
    benchmark_final_nav: float
    notes: tuple[str, ...]
    participating_symbols: tuple[str, ...]


def load_cross_sectional_backtest_inputs(
    config: BacktestConfig,
) -> CrossSectionalBacktestInputs:
    """Load the monthly universe, cross-sectional signals, and sparse bars for Phase B."""

    if config.universe_config is None:
        raise ValueError("Cross-sectional backtest requires universe_config.")

    prepare_data_paths(config.data_paths)
    usable_metadata_rows = load_stock_metadata(config.universe_config.usable_metadata_path)
    if len(usable_metadata_rows) < config.universe_config.top_n:
        raise ValueError(
            "Cross-sectional branch is invalid because the effective candidate pool is smaller than the configured "
            f"top-{config.universe_config.top_n} universe. "
            "Please rerun ingest and inspect twse_price_availability.csv."
        )
    validate_artifact_freshness(
        config.universe_config.usable_metadata_path,
        (config.universe_config.membership_path, config.backtest.signal_input_path),
    )

    membership_rows = tuple(
        load_universe_membership(
            path=config.universe_config.membership_path,
            start_date=config.start_date,
            end_date=config.end_date,
        )
    )
    if not membership_rows:
        raise ValueError(
            "Cross-sectional universe membership artifact is empty for the requested date range."
        )
    validate_membership_coverage(list(membership_rows), config.universe_config.top_n)

    signal_rows = tuple(
        load_cross_sectional_signal_rows(
            path=config.backtest.signal_input_path,
            start_date=config.start_date,
            end_date=config.end_date,
        )
    )

    benchmark_path = config.backtest.bar_input_dir / f"{config.benchmark}.csv"
    benchmark_rows = [
        row
        for row in read_normalized_csv(benchmark_path)
        if config.start_date <= row.date <= config.end_date
    ]
    if not benchmark_rows:
        raise ValueError(
            f"No benchmark rows were found for {config.benchmark} in the requested date range."
        )

    master_dates = tuple(row.date for row in benchmark_rows)
    benchmark_closes = {row.date: row.close for row in benchmark_rows}
    usable_symbol_set = {row["stock_id"] for row in usable_metadata_rows}
    unknown_membership_symbols = sorted({row.symbol for row in membership_rows} - usable_symbol_set)
    if unknown_membership_symbols:
        raise ValueError(
            "Universe membership artifact is inconsistent with the latest usable candidate pool. "
            f"Unexpected symbols: {', '.join(unknown_membership_symbols[:10])}. "
            "Please rerun signals after ingest."
        )
    participating_symbols = tuple(
        sorted(
            ({row.symbol for row in membership_rows} | {row.symbol for row in signal_rows})
            & usable_symbol_set
        )
    )
    if not participating_symbols:
        raise ValueError("Cross-sectional branch could not determine any participating symbols.")

    delayed_listing_count = 0
    close_asof_by_symbol: dict[str, dict[date, float | None]] = {}
    for symbol in participating_symbols:
        symbol_path = config.backtest.bar_input_dir / f"{symbol}.csv"
        if not symbol_path.exists():
            raise FileNotFoundError(f"Missing normalized bar file for {symbol}: {symbol_path}")

        rows = [
            row
            for row in read_normalized_csv(symbol_path)
            if config.start_date <= row.date <= config.end_date
        ]
        if not rows:
            raise ValueError(f"No normalized rows were found for {symbol} in the requested range.")
        if rows[0].date > master_dates[0]:
            delayed_listing_count += 1
        close_asof_by_symbol[symbol] = _build_asof_close_lookup(rows, master_dates)

    notes = [
        f"Cross-sectional branch 使用 {config.benchmark} 作為 master trading calendar，不再依賴所有股票的 full-date intersection。",
        (
            f"每月第一個 {config.benchmark} 交易日會用最近 "
            f"{config.universe_config.liquidity_lookback_days} 個有效交易日平均成交金額重建 "
            f"Top-{config.universe_config.top_n} 流動性 universe。"
        ),
        (
            f"每次換倉會在當期 universe 內依 signal_score 排名，挑選前 "
            f"{config.portfolio.max_positions} 檔股票並採 equal weight。"
        ),
        "新權重在訊號日收盤後才生效，下一個 benchmark 交易日才開始承擔新權重報酬，以避免 lookahead bias。",
        "稀疏個股日線會以 benchmark calendar 上的 as-of close 對齊；若缺少新成交日，持有期間報酬會近似為 0。",
    ]
    if delayed_listing_count > 0:
        notes.append(
            f"{delayed_listing_count} 檔股票在研究起點之後才開始有本地 bars；在其可用之前會被視為尚未上市或不可交易。"
        )

    return CrossSectionalBacktestInputs(
        master_dates=master_dates,
        participating_symbols=participating_symbols,
        benchmark_closes=benchmark_closes,
        benchmark_history_dates=master_dates,
        benchmark_history_closes=benchmark_closes,
        close_asof_by_symbol=close_asof_by_symbol,
        membership_rows=membership_rows,
        signal_rows=signal_rows,
        notes=tuple(notes),
    )


def slice_cross_sectional_backtest_inputs(
    inputs: CrossSectionalBacktestInputs,
    start_date: date,
    end_date: date,
) -> CrossSectionalBacktestInputs:
    """Return a time-sliced view of the loaded cross-sectional inputs."""

    master_dates = tuple(
        trading_date for trading_date in inputs.master_dates if start_date <= trading_date <= end_date
    )
    if not master_dates:
        raise ValueError("Cross-sectional walk-forward test window produced no benchmark trading dates.")

    return CrossSectionalBacktestInputs(
        master_dates=master_dates,
        participating_symbols=inputs.participating_symbols,
        benchmark_closes={trading_date: inputs.benchmark_closes[trading_date] for trading_date in master_dates},
        benchmark_history_dates=tuple(
            trading_date
            for trading_date in inputs.benchmark_history_dates
            if trading_date <= end_date
        ),
        benchmark_history_closes={
            trading_date: inputs.benchmark_history_closes[trading_date]
            for trading_date in inputs.benchmark_history_dates
            if trading_date <= end_date
        },
        close_asof_by_symbol={
            symbol: {trading_date: inputs.close_asof_by_symbol[symbol][trading_date] for trading_date in master_dates}
            for symbol in inputs.participating_symbols
        },
        membership_rows=tuple(
            row for row in inputs.membership_rows if start_date <= row.date <= end_date
        ),
        signal_rows=tuple(
            row for row in inputs.signal_rows if start_date <= row.date <= end_date
        ),
        notes=inputs.notes,
    )


def compute_cross_sectional_backtest_data(
    config: BacktestConfig,
    inputs: CrossSectionalBacktestInputs,
) -> CrossSectionalBacktestComputation:
    """Run the Phase B monthly cross-sectional backtest on already loaded local inputs."""

    rebalance_dates = tuple(sorted({row.date for row in inputs.membership_rows}))
    effective_rebalance_dates = _select_effective_rebalance_dates(
        rebalance_dates,
        config.risk_controls.rebalance_cadence_months,
    )
    signal_rows_by_date = _group_signal_rows_by_date(inputs.signal_rows)
    signal_scores_by_date = {
        row_date: {row.symbol: row.signal_score for row in rows}
        for row_date, rows in signal_rows_by_date.items()
    }
    benchmark_regime_by_date = _build_benchmark_regime_by_date(config, inputs)

    target_weights_by_date: dict[date, dict[str, float]] = {}
    regime_blocked_count = 0
    for rebalance_date in effective_rebalance_dates:
        if not benchmark_regime_by_date.get(rebalance_date, True):
            target_weights_by_date[rebalance_date] = _build_defensive_target_weights_for_date(
                config=config,
                participating_symbols=inputs.participating_symbols,
                signal_rows=signal_rows_by_date.get(rebalance_date, ()),
            )
            regime_blocked_count += 1
            continue
        target_weights_by_date[rebalance_date] = _build_target_weights_for_date(
            config=config,
            participating_symbols=inputs.participating_symbols,
            signal_rows=signal_rows_by_date.get(rebalance_date, ()),
        )
    applied_weights_by_date, weight_rows = _expand_daily_weights(
        trading_dates=inputs.master_dates,
        participating_symbols=inputs.participating_symbols,
        target_weights_by_date=target_weights_by_date,
        signal_scores_by_date=signal_scores_by_date,
    )
    nav_rows, benchmark_final_nav = _simulate_nav(
        config=config,
        master_dates=inputs.master_dates,
        benchmark_closes=inputs.benchmark_closes,
        close_asof_by_symbol=inputs.close_asof_by_symbol,
        participating_symbols=inputs.participating_symbols,
        applied_weights_by_date=applied_weights_by_date,
        target_weights_by_date=target_weights_by_date,
    )
    metrics = compute_metrics(nav_rows, config.backtest.initial_nav)
    notes = _build_cross_sectional_computation_notes(
        config=config,
        base_notes=inputs.notes,
        total_rebalance_dates=rebalance_dates,
        effective_rebalance_dates=effective_rebalance_dates,
        regime_blocked_count=regime_blocked_count,
    )
    return CrossSectionalBacktestComputation(
        nav_rows=tuple(nav_rows),
        weight_rows=tuple(weight_rows),
        metrics=metrics,
        benchmark_final_nav=benchmark_final_nav,
        notes=notes,
        participating_symbols=inputs.participating_symbols,
    )


def build_cross_sectional_variant_configs(
    config: BacktestConfig,
) -> tuple[tuple[str, BacktestConfig], ...]:
    return (
        (
            "original_monthly",
            replace(
                config,
                risk_controls=replace(
                    config.risk_controls,
                    benchmark_filter_enabled=False,
                    defensive_mode="cash",
                    rebalance_cadence_months=1,
                ),
            ),
        ),
        (
            "risk_controlled_3m_cash",
            replace(
                config,
                risk_controls=replace(
                    config.risk_controls,
                    benchmark_filter_enabled=True,
                    defensive_mode="cash",
                    rebalance_cadence_months=3,
                ),
            ),
        ),
        (
            "risk_controlled_3m_half_exposure",
            replace(
                config,
                risk_controls=replace(
                    config.risk_controls,
                    benchmark_filter_enabled=True,
                    defensive_mode="half_exposure",
                    rebalance_cadence_months=3,
                ),
            ),
        ),
        (
            "risk_controlled_3m_top5",
            replace(
                config,
                risk_controls=replace(
                    config.risk_controls,
                    benchmark_filter_enabled=True,
                    defensive_mode="top5",
                    rebalance_cadence_months=3,
                ),
            ),
        ),
    )


def _group_signal_rows_by_date(
    rows: tuple[CrossSectionalSignalRow, ...],
) -> dict[date, tuple[CrossSectionalSignalRow, ...]]:
    grouped: dict[date, list[CrossSectionalSignalRow]] = defaultdict(list)
    for row in rows:
        grouped[row.date].append(row)
    return {
        row_date: tuple(
            sorted(
                day_rows,
                key=lambda row: (
                    row.factor_rank if row.factor_rank is not None else 1_000_000,
                    -(row.signal_score if row.signal_score is not None else -1_000_000.0),
                    row.symbol,
                ),
            )
        )
        for row_date, day_rows in grouped.items()
    }


def _build_target_weights_for_date(
    config: BacktestConfig,
    participating_symbols: tuple[str, ...],
    signal_rows: tuple[CrossSectionalSignalRow, ...],
) -> dict[str, float]:
    selected_rows = _select_ranked_signal_rows(
        config=config,
        signal_rows=signal_rows,
        max_positions=config.portfolio.max_positions,
    )
    return _build_weight_map(
        participating_symbols=participating_symbols,
        selected_rows=selected_rows,
        max_weight=config.portfolio.max_weight,
        target_gross_exposure=1.0,
    )


def _build_defensive_target_weights_for_date(
    config: BacktestConfig,
    participating_symbols: tuple[str, ...],
    signal_rows: tuple[CrossSectionalSignalRow, ...],
) -> dict[str, float]:
    if config.risk_controls.defensive_mode == "cash":
        return {symbol: 0.0 for symbol in participating_symbols}

    max_positions = config.portfolio.max_positions
    if config.risk_controls.defensive_mode == "top5":
        max_positions = min(5, max_positions)

    selected_rows = _select_ranked_signal_rows(
        config=config,
        signal_rows=signal_rows,
        max_positions=max_positions,
    )
    return _build_weight_map(
        participating_symbols=participating_symbols,
        selected_rows=selected_rows,
        max_weight=config.portfolio.max_weight,
        target_gross_exposure=0.5,
    )


def _select_ranked_signal_rows(
    config: BacktestConfig,
    signal_rows: tuple[CrossSectionalSignalRow, ...],
    max_positions: int,
) -> tuple[CrossSectionalSignalRow, ...]:
    valid_rows = [
        row
        for row in signal_rows
        if row.signal_score is not None and row.signal_score > config.portfolio.min_signal_score
    ]
    selected_rows = valid_rows[:max_positions]
    if not selected_rows and not config.portfolio.hold_cash_when_inactive:
        fallback_rows = [row for row in signal_rows if row.signal_score is not None]
        selected_rows = fallback_rows[:max_positions]
    return tuple(selected_rows)


def _build_weight_map(
    participating_symbols: tuple[str, ...],
    selected_rows: tuple[CrossSectionalSignalRow, ...],
    max_weight: float,
    target_gross_exposure: float,
) -> dict[str, float]:
    weights = {symbol: 0.0 for symbol in participating_symbols}
    if not selected_rows or target_gross_exposure <= 0:
        return weights

    uncapped_weight = target_gross_exposure / len(selected_rows)
    assigned_weight = min(max_weight, uncapped_weight)
    for row in selected_rows:
        weights[row.symbol] = assigned_weight
    return weights


def _build_benchmark_regime_by_date(
    config: BacktestConfig,
    inputs: CrossSectionalBacktestInputs,
) -> dict[date, bool]:
    rebalance_dates = {row.date for row in inputs.membership_rows}
    if not config.risk_controls.benchmark_filter_enabled:
        return {rebalance_date: True for rebalance_date in rebalance_dates}

    closes: list[float] = []
    regime_by_date: dict[date, bool] = {}
    window = config.risk_controls.benchmark_ma_window

    for trading_date in inputs.benchmark_history_dates:
        closes.append(inputs.benchmark_history_closes[trading_date])
        if trading_date not in rebalance_dates:
            continue
        if len(closes) < window:
            regime_by_date[trading_date] = False
            continue
        moving_average = sum(closes[-window:]) / window
        regime_by_date[trading_date] = inputs.benchmark_history_closes[trading_date] > moving_average
    return regime_by_date


def _build_cross_sectional_computation_notes(
    config: BacktestConfig,
    base_notes: tuple[str, ...],
    total_rebalance_dates: tuple[date, ...],
    effective_rebalance_dates: tuple[date, ...],
    regime_blocked_count: int,
) -> tuple[str, ...]:
    notes = list(base_notes)
    if config.risk_controls.benchmark_filter_enabled:
        notes.append(
            f"Phase D 風控：只有當 {config.benchmark} 收盤高於 {config.risk_controls.benchmark_ma_window} 日移動平均時，"
            f"下一期才持有完整風險部位；若歷史不足或 benchmark trend 轉弱，防守模式會切換為 {config.risk_controls.defensive_mode}。"
        )
        notes.append(
            f"本次回測中共有 {regime_blocked_count} 個有效換倉訊號日被 benchmark regime filter 關閉。"
        )
    else:
        notes.append("Phase D 風控：benchmark regime filter 關閉，保留原始橫斷面持有規則。")
    if config.risk_controls.rebalance_cadence_months > 1:
        notes.append(
            f"雖然 universe 仍按月重建，但投組只會每 {config.risk_controls.rebalance_cadence_months} 個月更新一次；"
            "其餘月份延用既有持倉。"
        )
    notes.append(
        f"有效換倉訊號日共有 {len(total_rebalance_dates)} 個，其中實際執行投組更新的 cadence 節點有 {len(effective_rebalance_dates)} 個。"
    )
    return tuple(notes)


def _select_effective_rebalance_dates(
    rebalance_dates: tuple[date, ...],
    cadence_months: int,
) -> tuple[date, ...]:
    if cadence_months <= 1:
        return rebalance_dates
    return tuple(
        rebalance_date
        for index, rebalance_date in enumerate(rebalance_dates)
        if index % cadence_months == 0
    )

def _expand_daily_weights(
    trading_dates: tuple[date, ...],
    participating_symbols: tuple[str, ...],
    target_weights_by_date: dict[date, dict[str, float]],
    signal_scores_by_date: dict[date, dict[str, float | None]],
) -> tuple[dict[date, dict[str, float]], list[PortfolioWeightRow]]:
    current_weights = {symbol: 0.0 for symbol in participating_symbols}
    current_signal_scores = {symbol: None for symbol in participating_symbols}
    applied_weights_by_date: dict[date, dict[str, float]] = {}
    output_rows: list[PortfolioWeightRow] = []

    last_index = len(trading_dates) - 1
    for index, trading_date in enumerate(trading_dates):
        applied_weights_by_date[trading_date] = current_weights.copy()
        for symbol in participating_symbols:
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
                symbol: signal_scores_by_date.get(trading_date, {}).get(symbol)
                for symbol in participating_symbols
            }

    return applied_weights_by_date, output_rows


def _simulate_nav(
    config: BacktestConfig,
    master_dates: tuple[date, ...],
    benchmark_closes: dict[date, float],
    close_asof_by_symbol: dict[str, dict[date, float | None]],
    participating_symbols: tuple[str, ...],
    applied_weights_by_date: dict[date, dict[str, float]],
    target_weights_by_date: dict[date, dict[str, float]],
) -> tuple[list[NavRow], float]:
    nav_rows: list[NavRow] = []
    nav = config.backtest.initial_nav
    benchmark_nav = config.backtest.initial_nav
    last_index = len(master_dates) - 1

    for index, trading_date in enumerate(master_dates):
        starting_nav = nav
        applied_weights = applied_weights_by_date[trading_date]
        cash_weight = max(0.0, 1.0 - sum(applied_weights.values()))

        asset_return = 0.0
        benchmark_return = 0.0
        if index > 0:
            previous_date = master_dates[index - 1]
            asset_return = sum(
                applied_weights[symbol]
                * _close_to_close_return(
                    close_asof_by_symbol[symbol][previous_date],
                    close_asof_by_symbol[symbol][trading_date],
                )
                for symbol in participating_symbols
            )
            benchmark_return = _close_to_close_return(
                benchmark_closes[previous_date],
                benchmark_closes[trading_date],
            )

        nav_after_return = starting_nav * (1.0 + asset_return)
        benchmark_nav *= 1.0 + benchmark_return

        turnover = 0.0
        transaction_cost = 0.0
        if trading_date in target_weights_by_date and index < last_index:
            turnover, cost_rate = _compute_turnover_and_cost_rate(
                current_weights=applied_weights,
                target_weights=target_weights_by_date[trading_date],
                config=config,
                participating_symbols=participating_symbols,
            )
            transaction_cost = nav_after_return * cost_rate
            nav = nav_after_return - transaction_cost
        else:
            nav = nav_after_return

        daily_return = (nav / starting_nav) - 1.0 if starting_nav != 0 else 0.0
        nav_rows.append(
            NavRow(
                date=trading_date,
                nav=nav,
                daily_return=daily_return,
                gross_return=asset_return,
                benchmark_nav=benchmark_nav,
                benchmark_return=benchmark_return,
                turnover=turnover,
                transaction_cost=transaction_cost,
                cash_weight=cash_weight,
            )
        )

    return nav_rows, benchmark_nav


def _build_asof_close_lookup(
    rows: list[NormalizedBar],
    master_dates: tuple[date, ...],
) -> dict[date, float | None]:
    lookup: dict[date, float | None] = {}
    sorted_rows = sorted(rows, key=lambda row: row.date)
    index = 0
    last_close: float | None = None

    for trading_date in master_dates:
        while index < len(sorted_rows) and sorted_rows[index].date <= trading_date:
            last_close = sorted_rows[index].close
            index += 1
        lookup[trading_date] = last_close
    return lookup


def _compute_turnover_and_cost_rate(
    current_weights: dict[str, float],
    target_weights: dict[str, float],
    config: BacktestConfig,
    participating_symbols: tuple[str, ...],
) -> tuple[float, float]:
    total_delta = 0.0
    sell_notional = 0.0

    for symbol in participating_symbols:
        delta = target_weights.get(symbol, 0.0) - current_weights.get(symbol, 0.0)
        total_delta += abs(delta)
        if delta < 0:
            sell_notional += -delta

    turnover = total_delta / 2.0
    commission_and_slippage_rate = (
        config.trading_costs.commission_bps + config.trading_costs.slippage_bps
    ) / 10_000.0
    tax_rate = config.trading_costs.tax_bps / 10_000.0
    total_cost_rate = (commission_and_slippage_rate * total_delta) + (tax_rate * sell_notional)
    return turnover, total_cost_rate


def _close_to_close_return(
    previous_close: float | None,
    current_close: float | None,
) -> float:
    if previous_close is None or current_close is None or previous_close == 0:
        return 0.0
    return (current_close / previous_close) - 1.0
