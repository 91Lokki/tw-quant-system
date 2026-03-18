"""Daily decision and paper-trading helpers for the practical TWSE mainline."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date
import json
from pathlib import Path

from tw_quant.backtest.cross_sectional import (
    _build_benchmark_regime_by_date,
    _build_defensive_target_weights_for_date,
    _build_target_weights_for_date,
    _group_signal_rows_by_date,
    _select_effective_rebalance_dates,
    CrossSectionalBacktestInputs,
    load_cross_sectional_backtest_inputs,
)
from tw_quant.core.models import (
    AppConfig,
    CrossSectionalSignalRow,
    DailyDecisionResult,
    NormalizedBar,
    PaperTradingResult,
)
from tw_quant.data.store import read_normalized_csv
from tw_quant.universe import validate_artifact_freshness


PRACTICAL_STRATEGY_ID = "risk_controlled_3m_half_exposure_exp60_delay1"
EPSILON = 1e-12


@dataclass(frozen=True, slots=True)
class _OperationalContext:
    app_config: AppConfig
    inputs: CrossSectionalBacktestInputs
    trading_dates: tuple[date, ...]
    trading_index_by_date: dict[date, int]
    effective_rebalance_dates: tuple[date, ...]
    signal_rows_by_date: dict[date, tuple[CrossSectionalSignalRow, ...]]
    benchmark_regime_by_date: dict[date, bool]
    target_weights_by_signal_date: dict[date, dict[str, float]]
    exact_bars_by_symbol: dict[str, dict[date, NormalizedBar]]
    output_dir: Path
    decisions_dir: Path
    latest_decision_path: Path
    blotter_path: Path
    state_path: Path
    nav_history_path: Path


@dataclass(frozen=True, slots=True)
class _PaperState:
    last_date: date | None
    cash_balance: float
    shares_by_symbol: dict[str, float]


@dataclass(frozen=True, slots=True)
class _PendingDecision:
    decision_date: date
    execution_date: date
    status: str
    target_weights: dict[str, float]
    notes: tuple[str, ...]


def describe_paper_execution_scope() -> str:
    """Describe the paper-execution responsibility of this module."""

    return (
        "Paper execution now generates daily decision snapshots for the practical TWSE mainline, "
        "simulates next-open-after-delay fills, and records a persistent CSV ledger without touching a broker."
    )


def generate_daily_decision(
    app_config: AppConfig,
    as_of_date: date | None = None,
) -> DailyDecisionResult:
    """Generate the latest daily decision package for the practical mainline."""

    context = _build_operational_context(app_config)
    resolved_date, adjustment_note = _resolve_decision_date(context, as_of_date)
    previous_state = _load_paper_state(context.state_path, app_config.paper_trading.initial_cash)
    decision_payload = _build_decision_payload(
        context=context,
        decision_date=resolved_date,
        previous_weights=_weights_from_state(
            state=previous_state,
            context=context,
            valuation_date=resolved_date,
        ),
    )
    if adjustment_note is not None:
        decision_payload["notes"].append(adjustment_note)
    snapshot_path = _write_decision_snapshot(
        latest_path=context.latest_decision_path,
        dated_dir=context.decisions_dir,
        payload=decision_payload,
    )
    return DailyDecisionResult(
        strategy_id=decision_payload["strategy_id"],
        decision_date=resolved_date,
        status=decision_payload["status"],
        benchmark_regime_on=bool(decision_payload["benchmark_regime_on"]),
        rebalance_required=bool(decision_payload["rebalance_required"]),
        execution_model=decision_payload["execution_model"],
        execution_delay_days=int(decision_payload["execution_delay_days"]),
        signal_date=_parse_optional_date(decision_payload["signal_date"]),
        scheduled_execution_date=_parse_optional_date(
            decision_payload["scheduled_execution_date"]
        ),
        target_symbols=tuple(decision_payload["target_symbols"]),
        target_cash_weight=float(decision_payload["target_cash_weight"]),
        snapshot_path=snapshot_path,
        latest_snapshot_path=context.latest_decision_path,
        notes=tuple(decision_payload["notes"]),
    )


def update_paper_trading(
    app_config: AppConfig,
    as_of_date: date | None = None,
) -> PaperTradingResult:
    """Roll the paper-trading ledger forward through the requested date."""

    context = _build_operational_context(app_config)
    resolved_date, adjustment_note = _resolve_decision_date(context, as_of_date)
    state = _load_paper_state(context.state_path, app_config.paper_trading.initial_cash)
    existing_nav_rows = _load_csv_rows(context.nav_history_path)
    existing_blotter_rows = _load_csv_rows(context.blotter_path)
    rebuild_note: str | None = None
    if state.cash_balance < -EPSILON:
        rebuild_note = (
            "Detected a legacy negative-cash paper state from an earlier execution run; "
            "rebuilt the ledger from the initial cash balance with the current non-negative cash rule."
        )
        state = _PaperState(
            last_date=None,
            cash_balance=app_config.paper_trading.initial_cash,
            shares_by_symbol={},
        )
        existing_nav_rows = []
        existing_blotter_rows = []
    pending_decisions = _load_pending_decisions(
        decisions_dir=context.decisions_dir,
        last_processed_date=state.last_date,
        as_of_date=resolved_date,
    )

    processing_dates = tuple(
        trading_date
        for trading_date in context.trading_dates
        if (
            (state.last_date is None and trading_date <= resolved_date)
            or (state.last_date is not None and state.last_date < trading_date <= resolved_date)
        )
    )
    if not processing_dates:
        latest_decision = generate_daily_decision(app_config, resolved_date)
        notes = ["沒有新的 benchmark 交易日需要更新 paper ledger。"]
        if rebuild_note is not None:
            notes.append(rebuild_note)
        if adjustment_note is not None:
            notes.append(adjustment_note)
        return PaperTradingResult(
            strategy_id=PRACTICAL_STRATEGY_ID,
            as_of_date=latest_decision.decision_date,
            status="paper_ledger_already_current",
            final_nav=_load_latest_nav(existing_nav_rows, app_config.paper_trading.initial_cash),
            cash_balance=state.cash_balance,
            holdings_count=len([symbol for symbol, shares in state.shares_by_symbol.items() if shares > EPSILON]),
            decision_snapshot_path=latest_decision.snapshot_path,
            blotter_path=context.blotter_path,
            state_path=context.state_path,
            nav_history_path=context.nav_history_path,
            notes=tuple(notes),
        )

    nav_rows_to_append: list[dict[str, str]] = []
    blotter_rows_to_append: list[dict[str, str]] = []
    latest_decision_snapshot_path = context.latest_decision_path
    latest_day_notes: tuple[str, ...] = ()
    latest_day_status = "hold"

    for trading_date in processing_dates:
        if trading_date in context.effective_rebalance_dates:
            decision_payload = _build_decision_payload(
                context=context,
                decision_date=trading_date,
                previous_weights=_weights_from_state(
                    state=state,
                    context=context,
                    valuation_date=trading_date,
                ),
            )
            latest_decision_snapshot_path = _write_decision_snapshot(
                latest_path=context.latest_decision_path,
                dated_dir=context.decisions_dir,
                payload=decision_payload,
            )
            execution_date = _parse_optional_date(
                decision_payload["scheduled_execution_date"]
            )
            if (
                execution_date is not None
                and execution_date > (state.last_date or date.min)
                and execution_date <= resolved_date
            ):
                pending_decisions[execution_date] = _decision_payload_to_pending(
                    decision_payload
                )

        state, nav_row, blotter_rows, day_notes = _apply_paper_day(
            context=context,
            trading_date=trading_date,
            state=state,
            pending_decision=pending_decisions.pop(trading_date, None),
        )
        nav_rows_to_append.append(nav_row)
        blotter_rows_to_append.extend(blotter_rows)
        latest_day_notes = day_notes
        latest_day_status = nav_row["status"]

    all_nav_rows = _merge_rows_by_date(existing_nav_rows, nav_rows_to_append)
    all_blotter_rows = existing_blotter_rows + blotter_rows_to_append
    _write_csv(
        context.nav_history_path,
        all_nav_rows,
        fieldnames=(
            "date",
            "nav",
            "cash_balance",
            "positions_value",
            "gross_exposure",
            "turnover",
            "trade_count",
            "benchmark_close",
            "status",
            "notes",
        ),
    )
    _write_csv(
        context.blotter_path,
        all_blotter_rows,
        fieldnames=(
            "execution_date",
            "decision_date",
            "symbol",
            "action",
            "prior_shares",
            "target_shares",
            "delta_shares",
            "fill_price",
            "trade_notional",
            "commission_slippage_cost",
            "tax_cost",
            "prior_weight",
            "target_weight",
            "executed_weight",
            "notes",
        ),
    )
    final_nav = _write_paper_state(
        path=context.state_path,
        context=context,
        state=state,
        as_of_date=processing_dates[-1],
        state_status=latest_day_status,
        state_notes=latest_day_notes,
    )
    latest_decision_snapshot_path = _refresh_latest_decision_snapshot(
        context=context,
        decision_date=processing_dates[-1],
        state=state,
        extra_notes=latest_day_notes,
    )
    notes = [
        (
            "Paper ledger 採用固定執行假設：decision 在收盤後形成，"
            f"orders 於額外延遲 {app_config.paper_trading.execution_delay_days} 個 benchmark 交易日後，"
            "在下一個有效 execution day 開盤價成交。"
        ),
        "若 benchmark / signal / execution-day market data 不完整，paper ledger 會保留前一日持倉並寫入狀態註記。",
    ]
    if rebuild_note is not None:
        notes.append(rebuild_note)
    if adjustment_note is not None:
        notes.append(adjustment_note)
    return PaperTradingResult(
        strategy_id=PRACTICAL_STRATEGY_ID,
        as_of_date=processing_dates[-1],
        status="paper_ledger_updated",
        final_nav=final_nav,
        cash_balance=state.cash_balance,
        holdings_count=len([symbol for symbol, shares in state.shares_by_symbol.items() if shares > EPSILON]),
        decision_snapshot_path=latest_decision_snapshot_path,
        blotter_path=context.blotter_path,
        state_path=context.state_path,
        nav_history_path=context.nav_history_path,
        notes=tuple(notes),
    )


def _build_operational_context(app_config: AppConfig) -> _OperationalContext:
    if app_config.research_branch != "tw_top50_liquidity_cross_sectional":
        raise ValueError("Daily decision and paper trading currently support only the TWSE cross-sectional branch.")
    if app_config.paper_trading is None:
        raise ValueError("paper_trading config is required for daily decision and paper trading.")
    _validate_practical_mainline(app_config)
    validate_artifact_freshness(
        app_config.universe_config.usable_metadata_path,
        (
            app_config.universe_config.membership_path,
            app_config.backtest.signal_input_path,
        ),
    )
    backtest_config = app_config.to_backtest_config()
    inputs = load_cross_sectional_backtest_inputs(backtest_config)
    signal_rows_by_date = _group_signal_rows_by_date(inputs.signal_rows)
    rebalance_dates = tuple(sorted({row.date for row in inputs.membership_rows}))
    effective_rebalance_dates = _select_effective_rebalance_dates(
        rebalance_dates,
        app_config.risk_controls.rebalance_cadence_months,
    )
    benchmark_regime_by_date = _build_benchmark_regime_by_date(backtest_config, inputs)
    target_weights_by_signal_date: dict[date, dict[str, float]] = {}
    for rebalance_date in effective_rebalance_dates:
        if not benchmark_regime_by_date.get(rebalance_date, True):
            target_weights_by_signal_date[rebalance_date] = _build_defensive_target_weights_for_date(
                config=backtest_config,
                participating_symbols=inputs.participating_symbols,
                signal_rows=signal_rows_by_date.get(rebalance_date, ()),
            )
            continue
        target_weights_by_signal_date[rebalance_date] = _build_target_weights_for_date(
            config=backtest_config,
            participating_symbols=inputs.participating_symbols,
            signal_rows=signal_rows_by_date.get(rebalance_date, ()),
        )
    exact_bars_by_symbol = _load_exact_bars(
        normalized_dir=app_config.backtest.bar_input_dir,
        symbols=(*inputs.participating_symbols, app_config.benchmark),
    )
    output_dir = app_config.paper_trading.output_dir / app_config.project_name
    decisions_dir = output_dir / "daily_decision" / "decisions"
    return _OperationalContext(
        app_config=app_config,
        inputs=inputs,
        trading_dates=inputs.master_dates,
        trading_index_by_date={trading_date: index for index, trading_date in enumerate(inputs.master_dates)},
        effective_rebalance_dates=effective_rebalance_dates,
        signal_rows_by_date=signal_rows_by_date,
        benchmark_regime_by_date=benchmark_regime_by_date,
        target_weights_by_signal_date=target_weights_by_signal_date,
        exact_bars_by_symbol=exact_bars_by_symbol,
        output_dir=output_dir,
        decisions_dir=decisions_dir,
        latest_decision_path=output_dir / "daily_decision" / "latest.json",
        blotter_path=output_dir / "paper_trade_blotter.csv",
        state_path=output_dir / "paper_portfolio_state.csv",
        nav_history_path=output_dir / "paper_nav_history.csv",
    )


def _validate_practical_mainline(app_config: AppConfig) -> None:
    if not app_config.risk_controls.benchmark_filter_enabled:
        raise ValueError("Paper-trading workflow requires benchmark_filter_enabled = true.")
    if app_config.risk_controls.benchmark_ma_window != 200:
        raise ValueError("Paper-trading workflow requires benchmark_ma_window = 200.")
    if app_config.risk_controls.defensive_mode != "half_exposure":
        raise ValueError('Paper-trading workflow requires defensive_mode = "half_exposure".')
    if abs(app_config.risk_controls.defensive_gross_exposure - 0.6) > EPSILON:
        raise ValueError(
            "Paper-trading workflow requires defensive_gross_exposure = 0.6."
        )
    if app_config.risk_controls.rebalance_cadence_months != 3:
        raise ValueError(
            "Paper-trading workflow requires rebalance_cadence_months = 3."
        )
    if app_config.paper_trading.execution_model != "next_open_after_delay":
        raise ValueError(
            "Paper-trading workflow requires execution_model = next_open_after_delay."
        )


def _resolve_decision_date(
    context: _OperationalContext,
    as_of_date: date | None,
) -> tuple[date, str | None]:
    requested_date = as_of_date or context.app_config.end_date
    eligible_dates = [trading_date for trading_date in context.trading_dates if trading_date <= requested_date]
    if not eligible_dates:
        raise ValueError(
            f"No benchmark trading dates are available on or before {requested_date.isoformat()}."
        )
    resolved_date = eligible_dates[-1]
    if resolved_date == requested_date:
        return resolved_date, None
    return (
        resolved_date,
        (
            f"Requested as-of date {requested_date.isoformat()} is not a benchmark trading day; "
            f"used latest available date {resolved_date.isoformat()} instead."
        ),
    )


def _build_decision_payload(
    context: _OperationalContext,
    decision_date: date,
    previous_weights: dict[str, float],
) -> dict[str, object]:
    latest_signal_date = _latest_signal_date(context, decision_date)
    target_weights = (
        context.target_weights_by_signal_date.get(latest_signal_date, {})
        if latest_signal_date is not None
        else {symbol: 0.0 for symbol in context.inputs.participating_symbols}
    )
    benchmark_regime_on = (
        context.benchmark_regime_by_date.get(latest_signal_date, False)
        if latest_signal_date is not None
        else False
    )
    target_cash_weight = max(0.0, 1.0 - sum(target_weights.values()))
    scheduled_execution_date = (
        _scheduled_execution_date(context, decision_date)
        if decision_date in context.effective_rebalance_dates
        else None
    )
    notes: list[str] = []
    status = "hold_existing"
    rebalance_required = False

    guardrail_error = _evaluate_decision_guardrails(
        context=context,
        decision_date=decision_date,
        scheduled_execution_date=scheduled_execution_date,
        target_weights=target_weights,
    )
    if decision_date in context.effective_rebalance_dates and guardrail_error is None:
        rebalance_required = scheduled_execution_date is not None
        status = "rebalance_scheduled" if rebalance_required else "hold_existing"
    elif guardrail_error is not None:
        status = "guardrail_blocked"
        notes.append(guardrail_error)

    if latest_signal_date is None:
        notes.append("No effective rebalance signal is available yet; target portfolio remains fully in cash.")
    elif rebalance_required:
        notes.append(
            "Paper execution enforces a non-negative cash rule; if execution-day costs leave insufficient cash, "
            "buy orders will be scaled proportionally at the open."
        )

    target_rows = [
        {
            "symbol": symbol,
            "weight": target_weights.get(symbol, 0.0),
            "signal_score": _signal_score_for_symbol(
                context.signal_rows_by_date.get(latest_signal_date, ()) if latest_signal_date else (),
                symbol,
            ),
        }
        for symbol in sorted(context.inputs.participating_symbols)
        if target_weights.get(symbol, 0.0) > EPSILON
    ]
    trade_list = _build_trade_list(previous_weights, target_weights)
    return {
        "strategy_id": PRACTICAL_STRATEGY_ID,
        "decision_date": decision_date.isoformat(),
        "status": status,
        "benchmark_regime_on": benchmark_regime_on,
        "rebalance_required": rebalance_required,
        "execution_model": context.app_config.paper_trading.execution_model,
        "execution_delay_days": context.app_config.paper_trading.execution_delay_days,
        "signal_date": latest_signal_date.isoformat() if latest_signal_date else None,
        "scheduled_execution_date": (
            scheduled_execution_date.isoformat() if scheduled_execution_date else None
        ),
        "target_cash_weight": target_cash_weight,
        "target_symbols": [row["symbol"] for row in target_rows],
        "target_weights": target_rows,
        "previous_holdings": [
            {
                "symbol": symbol,
                "weight": previous_weights.get(symbol, 0.0),
            }
            for symbol in sorted(context.inputs.participating_symbols)
            if previous_weights.get(symbol, 0.0) > EPSILON
        ],
        "trade_list": trade_list,
        "notes": notes,
    }


def _evaluate_decision_guardrails(
    context: _OperationalContext,
    decision_date: date,
    scheduled_execution_date: date | None,
    target_weights: dict[str, float],
) -> str | None:
    if decision_date not in context.trading_index_by_date:
        return f"Missing benchmark data for decision date {decision_date.isoformat()}."
    total_weight = sum(target_weights.values())
    if total_weight > 1.0 + EPSILON:
        return f"Target gross exposure {total_weight:.4f} exceeds 1.0."
    overweight_symbols = [
        symbol
        for symbol, weight in target_weights.items()
        if weight > context.app_config.portfolio.max_weight + EPSILON
    ]
    if overweight_symbols:
        return (
            "Target weights exceed portfolio.max_weight for symbols: "
            + ", ".join(sorted(overweight_symbols))
        )
    if scheduled_execution_date is None and decision_date in context.effective_rebalance_dates:
        return (
            f"No valid execution date is available after {decision_date.isoformat()} "
            "for the configured execution delay."
        )
    if scheduled_execution_date is not None:
        benchmark_bar = context.exact_bars_by_symbol.get(context.app_config.benchmark, {}).get(
            scheduled_execution_date
        )
        if benchmark_bar is None:
            return f"Missing benchmark market data on execution date {scheduled_execution_date.isoformat()}."
        for symbol, weight in target_weights.items():
            if weight <= EPSILON:
                continue
            if context.exact_bars_by_symbol.get(symbol, {}).get(scheduled_execution_date) is None:
                return (
                    f"Missing current market data for target symbol {symbol} on "
                    f"{scheduled_execution_date.isoformat()}."
                )
    if decision_date in context.effective_rebalance_dates and not context.signal_rows_by_date.get(
        decision_date
    ):
        return f"Missing signal rows for effective rebalance date {decision_date.isoformat()}."
    return None


def _latest_signal_date(context: _OperationalContext, decision_date: date) -> date | None:
    eligible_dates = [
        rebalance_date
        for rebalance_date in context.effective_rebalance_dates
        if rebalance_date <= decision_date
    ]
    if not eligible_dates:
        return None
    return eligible_dates[-1]


def _scheduled_execution_date(context: _OperationalContext, decision_date: date) -> date | None:
    decision_index = context.trading_index_by_date[decision_date]
    target_index = decision_index + 1 + context.app_config.paper_trading.execution_delay_days
    if target_index >= len(context.trading_dates):
        return None
    return context.trading_dates[target_index]


def _signal_score_for_symbol(
    signal_rows: tuple[CrossSectionalSignalRow, ...],
    symbol: str,
) -> float | None:
    for row in signal_rows:
        if row.symbol == symbol:
            return row.signal_score
    return None


def _build_trade_list(
    previous_weights: dict[str, float],
    target_weights: dict[str, float],
) -> list[dict[str, float | str]]:
    rows: list[dict[str, float | str]] = []
    for symbol in sorted(set(previous_weights) | set(target_weights)):
        from_weight = previous_weights.get(symbol, 0.0)
        to_weight = target_weights.get(symbol, 0.0)
        delta_weight = to_weight - from_weight
        if abs(delta_weight) <= EPSILON:
            continue
        rows.append(
            {
                "symbol": symbol,
                "from_weight": from_weight,
                "to_weight": to_weight,
                "delta_weight": delta_weight,
            }
        )
    return rows


def _load_paper_state(path: Path, initial_cash: float) -> _PaperState:
    if not path.exists():
        return _PaperState(last_date=None, cash_balance=initial_cash, shares_by_symbol={})

    shares_by_symbol: dict[str, float] = {}
    cash_balance = initial_cash
    last_date: date | None = None
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            row_date = date.fromisoformat(row["as_of_date"])
            last_date = row_date if last_date is None or row_date > last_date else last_date
            if row["symbol"] == "CASH":
                cash_balance = float(row["market_value"])
                continue
            shares = float(row["shares"])
            if abs(shares) > EPSILON:
                shares_by_symbol[row["symbol"]] = shares
    return _PaperState(last_date=last_date, cash_balance=cash_balance, shares_by_symbol=shares_by_symbol)


def _weights_from_state(
    state: _PaperState,
    context: _OperationalContext,
    valuation_date: date,
) -> dict[str, float]:
    weights = {symbol: 0.0 for symbol in context.inputs.participating_symbols}
    nav = _portfolio_value_at_close(state, context, valuation_date)
    if nav <= EPSILON:
        return weights
    for symbol, shares in state.shares_by_symbol.items():
        if abs(shares) <= EPSILON:
            continue
        close_price = context.inputs.close_asof_by_symbol[symbol][valuation_date]
        if close_price is None:
            continue
        weights[symbol] = (shares * close_price) / nav
    return weights


def _portfolio_value_at_close(
    state: _PaperState,
    context: _OperationalContext,
    valuation_date: date,
) -> float:
    positions_value = 0.0
    for symbol, shares in state.shares_by_symbol.items():
        if abs(shares) <= EPSILON:
            continue
        close_price = context.inputs.close_asof_by_symbol[symbol][valuation_date]
        if close_price is None:
            continue
        positions_value += shares * close_price
    return state.cash_balance + positions_value


def _load_pending_decisions(
    decisions_dir: Path,
    last_processed_date: date | None,
    as_of_date: date,
) -> dict[date, _PendingDecision]:
    pending: dict[date, _PendingDecision] = {}
    if not decisions_dir.exists():
        return pending
    for path in sorted(decisions_dir.glob("*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        execution_date = _parse_optional_date(payload.get("scheduled_execution_date"))
        if execution_date is None:
            continue
        if last_processed_date is not None and execution_date <= last_processed_date:
            continue
        if execution_date > as_of_date:
            continue
        pending[execution_date] = _decision_payload_to_pending(payload)
    return pending


def _decision_payload_to_pending(payload: dict[str, object]) -> _PendingDecision:
    return _PendingDecision(
        decision_date=date.fromisoformat(str(payload["decision_date"])),
        execution_date=date.fromisoformat(str(payload["scheduled_execution_date"])),
        status=str(payload["status"]),
        target_weights={
            str(row["symbol"]): float(row["weight"])
            for row in payload["target_weights"]
        },
        notes=tuple(str(note) for note in payload.get("notes", [])),
    )


def _apply_paper_day(
    context: _OperationalContext,
    trading_date: date,
    state: _PaperState,
    pending_decision: _PendingDecision | None,
) -> tuple[_PaperState, dict[str, str], list[dict[str, str]], tuple[str, ...]]:
    blotter_rows: list[dict[str, str]] = []
    notes: list[str] = []
    turnover = 0.0
    trade_count = 0
    status = "hold"

    if pending_decision is not None and pending_decision.status != "guardrail_blocked":
        next_state, blotter_rows, turnover, status, execution_notes = _execute_pending_decision(
            context=context,
            trading_date=trading_date,
            state=state,
            pending_decision=pending_decision,
        )
        state = next_state
        trade_count = len(blotter_rows)
        notes.extend(execution_notes)
    elif pending_decision is not None:
        status = "blocked"
        notes.extend(pending_decision.notes)

    positions_value = 0.0
    for symbol, shares in state.shares_by_symbol.items():
        if abs(shares) <= EPSILON:
            continue
        close_price = context.inputs.close_asof_by_symbol[symbol][trading_date]
        if close_price is None:
            continue
        positions_value += shares * close_price
    nav = state.cash_balance + positions_value
    gross_exposure = positions_value / nav if nav > EPSILON else 0.0
    benchmark_close = context.inputs.benchmark_closes[trading_date]
    nav_row = {
        "date": trading_date.isoformat(),
        "nav": f"{nav}",
        "cash_balance": f"{state.cash_balance}",
        "positions_value": f"{positions_value}",
        "gross_exposure": f"{gross_exposure}",
        "turnover": f"{turnover}",
        "trade_count": str(trade_count),
        "benchmark_close": f"{benchmark_close}",
        "status": status,
        "notes": " | ".join(notes),
    }
    return (
        _PaperState(
            last_date=trading_date,
            cash_balance=state.cash_balance,
            shares_by_symbol={symbol: shares for symbol, shares in state.shares_by_symbol.items() if abs(shares) > EPSILON},
        ),
        nav_row,
        blotter_rows,
        tuple(notes),
    )


def _execute_pending_decision(
    context: _OperationalContext,
    trading_date: date,
    state: _PaperState,
    pending_decision: _PendingDecision,
) -> tuple[_PaperState, list[dict[str, str]], float, str, list[str]]:
    notes: list[str] = []
    trade_rows: list[dict[str, str]] = []
    target_weights = {
        symbol: pending_decision.target_weights.get(symbol, 0.0)
        for symbol in context.inputs.participating_symbols
    }
    current_position_values = {}
    prior_weights = {symbol: 0.0 for symbol in context.inputs.participating_symbols}
    portfolio_value = state.cash_balance
    for symbol in context.inputs.participating_symbols:
        shares = state.shares_by_symbol.get(symbol, 0.0)
        bar = context.exact_bars_by_symbol.get(symbol, {}).get(trading_date)
        if bar is None:
            if target_weights.get(symbol, 0.0) > EPSILON or abs(shares) > EPSILON:
                notes.append(
                    f"Missing execution-day open for required symbol {symbol}; preserved prior holdings."
                )
                return state, trade_rows, 0.0, "blocked", notes
            continue
        current_position_value = shares * bar.open
        current_position_values[symbol] = current_position_value
        portfolio_value += current_position_value

    if portfolio_value <= EPSILON:
        notes.append("Portfolio value is non-positive on the scheduled execution date; skipped trading.")
        return state, trade_rows, 0.0, "blocked", notes

    for symbol in context.inputs.participating_symbols:
        current_position_value = current_position_values.get(symbol, 0.0)
        prior_weights[symbol] = current_position_value / portfolio_value

    commission_slippage_rate = (
        context.app_config.trading_costs.commission_bps
        + context.app_config.trading_costs.slippage_bps
    ) / 10_000.0
    tax_rate = context.app_config.trading_costs.tax_bps / 10_000.0
    cash_balance = state.cash_balance
    new_shares = dict(state.shares_by_symbol)
    sell_rows: list[dict[str, float | str]] = []
    buy_rows: list[dict[str, float | str]] = []
    total_sell_notional = 0.0
    total_desired_buy_notional = 0.0

    for symbol in context.inputs.participating_symbols:
        bar = context.exact_bars_by_symbol.get(symbol, {}).get(trading_date)
        if bar is None:
            continue
        prior_shares = state.shares_by_symbol.get(symbol, 0.0)
        current_value = prior_shares * bar.open
        target_value = portfolio_value * target_weights.get(symbol, 0.0)
        target_shares = target_value / bar.open if bar.open > 0 else 0.0
        delta_shares = target_shares - prior_shares
        if delta_shares < -EPSILON:
            trade_notional = -delta_shares * bar.open
            total_sell_notional += trade_notional
            sell_rows.append(
                {
                    "symbol": symbol,
                    "prior_shares": prior_shares,
                    "target_shares": target_shares,
                    "delta_shares": delta_shares,
                    "fill_price": bar.open,
                    "trade_notional": trade_notional,
                    "target_weight": target_weights.get(symbol, 0.0),
                }
            )
            new_shares[symbol] = target_shares
        elif delta_shares > EPSILON:
            desired_buy_notional = delta_shares * bar.open
            total_desired_buy_notional += desired_buy_notional
            buy_rows.append(
                {
                    "symbol": symbol,
                    "prior_shares": prior_shares,
                    "desired_target_shares": target_shares,
                    "desired_buy_notional": desired_buy_notional,
                    "fill_price": bar.open,
                    "target_weight": target_weights.get(symbol, 0.0),
                }
            )
        else:
            new_shares[symbol] = target_shares

    sell_commission_slippage_cost = total_sell_notional * commission_slippage_rate
    sell_tax_cost = total_sell_notional * tax_rate
    cash_after_sells = (
        cash_balance
        + total_sell_notional
        - sell_commission_slippage_cost
        - sell_tax_cost
    )
    max_affordable_buy_notional = max(0.0, cash_after_sells) / (1.0 + commission_slippage_rate)
    buy_scale = 1.0
    if total_desired_buy_notional > EPSILON and total_desired_buy_notional > max_affordable_buy_notional + EPSILON:
        buy_scale = max_affordable_buy_notional / total_desired_buy_notional
        notes.append(
            "Cash-constrained execution: buy orders were scaled to "
            f"{buy_scale:.2%} of desired notional so paper cash remains non-negative after estimated costs."
        )
        notes.append(
            f"Available cash after sells and sell-side costs was {cash_after_sells:.2f}, "
            f"while desired buy notional was {total_desired_buy_notional:.2f}."
        )

    for row in sell_rows:
        symbol = str(row["symbol"])
        trade_notional = float(row["trade_notional"])
        trade_rows.append(
            {
                "execution_date": trading_date.isoformat(),
                "decision_date": pending_decision.decision_date.isoformat(),
                "symbol": symbol,
                "action": "sell",
                "prior_shares": f"{row['prior_shares']}",
                "target_shares": f"{row['target_shares']}",
                "delta_shares": f"{row['delta_shares']}",
                "fill_price": f"{row['fill_price']}",
                "trade_notional": f"{trade_notional}",
                "commission_slippage_cost": f"{trade_notional * commission_slippage_rate}",
                "tax_cost": f"{trade_notional * tax_rate}",
                "prior_weight": f"{prior_weights[symbol]}",
                "target_weight": f"{row['target_weight']}",
                "executed_weight": "0.0",
                "notes": "",
            }
        )

    buy_scaling_note = (
        ""
        if buy_scale >= 1.0 - EPSILON
        else f"Scaled for cash after costs (scale={buy_scale:.6f})."
    )
    total_executed_buy_notional = 0.0
    for row in buy_rows:
        symbol = str(row["symbol"])
        prior_shares = float(row["prior_shares"])
        desired_buy_notional = float(row["desired_buy_notional"])
        fill_price = float(row["fill_price"])
        executed_buy_notional = desired_buy_notional * buy_scale
        executed_target_shares = prior_shares + (
            executed_buy_notional / fill_price if fill_price > 0 else 0.0
        )
        delta_shares = executed_target_shares - prior_shares
        total_executed_buy_notional += executed_buy_notional
        new_shares[symbol] = executed_target_shares
        trade_rows.append(
            {
                "execution_date": trading_date.isoformat(),
                "decision_date": pending_decision.decision_date.isoformat(),
                "symbol": symbol,
                "action": "buy",
                "prior_shares": f"{prior_shares}",
                "target_shares": f"{executed_target_shares}",
                "delta_shares": f"{delta_shares}",
                "fill_price": f"{fill_price}",
                "trade_notional": f"{executed_buy_notional}",
                "commission_slippage_cost": f"{executed_buy_notional * commission_slippage_rate}",
                "tax_cost": "0.0",
                "prior_weight": f"{prior_weights[symbol]}",
                "target_weight": f"{row['target_weight']}",
                "executed_weight": "0.0",
                "notes": buy_scaling_note,
            }
        )

    buy_commission_slippage_cost = total_executed_buy_notional * commission_slippage_rate
    total_cost = sell_commission_slippage_cost + sell_tax_cost + buy_commission_slippage_cost
    cash_balance = cash_after_sells - total_executed_buy_notional - buy_commission_slippage_cost
    if cash_balance < 0.0 and abs(cash_balance) <= 1e-6:
        cash_balance = 0.0
    if cash_balance < -1e-6:
        notes.append(
            "Cash enforcement failed unexpectedly after execution; preserved prior holdings instead."
        )
        return state, [], 0.0, "blocked", notes

    executed_weights = {symbol: 0.0 for symbol in context.inputs.participating_symbols}
    executed_nav_at_open = cash_balance
    for symbol in context.inputs.participating_symbols:
        bar = context.exact_bars_by_symbol.get(symbol, {}).get(trading_date)
        if bar is None:
            continue
        executed_nav_at_open += new_shares.get(symbol, 0.0) * bar.open
    if executed_nav_at_open > EPSILON:
        for symbol in context.inputs.participating_symbols:
            bar = context.exact_bars_by_symbol.get(symbol, {}).get(trading_date)
            if bar is None:
                continue
            executed_weights[symbol] = (
                new_shares.get(symbol, 0.0) * bar.open
            ) / executed_nav_at_open

    for row in trade_rows:
        row["executed_weight"] = f"{executed_weights[row['symbol']]}"

    turnover = (
        sum(
            abs(executed_weights.get(symbol, 0.0) - prior_weights.get(symbol, 0.0))
            for symbol in context.inputs.participating_symbols
        )
        / 2.0
    )
    notes.append(
        f"Executed {len(trade_rows)} trade(s) at the {trading_date.isoformat()} open using next_open_after_delay."
    )
    if total_cost > EPSILON:
        notes.append(f"Applied paper trading costs of {total_cost:.2f}.")
    return (
        _PaperState(
            last_date=trading_date,
            cash_balance=cash_balance,
            shares_by_symbol={symbol: shares for symbol, shares in new_shares.items() if abs(shares) > EPSILON},
        ),
        trade_rows,
        turnover,
        "executed_cash_constrained" if buy_scale < 1.0 - EPSILON else "executed",
        notes,
    )


def _write_decision_snapshot(
    latest_path: Path,
    dated_dir: Path,
    payload: dict[str, object],
) -> Path:
    dated_dir.mkdir(parents=True, exist_ok=True)
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot_path = dated_dir / f"{payload['decision_date']}.json"
    serialized = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
    snapshot_path.write_text(serialized, encoding="utf-8")
    latest_path.write_text(serialized, encoding="utf-8")
    return snapshot_path


def _write_paper_state(
    path: Path,
    context: _OperationalContext,
    state: _PaperState,
    as_of_date: date,
    state_status: str,
    state_notes: tuple[str, ...],
) -> float:
    rows: list[dict[str, str]] = []
    positions_value = 0.0
    notes_text = " | ".join(state_notes)
    for symbol, shares in sorted(state.shares_by_symbol.items()):
        close_price = context.inputs.close_asof_by_symbol[symbol][as_of_date]
        if close_price is None:
            continue
        market_value = shares * close_price
        positions_value += market_value
        rows.append(
            {
                "as_of_date": as_of_date.isoformat(),
                "symbol": symbol,
                "shares": f"{shares}",
                "close_price": f"{close_price}",
                "market_value": f"{market_value}",
                "portfolio_weight": "0.0",
                "paper_nav": "0.0",
                "cash_balance": f"{state.cash_balance}",
                "state_status": state_status,
                "notes": notes_text,
            }
        )
    nav = state.cash_balance + positions_value
    for row in rows:
        market_value = float(row["market_value"])
        row["portfolio_weight"] = f"{market_value / nav if nav > EPSILON else 0.0}"
        row["paper_nav"] = f"{nav}"
    rows.append(
        {
            "as_of_date": as_of_date.isoformat(),
            "symbol": "CASH",
            "shares": "0.0",
            "close_price": "1.0",
            "market_value": f"{state.cash_balance}",
            "portfolio_weight": f"{state.cash_balance / nav if nav > EPSILON else 1.0}",
            "paper_nav": f"{nav}",
            "cash_balance": f"{state.cash_balance}",
            "state_status": state_status,
            "notes": notes_text,
        }
    )
    _write_csv(
        path,
        rows,
        fieldnames=(
            "as_of_date",
            "symbol",
            "shares",
            "close_price",
            "market_value",
            "portfolio_weight",
            "paper_nav",
            "cash_balance",
            "state_status",
            "notes",
        ),
    )
    return nav


def _refresh_latest_decision_snapshot(
    context: _OperationalContext,
    decision_date: date,
    state: _PaperState,
    extra_notes: tuple[str, ...],
) -> Path:
    payload = _build_decision_payload(
        context=context,
        decision_date=decision_date,
        previous_weights=_weights_from_state(
            state=state,
            context=context,
            valuation_date=decision_date,
        ),
    )
    for note in extra_notes:
        if note and note not in payload["notes"]:
            payload["notes"].append(note)
    return _write_decision_snapshot(
        latest_path=context.latest_decision_path,
        dated_dir=context.decisions_dir,
        payload=payload,
    )


def _load_exact_bars(
    normalized_dir: Path,
    symbols: tuple[str, ...],
) -> dict[str, dict[date, NormalizedBar]]:
    lookup: dict[str, dict[date, NormalizedBar]] = {}
    for symbol in symbols:
        path = normalized_dir / f"{symbol}.csv"
        rows = read_normalized_csv(path)
        lookup[symbol] = {row.date: row for row in rows}
    return lookup


def _merge_rows_by_date(
    existing_rows: list[dict[str, str]],
    new_rows: list[dict[str, str]],
) -> list[dict[str, str]]:
    by_date = {row["date"]: row for row in existing_rows}
    for row in new_rows:
        by_date[row["date"]] = row
    return [by_date[key] for key in sorted(by_date)]


def _load_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _write_csv(
    path: Path,
    rows: list[dict[str, str]],
    fieldnames: tuple[str, ...],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _parse_optional_date(raw_value: object) -> date | None:
    if raw_value in (None, ""):
        return None
    return date.fromisoformat(str(raw_value))


def _load_latest_nav(
    nav_rows: list[dict[str, str]],
    initial_cash: float,
) -> float:
    if not nav_rows:
        return initial_cash
    return float(nav_rows[-1]["nav"])
