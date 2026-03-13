# Project Overview

## Summary

`tw_quant` is a portfolio-grade Python project for Taiwan equities quantitative research. It is designed around a realistic research pipeline rather than a single notebook or backtest script.

The project currently supports:

- daily market data ingestion
- data normalization and local caching
- signal generation
- portfolio construction
- local-data backtesting
- walk-forward out-of-sample evaluation
- diagnostics on yearly performance, walk-forward windows, exposure, and signal behavior
- report and chart generation
- a lightweight local Streamlit demo for artifact inspection
- bilingual presentation switching in the local demo layer
- a Taiwan cross-sectional branch based on a reproducible `TWSE top-50 liquidity` universe and monthly `volatility-adjusted momentum` signals

The implementation is intentionally scoped for end-of-day research. It is not a high-frequency system or a live trading product.

## Problem Statement

Many student quant projects demonstrate ideas, but not systems. They often:

- rely on ad hoc notebooks
- mix data access, signal logic, and backtest logic in one file
- produce results that are hard to reproduce
- leave no clear path to extension

This project addresses that gap by building a modular research workflow for Taiwan equities that is:

- reproducible
- inspectable
- locally runnable
- extendable toward paper trading and future execution

The original `2330/0050` route remains in the repository, but it is now explicitly treated as an early narrow baseline and a diagnosed failure case. The main forward research direction is the Taiwan cross-sectional branch.

## System Design

The system is organized as a sequence of clearly separated stages:

```text
ingest
  -> normalized bars
  -> signals
  -> portfolio construction
  -> backtest
  -> reports
```

Expanded data flow:

```text
baseline branch: FinMind API
cross-sectional branch: TWSE official daily market data + TAIEX history
    ->
raw JSON cache
    ->
normalized daily bar CSVs
    ->
signal panel CSV
    ->
target portfolio weights
    ->
daily NAV and metrics
    ->
markdown summary + SVG charts

Optional evaluation branch:

```text
local bars + signal panel
    ->
walk-forward train/test splitting
    ->
repeated OOS backtest segments
    ->
combined OOS NAV + walk-forward report
```
```

This structure keeps each stage understandable on its own while allowing the whole workflow to run end to end.

Current branch split:

- baseline branch
  - `2330/0050 + TAIEX`
  - fully wired through backtest, walk-forward, diagnostics, and reports
  - retained as a weak baseline / failure case
- Taiwan cross-sectional branch
  - `TWSE top-50 liquidity`
  - monthly universe membership + monthly cross-sectional signal panel
  - now connected to branch-specific backtest, walk-forward, and diagnostics workflows
  - Phase F keeps the same alpha line but narrows the practical comparison to:
    - `original_monthly`
    - `risk_controlled_3m_half_exposure`
    - `risk_controlled_3m_half_exposure_ma150`
    - `risk_controlled_3m_half_exposure_exp60`

## Major Modules

### `src/tw_quant/data/`

Responsible for:

- provider access
- source normalization
- local storage
- normalized dataset loading

### `src/tw_quant/signals/`

Responsible for:

- computing daily features and signals
- writing the combined signal panel used by downstream stages

### `src/tw_quant/portfolio/`

Responsible for:

- converting signal outputs into target weights
- applying explicit rebalance rules
- propagating daily portfolio weights between rebalance dates

### `src/tw_quant/backtest/`

Responsible for:

- reading local bars and local signals
- simulating daily portfolio returns
- applying transaction cost assumptions
- computing core performance metrics

### `src/tw_quant/reporting/`

Responsible for:

- generating markdown summaries
- generating equity curve and drawdown charts
- keeping outputs presentation-ready for review

### `src/tw_quant/pipelines/`

Responsible for:

- keeping CLI commands thin
- wiring together the underlying modules into reproducible workflows

## Current Implemented Features

The project currently includes:

- branch-aware ingestion:
  - FinMind for the narrow `2330/0050` baseline
  - TWSE official daily market ingestion for the Taiwan top-50 liquidity cross-sectional branch
- normalized bar storage in local CSV form
- signal generation with:
  - moving average trend
  - momentum
  - volatility filter
- equal-weight portfolio construction driven by signal score
- configurable rebalance frequency
- daily NAV simulation with bps-based transaction costs
- walk-forward evaluation with configurable expanding / rolling windows
- markdown summary report generation
- SVG equity curve and drawdown chart generation
- automated tests for config, data flow, signals, backtest, and reporting
- Phase A cross-sectional research artifacts:
  - `data/processed/metadata/twse_stock_info.csv`
  - `data/processed/universe/tw_top50_liquidity_membership.csv`
  - `data/processed/signals/monthly/cross_sectional_signal_panel.csv`

## Output Artifacts

The current workflow generates artifacts that are easy to inspect and show in a portfolio:

- `data/processed/market_data/daily/<symbol>.csv`
- `data/processed/signals/daily/signal_panel.csv`
- `data/processed/metadata/twse_usable_stock_info.csv`
- `data/processed/metadata/twse_price_availability.csv`
- `data/processed/backtests/<project_name>/daily_nav.csv`
- `data/processed/backtests/<project_name>/daily_weights.csv`
- `data/processed/reports/<project_name>/backtest_summary.md`
- `data/processed/reports/<project_name>/equity_curve.svg`
- `data/processed/reports/<project_name>/drawdown.svg`
- `data/processed/backtests/<project_name>/walkforward/walkforward_nav.csv`
- `data/processed/backtests/<project_name>/walkforward/window_summary.csv`
- `data/processed/reports/<project_name>/walkforward/walkforward_summary.md`
- `data/processed/backtests/<project_name>/diagnostics/yearly_return_table.csv`
- `data/processed/backtests/<project_name>/diagnostics/walkforward_window_diagnostics.csv`
- `data/processed/backtests/<project_name>/diagnostics/symbol_exposure_summary.csv`
- `data/processed/backtests/<project_name>/diagnostics/signal_diagnostics.csv`
- `data/processed/reports/<project_name>/diagnostics/diagnostics_summary.md`

## Sample Results

Current sample run from the repository:

- project: `tw_quant_v1`
- period: `2014-01-01` to `2026-03-10`
- tradable symbols: `2330`, `0050`
- benchmark: `TAIEX`
- rebalance frequency: `monthly`

Metrics from the latest generated report:

- final NAV: `1.078122`
- cumulative return: `7.8122%`
- annualized return: `0.6382%`
- annualized volatility: `26.4214%`
- Sharpe ratio: `0.2690`
- max drawdown: `-76.3137%`
- cumulative turnover: `38.000000`

Latest walk-forward out-of-sample metrics:

- combined OOS period: `2015-01-09` to `2026-03-10`
- final NAV: `1.071010`
- cumulative return: `7.1010%`
- annualized return: `0.6391%`
- annualized volatility: `27.2575%`
- Sharpe ratio: `0.2786`
- max drawdown: `-77.1872%`

Artifacts to inspect:

- [Backtest Summary](../data/processed/reports/tw_quant_v1/backtest_summary.md)
- [Equity Curve](../data/processed/reports/tw_quant_v1/equity_curve.svg)
- [Drawdown Chart](../data/processed/reports/tw_quant_v1/drawdown.svg)
- [Top-50 Liquidity Config](../configs/tw_top50_liquidity.example.toml)

## Stable Workflow

The stable execution path is:

```bash
uv sync
uv run pytest
uv run python -m tw_quant ingest --config configs/settings.example.toml --refresh
uv run python -m tw_quant signals --config configs/settings.example.toml
uv run python -m tw_quant backtest --config configs/settings.example.toml
uv run python -m tw_quant walkforward --config configs/settings.example.toml
uv run python -m tw_quant diagnostics --config configs/settings.example.toml
uv run python -m streamlit run app/streamlit_app.py
```

Taiwan cross-sectional workflow:

```bash
uv run python -m tw_quant ingest --config configs/tw_top50_liquidity.example.toml --refresh
uv run python -m tw_quant signals --config configs/tw_top50_liquidity.example.toml
uv run python -m tw_quant backtest --config configs/tw_top50_liquidity.example.toml
uv run python -m tw_quant walkforward --config configs/tw_top50_liquidity.example.toml
uv run python -m tw_quant diagnostics --config configs/tw_top50_liquidity.example.toml
```

This workflow is the one to demonstrate to a reviewer because it exercises the full local pipeline.
Use `--refresh` on the ingest step when you want to refetch official / provider data and update the local history horizon.

The Streamlit demo is meant to demonstrate:

- what the current pipeline does
- which local artifacts exist right now
- what the latest backtest results look like
- how the latest portfolio state can be inspected quickly

## Why This Is a Strong CS + Quant Project

This project demonstrates several things that are valuable in a university CS portfolio:

- modular system decomposition
- reproducible data pipelines
- explicit data contracts
- clean separation between data, signals, portfolio logic, and reporting
- deterministic outputs that can be tested
- a domain-specific application area with real market constraints

It also demonstrates quantitative reasoning without pretending to be a production trading system before it is ready.

## Current Limitations

- the benchmark uses a normalized TAIEX proxy series rather than full benchmark OHLCV data
- the current portfolio rule is intentionally simple and long-only
- the transaction cost model is simplified
- the project is local-file based and does not yet include a database or orchestration service
- there is no paper execution or broker connectivity yet

## Future Roadmap

Planned next steps:

1. richer Taiwan equity coverage and improved data handling
2. stronger cross-sectional signal and ranking logic
3. more sophisticated portfolio constraints
4. richer performance and risk reporting
5. paper execution on top of the current research outputs
6. future broker execution once the research loop is stable
