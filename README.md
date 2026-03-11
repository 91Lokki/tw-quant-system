# tw_quant

`tw_quant` is a modular Python project for quantitative research on Taiwan equities. It is built as a realistic end-of-day research and backtesting system that can later grow into paper trading or broker-connected execution, while remaining small enough to explain clearly in a university CS portfolio.

## What This Project Does

This repository implements a local research pipeline for Taiwan equities:

1. ingest daily market data for Taiwan stocks, ETFs, and a benchmark
2. normalize and cache the data locally
3. compute simple but usable daily signals
4. convert signals into target portfolio weights
5. run a deterministic backtest with transaction costs
6. generate reports and visual artifacts
7. run walk-forward out-of-sample evaluation on the same local artifacts

The current project is focused on daily or slower strategies. It is not a high-frequency system, a web app, or an auto-trading bot.

## Problem Statement

Many student quant projects stop at a notebook, a single script, or a one-off backtest. This project is meant to solve a more engineering-oriented problem:

- build a clean research pipeline that is modular enough to extend
- keep the data, signal, portfolio, backtest, and reporting stages separate
- make the current system useful now without overbuilding a framework too early

The result is a project that is credible as both a CS systems project and an applied quantitative research project.

## End-to-End Pipeline

```text
FinMind daily data
    ->
raw JSON cache
    ->
normalized daily bars
    ->
signal generation
    ->
portfolio construction
    ->
backtest engine
    ->
markdown report + SVG charts
```

Expanded workflow:

```text
ingest
  -> data/raw/finmind/*.json
  -> data/processed/market_data/daily/*.csv

signals
  -> data/processed/signals/daily/signal_panel.csv

backtest
  -> data/processed/backtests/<project_name>/daily_nav.csv
  -> data/processed/backtests/<project_name>/daily_weights.csv
  -> data/processed/reports/<project_name>/backtest_summary.md
  -> data/processed/reports/<project_name>/equity_curve.svg
  -> data/processed/reports/<project_name>/drawdown.svg

walkforward
  -> data/processed/backtests/<project_name>/walkforward/walkforward_nav.csv
  -> data/processed/backtests/<project_name>/walkforward/window_summary.csv
  -> data/processed/reports/<project_name>/walkforward/walkforward_summary.md

diagnostics
  -> data/processed/backtests/<project_name>/diagnostics/yearly_return_table.csv
  -> data/processed/backtests/<project_name>/diagnostics/walkforward_window_diagnostics.csv
  -> data/processed/backtests/<project_name>/diagnostics/symbol_exposure_summary.csv
  -> data/processed/backtests/<project_name>/diagnostics/signal_diagnostics.csv
  -> data/processed/reports/<project_name>/diagnostics/diagnostics_summary.md

demo app
  -> app/streamlit_app.py
  -> reads existing local artifacts without rerunning the quant engine
```

## Current Stable Workflow

The currently recommended execution path is:

```bash
uv sync
uv run pytest
uv run python -m tw_quant --help
uv run python -m tw_quant ingest --config configs/settings.example.toml --refresh
uv run python -m tw_quant signals --config configs/settings.example.toml
uv run python -m tw_quant backtest --config configs/settings.example.toml
uv run python -m tw_quant walkforward --config configs/settings.example.toml
uv run python -m tw_quant diagnostics --config configs/settings.example.toml
uv run python -m streamlit run app/streamlit_app.py
```

This direct module invocation is the stable workflow for the project at the moment.
Use `--refresh` on the ingest step whenever you want to refetch FinMind data and extend local artifacts to the latest stable historical close.

## Current Implemented Features

- typed TOML configuration for ingestion, signals, portfolio, and backtest settings
- FinMind-based Taiwan daily market data ingestion
- local raw JSON caching and normalized CSV storage
- schema validation and shared-date alignment for normalized bars
- signal generation with:
  - moving average trend
  - lookback momentum
  - rolling volatility filter
- long-only portfolio construction with explicit rebalance rules
- daily NAV simulation with transaction cost modeling
- walk-forward out-of-sample evaluation with configurable train/test windows
- diagnostics for yearly breakdowns, walk-forward distributions, exposure behavior, and signal activity
- markdown backtest report generation
- SVG equity curve and drawdown chart generation
- a lightweight Streamlit demo app for browsing local artifacts and backtest outputs
- unit and integration tests around the main workflow

## Major Modules

- `src/tw_quant/data/`: ingestion, normalization, storage, and local dataset loading
- `src/tw_quant/signals/`: daily signal generation and signal panel loading
- `src/tw_quant/portfolio/`: target weight construction and weight propagation
- `src/tw_quant/backtest/`: return simulation and metric calculation
- `src/tw_quant/reporting/`: markdown reports and SVG performance charts
- `src/tw_quant/pipelines/`: thin orchestration layer used by the CLI
- `app/`: local Streamlit demo for showing project artifacts interactively

## Sample Results

Example backtest from the current repository artifacts:

- project: `tw_quant_v1`
- period: `2014-01-01` to `2026-03-10`
- tradable symbols: `2330`, `0050`
- benchmark: `TAIEX`
- rebalance frequency: monthly

Headline metrics:

- final NAV: `1.078122`
- cumulative return: `7.8122%`
- annualized return: `0.6382%`
- annualized volatility: `26.4214%`
- Sharpe ratio: `0.2690`
- max drawdown: `-76.3137%`
- cumulative turnover: `38.000000`

Current walk-forward out-of-sample headline metrics:

- combined OOS period: `2015-01-09` to `2026-03-10`
- walk-forward final NAV: `1.071010`
- walk-forward cumulative return: `7.1010%`
- walk-forward annualized return: `0.6391%`
- walk-forward annualized volatility: `27.2575%`
- walk-forward Sharpe ratio: `0.2786`
- walk-forward max drawdown: `-77.1872%`

Generated portfolio-facing artifacts:

- [Backtest Summary](data/processed/reports/tw_quant_v1/backtest_summary.md)
- [Equity Curve SVG](data/processed/reports/tw_quant_v1/equity_curve.svg)
- [Drawdown SVG](data/processed/reports/tw_quant_v1/drawdown.svg)

The local demo app can display these artifacts directly and also show recent NAV rows, recent weights, and artifact status.

## Outputs Generated by the Pipeline

After running the stable workflow, the main outputs are:

- normalized daily bars in `data/processed/market_data/daily/`
- signal panel in `data/processed/signals/daily/signal_panel.csv`
- daily NAV in `data/processed/backtests/<project_name>/daily_nav.csv`
- daily weights in `data/processed/backtests/<project_name>/daily_weights.csv`
- walk-forward NAV in `data/processed/backtests/<project_name>/walkforward/walkforward_nav.csv`
- walk-forward window summary in `data/processed/backtests/<project_name>/walkforward/window_summary.csv`
- yearly diagnostics table in `data/processed/backtests/<project_name>/diagnostics/yearly_return_table.csv`
- walk-forward diagnostics table in `data/processed/backtests/<project_name>/diagnostics/walkforward_window_diagnostics.csv`
- symbol exposure summary in `data/processed/backtests/<project_name>/diagnostics/symbol_exposure_summary.csv`
- signal diagnostics summary in `data/processed/backtests/<project_name>/diagnostics/signal_diagnostics.csv`
- markdown report in `data/processed/reports/<project_name>/backtest_summary.md`
- walk-forward markdown report in `data/processed/reports/<project_name>/walkforward/walkforward_summary.md`
- diagnostics markdown report in `data/processed/reports/<project_name>/diagnostics/diagnostics_summary.md`
- equity curve chart in `data/processed/reports/<project_name>/equity_curve.svg`
- drawdown chart in `data/processed/reports/<project_name>/drawdown.svg`
- interactive local demo app in `app/streamlit_app.py`

## Walk-Forward Evaluation

The project now includes a lightweight walk-forward workflow so evaluation is not limited to a single full-period historical backtest.

For v1, the recommended design is an `expanding` window:

- reserve an initial in-sample history window
- evaluate the next out-of-sample test window
- roll forward and repeat
- aggregate only the out-of-sample results into a combined NAV series

This matters because it makes the project look more like a real research system and less like a single in-sample result.

Run it with:

```bash
uv run python -m tw_quant walkforward --config configs/settings.example.toml
```

## Local Demo App

The repository includes a small Streamlit-based local demo interface for presentation and review.

It is designed to help a reviewer quickly inspect:

- what artifacts currently exist
- current backtest summary metrics
- generated equity curve and drawdown charts
- recent weights and signal scores
- recent NAV rows and timeseries behavior
- a bilingual presentation view through Traditional Chinese / English language switching

The demo is intentionally portfolio-oriented:

- a top-level project summary
- scan-friendly KPI cards
- pipeline status and artifact freshness
- latest portfolio snapshot
- recent NAV and weight inspection tables

Run it with:

```bash
uv run python -m streamlit run app/streamlit_app.py
```

The app reads existing local artifacts. It does not rerun ingestion, signal generation, or the backtest engine.

## Why This Is a Meaningful CS Portfolio Project

This project is stronger than a toy quant script because it demonstrates:

- modular system design instead of notebook-only analysis
- explicit data contracts and local storage conventions
- deterministic pipeline stages that can be tested independently
- realistic engineering tradeoffs around scope, extensibility, and maintainability
- a direct path from research workflow to future paper trading or execution support

It is also specific enough to be memorable: the project is about Taiwan equities, not a generic stock-market demo.

## Current Limitations

- the benchmark series uses a normalized TAIEX proxy source and does not include full OHLCV fields
- the strategy logic is intentionally simple and long-only
- the transaction cost model is bps-based and does not model lot size or fill mechanics
- the project is local-file based and does not include a database layer
- there is no paper execution monitoring or broker integration yet

## Roadmap

Short-to-medium term roadmap:

1. richer Taiwan equity universe coverage and stronger data validation
2. better ranking and portfolio sizing logic
3. richer reporting and benchmark-relative analytics
4. more realistic execution assumptions in the backtest engine
5. paper trading built on top of the existing local pipeline
6. future broker execution once the research workflow is stable

## Documentation Guide

Recommended files to open:

- [Project Overview](docs/project_overview.md)
- [Architecture Notes](docs/architecture.md)
- [Latest Sample Backtest Report](data/processed/reports/tw_quant_v1/backtest_summary.md)
- [Local Demo App](app/streamlit_app.py)

## Disclaimer

This repository is a software engineering and quantitative research project. It is not investment advice, and it is not configured for live trading.
