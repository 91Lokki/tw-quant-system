# Architecture

## Overview

`tw_quant` is organized around the core stages of a daily quantitative trading workflow for Taiwan equities:

```text
ingest
  -> normalized bars
  -> signals
  -> portfolio construction
  -> backtest
  -> reports
```

Full data flow:

```text
baseline branch: FinMind provider
cross-sectional branch: TWSE official daily market + TAIEX history
    ->
raw JSON cache
    ->
normalized daily bar CSVs
    ->
signal panel CSV
    ->
target portfolio weights
    ->
daily NAV simulation
    ->
markdown report + SVG charts
```

Optional research evaluation branch:

```text
aligned local bars + signal panel
    ->
walk-forward window splitting
    ->
repeated out-of-sample backtest segments
    ->
combined OOS NAV + walk-forward report
```

Optional analysis branch:

```text
backtest NAV + weights + signal panel + walk-forward windows
    ->
diagnostics aggregation
    ->
yearly tables + exposure summaries + signal summaries + diagnostics report
```

Presentation layer:

```text
local artifacts
    ->
Streamlit demo app
    ->
interactive inspection for reviewers
```

Taiwan cross-sectional branch:

```text
TWSE official daily market snapshots + TWSE official TAIEX history
    ->
observed TWSE common-stock candidate master
    ->
monthly top-50 liquidity membership (60-day average Trading_money)
    ->
monthly volatility-adjusted momentum signal panel
    ->
benchmark regime filter + focused practical-line comparison
    ->
backtest / walk-forward / diagnostics
```

The current practical candidate remains `risk_controlled_3m_half_exposure_exp60`.
Phase G does not add new alpha or new data paths; it only applies tiny execution-realism checks around that same line:

- `delay1`
- `delay3`
- `w08`

The CLI is intentionally thin. Its job is to trigger a pipeline, not to hold business logic.

## Module Boundaries

- `config.py` loads typed settings from TOML
- `core/models.py` defines shared dataclasses used across the implemented research workflow
- `data/providers.py` fetches raw market data from either FinMind or TWSE official endpoints, depending on the research branch
- `data/normalize.py` enforces the project-wide daily bar schema
- `data/loader.py` loads normalized local bars, validates schema, and aligns symbols by date
- `data/store.py` writes raw JSON caches and normalized CSV files
- `data/io.py` manages local directory conventions for raw data, processed data, and reports
- `signals/generate.py` computes the first real daily signal set from normalized bars
- `universe/liquidity.py` builds the concrete Phase A TWSE top-50 liquidity universe membership artifact
- `signals/loader.py` loads the persisted signal panel for portfolio construction and backtesting
- `portfolio/construct.py` turns signal rows into rebalance targets and daily applied weights
- `backtest/run.py` simulates both the legacy fixed-symbol baseline and the Taiwan top-50 cross-sectional branch
- `backtest/walkforward.py` runs expanding or rolling walk-forward evaluation on top of either branch
- `backtest/metrics.py` computes core performance metrics
- `diagnostics/analyze.py` reads persisted artifacts and explains yearly performance, walk-forward behavior, exposure usage, and signal activity
- `reporting/charts.py` renders simple SVG performance charts from the persisted NAV series
- `reporting/report.py` writes a markdown summary for each run and links the generated chart artifacts
- `app/streamlit_app.py` provides a lightweight local UI for browsing the generated artifacts
- `execution/paper.py` is a placeholder for future dry-run execution support
- `pipelines/ingest.py` orchestrates provider fetch, normalization, and local caching
- `pipelines/signals.py` orchestrates local dataset loading, alignment, signal generation, and output
- `pipelines/signals.py` also contains the branch split between the legacy daily baseline signals and the new monthly cross-sectional signals
- `pipelines/backtest.py` wires the workflow together for the CLI
- `pipelines/walkforward.py` exposes the walk-forward workflow through the CLI without changing the core engine
- `pipelines/diagnostics.py` exposes the post-run diagnostics workflow through the CLI

## Design Intent

This repository is intentionally structured like a real system, but it avoids abstractions that do not yet earn their keep.

Current design choices:

- local-file-first storage
- typed configuration instead of notebook-only parameters
- deterministic pipeline stages
- small, explicit contracts between modules
- simple portfolio and backtest logic that is easy to inspect
- a concrete, non-generic Taiwan cross-sectional branch for top-liquidity universe research

Not included yet:

- live broker integration
- database-backed storage
- dashboard or web service layers
- large plugin or strategy frameworks
- full survivorship-aware universe history and more complete listing / delisting state handling

## Why The Architecture Is Believable

The project is designed so that each stage already produces a real artifact for the next stage:

- ingestion produces normalized daily bars
- signals produce a persisted signal panel
- portfolio construction produces target and applied weights
- backtesting produces daily NAV and metrics
- walk-forward evaluation produces combined out-of-sample NAV and per-window summaries
- diagnostics produce failure-analysis tables that explain why a weak strategy is weak
- reporting produces markdown and chart artifacts

That makes the repository stronger than a one-off script or notebook pipeline, while keeping the codebase readable for a portfolio reviewer.

## Extension Path

Natural next steps from the current design:

1. richer Taiwan universe coverage and stronger corporate-action handling
2. more advanced signal ranking and portfolio constraints
3. richer risk and benchmark-relative reporting
4. paper execution built on top of the existing portfolio outputs
5. future broker execution without rewriting the research pipeline
