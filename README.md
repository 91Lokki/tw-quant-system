# tw_quant

`tw_quant` is a modular Python project for quantitative research and backtesting on Taiwan equities. It is designed to grow into a serious end-of-day trading system while staying small, readable, and believable as a university CS portfolio project.

## Why Taiwan Equities

Taiwan equities are a useful market for a systems-oriented quant project:

- they provide a concrete regional focus instead of a generic "global stocks" demo
- they have market-specific trading rules worth modeling later
- they allow the project to demonstrate engineering discipline, not just factor experimentation

This repository is aimed at daily or slower strategies. It is not a high-frequency trading system.

## Project Goals

- keep the codebase modular enough to extend into a full research and trading workflow
- separate data, signals, portfolio logic, backtesting, and reporting concerns
- make design choices that are easy to explain in a portfolio or interview setting
- keep v1 intentionally small so each module has a clear reason to exist

## Non-Goals for V1

- live broker integration
- automated trading
- intraday or high-frequency infrastructure
- web services or dashboards
- a large abstraction-heavy framework with no real workflow

## Architecture Overview

The current architecture follows the shape of a real quant workflow:

`CLI -> pipeline -> domain modules -> outputs`

Current module responsibilities:

- `config.py` loads typed settings from TOML
- `core/models.py` defines the typed contracts used across ingestion, signals, portfolio construction, and backtesting
- `data/providers.py` fetches raw market data from FinMind
- `data/normalize.py` defines the normalized daily bar schema
- `data/loader.py` loads normalized bars and aligns them by date for downstream use
- `data/store.py` persists raw JSON caches and normalized CSV files
- `data/io.py` manages local directory conventions for market data and generated artifacts
- `signals/generate.py` computes the first real daily signal set from normalized bars
- `portfolio/construct.py` converts signal outputs into target weights and daily applied weights
- `backtest/run.py` loads local bars and signals, simulates rebalancing, and produces NAV plus metrics
- `reporting/report.py` writes a markdown backtest summary
- `execution/paper.py` is a documented placeholder for future paper execution support
- `pipelines/ingest.py` orchestrates fetching, normalization, and local caching
- `pipelines/signals.py` loads local datasets, aligns symbols by date, computes signals, and writes outputs
- `pipelines/backtest.py` keeps the CLI thin by orchestrating the workflow

## Repository Layout

```text
.
├── configs/
├── data/
├── docs/
├── research/
├── src/tw_quant/
└── tests/
```

Highlights:

- production code lives in `src/tw_quant`
- exploratory work lives in `research`
- sample configuration lives in `configs`
- local datasets and generated reports stay under `data`

## Current Status

Current implemented capabilities:

- a packageable Python project with `pyproject.toml`
- a thin CLI exposed through `python -m tw_quant`
- a typed settings loader
- shared dataclasses for config, datasets, signals, portfolio weights, NAV rows, and metrics
- a real daily data ingestion pipeline for Taiwan equities and a benchmark
- a local dataset loader with schema validation and date alignment
- a first usable daily signal layer with moving average, momentum, and volatility-based filtering
- local raw JSON caching plus normalized CSV storage
- a real local-data portfolio construction and backtest workflow
- daily NAV, weights, markdown summary, and SVG chart outputs
- unit and integration tests for config, models, and CLI behavior

It intentionally does not include a real trading strategy or broker connectivity yet.

## Development Setup

This project is configured for `uv` and Python 3.12.

The recommended stable workflow is to invoke the package module directly:

```bash
uv sync
uv run pytest
uv run python -m tw_quant --help
uv run python -m tw_quant ingest --config configs/settings.example.toml
uv run python -m tw_quant signals --config configs/settings.example.toml
uv run python -m tw_quant backtest --config configs/settings.example.toml
```

If `uv` is not installed locally yet, install it first and then run the commands above.

## Planned Roadmap

Planned extensions are intentionally aligned with a realistic quant workflow:

1. richer data ingestion coverage for Taiwan equities
2. signal generation for cross-sectional or rules-based strategies
3. more advanced portfolio construction with richer constraints
4. more realistic backtesting assumptions such as slippage calibration and execution timing variants
5. reporting with performance and risk summaries
6. paper execution for dry-run order simulation
7. broker execution once the research workflow is stable

## Data Source Notes

The v1 data layer is built around FinMind:

- equities and ETFs such as `2330` and `0050` use `TaiwanStockPrice`
- the default benchmark uses `TaiwanStockTotalReturnIndex` with `TAIEX`

Important limitation:

- the chosen FinMind benchmark dataset provides a daily price-like index series, not full OHLCV bars
- the normalized benchmark output therefore maps that single price into `open/high/low/close` and leaves `volume` empty

This is deliberate for v1: it keeps the source practical and the architecture clean while making the limitation explicit.

## Signal Layer Notes

The first signal layer reads locally normalized bars and writes a combined daily signal panel to `data/processed/signals/daily/signal_panel.csv`.

Current signal set:

- moving-average trend signal
- lookback momentum
- rolling volatility filter

The initial `signal_score` is intentionally simple: it averages the binary trend and momentum directions, then zeroes the score when the rolling volatility filter fails.

## Backtest Layer Notes

The current backtest workflow reads:

- normalized daily bars from `data/processed/market_data/daily/`
- the generated signal panel from `data/processed/signals/daily/signal_panel.csv`

The v1 portfolio rule is intentionally explicit:

- benchmark is loaded for reference but is not treated as a held asset
- tradable symbols are selected from positive `signal_score` values
- weights are equal-weight among selected symbols
- rebalancing uses the first aligned trading day of each configured period
- new weights become active on the next trading day to avoid lookahead bias

Current backtest outputs:

- `data/processed/backtests/<project_name>/daily_nav.csv`
- `data/processed/backtests/<project_name>/daily_weights.csv`
- `data/processed/reports/<project_name>/backtest_summary.md`
- `data/processed/reports/<project_name>/equity_curve.svg`
- `data/processed/reports/<project_name>/drawdown.svg`

## Disclaimer

This repository is a software engineering and quantitative research project. It is not investment advice, and it is not configured for live trading.
