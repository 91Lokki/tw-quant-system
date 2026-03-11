# tw_quant

`tw_quant` is a modular Python project scaffold for quantitative research and backtesting on Taiwan equities. It is designed to grow into a serious end-of-day trading system while staying small, readable, and believable as a university CS portfolio project.

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
- `core/models.py` defines the small set of shared dataclasses used by the scaffold
- `data/io.py` manages local directory conventions for market data and generated artifacts
- `signals/generate.py` is the future signal entrypoint
- `portfolio/construct.py` is the future target-weight construction entrypoint
- `backtest/run.py` wires the early research flow into a single backtest result
- `reporting/report.py` writes a lightweight run summary
- `execution/paper.py` is a documented placeholder for future paper execution support
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

This v1 scaffold includes:

- a packageable Python project with `pyproject.toml`
- a thin CLI entrypoint, `twq`
- a typed settings loader
- shared dataclasses for run configuration and scaffold backtest results
- a minimal backtest pipeline that produces a report artifact
- unit and integration tests for config, models, and CLI behavior

It intentionally does not include a real trading strategy, real market data ingestion, or broker connectivity yet.

## Development Setup

This project is configured for `uv` and Python 3.12.

```bash
uv sync
uv run pytest
uv run twq --help
uv run twq backtest --config configs/settings.example.toml
```

If `uv` is not installed locally yet, install it first and then run the commands above.

## Planned Roadmap

Planned extensions are intentionally aligned with a realistic quant workflow:

1. data ingestion for Taiwan equities
2. signal generation for cross-sectional or rules-based strategies
3. portfolio construction with position sizing and constraints
4. backtesting with transaction cost modeling
5. reporting with performance and risk summaries
6. paper execution for dry-run order simulation
7. broker execution once the research workflow is stable

## Disclaimer

This repository is a software engineering and quantitative research project. It is not investment advice, and it is not configured for live trading.
