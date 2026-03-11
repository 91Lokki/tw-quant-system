# Architecture

## Overview

`tw_quant` is organized around the core stages of a daily quantitative trading workflow for Taiwan equities:

`config -> data -> signals -> portfolio -> backtest -> report`

The CLI is intentionally thin. Its job is to trigger a pipeline, not to hold business logic.

## Module Boundaries

- `config.py` loads typed settings from TOML
- `core/models.py` defines shared dataclasses used across the implemented research workflow
- `data/providers.py` fetches raw market data from FinMind
- `data/normalize.py` enforces the project-wide daily bar schema
- `data/loader.py` loads normalized local bars, validates schema, and aligns symbols by date
- `data/store.py` writes raw JSON caches and normalized CSV files
- `data/io.py` manages local directory conventions for raw data, processed data, and reports
- `signals/generate.py` computes the first real daily signal set from normalized bars
- `signals/loader.py` loads the persisted signal panel for portfolio construction and backtesting
- `portfolio/construct.py` turns signal rows into rebalance targets and daily applied weights
- `backtest/run.py` simulates the local-data backtest flow and writes NAV plus weight artifacts
- `backtest/metrics.py` computes core performance metrics
- `reporting/charts.py` renders simple SVG performance charts from the persisted NAV series
- `reporting/report.py` writes a markdown summary for each run and links the generated chart artifacts
- `execution/paper.py` is a placeholder for future dry-run execution support
- `pipelines/ingest.py` orchestrates provider fetch, normalization, and local caching
- `pipelines/signals.py` orchestrates local dataset loading, alignment, signal generation, and output
- `pipelines/backtest.py` wires the workflow together for the CLI

## Why It Is Intentionally Simple

This codebase avoids extra layers that would not yet carry their own weight:

- no large provider interface hierarchy
- no broker adapter base classes
- no abstract strategy framework
- no database abstraction layer

There is one intentionally small provider contract because the ingestion pipeline already benefits from swapping a real provider for a mocked one in tests.

The dataset loader is also intentionally small: it validates the normalized CSV schema and offers shared-date alignment, but it does not introduce a DataFrame framework or a multi-backend data abstraction layer.

The same design principle is used in portfolio construction and backtesting: the v1 engine is deterministic, long-only, and close-to-close. It is intended to be easy to inspect, reason about, and extend.

## Extension Path

The next meaningful additions should be:

1. richer Taiwan equity universe coverage and corporate-action handling
2. richer signal libraries and cross-sectional ranking logic
3. portfolio constraints such as sector caps, turnover limits, and benchmark-relative sizing
4. more realistic execution assumptions in the backtest engine
5. paper execution that reuses the same portfolio outputs

The design goal is to let those features grow naturally from the current modules instead of forcing them into a large framework too early.
