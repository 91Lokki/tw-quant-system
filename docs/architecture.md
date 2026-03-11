# Architecture

## Overview

`tw_quant` is organized around the core stages of a daily quantitative trading workflow for Taiwan equities:

`config -> data -> signals -> portfolio -> backtest -> report`

The CLI is intentionally thin. Its job is to trigger a pipeline, not to hold business logic.

## Module Boundaries

- `config.py` loads typed settings from TOML
- `core/models.py` defines shared dataclasses used across the scaffold
- `data/providers.py` fetches raw market data from FinMind
- `data/normalize.py` enforces the project-wide daily bar schema
- `data/loader.py` loads normalized local bars, validates schema, and aligns symbols by date
- `data/store.py` writes raw JSON caches and normalized CSV files
- `data/io.py` manages local directory conventions for raw data, processed data, and reports
- `signals/generate.py` computes the first real daily signal set from normalized bars
- `portfolio/construct.py` represents the future target-weight construction step
- `backtest/run.py` coordinates the current scaffold backtest flow
- `reporting/report.py` writes a lightweight markdown summary for each run
- `execution/paper.py` is a placeholder for future dry-run execution support
- `pipelines/ingest.py` orchestrates provider fetch, normalization, and local caching
- `pipelines/signals.py` orchestrates local dataset loading, alignment, signal generation, and output
- `pipelines/backtest.py` wires the workflow together for the CLI

## Why It Is Intentionally Simple

This scaffold avoids extra layers that would not yet carry their own weight:

- no large provider interface hierarchy
- no broker adapter base classes
- no abstract strategy framework
- no database abstraction layer

There is one intentionally small provider contract because the ingestion pipeline already benefits from swapping a real provider for a mocked one in tests.

The dataset loader is also intentionally small: it validates the normalized CSV schema and offers shared-date alignment, but it does not introduce a DataFrame framework or a multi-backend data abstraction layer.

## Extension Path

The next meaningful additions should be:

1. ingestion logic for Taiwan equity OHLCV data
2. real signal generation using reproducible research code
3. portfolio construction with constraints and trading cost assumptions
4. a backtest engine with position and PnL accounting
5. paper execution that reuses the same portfolio outputs

The design goal is to let those features grow naturally from the current modules instead of forcing them into a large framework too early.
