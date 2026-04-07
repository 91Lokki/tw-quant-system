# Project Overview

## Summary

`tw_quant` is an end-of-day quantitative research and operational rehearsal system for Taiwan equities.

It is designed to be:

- local and reproducible
- artifact-driven
- modular enough to review stage by stage
- narrow enough to stay explainable

The repository now supports two distinct but connected layers:

- a research layer for ingestion, signals, backtest, walk-forward, diagnostics, and reporting
- an operational layer for daily decision generation and paper-trading rehearsal

## Current Mainline

The main forward branch is the TWSE cross-sectional workflow built from:

- TWSE common-stock history
- a monthly top-50 liquidity universe
- monthly volatility-adjusted momentum ranking
- long-only portfolio construction
- benchmark regime control on `TAIEX`

The current practical operational mainline is:

- `risk_controlled_3m_half_exposure_exp60_delay1`

The current pure research benchmark is:

- `original_monthly`

The narrow `2330/0050` route remains in the repository as a baseline / failure-case reference, but it is not the recommended forward line.

## Why This Project Exists

Many student quant projects stop at:

- a notebook
- a single backtest script
- one in-sample result

This project intentionally goes further by separating:

- data ingestion
- normalized local storage
- signal generation
- portfolio construction
- historical simulation
- diagnostics
- reporting
- operational decision artifacts

That makes the repository useful both as:

- a software engineering portfolio project
- a realistic quantitative research workflow

## System Shape

The implemented pipeline is:

```text
ingest
  -> normalized bars
  -> signals
  -> portfolio weights
  -> backtest
  -> walk-forward
  -> diagnostics
  -> markdown reports + charts
  -> daily decision + paper-trading ledger
```

Branch split:

- baseline branch
  - `configs/settings.example.toml`
  - narrow `2330/0050 + TAIEX` baseline
  - retained for reference and diagnostics
- TWSE cross-sectional branch
  - `configs/tw_top50_liquidity.example.toml`
  - top-50 liquidity universe
  - monthly cross-sectional signal panel
  - dynamic backtest / walk-forward / diagnostics
  - Phase H daily decision and paper-trading scaffold

## Practical Interpretation

The repository now distinguishes between:

- the compact historical comparison surface used in research reports
- the practical operational mainline used by `decision` and `paper`

Current comparison rows:

- `original_monthly`
- `risk_controlled_3m_half_exposure_exp60_delay1`
- `risk_controlled_3m_half_exposure_exp60`
- `risk_controlled_3m_half_exposure_exp60_delay3`
- `risk_controlled_3m_half_exposure_exp60_w08`

Meaning:

- `original_monthly`: pure-alpha benchmark
- `delay1`: practical mainline and paper-trading line
- `exp60`: direct no-extra-delay reference
- `delay3`: slower execution robustness confirmation
- `w08`: conservative concentration appendix

## Operational Layer

Phase H adds the smallest useful operational scaffold without redesigning the research engine.

It currently supports:

- daily decision snapshots
- scheduled execution dates under one explicit delay convention
- file-based paper-trading blotter
- file-based paper portfolio state
- file-based paper NAV history

The operational layer is intentionally narrow:

- no broker connectivity
- no board-lot sizing
- no partial fill engine
- no multiple execution models
- no live-trading orchestration framework

## Main Artifacts

Research artifacts:

- `data/processed/market_data/daily/`
- `data/processed/signals/monthly/`
- `data/processed/backtests/<project>/`
- `data/processed/reports/<project>/`

Operational artifacts:

- `data/processed/paper_trading/<project>/daily_decision/latest.json`
- `data/processed/paper_trading/<project>/paper_trade_blotter.csv`
- `data/processed/paper_trading/<project>/paper_portfolio_state.csv`
- `data/processed/paper_trading/<project>/paper_nav_history.csv`

Recommended first files to inspect for the mainline:

- `data/processed/backtests/tw_top50_liquidity_v1/risk_comparison.csv`
- `data/processed/backtests/tw_top50_liquidity_v1/walkforward/risk_comparison.csv`
- `data/processed/reports/tw_top50_liquidity_v1/backtest_summary.md`
- `data/processed/reports/tw_top50_liquidity_v1/walkforward/walkforward_summary.md`
- `data/processed/reports/tw_top50_liquidity_v1/diagnostics/diagnostics_summary.md`

Recommended first files to inspect for Phase H:

- `data/processed/paper_trading/tw_top50_liquidity_v1/daily_decision/latest.json`
- `data/processed/paper_trading/tw_top50_liquidity_v1/paper_trade_blotter.csv`
- `data/processed/paper_trading/tw_top50_liquidity_v1/paper_portfolio_state.csv`
- `data/processed/paper_trading/tw_top50_liquidity_v1/paper_nav_history.csv`

## Major Modules

- `src/tw_quant/data/`
  - provider access, normalization, storage, loading
- `src/tw_quant/universe/`
  - TWSE liquidity membership construction
- `src/tw_quant/signals/`
  - signal generation and signal-panel loading
- `src/tw_quant/portfolio/`
  - target weight construction
- `src/tw_quant/backtest/`
  - simulation, metrics, walk-forward
- `src/tw_quant/diagnostics/`
  - artifact-based post-run analysis
- `src/tw_quant/reporting/`
  - markdown summaries and charts
- `src/tw_quant/execution/`
  - daily decision and paper-trading helpers
- `src/tw_quant/pipelines/`
  - thin orchestration layer behind the CLI

## Stable Usage

Mainline workflow:

```bash
uv sync
uv run pytest
uv run python -m tw_quant ingest --config configs/tw_top50_liquidity.example.toml --refresh
uv run python -m tw_quant signals --config configs/tw_top50_liquidity.example.toml
uv run python -m tw_quant backtest --config configs/tw_top50_liquidity.example.toml
uv run python -m tw_quant walkforward --config configs/tw_top50_liquidity.example.toml
uv run python -m tw_quant diagnostics --config configs/tw_top50_liquidity.example.toml
uv run python -m tw_quant decision --config configs/tw_top50_liquidity.example.toml
uv run python -m tw_quant paper --config configs/tw_top50_liquidity.example.toml
```

## Out Of Scope

Still intentionally out of scope:

- new alpha-family exploration in the operational layer
- broker integration
- live order routing
- dashboard-heavy productization
- ML-based modeling
- generalized optimization frameworks

## Related Files

- [`../README.md`](../README.md)
- [`architecture.md`](architecture.md)
- [`../configs/tw_top50_liquidity.example.toml`](../configs/tw_top50_liquidity.example.toml)
