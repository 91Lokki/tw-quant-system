# Architecture

## Overview

`tw_quant` is structured as a staged local workflow for Taiwan equities.

The architecture is intentionally simple:

- local-file-first
- typed configuration
- deterministic pipeline stages
- explicit artifacts between stages
- thin CLI orchestration

The goal is not to build a general quant platform.
The goal is to build one believable, maintainable research system that can extend into paper trading without rewriting everything.

## Top-Level Flow

```text
ingest
  -> normalized bars
  -> signals
  -> portfolio targets
  -> backtest
  -> walk-forward
  -> diagnostics
  -> reports
  -> daily decision
  -> paper-trading ledger
```

## Branch Structure

There are two active research branches in the same codebase.

### Baseline Branch

- narrow `2330/0050 + TAIEX` baseline
- config: `configs/settings.example.toml`
- retained as a failure-case / diagnostic reference

### TWSE Cross-Sectional Branch

- top-50 liquidity universe
- monthly cross-sectional signal panel
- dynamic portfolio weights
- branch-specific backtest / walk-forward / diagnostics
- operational daily decision + paper-trading scaffold

This is the main forward branch.

## Data Flow

### Research Data Flow

```text
TWSE official stock market data + stable TAIEX benchmark history
    ->
raw cache
    ->
normalized daily bar CSVs
    ->
monthly universe membership
    ->
monthly signal panel
    ->
target weights
    ->
daily NAV simulation
    ->
reports + diagnostics
```

### Operational Data Flow

```text
practical-mainline config + existing local artifacts
    ->
daily decision snapshot
    ->
scheduled paper execution
    ->
trade blotter
    ->
latest portfolio state
    ->
paper NAV history
```

## Mainline Strategy Positioning

The repository distinguishes between:

- the pure-alpha benchmark line
- the practical operational mainline
- a few compact supporting comparison rows

Current interpretation:

- `original_monthly`
  - pure-alpha benchmark
- `risk_controlled_3m_half_exposure_exp60_delay1`
  - practical operational mainline
- `risk_controlled_3m_half_exposure_exp60`
  - no-extra-delay reference
- `risk_controlled_3m_half_exposure_exp60_delay3`
  - robustness-confirmation line
- `risk_controlled_3m_half_exposure_exp60_w08`
  - conservative appendix

The architecture supports these comparisons without turning the codebase into a general tuning framework.

## Module Boundaries

### `config.py`

Loads typed settings from TOML and validates:

- research branch selection
- cost settings
- risk-control settings
- paper-trading settings

### `core/models.py`

Defines the shared dataclasses used across:

- ingestion
- signal generation
- backtest
- walk-forward
- diagnostics
- decision outputs
- paper-trading outputs

### `data/`

Responsible for:

- provider access
- raw payload handling
- daily-bar normalization
- local CSV persistence
- local dataset loading

Important design choice:

- the stable TWSE historical ingest path and the stable benchmark-history path are kept separate from strategy logic

### `universe/`

Responsible for:

- TWSE candidate filtering
- liquidity-based membership construction

### `signals/`

Responsible for:

- signal calculation
- persisted signal-panel loading

### `portfolio/`

Responsible for:

- converting signal rows into target portfolio weights

### `backtest/`

Responsible for:

- historical simulation
- metrics
- walk-forward splitting and aggregation
- compact comparison artifact generation

Important design choice:

- historical backtest remains a research engine
- it is not rewritten as a broker-style execution simulator

### `diagnostics/`

Responsible for:

- artifact-based post-run interpretation
- yearly tables
- walk-forward diagnostics
- symbol exposure summaries
- signal diagnostics

### `reporting/`

Responsible for:

- markdown summaries
- SVG equity-curve and drawdown charts

### `execution/`

Responsible for:

- daily decision generation
- paper-trading ledger maintenance

Important design choice:

- one explicit execution convention
- no broker integration
- no generalized live orchestration engine

### `pipelines/`

Responsible for:

- thin orchestration behind CLI commands

The pipelines wire modules together but do not hold strategy logic.

## Phase H Operational Layer

The operational layer is intentionally thin.

It adds:

- `decision`
  - latest daily actionable portfolio decision
- `paper`
  - file-based paper-trading ledger update

The paper layer uses one explicit convention:

- decision after close
- execution at the next valid open after the configured delay
- current practical mainline delay = 1 extra benchmark trading day

Guardrails remain simple and explicit:

- block when required benchmark data is missing
- block when signals / membership artifacts are stale
- block when current market data is incomplete for target names
- constrain buys to preserve non-negative cash

## Why This Architecture Works

Each stage produces a concrete artifact for the next stage:

- ingest produces normalized bars
- signals produce persisted signal panels
- portfolio logic produces target weights
- backtest produces NAV and weight histories
- walk-forward produces OOS NAV and window summaries
- diagnostics produce explanatory tables
- decision / paper produce operational artifacts

That keeps the system:

- reviewable
- testable
- extensible

without introducing unnecessary abstraction too early.

## Intentional Limits

This architecture still does not include:

- broker connectivity
- board-lot sizing
- partial fills
- multiple execution models
- database-backed storage
- service-oriented deployment
- plugin-style strategy frameworks
- ML optimization layers

Those are future choices, not current requirements.

## Recommended Reading Order

1. [`../README.md`](../README.md)
2. [`project_overview.md`](project_overview.md)
3. [`../configs/tw_top50_liquidity.example.toml`](../configs/tw_top50_liquidity.example.toml)
4. [`../src/tw_quant/cli.py`](../src/tw_quant/cli.py)
5. [`../src/tw_quant/execution/paper.py`](../src/tw_quant/execution/paper.py)
