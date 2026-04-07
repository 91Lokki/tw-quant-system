# tw_quant

`tw_quant` is a local, artifact-driven quantitative research system for Taiwan equities.

The repository now has two clearly separated roles:

- a research stack for ingestion, signals, backtest, walk-forward, diagnostics, and reporting
- a thin operational stack for daily decision generation and paper-trading rehearsal

The current practical mainline is:

- `risk_controlled_3m_half_exposure_exp60_delay1`

The current pure research benchmark line is:

- `original_monthly`

This project is intentionally end-of-day, file-based, and explainable. It is not a broker-connected live trading system.

## Current Focus

The main forward research branch is:

- `configs/tw_top50_liquidity.example.toml`

It keeps the same core strategy identity:

- TWSE common-stock universe
- top-50 liquidity membership
- volatility-adjusted momentum ranking
- long-only portfolio construction
- benchmark regime filter on `TAIEX`
- 3-month rebalance cadence
- half-exposure defensive mode
- 60% defensive gross exposure
- 1 extra execution-delay day in the practical operational mainline

The original narrow baseline is still retained in:

- `configs/settings.example.toml`

That branch remains useful as a small baseline / failure-case reference, but it is not the recommended forward line.

## What The Repository Does

At a high level, the pipeline is:

```text
ingest
  -> normalized daily bars
  -> signals
  -> portfolio weights
  -> backtest
  -> walk-forward
  -> diagnostics
  -> markdown reports + charts
  -> daily decision + paper-trading ledger
```

The cross-sectional branch uses:

- TWSE official daily market data for stock history
- the stable working TAIEX benchmark-history path
- monthly top-50 liquidity universe membership
- monthly cross-sectional signal generation

## Main Commands

Sync the environment first:

```bash
uv sync
```

Show CLI help:

```bash
uv run python -m tw_quant --help
```

### Recommended Mainline Workflow

Use this for the current TWSE practical line:

```bash
uv run python -m tw_quant ingest --config configs/tw_top50_liquidity.example.toml --refresh
uv run python -m tw_quant signals --config configs/tw_top50_liquidity.example.toml
uv run python -m tw_quant backtest --config configs/tw_top50_liquidity.example.toml
uv run python -m tw_quant walkforward --config configs/tw_top50_liquidity.example.toml
uv run python -m tw_quant diagnostics --config configs/tw_top50_liquidity.example.toml
uv run python -m tw_quant decision --config configs/tw_top50_liquidity.example.toml
uv run python -m tw_quant paper --config configs/tw_top50_liquidity.example.toml
```

Optional `as-of` usage for the operational layer:

```bash
uv run python -m tw_quant decision --config configs/tw_top50_liquidity.example.toml --as-of 2026-03-10
uv run python -m tw_quant paper --config configs/tw_top50_liquidity.example.toml --as-of 2026-03-10
```

### Baseline Workflow

Use this only for the original narrow baseline branch:

```bash
uv run python -m tw_quant ingest --config configs/settings.example.toml --refresh
uv run python -m tw_quant signals --config configs/settings.example.toml
uv run python -m tw_quant backtest --config configs/settings.example.toml
uv run python -m tw_quant walkforward --config configs/settings.example.toml
uv run python -m tw_quant diagnostics --config configs/settings.example.toml
```

## Practical Mainline And Comparison Surface

The repo now distinguishes between:

- the practical operational mainline used by `decision` and `paper`
- the historical comparison surface used by backtest and walk-forward reports

### Operational Mainline

- `risk_controlled_3m_half_exposure_exp60_delay1`

This is the current file-based paper-trading line.

### Default Comparison Rows

The compact comparison artifacts are centered on:

- `original_monthly`
- `risk_controlled_3m_half_exposure_exp60_delay1`
- `risk_controlled_3m_half_exposure_exp60`
- `risk_controlled_3m_half_exposure_exp60_delay3`
- `risk_controlled_3m_half_exposure_exp60_w08`

Interpretation:

- `original_monthly`: pure-alpha benchmark
- `risk_controlled_3m_half_exposure_exp60_delay1`: practical candidate / operational mainline
- `risk_controlled_3m_half_exposure_exp60`: no-extra-delay reference
- `risk_controlled_3m_half_exposure_exp60_delay3`: slower-execution robustness confirmation
- `risk_controlled_3m_half_exposure_exp60_w08`: conservative concentration-control appendix

## Phase H Operational Layer

Phase H adds a narrow operational scaffold around the practical mainline.

It is designed to answer:

- what is today’s target portfolio?
- do I need to rebalance?
- when should the rebalance take effect?
- what would the paper portfolio and NAV look like if I follow the current operational rule set?

### Daily Decision Output

The `decision` command generates a daily decision snapshot with:

- decision date
- strategy identifier
- benchmark regime state
- rebalance-required status
- execution delay
- scheduled execution date
- target symbols
- target weights
- target cash weight
- trade list versus prior holdings

Primary artifact:

- `data/processed/paper_trading/<project>/daily_decision/latest.json`

### Paper-Trading Ledger

The `paper` command maintains a persistent paper-trading ledger with:

- decision snapshots
- trade blotter
- latest portfolio state
- NAV history

Primary artifacts:

- `data/processed/paper_trading/<project>/paper_trade_blotter.csv`
- `data/processed/paper_trading/<project>/paper_portfolio_state.csv`
- `data/processed/paper_trading/<project>/paper_nav_history.csv`

### Execution Convention

Phase H intentionally uses exactly one simple execution rule:

- the decision is formed after the close
- execution happens at the next valid open after the configured delay
- the practical mainline uses `execution_delay_days = 1`

This rule is for the operational paper-trading layer only.
It does not turn the historical backtest into a full execution simulator.

### Operational Guardrails

The paper-trading layer stays narrow and explicit.
It will block or constrain trading when required inputs are not safe enough.

Examples:

- missing benchmark data
- missing current market data for a target symbol
- stale signal / membership artifacts
- target weights violating hard portfolio limits
- insufficient cash after transaction costs

Cash handling is deterministic and auditable:

- paper cash is not allowed to go negative
- if buy orders would breach cash after estimated costs, they are scaled down proportionally
- the scaling note is written to the blotter, NAV history, and paper state artifacts

## Key Output Paths

### Research Artifacts

- normalized daily bars:
  - `data/processed/market_data/daily/`
- signal panels:
  - `data/processed/signals/daily/`
  - `data/processed/signals/monthly/`
- backtests:
  - `data/processed/backtests/<project>/`
- reports:
  - `data/processed/reports/<project>/`

### Useful Files To Inspect First

For the mainline research branch:

- `data/processed/backtests/tw_top50_liquidity_v1/risk_comparison.csv`
- `data/processed/backtests/tw_top50_liquidity_v1/walkforward/risk_comparison.csv`
- `data/processed/reports/tw_top50_liquidity_v1/backtest_summary.md`
- `data/processed/reports/tw_top50_liquidity_v1/walkforward/walkforward_summary.md`
- `data/processed/reports/tw_top50_liquidity_v1/diagnostics/diagnostics_summary.md`

For the Phase H operational layer:

- `data/processed/paper_trading/tw_top50_liquidity_v1/daily_decision/latest.json`
- `data/processed/paper_trading/tw_top50_liquidity_v1/paper_trade_blotter.csv`
- `data/processed/paper_trading/tw_top50_liquidity_v1/paper_portfolio_state.csv`
- `data/processed/paper_trading/tw_top50_liquidity_v1/paper_nav_history.csv`

## Repository Structure

Main modules:

- `src/tw_quant/data/`
  - provider access, normalization, storage, loading
- `src/tw_quant/universe/`
  - TWSE liquidity-universe construction
- `src/tw_quant/signals/`
  - daily and cross-sectional signal generation / loading
- `src/tw_quant/portfolio/`
  - target weight construction
- `src/tw_quant/backtest/`
  - historical simulation, metrics, walk-forward
- `src/tw_quant/diagnostics/`
  - artifact-based post-run analysis
- `src/tw_quant/reporting/`
  - markdown summaries and SVG charts
- `src/tw_quant/execution/`
  - daily decision and paper-trading helpers
- `src/tw_quant/pipelines/`
  - thin CLI-facing orchestration layer
- `app/`
  - local Streamlit artifact browser

## Configuration

The main production-like example config is:

- [`configs/tw_top50_liquidity.example.toml`](configs/tw_top50_liquidity.example.toml)

It currently defaults to:

- `benchmark_filter_enabled = true`
- `benchmark_ma_window = 200`
- `defensive_mode = "half_exposure"`
- `defensive_gross_exposure = 0.6`
- `execution_delay_days = 1`
- `rebalance_cadence_months = 3`

## Testing

Run the test suite with:

```bash
uv run pytest
```

This covers:

- config loading and validation
- provider parsing
- signal / backtest behavior
- walk-forward behavior
- diagnostics compatibility
- daily decision generation
- paper-trading ledger behavior

## What Is Intentionally Not Included

This repository deliberately does not try to do everything.

Still out of scope:

- new alpha-family expansion in the operational layer
- broker API integration
- live order routing
- board-lot sizing
- partial fills
- multiple execution models
- generic optimization frameworks
- ML-based modeling
- dashboard-heavy productization

## Optional Demo App

There is also a local Streamlit app for browsing existing artifacts:

```bash
uv run python -m streamlit run app/streamlit_app.py
```

The app reads local outputs only.
It does not rerun ingestion, signals, or backtests.

## Recommended Companion Docs

- [`docs/project_overview.md`](docs/project_overview.md)
- [`docs/architecture.md`](docs/architecture.md)
- [`configs/tw_top50_liquidity.example.toml`](configs/tw_top50_liquidity.example.toml)
- [`src/tw_quant/cli.py`](src/tw_quant/cli.py)

## Disclaimer

This repository is a software engineering and quantitative research project.
It is not investment advice, and it is not a broker-connected live trading system.
