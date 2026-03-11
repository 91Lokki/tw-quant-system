# Data Layout

This directory is reserved for local datasets and generated artifacts.

- `raw/` stores unmodified source files or vendor downloads
- `processed/` stores cleaned datasets, features, normalized bars, and generated reports

## Ingestion Layout

The v1 Taiwan equities ingestion flow uses:

- `raw/finmind/<dataset>/...json` for raw FinMind API payloads
- `processed/market_data/daily/<symbol>.csv` for normalized daily bars
- `processed/signals/daily/signal_panel.csv` for the combined daily signal dataset
- `processed/backtests/<project_name>/daily_nav.csv` for the daily equity curve
- `processed/backtests/<project_name>/daily_weights.csv` for daily applied portfolio weights
- `processed/reports/<project_name>/backtest_summary.md` for the markdown summary report
- `processed/reports/<project_name>/equity_curve.svg` for the portfolio-vs-benchmark equity chart
- `processed/reports/<project_name>/drawdown.svg` for the drawdown chart

## Normalized Schema

The normalized daily bar schema is intentionally small and explicit:

- `date`
- `symbol`
- `open`
- `high`
- `low`
- `close`
- `volume`

For `TAIEX`, the current FinMind benchmark source is `TaiwanStockTotalReturnIndex`, which exposes a daily `price` series rather than full OHLCV bars. In the normalized CSV:

- `open`, `high`, `low`, and `close` are all mapped from `price`
- `volume` is left empty

This keeps the benchmark usable for future comparison and reporting while documenting the source limitation clearly.

## Signal Output Schema

The first real signal layer stores a combined CSV panel under `processed/signals/daily/` with these columns:

- `date`
- `symbol`
- `close`
- `ma_fast`
- `ma_slow`
- `trend_signal`
- `momentum_n`
- `momentum_signal`
- `volatility_n`
- `volatility_filter`
- `signal_score`

Signal rows are generated from locally cached normalized bars. The loader can align requested symbols by the shared trading-date intersection before signals are computed.

## Backtest Output Schema

The current backtest layer writes:

- a NAV dataset with `date,nav,daily_return,gross_return,benchmark_nav,benchmark_return,turnover,transaction_cost,cash_weight`
- a weights dataset with `date,symbol,weight,signal_score`

The project keeps data local and file-based. A database layer is intentionally deferred until the workflow complexity clearly justifies it.
