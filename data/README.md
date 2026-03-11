# Data Layout

This directory is reserved for local datasets and generated artifacts.

- `raw/` stores unmodified source files or vendor downloads
- `processed/` stores cleaned datasets, features, normalized bars, and generated reports

## Ingestion Layout

The v1 Taiwan equities ingestion flow uses:

- `raw/finmind/<dataset>/...json` for raw FinMind API payloads
- `processed/market_data/daily/<symbol>.csv` for normalized daily bars

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

The scaffold keeps data local and file-based. A database layer is intentionally deferred until the project has real ingestion and backtest requirements.
