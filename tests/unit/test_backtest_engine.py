from __future__ import annotations

import csv
from pathlib import Path
import sys
import tempfile
import textwrap
import unittest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from tw_quant.backtest.run import run_backtest
from tw_quant.config import load_backtest_settings


class BacktestEngineTests(unittest.TestCase):
    def test_run_backtest_uses_shifted_weights_without_lookahead(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            processed_root = temp_root / "artifacts" / "processed"
            _write_normalized_csv(
                processed_root / "market_data" / "daily" / "2330.csv",
                [
                    ["2024-01-02", "2330", "100", "100", "100", "100", "1000"],
                    ["2024-01-03", "2330", "110", "110", "110", "110", "1000"],
                    ["2024-01-04", "2330", "121", "121", "121", "121", "1000"],
                ],
            )
            _write_normalized_csv(
                processed_root / "market_data" / "daily" / "0050.csv",
                [
                    ["2024-01-02", "0050", "100", "100", "100", "100", "1000"],
                    ["2024-01-03", "0050", "90", "90", "90", "90", "1000"],
                    ["2024-01-04", "0050", "99", "99", "99", "99", "1000"],
                ],
            )
            _write_normalized_csv(
                processed_root / "market_data" / "daily" / "TAIEX.csv",
                [
                    ["2024-01-02", "TAIEX", "18000", "18000", "18000", "18000", ""],
                    ["2024-01-03", "TAIEX", "18180", "18180", "18180", "18180", ""],
                    ["2024-01-04", "TAIEX", "18361.8", "18361.8", "18361.8", "18361.8", ""],
                ],
            )
            _write_signal_csv(
                processed_root / "signals" / "daily" / "signal_panel.csv",
                [
                    ["2024-01-02", "2330", "100", "", "", "0", "", "0", "", "0", "1.0"],
                    ["2024-01-02", "0050", "100", "", "", "0", "", "0", "", "0", "0.0"],
                    ["2024-01-03", "2330", "110", "", "", "0", "", "0", "", "0", "0.0"],
                    ["2024-01-03", "0050", "90", "", "", "0", "", "0", "", "0", "1.0"],
                    ["2024-01-04", "2330", "121", "", "", "0", "", "0", "", "0", "0.0"],
                    ["2024-01-04", "0050", "99", "", "", "0", "", "0", "", "0", "1.0"],
                ],
            )

            config_path = temp_root / "settings.toml"
            config_path.write_text(
                textwrap.dedent(
                    """
                    project_name = "deterministic_backtest"
                    market = "TW cash equities"
                    universe = "unit_test_universe"
                    benchmark = "TAIEX"
                    start_date = "2024-01-02"
                    end_date = "2024-01-04"

                    [paths]
                    project_root = "."
                    raw = "artifacts/raw"
                    processed = "artifacts/processed"
                    reports = "artifacts/reports"

                    [costs]
                    commission_bps = 0.0
                    tax_bps = 0.0
                    slippage_bps = 0.0

                    [ingest]
                    provider = "finmind"
                    symbols = ["2330", "0050"]
                    refresh = false
                    storage_format = "csv"
                    token_env_var = "FINMIND_API_TOKEN"
                    raw_cache_subdir = "finmind"
                    normalized_subdir = "market_data/daily"

                    [signals]
                    enabled_symbols = ["2330", "0050"]
                    benchmark = "TAIEX"
                    ma_fast_window = 2
                    ma_slow_window = 3
                    momentum_window = 2
                    volatility_window = 2
                    volatility_cap = 5.0
                    align_by_date = true
                    input_subdir = "market_data/daily"
                    output_subdir = "signals/daily"
                    output_file = "signal_panel.csv"

                    [portfolio]
                    tradable_symbols = ["2330", "0050"]
                    benchmark = "TAIEX"
                    rebalance_frequency = "daily"
                    weighting = "equal"
                    min_signal_score = 0.0
                    max_positions = 1
                    max_weight = 1.0
                    hold_cash_when_inactive = true

                    [backtest]
                    initial_nav = 1.0
                    bar_input_subdir = "market_data/daily"
                    signal_input_subdir = "signals/daily"
                    signal_input_file = "signal_panel.csv"
                    output_subdir = "backtests"
                    nav_file = "daily_nav.csv"
                    weights_file = "daily_weights.csv"
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            result = run_backtest(load_backtest_settings(config_path))

            self.assertAlmostEqual(result.final_nav, 1.21)
            self.assertAlmostEqual(result.metrics.cumulative_return, 0.21)
            self.assertAlmostEqual(result.metrics.turnover, 1.5)
            self.assertTrue(result.nav_path.exists())
            self.assertTrue(result.weights_path.exists())
            self.assertTrue(result.report_path.exists())
            self.assertTrue(result.equity_curve_path.exists())
            self.assertTrue(result.drawdown_path.exists())

            with result.nav_path.open("r", newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(rows[-1]["nav"], "1.2100000000000002")
            report_text = result.report_path.read_text(encoding="utf-8")
            self.assertIn("## Charts", report_text)
            self.assertIn("## Strategy Logic", report_text)


def _write_normalized_csv(path: Path, rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "date,symbol,open,high,low,close,volume\n" + "\n".join(",".join(row) for row in rows) + "\n",
        encoding="utf-8",
    )


def _write_signal_csv(path: Path, rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "date,symbol,close,ma_fast,ma_slow,trend_signal,momentum_n,momentum_signal,volatility_n,volatility_filter,signal_score\n"
        + "\n".join(",".join(row) for row in rows)
        + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    unittest.main()
