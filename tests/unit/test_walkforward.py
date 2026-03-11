from __future__ import annotations

import csv
from datetime import timedelta
from pathlib import Path
import sys
import tempfile
import textwrap
import unittest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from tw_quant.backtest.walkforward import build_walkforward_windows, run_walkforward
from tw_quant.config import load_backtest_settings


class WalkForwardTests(unittest.TestCase):
    def test_build_walkforward_windows_supports_expanding_and_rolling(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = _write_settings(Path(temp_dir), window_type="expanding")
            config = load_backtest_settings(config_path)
            trading_dates = tuple(config.start_date + timedelta(days=offset) for offset in range(10))

            expanding_windows = build_walkforward_windows(trading_dates, config)

            self.assertEqual(len(expanding_windows), 3)
            self.assertEqual(expanding_windows[0].train_size, 4)
            self.assertEqual(expanding_windows[0].test_size, 2)
            self.assertEqual(expanding_windows[1].train_size, 6)
            self.assertLess(expanding_windows[0].train_end, expanding_windows[0].test_start)

            rolling_config_path = _write_settings(Path(temp_dir), project_name="rolling_demo", window_type="rolling")
            rolling_config = load_backtest_settings(rolling_config_path)

            rolling_windows = build_walkforward_windows(trading_dates, rolling_config)

            self.assertEqual(len(rolling_windows), 3)
            self.assertEqual(rolling_windows[0].train_size, 4)
            self.assertEqual(rolling_windows[1].train_size, 4)
            self.assertEqual(rolling_windows[1].train_start, trading_dates[2])
            self.assertEqual(rolling_windows[1].test_start, trading_dates[6])

    def test_run_walkforward_writes_combined_oos_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            processed_root = temp_root / "artifacts" / "processed"
            _write_normalized_csv(
                processed_root / "market_data" / "daily" / "2330.csv",
                [
                    ["2024-01-02", "2330", "100", "100", "100", "100", "1000"],
                    ["2024-01-03", "2330", "110", "110", "110", "110", "1000"],
                    ["2024-01-04", "2330", "121", "121", "121", "121", "1000"],
                    ["2024-01-05", "2330", "133.1", "133.1", "133.1", "133.1", "1000"],
                    ["2024-01-06", "2330", "146.41", "146.41", "146.41", "146.41", "1000"],
                    ["2024-01-07", "2330", "161.051", "161.051", "161.051", "161.051", "1000"],
                    ["2024-01-08", "2330", "177.1561", "177.1561", "177.1561", "177.1561", "1000"],
                ],
            )
            _write_normalized_csv(
                processed_root / "market_data" / "daily" / "0050.csv",
                [
                    ["2024-01-02", "0050", "100", "100", "100", "100", "1000"],
                    ["2024-01-03", "0050", "100", "100", "100", "100", "1000"],
                    ["2024-01-04", "0050", "100", "100", "100", "100", "1000"],
                    ["2024-01-05", "0050", "100", "100", "100", "100", "1000"],
                    ["2024-01-06", "0050", "100", "100", "100", "100", "1000"],
                    ["2024-01-07", "0050", "100", "100", "100", "100", "1000"],
                    ["2024-01-08", "0050", "100", "100", "100", "100", "1000"],
                ],
            )
            _write_normalized_csv(
                processed_root / "market_data" / "daily" / "TAIEX.csv",
                [
                    ["2024-01-02", "TAIEX", "18000", "18000", "18000", "18000", ""],
                    ["2024-01-03", "TAIEX", "18100", "18100", "18100", "18100", ""],
                    ["2024-01-04", "TAIEX", "18200", "18200", "18200", "18200", ""],
                    ["2024-01-05", "TAIEX", "18300", "18300", "18300", "18300", ""],
                    ["2024-01-06", "TAIEX", "18400", "18400", "18400", "18400", ""],
                    ["2024-01-07", "TAIEX", "18500", "18500", "18500", "18500", ""],
                    ["2024-01-08", "TAIEX", "18600", "18600", "18600", "18600", ""],
                ],
            )
            _write_signal_csv(
                processed_root / "signals" / "daily" / "signal_panel.csv",
                [
                    ["2024-01-02", "2330", "100", "", "", "0", "", "0", "", "0", "1.0"],
                    ["2024-01-02", "0050", "100", "", "", "0", "", "0", "", "0", "0.0"],
                    ["2024-01-03", "2330", "110", "", "", "0", "", "0", "", "0", "1.0"],
                    ["2024-01-03", "0050", "100", "", "", "0", "", "0", "", "0", "0.0"],
                    ["2024-01-04", "2330", "121", "", "", "0", "", "0", "", "0", "1.0"],
                    ["2024-01-04", "0050", "100", "", "", "0", "", "0", "", "0", "0.0"],
                    ["2024-01-05", "2330", "133.1", "", "", "0", "", "0", "", "0", "1.0"],
                    ["2024-01-05", "0050", "100", "", "", "0", "", "0", "", "0", "0.0"],
                    ["2024-01-06", "2330", "146.41", "", "", "0", "", "0", "", "0", "1.0"],
                    ["2024-01-06", "0050", "100", "", "", "0", "", "0", "", "0", "0.0"],
                    ["2024-01-07", "2330", "161.051", "", "", "0", "", "0", "", "0", "1.0"],
                    ["2024-01-07", "0050", "100", "", "", "0", "", "0", "", "0", "0.0"],
                    ["2024-01-08", "2330", "177.1561", "", "", "0", "", "0", "", "0", "1.0"],
                    ["2024-01-08", "0050", "100", "", "", "0", "", "0", "", "0", "0.0"],
                ],
            )

            config_path = _write_settings(temp_root, train_window_days=3, minimum_history_days=3)
            result = run_walkforward(load_backtest_settings(config_path))

            self.assertEqual(result.window_count, 2)
            self.assertAlmostEqual(result.final_nav, 1.21)
            self.assertTrue(result.nav_path.exists())
            self.assertTrue(result.window_summary_path.exists())
            self.assertTrue(result.report_path.exists())

            with result.nav_path.open("r", newline="", encoding="utf-8") as handle:
                nav_rows = list(csv.DictReader(handle))
            self.assertEqual(nav_rows[0]["date"], "2024-01-05")
            self.assertEqual(nav_rows[-1]["date"], "2024-01-08")
            self.assertEqual(len(nav_rows), 4)

            with result.window_summary_path.open("r", newline="", encoding="utf-8") as handle:
                window_rows = list(csv.DictReader(handle))
            self.assertEqual(len(window_rows), 2)
            self.assertEqual(window_rows[0]["train_end"], "2024-01-04")
            self.assertEqual(window_rows[0]["test_start"], "2024-01-05")

            report_text = result.report_path.read_text(encoding="utf-8")
            self.assertIn("# wf_demo Walk-Forward Summary", report_text)
            self.assertIn("- Window Type: expanding", report_text)
            self.assertIn("- Windows Evaluated: 2", report_text)
            self.assertIn("| Window | Train Range | Test Range |", report_text)


def _write_settings(
    temp_root: Path,
    project_name: str = "wf_demo",
    window_type: str = "expanding",
    train_window_days: int = 4,
    minimum_history_days: int = 4,
) -> Path:
    config_path = temp_root / f"{project_name}.toml"
    config_path.write_text(
        textwrap.dedent(
            f"""
            project_name = "{project_name}"
            market = "TW cash equities"
            universe = "unit_test_universe"
            benchmark = "TAIEX"
            start_date = "2024-01-02"
            end_date = "2024-01-08"

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

            [walkforward]
            enabled = true
            window_type = "{window_type}"
            train_window_days = {train_window_days}
            test_window_days = 2
            minimum_history_days = {minimum_history_days}
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    return config_path


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
