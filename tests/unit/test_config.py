from __future__ import annotations

from pathlib import Path
import sys
import unittest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from tw_quant.config import load_settings


class LoadSettingsTests(unittest.TestCase):
    def test_load_settings_parses_example_config(self) -> None:
        config = load_settings(PROJECT_ROOT / "configs" / "settings.example.toml")

        self.assertEqual(config.project_name, "tw_quant_v1")
        self.assertEqual(config.market, "TW cash equities")
        self.assertEqual(config.benchmark, "TAIEX")
        self.assertEqual(config.data_paths.project_root, PROJECT_ROOT)
        self.assertEqual(config.data_paths.raw_dir, PROJECT_ROOT / "data" / "raw")
        self.assertEqual(config.data_paths.reports_dir, PROJECT_ROOT / "data" / "processed" / "reports")
        self.assertAlmostEqual(config.trading_costs.commission_bps, 14.25)
        self.assertEqual(config.ingest.provider, "finmind")
        self.assertEqual(config.ingest.symbols, ("2330", "0050"))
        self.assertEqual(config.ingest.benchmark, "TAIEX")
        self.assertEqual(config.ingest.storage_format, "csv")
        self.assertEqual(config.ingest.raw_cache_dir, PROJECT_ROOT / "data" / "raw" / "finmind")
        self.assertEqual(
            config.ingest.normalized_dir,
            PROJECT_ROOT / "data" / "processed" / "market_data" / "daily",
        )
        self.assertEqual(config.signals.enabled_symbols, ("2330", "0050"))
        self.assertEqual(config.signals.benchmark, "TAIEX")
        self.assertEqual(config.signals.ma_fast_window, 20)
        self.assertEqual(config.signals.ma_slow_window, 60)
        self.assertEqual(config.signals.output_dir, PROJECT_ROOT / "data" / "processed" / "signals" / "daily")
        self.assertEqual(config.signals.output_file, "signal_panel.csv")
        self.assertEqual(config.portfolio.tradable_symbols, ("2330", "0050"))
        self.assertEqual(config.portfolio.benchmark, "TAIEX")
        self.assertEqual(config.portfolio.rebalance_frequency, "monthly")
        self.assertEqual(config.portfolio.weighting, "equal")
        self.assertTrue(config.portfolio.hold_cash_when_inactive)
        self.assertEqual(config.backtest.initial_nav, 1.0)
        self.assertEqual(config.backtest.bar_input_dir, PROJECT_ROOT / "data" / "processed" / "market_data" / "daily")
        self.assertEqual(config.backtest.signal_input_path, PROJECT_ROOT / "data" / "processed" / "signals" / "daily" / "signal_panel.csv")
        self.assertEqual(config.backtest.output_dir, PROJECT_ROOT / "data" / "processed" / "backtests")


if __name__ == "__main__":
    unittest.main()
