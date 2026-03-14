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
        self.assertEqual(config.research_branch, "baseline_failure_case")
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
        self.assertEqual(config.universe_config.candidate_market, "twse")
        self.assertEqual(config.universe_config.top_n, 50)
        self.assertEqual(config.signals.enabled_symbols, ("2330", "0050"))
        self.assertEqual(config.signals.mode, "time_series_baseline")
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
        self.assertFalse(config.risk_controls.benchmark_filter_enabled)
        self.assertEqual(config.risk_controls.benchmark_ma_window, 200)
        self.assertEqual(config.risk_controls.rebalance_cadence_months, 1)
        self.assertEqual(config.risk_controls.defensive_mode, "cash")
        self.assertEqual(config.backtest.initial_nav, 1.0)
        self.assertEqual(config.backtest.bar_input_dir, PROJECT_ROOT / "data" / "processed" / "market_data" / "daily")
        self.assertEqual(config.backtest.signal_input_path, PROJECT_ROOT / "data" / "processed" / "signals" / "daily" / "signal_panel.csv")
        self.assertEqual(config.backtest.output_dir, PROJECT_ROOT / "data" / "processed" / "backtests")
        self.assertTrue(config.walkforward.enabled)
        self.assertEqual(config.walkforward.window_type, "expanding")
        self.assertEqual(config.walkforward.train_window_days, 252)
        self.assertEqual(config.walkforward.test_window_days, 63)
        self.assertEqual(config.walkforward.minimum_history_days, 252)

    def test_load_settings_parses_cross_sectional_config(self) -> None:
        config = load_settings(PROJECT_ROOT / "configs" / "tw_top50_liquidity.example.toml")

        self.assertEqual(config.project_name, "tw_top50_liquidity_v1")
        self.assertEqual(config.research_branch, "tw_top50_liquidity_cross_sectional")
        self.assertEqual(config.ingest.provider, "twse")
        self.assertEqual(config.ingest.symbols, ())
        self.assertEqual(config.ingest.raw_cache_dir, PROJECT_ROOT / "data" / "raw" / "twse")
        self.assertEqual(config.universe_config.liquidity_lookback_days, 60)
        self.assertEqual(config.universe_config.top_n, 50)
        self.assertEqual(
            config.universe_config.usable_metadata_path,
            PROJECT_ROOT / "data" / "processed" / "metadata" / "twse_usable_stock_info.csv",
        )
        self.assertEqual(
            config.universe_config.availability_path,
            PROJECT_ROOT / "data" / "processed" / "metadata" / "twse_price_availability.csv",
        )
        self.assertEqual(config.signals.mode, "cross_sectional_vol_adj_momentum")
        self.assertEqual(config.signals.output_dir, PROJECT_ROOT / "data" / "processed" / "signals" / "monthly")
        self.assertEqual(config.signals.output_file, "cross_sectional_signal_panel.csv")
        self.assertEqual(config.portfolio.tradable_symbols, ())
        self.assertTrue(config.risk_controls.benchmark_filter_enabled)
        self.assertEqual(config.risk_controls.benchmark_ma_window, 200)
        self.assertEqual(config.risk_controls.defensive_mode, "half_exposure")
        self.assertEqual(config.risk_controls.defensive_gross_exposure, 0.6)
        self.assertEqual(config.risk_controls.execution_delay_days, 0)
        self.assertEqual(config.risk_controls.rebalance_cadence_months, 3)

    def test_load_settings_rejects_invalid_defensive_mode(self) -> None:
        config_path = PROJECT_ROOT / "configs" / "tw_top50_liquidity.example.toml"
        bad_text = config_path.read_text(encoding="utf-8").replace(
            'defensive_mode = "half_exposure"',
            'defensive_mode = "invalid_mode"',
            1,
        )

        temp_path = PROJECT_ROOT / "configs" / ".tmp_invalid_defensive_mode.toml"
        try:
            temp_path.write_text(bad_text, encoding="utf-8")
            with self.assertRaisesRegex(
                ValueError,
                "risk_controls.defensive_mode must be one of cash, half_exposure, top5",
            ):
                load_settings(temp_path)
        finally:
            if temp_path.exists():
                temp_path.unlink()

    def test_load_settings_rejects_invalid_defensive_gross_exposure(self) -> None:
        config_path = PROJECT_ROOT / "configs" / "tw_top50_liquidity.example.toml"
        bad_text = config_path.read_text(encoding="utf-8").replace(
            "defensive_gross_exposure = 0.6",
            "defensive_gross_exposure = 1.2",
            1,
        )

        temp_path = PROJECT_ROOT / "configs" / ".tmp_invalid_defensive_gross_exposure.toml"
        try:
            temp_path.write_text(bad_text, encoding="utf-8")
            with self.assertRaisesRegex(
                ValueError,
                "risk_controls.defensive_gross_exposure must be within \\(0, 1\\]",
            ):
                load_settings(temp_path)
        finally:
            if temp_path.exists():
                temp_path.unlink()

    def test_load_settings_rejects_negative_execution_delay_days(self) -> None:
        config_path = PROJECT_ROOT / "configs" / "tw_top50_liquidity.example.toml"
        bad_text = config_path.read_text(encoding="utf-8").replace(
            "execution_delay_days = 0",
            "execution_delay_days = -1",
            1,
        )

        temp_path = PROJECT_ROOT / "configs" / ".tmp_invalid_execution_delay.toml"
        try:
            temp_path.write_text(bad_text, encoding="utf-8")
            with self.assertRaisesRegex(
                ValueError,
                "risk_controls.execution_delay_days must be non-negative",
            ):
                load_settings(temp_path)
        finally:
            if temp_path.exists():
                temp_path.unlink()


if __name__ == "__main__":
    unittest.main()
