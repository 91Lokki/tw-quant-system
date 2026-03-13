from __future__ import annotations

import csv
from dataclasses import replace
from datetime import date
import os
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
from tw_quant.backtest.walkforward import run_walkforward
from tw_quant.backtest.cross_sectional import (
    _build_defensive_target_weights_for_date,
    build_cross_sectional_variant_configs,
)
from tw_quant.config import load_backtest_settings
from tw_quant.core.models import CrossSectionalSignalRow
from tw_quant.diagnostics import run_diagnostics


class CrossSectionalBranchTests(unittest.TestCase):
    def test_defensive_half_exposure_keeps_same_ranking_with_configured_gross(self) -> None:
        config = load_backtest_settings(PROJECT_ROOT / "configs" / "tw_top50_liquidity.example.toml")
        variant_config = replace(
            config,
            portfolio=replace(config.portfolio, max_positions=2, max_weight=0.5),
            risk_controls=replace(
                config.risk_controls,
                defensive_mode="half_exposure",
                defensive_gross_exposure=0.6,
            ),
        )
        rows = (
            CrossSectionalSignalRow(
                date=_date("2024-01-02"),
                symbol="1101",
                close=100.0,
                avg_traded_value_60d=110000.0,
                liquidity_rank=1,
                momentum_126=0.2,
                volatility_20=0.1,
                signal_score=2.0,
                factor_rank=1,
                universe_name="demo",
            ),
            CrossSectionalSignalRow(
                date=_date("2024-01-02"),
                symbol="1102",
                close=100.0,
                avg_traded_value_60d=100000.0,
                liquidity_rank=2,
                momentum_126=0.15,
                volatility_20=0.1,
                signal_score=1.5,
                factor_rank=2,
                universe_name="demo",
            ),
            CrossSectionalSignalRow(
                date=_date("2024-01-02"),
                symbol="1103",
                close=100.0,
                avg_traded_value_60d=90000.0,
                liquidity_rank=3,
                momentum_126=0.05,
                volatility_20=0.1,
                signal_score=0.5,
                factor_rank=3,
                universe_name="demo",
            ),
        )

        weights = _build_defensive_target_weights_for_date(
            config=variant_config,
            participating_symbols=("1101", "1102", "1103"),
            signal_rows=rows,
        )

        self.assertEqual(weights, {"1101": 0.3, "1102": 0.3, "1103": 0.0})
        self.assertAlmostEqual(sum(weights.values()), 0.6, places=6)

    def test_defensive_top5_caps_holdings_and_uses_configured_gross(self) -> None:
        config = load_backtest_settings(PROJECT_ROOT / "configs" / "tw_top50_liquidity.example.toml")
        variant_config = replace(
            config,
            portfolio=replace(config.portfolio, max_positions=6, max_weight=0.2),
            risk_controls=replace(
                config.risk_controls,
                defensive_mode="top5",
                defensive_gross_exposure=0.6,
            ),
        )
        rows = tuple(
            CrossSectionalSignalRow(
                date=_date("2024-01-02"),
                symbol=f"11{index:02d}",
                close=100.0,
                avg_traded_value_60d=100000.0 - index,
                liquidity_rank=index + 1,
                momentum_126=0.3 - (index * 0.01),
                volatility_20=0.1,
                signal_score=3.0 - (index * 0.4),
                factor_rank=index + 1,
                universe_name="demo",
            )
            for index in range(6)
        )

        weights = _build_defensive_target_weights_for_date(
            config=variant_config,
            participating_symbols=tuple(row.symbol for row in rows),
            signal_rows=rows,
        )

        held_symbols = {symbol for symbol, weight in weights.items() if weight > 0.0}
        self.assertEqual(held_symbols, {"1100", "1101", "1102", "1103", "1104"})
        self.assertAlmostEqual(sum(weights.values()), 0.6, places=6)
        self.assertEqual(weights["1105"], 0.0)

    def test_build_cross_sectional_variant_configs_returns_fixed_phase_f_labels(self) -> None:
        config = load_backtest_settings(PROJECT_ROOT / "configs" / "tw_top50_liquidity.example.toml")

        variants = build_cross_sectional_variant_configs(config)

        self.assertEqual(
            tuple(label for label, _ in variants),
            (
                "original_monthly",
                "risk_controlled_3m_half_exposure",
                "risk_controlled_3m_half_exposure_ma150",
                "risk_controlled_3m_half_exposure_exp60",
            ),
        )

    def test_run_backtest_supports_monthly_dynamic_holdings_without_lookahead(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            config_path = _prepare_cross_sectional_artifacts(temp_root, walkforward_enabled=False)

            result = run_backtest(load_backtest_settings(config_path))

            self.assertAlmostEqual(result.final_nav, 1.4641, places=6)
            self.assertEqual(result.tradable_symbols, ("1101", "1102", "1103"))
            self.assertTrue(result.report_path.exists())

            with result.weights_path.open("r", newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))

            january_02 = [row for row in rows if row["date"] == "2024-01-02"]
            january_03 = [row for row in rows if row["date"] == "2024-01-03"]
            february_01 = [row for row in rows if row["date"] == "2024-02-01"]
            february_02 = [row for row in rows if row["date"] == "2024-02-02"]

            self.assertTrue(all(float(row["weight"]) == 0.0 for row in january_02))
            self.assertEqual(
                {row["symbol"]: float(row["weight"]) for row in january_03},
                {"1101": 0.5, "1102": 0.5, "1103": 0.0},
            )
            self.assertEqual(
                {row["symbol"]: float(row["weight"]) for row in february_01},
                {"1101": 0.5, "1102": 0.5, "1103": 0.0},
            )
            self.assertEqual(
                {row["symbol"]: float(row["weight"]) for row in february_02},
                {"1101": 0.0, "1102": 0.5, "1103": 0.5},
            )

    def test_run_walkforward_supports_cross_sectional_branch(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            config_path = _prepare_cross_sectional_artifacts(temp_root, walkforward_enabled=True)

            result = run_walkforward(load_backtest_settings(config_path))

            self.assertEqual(result.window_count, 2)
            self.assertAlmostEqual(result.final_nav, 1.1, places=6)
            self.assertTrue(result.nav_path.exists())
            self.assertTrue(result.window_summary_path.exists())
            self.assertTrue(result.report_path.exists())

            with result.nav_path.open("r", newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(rows[0]["date"], "2024-02-01")
            self.assertEqual(rows[-1]["date"], "2024-02-05")

    def test_run_backtest_applies_benchmark_regime_filter_and_cash_defense(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            config_path = _prepare_cross_sectional_artifacts(
                temp_root,
                walkforward_enabled=False,
                risk_controls_block=textwrap.dedent(
                    """
                    [risk_controls]
                    benchmark_filter_enabled = true
                    benchmark_ma_window = 3
                    defensive_mode = "cash"
                    rebalance_cadence_months = 1
                    """
                ).strip(),
            )

            result = run_backtest(load_backtest_settings(config_path))

            self.assertIsNotNone(result.comparison_path)
            assert result.comparison_path is not None
            self.assertTrue(result.comparison_path.exists())

            with result.weights_path.open("r", newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))

            january_03 = [row for row in rows if row["date"] == "2024-01-03"]
            february_01 = [row for row in rows if row["date"] == "2024-02-01"]
            february_02 = [row for row in rows if row["date"] == "2024-02-02"]

            self.assertTrue(all(float(row["weight"]) == 0.0 for row in january_03))
            self.assertTrue(all(float(row["weight"]) == 0.0 for row in february_01))
            self.assertEqual(
                {row["symbol"]: float(row["weight"]) for row in february_02},
                {"1101": 0.0, "1102": 0.5, "1103": 0.5},
            )

            comparison_text = result.comparison_path.read_text(encoding="utf-8")
            self.assertIn("original_monthly", comparison_text)
            self.assertIn("risk_controlled_3m_half_exposure", comparison_text)
            comparison_rows = list(csv.DictReader(result.comparison_path.open("r", newline="", encoding="utf-8")))
            self.assertEqual(
                [row["label"] for row in comparison_rows],
                [
                    "original_monthly",
                    "risk_controlled_3m_half_exposure",
                    "risk_controlled_3m_half_exposure_ma150",
                    "risk_controlled_3m_half_exposure_exp60",
                ],
            )
            primary_row = next(
                row for row in comparison_rows if row["label"] == "risk_controlled_3m_half_exposure"
            )
            self.assertEqual(primary_row["defensive_gross_exposure"], "0.5")

    def test_run_backtest_supports_rebalance_cadence_sensitivity(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            config_path = _prepare_cross_sectional_artifacts(
                temp_root,
                walkforward_enabled=False,
                risk_controls_block=textwrap.dedent(
                    """
                    [risk_controls]
                    benchmark_filter_enabled = false
                    benchmark_ma_window = 3
                    defensive_mode = "cash"
                    rebalance_cadence_months = 2
                    """
                ).strip(),
            )

            result = run_backtest(load_backtest_settings(config_path))

            with result.weights_path.open("r", newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))

            february_02 = [row for row in rows if row["date"] == "2024-02-02"]
            self.assertEqual(
                {row["symbol"]: float(row["weight"]) for row in february_02},
                {"1101": 0.5, "1102": 0.5, "1103": 0.0},
            )

    def test_run_walkforward_supports_cross_sectional_risk_controls(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            config_path = _prepare_cross_sectional_artifacts(
                temp_root,
                walkforward_enabled=True,
                risk_controls_block=textwrap.dedent(
                    """
                    [risk_controls]
                    benchmark_filter_enabled = true
                    benchmark_ma_window = 200
                    defensive_mode = "half_exposure"
                    defensive_gross_exposure = 0.5
                    rebalance_cadence_months = 3
                    """
                ).strip(),
            )

            result = run_walkforward(load_backtest_settings(config_path))

            self.assertTrue(result.report_path.exists())
            self.assertIsNotNone(result.comparison_path)
            assert result.comparison_path is not None
            self.assertTrue(result.comparison_path.exists())
            self.assertAlmostEqual(result.final_nav, 1.05, places=6)
            self.assertGreater(result.metrics.turnover, 0.0)

            comparison_rows = list(
                csv.DictReader(result.comparison_path.open("r", newline="", encoding="utf-8"))
            )
            primary_row = next(
                row for row in comparison_rows if row["label"] == "risk_controlled_3m_half_exposure"
            )
            self.assertAlmostEqual(float(primary_row["final_nav"]), result.final_nav, places=6)
            self.assertGreater(float(primary_row["turnover"]), 0.0)
            labels = [row["label"] for row in comparison_rows]
            self.assertEqual(
                labels,
                [
                    "original_monthly",
                    "risk_controlled_3m_half_exposure",
                    "risk_controlled_3m_half_exposure_ma150",
                    "risk_controlled_3m_half_exposure_exp60",
                ],
            )

    def test_run_walkforward_can_stay_in_cash_when_benchmark_regime_never_turns_on(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            config_path = _prepare_cross_sectional_artifacts(
                temp_root,
                walkforward_enabled=True,
                risk_controls_block=textwrap.dedent(
                    """
                    [risk_controls]
                    benchmark_filter_enabled = true
                    benchmark_ma_window = 3
                    defensive_mode = "cash"
                    rebalance_cadence_months = 3
                    """
                ).strip(),
                benchmark_rows=[
                    ["2024-01-02", "TAIEX", "100", "100", "100", "100", "", ""],
                    ["2024-01-03", "TAIEX", "99", "99", "99", "99", "", ""],
                    ["2024-01-04", "TAIEX", "98", "98", "98", "98", "", ""],
                    ["2024-02-01", "TAIEX", "97", "97", "97", "97", "", ""],
                    ["2024-02-02", "TAIEX", "96", "96", "96", "96", "", ""],
                    ["2024-02-05", "TAIEX", "95", "95", "95", "95", "", ""],
                ],
            )

            result = run_walkforward(load_backtest_settings(config_path))

            self.assertAlmostEqual(result.final_nav, 1.0, places=6)
            self.assertAlmostEqual(result.metrics.turnover, 0.0, places=6)

    def test_run_diagnostics_supports_cross_sectional_branch_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            config_path = _prepare_cross_sectional_artifacts(temp_root, walkforward_enabled=True)
            config = load_backtest_settings(config_path)

            run_backtest(config)
            run_walkforward(config)
            result = run_diagnostics(config)

            self.assertTrue(result.report_path.exists())
            self.assertTrue(result.yearly_table_path.exists())
            self.assertTrue(result.walkforward_table_path.exists())
            self.assertTrue(result.symbol_exposure_path.exists())
            self.assertTrue(result.signal_diagnostics_path.exists())

            report_text = result.report_path.read_text(encoding="utf-8")
            self.assertIn("## Major Findings", report_text)
            self.assertIn("## Signal Diagnostics", report_text)

            signal_text = result.signal_diagnostics_path.read_text(encoding="utf-8")
            self.assertIn("1102", signal_text)

    def test_run_diagnostics_reports_risk_control_context(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            config_path = _prepare_cross_sectional_artifacts(
                temp_root,
                walkforward_enabled=True,
                risk_controls_block=textwrap.dedent(
                    """
                    [risk_controls]
                    benchmark_filter_enabled = true
                    benchmark_ma_window = 3
                    defensive_mode = "cash"
                    rebalance_cadence_months = 3
                    """
                ).strip(),
            )
            config = load_backtest_settings(config_path)

            run_backtest(config)
            run_walkforward(config)
            result = run_diagnostics(config)

            report_text = result.report_path.read_text(encoding="utf-8")
            self.assertIn("Benchmark Regime Filter: enabled", report_text)
            self.assertIn("Rebalance Cadence: every 3 month(s)", report_text)

    def test_run_backtest_rejects_stale_membership_and_signal_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            config_path = _prepare_cross_sectional_artifacts(temp_root, walkforward_enabled=False)
            config = load_backtest_settings(config_path)

            usable_metadata_path = config.universe_config.usable_metadata_path
            assert usable_metadata_path is not None
            newer_time = usable_metadata_path.stat().st_mtime + 5.0
            os.utime(usable_metadata_path, (newer_time, newer_time))

            with self.assertRaisesRegex(ValueError, "stale downstream artifacts"):
                run_backtest(config)


def _prepare_cross_sectional_artifacts(
    temp_root: Path,
    walkforward_enabled: bool,
    risk_controls_block: str = "",
    benchmark_rows: list[list[str]] | None = None,
) -> Path:
    processed_root = temp_root / "artifacts" / "processed"
    market_root = processed_root / "market_data" / "daily"
    signal_root = processed_root / "signals" / "monthly"
    universe_root = processed_root / "universe"
    metadata_root = processed_root / "metadata"

    _write_normalized_csv(
        market_root / "1101.csv",
        [
            ["2024-01-02", "1101", "100", "100", "100", "100", "1000", "100000"],
            ["2024-01-03", "1101", "110", "110", "110", "110", "1000", "110000"],
            ["2024-01-04", "1101", "121", "121", "121", "121", "1000", "121000"],
            ["2024-02-01", "1101", "133.1", "133.1", "133.1", "133.1", "1000", "133100"],
            ["2024-02-02", "1101", "119.79", "119.79", "119.79", "119.79", "1000", "119790"],
            ["2024-02-05", "1101", "119.79", "119.79", "119.79", "119.79", "1000", "119790"],
        ],
    )
    _write_normalized_csv(
        market_root / "1102.csv",
        [
            ["2024-01-02", "1102", "100", "100", "100", "100", "1000", "100000"],
            ["2024-01-03", "1102", "90", "90", "90", "90", "1000", "90000"],
            ["2024-01-04", "1102", "99", "99", "99", "99", "1000", "99000"],
            ["2024-02-01", "1102", "108.9", "108.9", "108.9", "108.9", "1000", "108900"],
            ["2024-02-02", "1102", "119.79", "119.79", "119.79", "119.79", "1000", "119790"],
            ["2024-02-05", "1102", "131.769", "131.769", "131.769", "131.769", "1000", "131769"],
        ],
    )
    _write_normalized_csv(
        market_root / "1103.csv",
        [
            ["2024-01-02", "1103", "100", "100", "100", "100", "1000", "100000"],
            ["2024-01-03", "1103", "100", "100", "100", "100", "1000", "100000"],
            ["2024-01-04", "1103", "100", "100", "100", "100", "1000", "100000"],
            ["2024-02-01", "1103", "100", "100", "100", "100", "1000", "100000"],
            ["2024-02-02", "1103", "110", "110", "110", "110", "1000", "110000"],
            ["2024-02-05", "1103", "121", "121", "121", "121", "1000", "121000"],
        ],
    )
    _write_normalized_csv(
        market_root / "TAIEX.csv",
        benchmark_rows
        or [
            ["2024-01-02", "TAIEX", "18000", "18000", "18000", "18000", "", ""],
            ["2024-01-03", "TAIEX", "18050", "18050", "18050", "18050", "", ""],
            ["2024-01-04", "TAIEX", "18100", "18100", "18100", "18100", "", ""],
            ["2024-02-01", "TAIEX", "18200", "18200", "18200", "18200", "", ""],
            ["2024-02-02", "TAIEX", "18300", "18300", "18300", "18300", "", ""],
            ["2024-02-05", "TAIEX", "18400", "18400", "18400", "18400", "", ""],
        ],
    )

    _write_metadata_csv(
        metadata_root / "twse_usable_stock_info.csv",
        [
            ["1101", "Name1101", "twse", "Cement", "2024-01-02"],
            ["1102", "Name1102", "twse", "Cement", "2024-01-02"],
            ["1103", "Name1103", "twse", "Cement", "2024-01-02"],
        ],
    )
    _write_availability_csv(
        metadata_root / "twse_price_availability.csv",
        [
            ["1101", "1", "fetched", "6", "2024-01-02", "2024-02-05"],
            ["1102", "1", "fetched", "6", "2024-01-02", "2024-02-05"],
            ["1103", "1", "fetched", "6", "2024-01-02", "2024-02-05"],
        ],
    )
    _write_membership_csv(
        universe_root / "tw_top50_liquidity_membership.csv",
        [
            ["2024-01-02", "1101", "1", "110000", "twse_top50_liquidity", "1"],
            ["2024-01-02", "1102", "2", "105000", "twse_top50_liquidity", "1"],
            ["2024-01-02", "1103", "3", "100000", "twse_top50_liquidity", "1"],
            ["2024-02-01", "1101", "2", "120000", "twse_top50_liquidity", "1"],
            ["2024-02-01", "1102", "1", "121000", "twse_top50_liquidity", "1"],
            ["2024-02-01", "1103", "3", "100000", "twse_top50_liquidity", "1"],
        ],
    )
    _write_cross_signal_csv(
        signal_root / "cross_sectional_signal_panel.csv",
        [
            ["2024-01-02", "1101", "100", "110000", "1", "0.25", "0.10", "2.5", "1", "twse_top50_liquidity"],
            ["2024-01-02", "1102", "100", "105000", "2", "0.15", "0.10", "1.5", "2", "twse_top50_liquidity"],
            ["2024-01-02", "1103", "100", "100000", "3", "-0.05", "0.10", "-0.5", "3", "twse_top50_liquidity"],
            ["2024-02-01", "1101", "133.1", "120000", "2", "-0.02", "0.10", "-0.2", "3", "twse_top50_liquidity"],
            ["2024-02-01", "1102", "108.9", "121000", "1", "0.08", "0.10", "0.8", "2", "twse_top50_liquidity"],
            ["2024-02-01", "1103", "100", "100000", "3", "0.12", "0.10", "1.2", "1", "twse_top50_liquidity"],
        ],
    )

    config_path = temp_root / "cross_sectional.toml"
    config_path.write_text(
        textwrap.dedent(
            f"""
            project_name = "cross_sectional_demo"
            market = "TW cash equities"
            universe = "twse_top50_liquidity"
            benchmark = "TAIEX"
            start_date = "2024-01-02"
            end_date = "2024-02-05"

            [research]
            branch = "tw_top50_liquidity_cross_sectional"

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
            symbols = []
            refresh = false
            storage_format = "csv"
            token_env_var = "FINMIND_API_TOKEN"
            raw_cache_subdir = "finmind"
            normalized_subdir = "market_data/daily"

            [universe_selection]
            candidate_market = "twse"
            selection_rule = "top_liquidity"
            liquidity_lookback_days = 60
            top_n = 3
            reconstitution_frequency = "monthly"
            metadata_output_subdir = "metadata"
            membership_output_subdir = "universe"
            membership_file = "tw_top50_liquidity_membership.csv"

            [signals]
            mode = "cross_sectional_vol_adj_momentum"
            enabled_symbols = []
            benchmark = "TAIEX"
            ma_fast_window = 1
            ma_slow_window = 2
            momentum_window = 126
            volatility_window = 20
            volatility_cap = 0.35
            align_by_date = false
            input_subdir = "market_data/daily"
            output_subdir = "signals/monthly"
            output_file = "cross_sectional_signal_panel.csv"

            [portfolio]
            tradable_symbols = []
            benchmark = "TAIEX"
            rebalance_frequency = "monthly"
            weighting = "equal"
            min_signal_score = 0.0
            max_positions = 2
            max_weight = 0.5
            hold_cash_when_inactive = true

            {risk_controls_block}

            [backtest]
            initial_nav = 1.0
            bar_input_subdir = "market_data/daily"
            signal_input_subdir = "signals/monthly"
            signal_input_file = "cross_sectional_signal_panel.csv"
            output_subdir = "backtests"
            nav_file = "daily_nav.csv"
            weights_file = "daily_weights.csv"

            [walkforward]
            enabled = {str(walkforward_enabled).lower()}
            window_type = "expanding"
            train_window_days = 3
            test_window_days = 2
            minimum_history_days = 3
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    return config_path


def _write_normalized_csv(path: Path, rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "date,symbol,open,high,low,close,volume,traded_value\n"
        + "\n".join(",".join(row) for row in rows)
        + "\n",
        encoding="utf-8",
    )


def _write_membership_csv(path: Path, rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "date,symbol,liquidity_rank,avg_traded_value_60d,universe_name,is_member\n"
        + "\n".join(",".join(row) for row in rows)
        + "\n",
        encoding="utf-8",
    )


def _write_cross_signal_csv(path: Path, rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "date,symbol,close,avg_traded_value_60d,liquidity_rank,momentum_126,volatility_20,signal_score,factor_rank,universe_name\n"
        + "\n".join(",".join(row) for row in rows)
        + "\n",
        encoding="utf-8",
    )


def _write_metadata_csv(path: Path, rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "stock_id,stock_name,type,industry_category,date\n"
        + "\n".join(",".join(row) for row in rows)
        + "\n",
        encoding="utf-8",
    )


def _write_availability_csv(path: Path, rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "stock_id,has_usable_price_data,status,row_count,first_date,last_date\n"
        + "\n".join(",".join(row) for row in rows)
        + "\n",
        encoding="utf-8",
    )


def _date(raw: str) -> date:
    return date.fromisoformat(raw)


if __name__ == "__main__":
    unittest.main()
