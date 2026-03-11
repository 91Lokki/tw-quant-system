from __future__ import annotations

from datetime import date
from pathlib import Path
import sys
import tempfile
import textwrap
import unittest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from tw_quant.config import load_backtest_settings
from tw_quant.core.models import NavRow, PortfolioWeightRow, SignalRow
from tw_quant.diagnostics.analyze import (
    build_signal_diagnostics,
    build_symbol_exposure_summary,
    build_walkforward_window_diagnostics,
    build_yearly_return_rows,
    run_diagnostics,
)


class DiagnosticsTests(unittest.TestCase):
    def test_build_yearly_return_rows_aggregates_returns_and_drawdowns(self) -> None:
        nav_rows = [
            NavRow(date(2023, 12, 29), 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0),
            NavRow(date(2024, 1, 2), 1.1, 0.1, 0.1, 1.02, 0.02, 0.0, 0.0, 0.0),
            NavRow(date(2024, 1, 3), 1.0, -0.0909090909, -0.0909090909, 1.01, -0.0098039216, 0.0, 0.0, 0.0),
            NavRow(date(2025, 1, 2), 1.2, 0.2, 0.2, 1.11, 0.09900990099, 0.0, 0.0, 0.0),
        ]

        rows, summary = build_yearly_return_rows(nav_rows)

        self.assertEqual([row["year"] for row in rows], ["2023", "2024", "2025"])
        self.assertAlmostEqual(float(rows[1]["strategy_return"]), 0.0, places=6)
        self.assertLess(float(rows[1]["strategy_max_drawdown"]), 0.0)
        self.assertEqual(summary["best_year"]["year"], 2025)
        self.assertEqual(summary["worst_year"]["year"], 2023)

    def test_build_walkforward_window_diagnostics_summarizes_distribution(self) -> None:
        rows, summary = build_walkforward_window_diagnostics(
            [
                {
                    "window_id": 1,
                    "train_start": date(2024, 1, 1),
                    "train_end": date(2024, 3, 31),
                    "test_start": date(2024, 4, 1),
                    "test_end": date(2024, 6, 30),
                    "train_size": 60,
                    "test_size": 20,
                    "final_nav": 1.1,
                    "benchmark_final_nav": 1.02,
                    "cumulative_return": 0.1,
                    "annualized_return": 0.2,
                    "annualized_volatility": 0.1,
                    "max_drawdown": -0.03,
                    "sharpe_ratio": 1.2,
                    "turnover": 0.5,
                },
                {
                    "window_id": 2,
                    "train_start": date(2024, 2, 1),
                    "train_end": date(2024, 4, 30),
                    "test_start": date(2024, 5, 1),
                    "test_end": date(2024, 7, 31),
                    "train_size": 60,
                    "test_size": 20,
                    "final_nav": 0.95,
                    "benchmark_final_nav": 1.03,
                    "cumulative_return": -0.05,
                    "annualized_return": -0.1,
                    "annualized_volatility": 0.2,
                    "max_drawdown": -0.07,
                    "sharpe_ratio": -0.4,
                    "turnover": 1.0,
                },
            ]
        )

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["outcome_label"], "positive")
        self.assertEqual(rows[1]["outcome_label"], "negative")
        self.assertEqual(summary["positive_count"], 1)
        self.assertEqual(summary["negative_count"], 1)
        self.assertEqual(summary["best_window"]["window_id"], 1)
        self.assertEqual(summary["worst_window"]["window_id"], 2)

    def test_build_symbol_exposure_summary_reports_concentration_and_cash(self) -> None:
        symbol_rows, summary = build_symbol_exposure_summary(
            weight_rows=[
                PortfolioWeightRow(date(2024, 1, 2), "2330", 0.0, None),
                PortfolioWeightRow(date(2024, 1, 2), "0050", 0.0, None),
                PortfolioWeightRow(date(2024, 1, 3), "2330", 1.0, 1.0),
                PortfolioWeightRow(date(2024, 1, 3), "0050", 0.0, 0.0),
                PortfolioWeightRow(date(2024, 1, 4), "2330", 0.5, 1.0),
                PortfolioWeightRow(date(2024, 1, 4), "0050", 0.5, 0.5),
            ],
            nav_rows=[
                NavRow(date(2024, 1, 2), 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0),
                NavRow(date(2024, 1, 3), 1.02, 0.02, 0.02, 1.0, 0.0, 0.5, 0.0, 0.0),
                NavRow(date(2024, 1, 4), 1.03, 0.01, 0.01, 1.0, 0.0, 0.5, 0.0, 0.0),
            ],
            tradable_symbols=("2330", "0050"),
        )

        self.assertEqual(len(symbol_rows), 2)
        self.assertAlmostEqual(float(symbol_rows[0]["held_ratio"]), 2 / 3)
        self.assertAlmostEqual(summary["inactive_days_ratio"], 1 / 3)
        self.assertAlmostEqual(summary["average_cash_weight"], 1 / 3)
        self.assertEqual(summary["dominant_symbol"]["symbol"], "2330")

    def test_build_signal_diagnostics_reports_inactivity_and_suppression(self) -> None:
        rows, summary = build_signal_diagnostics(
            signal_rows=[
                SignalRow(date(2024, 1, 2), "2330", 100.0, None, None, 0, None, 0, None, 0, 0.0),
                SignalRow(date(2024, 1, 3), "2330", 101.0, 101.0, 100.0, 1, 0.01, 1, 0.4, 0, 0.0),
                SignalRow(date(2024, 1, 4), "2330", 102.0, 101.0, 100.0, 1, 0.02, 1, 0.1, 1, 1.0),
                SignalRow(date(2024, 1, 2), "0050", 50.0, None, None, 0, None, 0, None, 0, 0.0),
                SignalRow(date(2024, 1, 3), "0050", 49.0, 49.0, 50.0, -1, -0.01, -1, 0.1, 1, -1.0),
                SignalRow(date(2024, 1, 4), "0050", 49.5, 49.5, 49.8, -1, 0.0, 0, 0.5, 0, 0.0),
            ],
            tradable_symbols=("2330", "0050"),
        )

        self.assertEqual(len(rows), 2)
        self.assertGreater(float(rows[0]["volatility_suppressed_ratio"]), 0.0)
        self.assertGreater(summary["average_inactive_ratio"], 0.0)
        self.assertIn(summary["dominant_symbol"]["symbol"], {"2330", "0050"})

    def test_run_diagnostics_writes_report_and_csv_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            processed_root = temp_root / "artifacts" / "processed"
            reports_root = temp_root / "artifacts" / "reports"
            backtest_dir = processed_root / "backtests" / "diag_demo"
            walkforward_dir = backtest_dir / "walkforward"
            signal_dir = processed_root / "signals" / "daily"
            backtest_dir.mkdir(parents=True, exist_ok=True)
            walkforward_dir.mkdir(parents=True, exist_ok=True)
            signal_dir.mkdir(parents=True, exist_ok=True)

            (backtest_dir / "daily_nav.csv").write_text(
                "\n".join(
                    [
                        "date,nav,daily_return,gross_return,benchmark_nav,benchmark_return,turnover,transaction_cost,cash_weight",
                        "2024-01-02,1.0,0.0,0.0,1.0,0.0,0.0,0.0,1.0",
                        "2024-01-03,1.05,0.05,0.05,1.02,0.02,0.5,0.0,0.0",
                        "2024-01-04,1.0,-0.0476190476,-0.0476190476,1.01,-0.0098039216,0.0,0.0,0.0",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (backtest_dir / "daily_weights.csv").write_text(
                "\n".join(
                    [
                        "date,symbol,weight,signal_score",
                        "2024-01-02,2330,0.0,",
                        "2024-01-02,0050,0.0,",
                        "2024-01-03,2330,1.0,1.0",
                        "2024-01-03,0050,0.0,0.0",
                        "2024-01-04,2330,0.5,1.0",
                        "2024-01-04,0050,0.5,0.5",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (signal_dir / "signal_panel.csv").write_text(
                "\n".join(
                    [
                        "date,symbol,close,ma_fast,ma_slow,trend_signal,momentum_n,momentum_signal,volatility_n,volatility_filter,signal_score",
                        "2024-01-02,2330,100,,,0,,0,,0,0.0",
                        "2024-01-02,0050,50,,,0,,0,,0,0.0",
                        "2024-01-03,2330,101,101,100,1,0.01,1,0.4,0,0.0",
                        "2024-01-03,0050,49,49,50,-1,-0.01,-1,0.1,1,-1.0",
                        "2024-01-04,2330,102,101,100,1,0.02,1,0.1,1,1.0",
                        "2024-01-04,0050,49.5,49.5,49.8,-1,0.0,0,0.5,0,0.0",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (walkforward_dir / "walkforward_nav.csv").write_text(
                "\n".join(
                    [
                        "date,nav,daily_return,gross_return,benchmark_nav,benchmark_return,turnover,transaction_cost,cash_weight",
                        "2024-01-03,1.0,0.0,0.0,1.0,0.0,0.0,0.0,1.0",
                        "2024-01-04,1.03,0.03,0.03,1.01,0.01,0.5,0.0,0.0",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (walkforward_dir / "window_summary.csv").write_text(
                "\n".join(
                    [
                        "window_id,train_start,train_end,test_start,test_end,train_size,test_size,final_nav,benchmark_final_nav,cumulative_return,annualized_return,annualized_volatility,max_drawdown,sharpe_ratio,turnover",
                        "1,2023-10-01,2023-12-31,2024-01-03,2024-01-04,60,2,1.03,1.01,0.03,0.4,0.1,-0.01,1.2,0.5",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            config_path = temp_root / "settings.toml"
            config_path.write_text(
                textwrap.dedent(
                    """
                    project_name = "diag_demo"
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
                    commission_bps = 10.0
                    tax_bps = 30.0
                    slippage_bps = 5.0

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
                    volatility_cap = 0.35
                    align_by_date = true
                    input_subdir = "market_data/daily"
                    output_subdir = "signals/daily"
                    output_file = "signal_panel.csv"

                    [portfolio]
                    tradable_symbols = ["2330", "0050"]
                    benchmark = "TAIEX"
                    rebalance_frequency = "monthly"
                    weighting = "equal"
                    min_signal_score = 0.0
                    max_positions = 2
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
                    window_type = "expanding"
                    train_window_days = 252
                    test_window_days = 63
                    minimum_history_days = 252
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            result = run_diagnostics(load_backtest_settings(config_path))

            self.assertTrue(result.report_path.exists())
            self.assertTrue(result.yearly_table_path.exists())
            self.assertTrue(result.walkforward_table_path.exists())
            self.assertTrue(result.symbol_exposure_path.exists())
            self.assertTrue(result.signal_diagnostics_path.exists())
            report_text = result.report_path.read_text(encoding="utf-8")
            self.assertIn("## Major Findings", report_text)
            self.assertIn("## Walk-Forward Diagnostics", report_text)


if __name__ == "__main__":
    unittest.main()
