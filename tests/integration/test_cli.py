from __future__ import annotations

from pathlib import Path
import os
import subprocess
import sys
import tempfile
import textwrap
import unittest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"


def run_cli(*args: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    existing_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = (
        str(SRC_ROOT) if not existing_pythonpath else f"{SRC_ROOT}{os.pathsep}{existing_pythonpath}"
    )
    return subprocess.run(
        [sys.executable, "-m", "tw_quant", *args],
        cwd=PROJECT_ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )


class CliTests(unittest.TestCase):
    def test_repo_entrypoint_script_bootstraps_cli(self) -> None:
        env = os.environ.copy()
        env.pop("PYTHONPATH", None)

        result = subprocess.run(
            [sys.executable, str(PROJECT_ROOT / "bin" / "twq"), "--help"],
            cwd=PROJECT_ROOT,
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("backtest", result.stdout)
        self.assertIn("diagnostics", result.stdout)
        self.assertIn("ingest", result.stdout)
        self.assertIn("signals", result.stdout)
        self.assertIn("walkforward", result.stdout)

    def test_top_level_help(self) -> None:
        result = run_cli("--help")

        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("backtest", result.stdout)
        self.assertIn("diagnostics", result.stdout)
        self.assertIn("ingest", result.stdout)
        self.assertIn("signals", result.stdout)
        self.assertIn("walkforward", result.stdout)

    def test_backtest_help(self) -> None:
        result = run_cli("backtest", "--help")

        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("--config", result.stdout)

    def test_ingest_help(self) -> None:
        result = run_cli("ingest", "--help")

        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("--config", result.stdout)
        self.assertIn("--refresh", result.stdout)

    def test_signals_help(self) -> None:
        result = run_cli("signals", "--help")

        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("--config", result.stdout)

    def test_walkforward_help(self) -> None:
        result = run_cli("walkforward", "--help")

        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("--config", result.stdout)

    def test_diagnostics_help(self) -> None:
        result = run_cli("diagnostics", "--help")

        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("--config", result.stdout)

    def test_backtest_command_writes_report(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            (temp_root / "artifacts" / "processed" / "market_data" / "daily").mkdir(parents=True, exist_ok=True)
            (temp_root / "artifacts" / "processed" / "signals" / "daily").mkdir(parents=True, exist_ok=True)
            _write_normalized_csv(
                temp_root / "artifacts" / "processed" / "market_data" / "daily" / "2330.csv",
                [
                    ["2024-01-02", "2330", "100", "101", "99", "100", "1000"],
                    ["2024-01-03", "2330", "102", "103", "101", "102", "1000"],
                    ["2024-01-04", "2330", "103", "104", "102", "103", "1000"],
                ],
            )
            _write_normalized_csv(
                temp_root / "artifacts" / "processed" / "market_data" / "daily" / "0050.csv",
                [
                    ["2024-01-02", "0050", "50", "51", "49", "50", "1000"],
                    ["2024-01-03", "0050", "51", "52", "50", "51", "1000"],
                    ["2024-01-04", "0050", "52", "53", "51", "52", "1000"],
                ],
            )
            _write_normalized_csv(
                temp_root / "artifacts" / "processed" / "market_data" / "daily" / "TAIEX.csv",
                [
                    ["2024-01-02", "TAIEX", "18000", "18000", "18000", "18000", ""],
                    ["2024-01-03", "TAIEX", "18100", "18100", "18100", "18100", ""],
                    ["2024-01-04", "TAIEX", "18200", "18200", "18200", "18200", ""],
                ],
            )
            _write_signal_csv(
                temp_root / "artifacts" / "processed" / "signals" / "daily" / "signal_panel.csv",
                [
                    ["2024-01-02", "2330", "100", "", "", "0", "", "0", "", "0", "0.0"],
                    ["2024-01-02", "0050", "50", "", "", "0", "", "0", "", "0", "0.0"],
                    ["2024-01-03", "2330", "102", "101", "", "0", "0.02", "1", "0.1", "1", "1.0"],
                    ["2024-01-03", "0050", "51", "50.5", "", "0", "0.02", "1", "0.1", "1", "0.5"],
                    ["2024-01-04", "2330", "103", "102.5", "101.67", "1", "0.03", "1", "0.1", "1", "1.0"],
                    ["2024-01-04", "0050", "52", "51.5", "51.0", "1", "0.04", "1", "0.1", "1", "0.5"],
                ],
            )
            config_path = temp_root / "settings.toml"
            config_path.write_text(
                textwrap.dedent(
                    """
                    project_name = "cli_test_project"
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
                    ma_fast_window = 20
                    ma_slow_window = 60
                    momentum_window = 20
                    volatility_window = 20
                    volatility_cap = 0.35
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
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            result = run_cli("backtest", "--config", str(config_path))

            report_path = temp_root / "artifacts" / "reports" / "cli_test_project" / "backtest_summary.md"
            nav_path = temp_root / "artifacts" / "processed" / "backtests" / "cli_test_project" / "daily_nav.csv"
            weights_path = temp_root / "artifacts" / "processed" / "backtests" / "cli_test_project" / "daily_weights.csv"
            equity_curve_path = temp_root / "artifacts" / "reports" / "cli_test_project" / "equity_curve.svg"
            drawdown_path = temp_root / "artifacts" / "reports" / "cli_test_project" / "drawdown.svg"
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertTrue(report_path.exists())
            self.assertTrue(nav_path.exists())
            self.assertTrue(weights_path.exists())
            self.assertTrue(equity_curve_path.exists())
            self.assertTrue(drawdown_path.exists())
            self.assertIn("回測完成", result.stdout)

    def test_signals_command_writes_output(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            (temp_root / "artifacts" / "market_data" / "daily").mkdir(parents=True, exist_ok=True)
            _write_normalized_csv(
                temp_root / "artifacts" / "market_data" / "daily" / "2330.csv",
                [
                    ["2024-01-02", "2330", "100", "101", "99", "100", "1000"],
                    ["2024-01-03", "2330", "102", "103", "101", "102", "1100"],
                    ["2024-01-04", "2330", "104", "105", "103", "104", "1200"],
                ],
            )
            _write_normalized_csv(
                temp_root / "artifacts" / "market_data" / "daily" / "0050.csv",
                [
                    ["2024-01-02", "0050", "50", "51", "49", "50", "2000"],
                    ["2024-01-03", "0050", "51", "52", "50", "51", "2100"],
                    ["2024-01-04", "0050", "52", "53", "51", "52", "2200"],
                ],
            )
            _write_normalized_csv(
                temp_root / "artifacts" / "market_data" / "daily" / "TAIEX.csv",
                [
                    ["2024-01-02", "TAIEX", "18000", "18000", "18000", "18000", ""],
                    ["2024-01-03", "TAIEX", "18100", "18100", "18100", "18100", ""],
                    ["2024-01-04", "TAIEX", "18200", "18200", "18200", "18200", ""],
                ],
            )

            config_path = temp_root / "settings.toml"
            config_path.write_text(
                textwrap.dedent(
                    """
                    project_name = "signals_test_project"
                    market = "TW cash equities"
                    universe = "unit_test_universe"
                    benchmark = "TAIEX"
                    start_date = "2024-01-02"
                    end_date = "2024-01-04"

                    [paths]
                    project_root = "."
                    raw = "artifacts/raw"
                    processed = "artifacts"
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
                    volatility_cap = 5.0
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
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            result = run_cli("signals", "--config", str(config_path))

            output_path = temp_root / "artifacts" / "signals" / "daily" / "signal_panel.csv"
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertTrue(output_path.exists())
            self.assertIn("訊號產生完成", result.stdout)

    def test_walkforward_command_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            (temp_root / "artifacts" / "processed" / "market_data" / "daily").mkdir(parents=True, exist_ok=True)
            (temp_root / "artifacts" / "processed" / "signals" / "daily").mkdir(parents=True, exist_ok=True)
            _write_normalized_csv(
                temp_root / "artifacts" / "processed" / "market_data" / "daily" / "2330.csv",
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
                temp_root / "artifacts" / "processed" / "market_data" / "daily" / "0050.csv",
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
                temp_root / "artifacts" / "processed" / "market_data" / "daily" / "TAIEX.csv",
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
                temp_root / "artifacts" / "processed" / "signals" / "daily" / "signal_panel.csv",
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
            config_path = temp_root / "settings.toml"
            config_path.write_text(
                textwrap.dedent(
                    """
                    project_name = "cli_wf_project"
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
                    window_type = "expanding"
                    train_window_days = 4
                    test_window_days = 2
                    minimum_history_days = 4
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            result = run_cli("walkforward", "--config", str(config_path))

            nav_path = temp_root / "artifacts" / "processed" / "backtests" / "cli_wf_project" / "walkforward" / "walkforward_nav.csv"
            window_summary_path = temp_root / "artifacts" / "processed" / "backtests" / "cli_wf_project" / "walkforward" / "window_summary.csv"
            report_path = temp_root / "artifacts" / "reports" / "cli_wf_project" / "walkforward" / "walkforward_summary.md"
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertTrue(nav_path.exists())
            self.assertTrue(window_summary_path.exists())
            self.assertTrue(report_path.exists())
            self.assertIn("Walk-forward 評估完成", result.stdout)

    def test_diagnostics_command_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            processed_root = temp_root / "artifacts" / "processed"
            reports_root = temp_root / "artifacts" / "reports"
            backtest_dir = processed_root / "backtests" / "cli_diag_project"
            walkforward_dir = backtest_dir / "walkforward"
            signal_dir = processed_root / "signals" / "daily"
            backtest_dir.mkdir(parents=True, exist_ok=True)
            walkforward_dir.mkdir(parents=True, exist_ok=True)
            signal_dir.mkdir(parents=True, exist_ok=True)

            _write_nav_csv(
                backtest_dir / "daily_nav.csv",
                [
                    ["2024-01-02", "1.0", "0.0", "0.0", "1.0", "0.0", "0.0", "0.0", "1.0"],
                    ["2024-01-03", "1.05", "0.05", "0.05", "1.02", "0.02", "0.5", "0.0", "0.0"],
                    ["2024-01-04", "1.0", "-0.0476190476", "-0.0476190476", "1.01", "-0.0098039216", "0.0", "0.0", "0.0"],
                ],
            )
            _write_weights_csv(
                backtest_dir / "daily_weights.csv",
                [
                    ["2024-01-02", "2330", "0.0", ""],
                    ["2024-01-02", "0050", "0.0", ""],
                    ["2024-01-03", "2330", "1.0", "1.0"],
                    ["2024-01-03", "0050", "0.0", "0.0"],
                    ["2024-01-04", "2330", "0.5", "1.0"],
                    ["2024-01-04", "0050", "0.5", "0.5"],
                ],
            )
            _write_signal_csv(
                signal_dir / "signal_panel.csv",
                [
                    ["2024-01-02", "2330", "100", "", "", "0", "", "0", "", "0", "0.0"],
                    ["2024-01-02", "0050", "50", "", "", "0", "", "0", "", "0", "0.0"],
                    ["2024-01-03", "2330", "101", "101", "100", "1", "0.01", "1", "0.4", "0", "0.0"],
                    ["2024-01-03", "0050", "49", "49", "50", "-1", "-0.01", "-1", "0.1", "1", "-1.0"],
                    ["2024-01-04", "2330", "102", "101", "100", "1", "0.02", "1", "0.1", "1", "1.0"],
                    ["2024-01-04", "0050", "49.5", "49.5", "49.8", "-1", "0.0", "0", "0.5", "0", "0.0"],
                ],
            )
            _write_nav_csv(
                walkforward_dir / "walkforward_nav.csv",
                [
                    ["2024-01-03", "1.0", "0.0", "0.0", "1.0", "0.0", "0.0", "0.0", "1.0"],
                    ["2024-01-04", "1.03", "0.03", "0.03", "1.01", "0.01", "0.5", "0.0", "0.0"],
                ],
            )
            _write_window_summary_csv(
                walkforward_dir / "window_summary.csv",
                [
                    ["1", "2023-10-01", "2023-12-31", "2024-01-03", "2024-01-04", "60", "2", "1.03", "1.01", "0.03", "0.4", "0.1", "-0.01", "1.2", "0.5"],
                ],
            )

            config_path = temp_root / "settings.toml"
            config_path.write_text(
                textwrap.dedent(
                    """
                    project_name = "cli_diag_project"
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

            result = run_cli("diagnostics", "--config", str(config_path))

            diagnostics_dir = backtest_dir / "diagnostics"
            report_path = reports_root / "cli_diag_project" / "diagnostics" / "diagnostics_summary.md"
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertTrue((diagnostics_dir / "yearly_return_table.csv").exists())
            self.assertTrue((diagnostics_dir / "walkforward_window_diagnostics.csv").exists())
            self.assertTrue((diagnostics_dir / "symbol_exposure_summary.csv").exists())
            self.assertTrue((diagnostics_dir / "signal_diagnostics.csv").exists())
            self.assertTrue(report_path.exists())
            self.assertIn("診斷分析完成", result.stdout)


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


def _write_nav_csv(path: Path, rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "date,nav,daily_return,gross_return,benchmark_nav,benchmark_return,turnover,transaction_cost,cash_weight\n"
        + "\n".join(",".join(row) for row in rows)
        + "\n",
        encoding="utf-8",
    )


def _write_weights_csv(path: Path, rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "date,symbol,weight,signal_score\n" + "\n".join(",".join(row) for row in rows) + "\n",
        encoding="utf-8",
    )


def _write_window_summary_csv(path: Path, rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "window_id,train_start,train_end,test_start,test_end,train_size,test_size,final_nav,benchmark_final_nav,cumulative_return,annualized_return,annualized_volatility,max_drawdown,sharpe_ratio,turnover\n"
        + "\n".join(",".join(row) for row in rows)
        + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    unittest.main()
