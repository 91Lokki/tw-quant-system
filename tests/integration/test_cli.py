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
    def test_top_level_help(self) -> None:
        result = run_cli("--help")

        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("backtest", result.stdout)
        self.assertIn("ingest", result.stdout)
        self.assertIn("signals", result.stdout)

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

    def test_backtest_command_writes_report(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            config_path = temp_root / "settings.toml"
            config_path.write_text(
                textwrap.dedent(
                    """
                    project_name = "cli_test_project"
                    market = "TW cash equities"
                    universe = "unit_test_universe"
                    benchmark = "TAIEX"
                    start_date = "2022-01-01"
                    end_date = "2022-12-31"

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
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            result = run_cli("backtest", "--config", str(config_path))

            report_path = temp_root / "artifacts" / "reports" / "cli_test_project_backtest_summary.md"
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertTrue(report_path.exists())
            self.assertIn("scaffold backtest completed", result.stdout)

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


def _write_normalized_csv(path: Path, rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "date,symbol,open,high,low,close,volume\n" + "\n".join(",".join(row) for row in rows) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    unittest.main()
