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

    def test_backtest_help(self) -> None:
        result = run_cli("backtest", "--help")

        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("--config", result.stdout)

    def test_ingest_help(self) -> None:
        result = run_cli("ingest", "--help")

        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("--config", result.stdout)
        self.assertIn("--refresh", result.stdout)

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


if __name__ == "__main__":
    unittest.main()
