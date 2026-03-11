from __future__ import annotations

from datetime import date
from pathlib import Path
import sys
import unittest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from tw_quant.core.models import BacktestConfig, BacktestResult, DataPaths, TradingCosts


class ModelTests(unittest.TestCase):
    def test_backtest_config_date_range_label(self) -> None:
        config = BacktestConfig(
            project_name="demo",
            market="TW cash equities",
            universe="demo_universe",
            benchmark="TAIEX",
            start_date=date(2021, 1, 1),
            end_date=date(2021, 12, 31),
            data_paths=DataPaths(
                project_root=PROJECT_ROOT,
                raw_dir=PROJECT_ROOT / "data" / "raw",
                processed_dir=PROJECT_ROOT / "data" / "processed",
                reports_dir=PROJECT_ROOT / "data" / "processed" / "reports",
            ),
            trading_costs=TradingCosts(
                commission_bps=14.25,
                tax_bps=30.0,
                slippage_bps=5.0,
            ),
        )

        self.assertEqual(config.date_range_label(), "2021-01-01 to 2021-12-31")

    def test_backtest_result_summary_text(self) -> None:
        result = BacktestResult(
            project_name="demo",
            market="TW cash equities",
            universe="demo_universe",
            benchmark="TAIEX",
            start_date=date(2021, 1, 1),
            end_date=date(2021, 12, 31),
            report_path=Path("data/processed/reports/demo_backtest_summary.md"),
            status="scaffold backtest completed",
            notes=("First note.", "Second note."),
        )

        summary = result.summary_text()

        self.assertIn("Project: demo", summary)
        self.assertIn("Status: scaffold backtest completed", summary)
        self.assertIn("- First note.", summary)


if __name__ == "__main__":
    unittest.main()
