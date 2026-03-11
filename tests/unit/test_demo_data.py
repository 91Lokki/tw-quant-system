from __future__ import annotations

from pathlib import Path
import sys
import tempfile
import unittest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.demo_data import (
    build_artifact_status_rows,
    build_latest_portfolio_snapshot,
    build_project_paths,
    discover_projects,
    load_summary_metrics,
    summarize_artifact_status_rows,
)


class DemoDataTests(unittest.TestCase):
    def test_discover_projects_from_backtests_and_reports(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / "data" / "processed" / "backtests" / "alpha").mkdir(parents=True)
            (root / "data" / "processed" / "reports" / "beta").mkdir(parents=True)
            (root / "data" / "processed" / "reports" / "alpha").mkdir(parents=True)

            projects = discover_projects(root)

            self.assertIn("alpha", projects)
            self.assertIn("beta", projects)

    def test_load_summary_metrics_reads_backtest_report_lines(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            report_path = Path(temp_dir) / "backtest_summary.md"
            report_path.write_text(
                "\n".join(
                    [
                        "# Demo",
                        "",
                        "- Project Name: demo",
                        "- Final NAV: 1.233352",
                        "- Cumulative Return: 23.3352%",
                        "- Sharpe Ratio: 0.4605",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            metrics = load_summary_metrics(report_path)

            self.assertEqual(metrics["Project Name"], "demo")
            self.assertEqual(metrics["Final NAV"], "1.233352")
            self.assertEqual(metrics["Sharpe Ratio"], "0.4605")

    def test_build_artifact_status_rows_marks_present_and_missing_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            paths = build_project_paths(root, "demo")
            paths["normalized_dir"].mkdir(parents=True, exist_ok=True)
            paths["signal_panel"].parent.mkdir(parents=True, exist_ok=True)
            paths["signal_panel"].write_text("date,symbol\n", encoding="utf-8")
            paths["report_dir"].mkdir(parents=True, exist_ok=True)

            rows = build_artifact_status_rows(root, "demo")
            status_by_name = {row["artifact"]: row["status"] for row in rows}

            self.assertEqual(status_by_name["Normalized Bars Directory"], "Present")
            self.assertEqual(status_by_name["Signal Panel CSV"], "Present")
            self.assertEqual(status_by_name["Daily NAV CSV"], "Missing")

    def test_summarize_artifact_status_rows_counts_present_and_missing(self) -> None:
        rows = [
            {"artifact": "A", "status": "Present", "modified_at": "2025-03-12 09:00:00"},
            {"artifact": "B", "status": "Missing", "modified_at": "-"},
            {"artifact": "C", "status": "Present", "modified_at": "2025-03-12 10:00:00"},
        ]

        summary = summarize_artifact_status_rows(rows)

        self.assertEqual(summary["present_count"], 2)
        self.assertEqual(summary["missing_count"], 1)
        self.assertEqual(summary["total_count"], 3)
        self.assertEqual(summary["latest_modified_at"], "2025-03-12 10:00:00")

    def test_build_latest_portfolio_snapshot_summarizes_latest_weights_and_cash(self) -> None:
        snapshot = build_latest_portfolio_snapshot(
            weight_rows=[
                {"date": "2025-03-10", "symbol": "2330", "weight": 0.5, "signal_score": 1.0},
                {"date": "2025-03-10", "symbol": "0050", "weight": 0.0, "signal_score": 0.0},
                {"date": "2025-03-11", "symbol": "2330", "weight": 0.6, "signal_score": 1.0},
                {"date": "2025-03-11", "symbol": "0050", "weight": 0.2, "signal_score": 0.5},
            ],
            nav_rows=[
                {"date": "2025-03-10", "cash_weight": 0.5},
                {"date": "2025-03-11", "cash_weight": 0.2},
            ],
        )

        self.assertEqual(snapshot["latest_date"], "2025-03-11")
        self.assertEqual(snapshot["held_count"], 2)
        self.assertEqual(snapshot["held_symbols"], ("0050", "2330"))
        self.assertAlmostEqual(snapshot["gross_exposure"], 0.8)
        self.assertAlmostEqual(snapshot["cash_weight"], 0.2)


if __name__ == "__main__":
    unittest.main()
