from __future__ import annotations

import csv
from datetime import date
import json
from pathlib import Path
import sys
import tempfile
import textwrap
import unittest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from tw_quant.pipelines.decision import execute_daily_decision
from tw_quant.pipelines.paper import execute_paper_update
from tests.unit.test_cross_sectional_backtest import _prepare_cross_sectional_artifacts


PRACTICAL_RISK_BLOCK = textwrap.dedent(
    """
    [risk_controls]
    benchmark_filter_enabled = true
    benchmark_ma_window = 200
    defensive_mode = "half_exposure"
    defensive_gross_exposure = 0.6
    rebalance_cadence_months = 3
    """
).strip()


class PaperExecutionTests(unittest.TestCase):
    def test_execute_daily_decision_writes_practical_snapshot_with_delay(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = _prepare_cross_sectional_artifacts(
                Path(temp_dir),
                walkforward_enabled=False,
                risk_controls_block=PRACTICAL_RISK_BLOCK,
            )

            result = execute_daily_decision(config_path, as_of_date=date(2024, 1, 2))

            self.assertEqual(result.strategy_id, "risk_controlled_3m_half_exposure_exp60_delay1")
            self.assertTrue(result.rebalance_required)
            self.assertFalse(result.benchmark_regime_on)
            self.assertEqual(result.execution_delay_days, 1)
            self.assertEqual(result.signal_date, date(2024, 1, 2))
            self.assertEqual(result.scheduled_execution_date, date(2024, 1, 4))
            self.assertAlmostEqual(result.target_cash_weight, 0.4, places=6)
            self.assertEqual(result.target_symbols, ("1101", "1102"))
            self.assertTrue(result.snapshot_path.exists())
            self.assertTrue(result.latest_snapshot_path.exists())

    def test_execute_paper_update_rolls_forward_and_executes_after_delay(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = _prepare_cross_sectional_artifacts(
                Path(temp_dir),
                walkforward_enabled=False,
                risk_controls_block=PRACTICAL_RISK_BLOCK,
            )

            first_result = execute_paper_update(config_path, as_of_date=date(2024, 1, 3))
            second_result = execute_paper_update(config_path, as_of_date=date(2024, 1, 4))

            self.assertAlmostEqual(first_result.final_nav, 1_000_000.0, places=6)
            self.assertAlmostEqual(second_result.final_nav, 1_000_000.0, places=6)
            self.assertEqual(second_result.holdings_count, 2)
            self.assertTrue(second_result.blotter_path.exists())
            self.assertTrue(second_result.state_path.exists())
            self.assertTrue(second_result.nav_history_path.exists())

            with second_result.blotter_path.open("r", newline="", encoding="utf-8") as handle:
                blotter_rows = list(csv.DictReader(handle))
            self.assertEqual(len(blotter_rows), 2)
            self.assertEqual({row["symbol"] for row in blotter_rows}, {"1101", "1102"})
            self.assertTrue(all(row["execution_date"] == "2024-01-04" for row in blotter_rows))

            with second_result.nav_history_path.open("r", newline="", encoding="utf-8") as handle:
                nav_rows = list(csv.DictReader(handle))
            self.assertEqual([row["date"] for row in nav_rows], ["2024-01-02", "2024-01-03", "2024-01-04"])
            self.assertEqual(nav_rows[-1]["status"], "executed")

            latest_payload = json.loads(second_result.decision_snapshot_path.read_text(encoding="utf-8"))
            self.assertEqual(latest_payload["strategy_id"], "risk_controlled_3m_half_exposure_exp60_delay1")
            self.assertEqual({row["symbol"] for row in latest_payload["previous_holdings"]}, {"1101", "1102"})

    def test_execute_paper_update_blocks_trade_when_execution_day_data_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            config_path = _prepare_cross_sectional_artifacts(
                temp_root,
                walkforward_enabled=False,
                risk_controls_block=PRACTICAL_RISK_BLOCK,
            )
            broken_symbol_path = temp_root / "artifacts" / "processed" / "market_data" / "daily" / "1102.csv"
            surviving_lines = [
                line
                for line in broken_symbol_path.read_text(encoding="utf-8").splitlines()
                if "2024-01-04" not in line
            ]
            broken_symbol_path.write_text("\n".join(surviving_lines) + "\n", encoding="utf-8")

            result = execute_paper_update(config_path, as_of_date=date(2024, 1, 4))

            self.assertAlmostEqual(result.final_nav, 1_000_000.0, places=6)
            self.assertEqual(result.holdings_count, 0)
            with result.blotter_path.open("r", newline="", encoding="utf-8") as handle:
                blotter_rows = list(csv.DictReader(handle))
            self.assertEqual(blotter_rows, [])
            with result.nav_history_path.open("r", newline="", encoding="utf-8") as handle:
                nav_rows = list(csv.DictReader(handle))
            self.assertEqual(nav_rows[-1]["status"], "blocked")
            self.assertIn("Missing current market data", nav_rows[-1]["notes"])

    def test_execute_daily_decision_requires_practical_mainline_settings(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = _prepare_cross_sectional_artifacts(
                Path(temp_dir),
                walkforward_enabled=False,
                risk_controls_block=textwrap.dedent(
                    """
                    [risk_controls]
                    benchmark_filter_enabled = true
                    benchmark_ma_window = 150
                    defensive_mode = "half_exposure"
                    defensive_gross_exposure = 0.6
                    rebalance_cadence_months = 3
                    """
                ).strip(),
            )

            with self.assertRaisesRegex(
                ValueError,
                "benchmark_ma_window = 200",
            ):
                execute_daily_decision(config_path, as_of_date=date(2024, 1, 2))

    def test_execute_paper_update_rebuilds_legacy_negative_cash_state(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = _prepare_cross_sectional_artifacts(
                Path(temp_dir),
                walkforward_enabled=False,
                risk_controls_block=PRACTICAL_RISK_BLOCK,
            )

            initial_result = execute_paper_update(config_path, as_of_date=date(2024, 1, 4))
            broken_lines = []
            with initial_result.state_path.open("r", newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                fieldnames = reader.fieldnames
                assert fieldnames is not None
                for row in reader:
                    if row["symbol"] == "CASH":
                        row["market_value"] = "-1.0"
                        row["cash_balance"] = "-1.0"
                    broken_lines.append(row)
            with initial_result.state_path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(broken_lines)

            repaired_result = execute_paper_update(config_path, as_of_date=date(2024, 1, 4))

            self.assertEqual(repaired_result.status, "paper_ledger_updated")
            self.assertGreaterEqual(repaired_result.cash_balance, 0.0)
            self.assertTrue(
                any("rebuilt the ledger" in note for note in repaired_result.notes)
            )

    def test_execute_paper_update_scales_buys_to_preserve_non_negative_cash(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            benchmark_rows: list[list[str]] = [
                [f"2023-06-{day:02d}", "TAIEX", "100", "100", "100", "100", "", ""]
                for day in range(1, 31)
            ]
            benchmark_rows.extend(
                [
                    [f"2023-07-{day:02d}", "TAIEX", "100", "100", "100", "100", "", ""]
                    for day in range(1, 32)
                ]
            )
            benchmark_rows.extend(
                [
                    [f"2023-08-{day:02d}", "TAIEX", "100", "100", "100", "100", "", ""]
                    for day in range(1, 32)
                ]
            )
            benchmark_rows.extend(
                [
                    [f"2023-09-{day:02d}", "TAIEX", "100", "100", "100", "100", "", ""]
                    for day in range(1, 31)
                ]
            )
            benchmark_rows.extend(
                [
                    [f"2023-10-{day:02d}", "TAIEX", "100", "100", "100", "100", "", ""]
                    for day in range(1, 32)
                ]
            )
            benchmark_rows.extend(
                [
                    [f"2023-11-{day:02d}", "TAIEX", "100", "100", "100", "100", "", ""]
                    for day in range(1, 31)
                ]
            )
            benchmark_rows.extend(
                [
                    [f"2023-12-{day:02d}", "TAIEX", "100", "100", "100", "100", "", ""]
                    for day in range(1, 32)
                ]
            )
            benchmark_rows.extend(
                [
                    ["2024-01-02", "TAIEX", "200", "200", "200", "200", "", ""],
                    ["2024-01-03", "TAIEX", "200", "200", "200", "200", "", ""],
                    ["2024-01-04", "TAIEX", "200", "200", "200", "200", "", ""],
                    ["2024-02-01", "TAIEX", "200", "200", "200", "200", "", ""],
                    ["2024-02-02", "TAIEX", "200", "200", "200", "200", "", ""],
                    ["2024-02-05", "TAIEX", "200", "200", "200", "200", "", ""],
                ]
            )
            config_path = _prepare_cross_sectional_artifacts(
                temp_root,
                walkforward_enabled=False,
                risk_controls_block=PRACTICAL_RISK_BLOCK,
                benchmark_rows=benchmark_rows,
            )
            config_text = config_path.read_text(encoding="utf-8")
            config_text = config_text.replace("commission_bps = 0.0", "commission_bps = 7000.0")
            config_path.write_text(config_text, encoding="utf-8")

            result = execute_paper_update(config_path, as_of_date=date(2024, 1, 4))

            with result.state_path.open("r", newline="", encoding="utf-8") as handle:
                state_rows = list(csv.DictReader(handle))
            cash_row = next(row for row in state_rows if row["symbol"] == "CASH")
            self.assertGreaterEqual(float(cash_row["market_value"]), 0.0)
            self.assertEqual(cash_row["state_status"], "executed_cash_constrained")
            self.assertIn("Cash-constrained execution", cash_row["notes"])

            with result.nav_history_path.open("r", newline="", encoding="utf-8") as handle:
                nav_rows = list(csv.DictReader(handle))
            self.assertEqual(nav_rows[-1]["status"], "executed_cash_constrained")
            self.assertGreaterEqual(float(nav_rows[-1]["cash_balance"]), 0.0)
            self.assertIn("Cash-constrained execution", nav_rows[-1]["notes"])

            with result.blotter_path.open("r", newline="", encoding="utf-8") as handle:
                blotter_rows = list(csv.DictReader(handle))
            self.assertTrue(any("Scaled for cash after costs" in row["notes"] for row in blotter_rows if row["action"] == "buy"))


if __name__ == "__main__":
    unittest.main()
