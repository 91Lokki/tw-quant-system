from __future__ import annotations

from datetime import date
from pathlib import Path
import csv
import sys
import tempfile
import unittest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from tw_quant.universe.liquidity import (
    build_top_liquidity_membership,
    filter_twse_common_stocks,
    validate_membership_coverage,
)


class UniverseLiquidityTests(unittest.TestCase):
    def test_filter_twse_common_stocks_excludes_non_common_instruments(self) -> None:
        rows = [
            {
                "stock_id": "2330",
                "stock_name": "TSMC",
                "type": "twse",
                "industry_category": "Semiconductor",
                "date": "2024-01-02",
            },
            {
                "stock_id": "0050",
                "stock_name": "Taiwan 50 ETF",
                "type": "twse",
                "industry_category": "ETF",
                "date": "2024-01-02",
            },
            {
                "stock_id": "6488",
                "stock_name": "GlobalWafers",
                "type": "otc",
                "industry_category": "Semiconductor",
                "date": "2024-01-02",
            },
        ]

        filtered = filter_twse_common_stocks(rows)

        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0]["stock_id"], "2330")

    def test_build_top_liquidity_membership_uses_monthly_top_n_ranking(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            _write_bar_csv(
                root / "TAIEX.csv",
                [
                    ["2024-01-02", "TAIEX", "18000", "18000", "18000", "18000", "", ""],
                    ["2024-01-03", "TAIEX", "18010", "18010", "18010", "18010", "", ""],
                    ["2024-02-01", "TAIEX", "18100", "18100", "18100", "18100", "", ""],
                ],
            )
            _write_bar_csv(
                root / "2330.csv",
                [
                    ["2024-01-02", "2330", "100", "101", "99", "100", "1000", "100"],
                    ["2024-01-03", "2330", "101", "102", "100", "101", "1000", "110"],
                    ["2024-02-01", "2330", "102", "103", "101", "102", "1000", "120"],
                ],
            )
            _write_bar_csv(
                root / "2317.csv",
                [
                    ["2024-01-02", "2317", "200", "201", "199", "200", "1000", "130"],
                    ["2024-01-03", "2317", "201", "202", "200", "201", "1000", "140"],
                    ["2024-02-01", "2317", "202", "203", "201", "202", "1000", "150"],
                ],
            )
            _write_bar_csv(
                root / "1101.csv",
                [
                    ["2024-01-02", "1101", "50", "51", "49", "50", "1000", "80"],
                    ["2024-01-03", "1101", "51", "52", "50", "51", "1000", "90"],
                    ["2024-02-01", "1101", "52", "53", "51", "52", "1000", "100"],
                ],
            )

            rows, reconstitution_dates, participating_symbols, notes = build_top_liquidity_membership(
                normalized_dir=root,
                candidate_symbols=("2330", "2317", "1101"),
                benchmark_symbol="TAIEX",
                start_date=date(2024, 1, 2),
                end_date=date(2024, 2, 1),
                liquidity_lookback_days=2,
                top_n=2,
            )

            self.assertEqual(reconstitution_dates, (date(2024, 1, 2), date(2024, 2, 1)))
            self.assertEqual(participating_symbols, ("2317", "2330"))
            self.assertEqual(notes, ())
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0].date, date(2024, 2, 1))
            self.assertEqual(rows[0].symbol, "2317")
            self.assertEqual(rows[0].liquidity_rank, 1)
            self.assertAlmostEqual(rows[0].avg_traded_value_60d, 145.0)
            self.assertEqual(rows[1].symbol, "2330")
            self.assertEqual(rows[1].liquidity_rank, 2)

    def test_build_top_liquidity_membership_reports_missing_bar_symbols(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            _write_bar_csv(
                root / "TAIEX.csv",
                [
                    ["2024-02-01", "TAIEX", "18100", "18100", "18100", "18100", "", ""],
                    ["2024-02-02", "TAIEX", "18110", "18110", "18110", "18110", "", ""],
                ],
            )
            _write_bar_csv(
                root / "2330.csv",
                [
                    ["2024-01-30", "2330", "100", "101", "99", "100", "1000", "100"],
                    ["2024-01-31", "2330", "101", "102", "100", "101", "1000", "110"],
                    ["2024-02-01", "2330", "102", "103", "101", "102", "1000", "120"],
                ],
            )

            rows, _, _, notes = build_top_liquidity_membership(
                normalized_dir=root,
                candidate_symbols=("2330", "1107"),
                benchmark_symbol="TAIEX",
                start_date=date(2024, 1, 30),
                end_date=date(2024, 2, 2),
                liquidity_lookback_days=2,
                top_n=1,
            )

            self.assertEqual(len(rows), 1)
            self.assertTrue(notes)
            self.assertIn("缺少 normalized bars", notes[0])

    def test_validate_membership_coverage_rejects_incomplete_top_n_membership(self) -> None:
        with self.assertRaisesRegex(ValueError, "only has 2 members, expected 3"):
            validate_membership_coverage(
                rows=[
                    _membership_row(date(2024, 1, 2), "1101", 1),
                    _membership_row(date(2024, 1, 2), "1102", 2),
                ],
                expected_top_n=3,
            )


def _write_bar_csv(path: Path, rows: list[list[str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["date", "symbol", "open", "high", "low", "close", "volume", "traded_value"])
        writer.writerows(rows)


def _membership_row(row_date: date, symbol: str, rank: int):
    from tw_quant.core.models import UniverseMembershipRow

    return UniverseMembershipRow(
        date=row_date,
        symbol=symbol,
        liquidity_rank=rank,
        avg_traded_value_60d=100.0,
        universe_name="twse_top50_liquidity",
        is_member=True,
    )


if __name__ == "__main__":
    unittest.main()
