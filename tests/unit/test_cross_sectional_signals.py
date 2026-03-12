from __future__ import annotations

import csv
from datetime import date
from pathlib import Path
import sys
import tempfile
import unittest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from tw_quant.core.models import UniverseMembershipRow
from tw_quant.signals.generate import build_cross_sectional_signal_rows


class CrossSectionalSignalTests(unittest.TestCase):
    def test_build_cross_sectional_signal_rows_ranks_symbols_by_vol_adjusted_momentum(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            _write_bar_csv(
                root / "2330.csv",
                [
                    ["2024-01-02", "2330", "10", "10", "10", "10", "1000", "100"],
                    ["2024-01-03", "2330", "11", "11", "11", "11", "1000", "110"],
                    ["2024-01-04", "2330", "12", "12", "12", "12", "1000", "120"],
                    ["2024-01-05", "2330", "13", "13", "13", "13", "1000", "130"],
                ],
            )
            _write_bar_csv(
                root / "2317.csv",
                [
                    ["2024-01-02", "2317", "10", "10", "10", "10", "1000", "100"],
                    ["2024-01-03", "2317", "10.5", "10.5", "10.5", "10.5", "1000", "110"],
                    ["2024-01-04", "2317", "10.7", "10.7", "10.7", "10.7", "1000", "120"],
                    ["2024-01-05", "2317", "10.8", "10.8", "10.8", "10.8", "1000", "130"],
                ],
            )

            rows = build_cross_sectional_signal_rows(
                normalized_dir=root,
                membership_rows=[
                    UniverseMembershipRow(
                        date=date(2024, 1, 5),
                        symbol="2330",
                        liquidity_rank=1,
                        avg_traded_value_60d=125.0,
                        universe_name="twse_top50_liquidity",
                        is_member=True,
                    ),
                    UniverseMembershipRow(
                        date=date(2024, 1, 5),
                        symbol="2317",
                        liquidity_rank=2,
                        avg_traded_value_60d=125.0,
                        universe_name="twse_top50_liquidity",
                        is_member=True,
                    ),
                ],
                momentum_window=2,
                volatility_window=2,
            )

            self.assertEqual(len(rows), 2)
            ranked = {row.symbol: row for row in rows}
            self.assertEqual(ranked["2330"].factor_rank, 1)
            self.assertEqual(ranked["2317"].factor_rank, 2)
            self.assertIsNotNone(ranked["2330"].signal_score)
            self.assertIsNotNone(ranked["2317"].signal_score)
            self.assertGreater(float(ranked["2330"].signal_score or 0.0), float(ranked["2317"].signal_score or 0.0))


def _write_bar_csv(path: Path, rows: list[list[str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["date", "symbol", "open", "high", "low", "close", "volume", "traded_value"])
        writer.writerows(rows)


if __name__ == "__main__":
    unittest.main()
