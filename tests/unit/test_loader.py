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

from tw_quant.data.loader import load_market_dataset


class LoaderTests(unittest.TestCase):
    def test_load_market_dataset_aligns_dates_and_filters_range(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            _write_csv(
                root / "2330.csv",
                [
                    ["2024-01-02", "2330", "100", "101", "99", "100", "1000"],
                    ["2024-01-03", "2330", "101", "102", "100", "101", "1100"],
                    ["2024-01-04", "2330", "102", "103", "101", "102", "1200"],
                ],
            )
            _write_csv(
                root / "0050.csv",
                [
                    ["2024-01-03", "0050", "50", "51", "49", "50", "2000"],
                    ["2024-01-04", "0050", "51", "52", "50", "51", "2100"],
                    ["2024-01-05", "0050", "52", "53", "51", "52", "2200"],
                ],
            )

            dataset = load_market_dataset(
                normalized_dir=root,
                symbols=("2330", "0050"),
                start_date=date(2024, 1, 2),
                end_date=date(2024, 1, 5),
                align_by_date=True,
            )

            self.assertEqual(dataset.aligned_dates, (date(2024, 1, 3), date(2024, 1, 4)))
            self.assertEqual(len(dataset.bars_by_symbol["2330"]), 2)
            self.assertEqual(len(dataset.bars_by_symbol["0050"]), 2)
            self.assertEqual(dataset.row_count, 4)

    def test_load_market_dataset_validates_required_columns(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            path = root / "TAIEX.csv"
            with path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.writer(handle)
                writer.writerow(["date", "symbol", "open", "high", "low", "close"])
                writer.writerow(["2024-01-02", "TAIEX", "18000", "18000", "18000", "18000"])

            with self.assertRaisesRegex(ValueError, "Missing required columns"):
                load_market_dataset(
                    normalized_dir=root,
                    symbols=("TAIEX",),
                    start_date=date(2024, 1, 2),
                    end_date=date(2024, 1, 2),
                    align_by_date=True,
                )


def _write_csv(path: Path, rows: list[list[str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["date", "symbol", "open", "high", "low", "close", "volume"])
        writer.writerows(rows)


if __name__ == "__main__":
    unittest.main()
