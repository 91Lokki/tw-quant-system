from __future__ import annotations

from datetime import date
from pathlib import Path
import sys
import unittest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from tw_quant.data.normalize import NORMALIZED_BAR_COLUMNS, normalize_benchmark_daily, normalize_security_daily
from tw_quant.data.providers import ProviderPayload


class NormalizeTests(unittest.TestCase):
    def test_normalize_security_daily_schema(self) -> None:
        payload = ProviderPayload(
            dataset="TaiwanStockPrice",
            symbol="2330",
            rows=[
                {
                    "date": "2024-01-02",
                    "stock_id": "2330",
                    "open": 590,
                    "max": 593,
                    "min": 589,
                    "close": 591,
                    "Trading_Volume": 12345678,
                }
            ],
            raw_payload={"status": 200, "data": []},
        )

        rows = normalize_security_daily(payload)

        self.assertEqual(NORMALIZED_BAR_COLUMNS, ("date", "symbol", "open", "high", "low", "close", "volume"))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].date, date(2024, 1, 2))
        self.assertEqual(rows[0].symbol, "2330")
        self.assertEqual(rows[0].open, 590.0)
        self.assertEqual(rows[0].high, 593.0)
        self.assertEqual(rows[0].low, 589.0)
        self.assertEqual(rows[0].close, 591.0)
        self.assertEqual(rows[0].volume, 12345678)

    def test_normalize_benchmark_daily_maps_single_price_to_ohlc(self) -> None:
        payload = ProviderPayload(
            dataset="TaiwanStockTotalReturnIndex",
            symbol="TAIEX",
            rows=[
                {
                    "date": "2024-01-02",
                    "stock_id": "TAIEX",
                    "price": 17853.76,
                }
            ],
            raw_payload={"status": 200, "data": []},
        )

        rows = normalize_benchmark_daily(payload)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].symbol, "TAIEX")
        self.assertEqual(rows[0].open, 17853.76)
        self.assertEqual(rows[0].high, 17853.76)
        self.assertEqual(rows[0].low, 17853.76)
        self.assertEqual(rows[0].close, 17853.76)
        self.assertIsNone(rows[0].volume)


if __name__ == "__main__":
    unittest.main()
