from __future__ import annotations

from datetime import date
from pathlib import Path
import sys
import unittest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from tw_quant.core.models import MarketDataset, NormalizedBar, SignalConfig
from tw_quant.signals.generate import build_signal_rows


class SignalGenerationTests(unittest.TestCase):
    def test_build_signal_rows_produces_expected_metrics(self) -> None:
        dataset = MarketDataset(
            symbols=("2330",),
            start_date=date(2024, 1, 2),
            end_date=date(2024, 1, 5),
            bars_by_symbol={
                "2330": (
                    NormalizedBar(date(2024, 1, 2), "2330", 10, 10, 10, 10, 1000),
                    NormalizedBar(date(2024, 1, 3), "2330", 12, 12, 12, 12, 1000),
                    NormalizedBar(date(2024, 1, 4), "2330", 15, 15, 15, 15, 1000),
                    NormalizedBar(date(2024, 1, 5), "2330", 14, 14, 14, 14, 1000),
                )
            },
            aligned_dates=(
                date(2024, 1, 2),
                date(2024, 1, 3),
                date(2024, 1, 4),
                date(2024, 1, 5),
            ),
            notes=(),
        )
        signal_config = SignalConfig(
            mode="time_series_baseline",
            enabled_symbols=("2330",),
            benchmark="TAIEX",
            ma_fast_window=2,
            ma_slow_window=3,
            momentum_window=2,
            volatility_window=2,
            volatility_cap=0.5,
            align_by_date=True,
            input_dir=PROJECT_ROOT / "data" / "processed" / "market_data" / "daily",
            output_dir=PROJECT_ROOT / "data" / "processed" / "signals" / "daily",
            output_file="signal_panel.csv",
        )

        rows = build_signal_rows(dataset, signal_config)

        self.assertEqual(len(rows), 4)
        target = rows[2]
        self.assertEqual(target.date, date(2024, 1, 4))
        self.assertAlmostEqual(target.ma_fast or 0.0, 13.5)
        self.assertAlmostEqual(target.ma_slow or 0.0, 12.333333333333334)
        self.assertEqual(target.trend_signal, 1)
        self.assertAlmostEqual(target.momentum_n or 0.0, 0.5)
        self.assertEqual(target.momentum_signal, 1)
        self.assertEqual(target.volatility_filter, 1)
        self.assertAlmostEqual(target.signal_score, 1.0)

        last_row = rows[-1]
        self.assertEqual(last_row.date, date(2024, 1, 5))
        self.assertEqual(last_row.volatility_filter, 0)
        self.assertEqual(last_row.signal_score, 0.0)


if __name__ == "__main__":
    unittest.main()
