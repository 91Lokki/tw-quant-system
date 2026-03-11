from __future__ import annotations

from datetime import date
from pathlib import Path
import sys
import unittest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from tw_quant.backtest.metrics import compute_metrics
from tw_quant.core.models import NavRow


class BacktestMetricsTests(unittest.TestCase):
    def test_compute_metrics_returns_expected_summary(self) -> None:
        nav_rows = [
            NavRow(date(2024, 1, 2), 1.0, 0.0, 0.0, 1.0, 0.0, 0.5, 0.0, 1.0),
            NavRow(date(2024, 1, 3), 1.1, 0.1, 0.1, 1.02, 0.02, 0.0, 0.0, 0.0),
            NavRow(date(2024, 1, 4), 1.05, -0.045454545454545414, -0.03, 1.01, -0.01, 0.5, 0.01, 0.5),
        ]

        metrics = compute_metrics(nav_rows, initial_nav=1.0)

        self.assertAlmostEqual(metrics.cumulative_return, 0.05)
        self.assertLess(metrics.max_drawdown, 0.0)
        self.assertGreater(metrics.annualized_volatility, 0.0)
        self.assertAlmostEqual(metrics.turnover, 1.0)


if __name__ == "__main__":
    unittest.main()
