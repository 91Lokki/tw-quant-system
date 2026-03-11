from __future__ import annotations

from datetime import date
from pathlib import Path
import sys
import unittest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from tw_quant.core.models import PortfolioConfig, SignalRow
from tw_quant.portfolio.construct import build_target_weights, determine_rebalance_dates


class PortfolioConstructTests(unittest.TestCase):
    def test_determine_rebalance_dates_uses_first_trading_day_of_period(self) -> None:
        trading_dates = (
            date(2024, 1, 31),
            date(2024, 2, 1),
            date(2024, 2, 2),
            date(2024, 3, 1),
        )

        rebalance_dates = determine_rebalance_dates(trading_dates, "monthly")

        self.assertEqual(rebalance_dates, (date(2024, 1, 31), date(2024, 2, 1)))

    def test_build_target_weights_selects_positive_signal_symbols(self) -> None:
        portfolio_config = PortfolioConfig(
            tradable_symbols=("2330", "0050"),
            benchmark="TAIEX",
            rebalance_frequency="daily",
            weighting="equal",
            min_signal_score=0.0,
            max_positions=1,
            max_weight=1.0,
            hold_cash_when_inactive=True,
        )
        signal_rows = [
            SignalRow(date(2024, 1, 2), "2330", 100, None, None, 0, None, 0, None, 0, 0.2),
            SignalRow(date(2024, 1, 2), "0050", 50, None, None, 0, None, 0, None, 0, 0.8),
        ]

        weights_by_date, scores_by_date = build_target_weights(
            signal_rows=signal_rows,
            portfolio_config=portfolio_config,
            rebalance_dates=(date(2024, 1, 2),),
        )

        self.assertEqual(weights_by_date[date(2024, 1, 2)]["2330"], 0.0)
        self.assertEqual(weights_by_date[date(2024, 1, 2)]["0050"], 1.0)
        self.assertEqual(scores_by_date[date(2024, 1, 2)]["2330"], 0.2)

    def test_build_target_weights_holds_cash_when_no_positive_signal(self) -> None:
        portfolio_config = PortfolioConfig(
            tradable_symbols=("2330", "0050"),
            benchmark="TAIEX",
            rebalance_frequency="daily",
            weighting="equal",
            min_signal_score=0.0,
            max_positions=2,
            max_weight=1.0,
            hold_cash_when_inactive=True,
        )
        signal_rows = [
            SignalRow(date(2024, 1, 2), "2330", 100, None, None, 0, None, 0, None, 0, 0.0),
            SignalRow(date(2024, 1, 2), "0050", 50, None, None, 0, None, 0, None, 0, -0.3),
        ]

        weights_by_date, _ = build_target_weights(
            signal_rows=signal_rows,
            portfolio_config=portfolio_config,
            rebalance_dates=(date(2024, 1, 2),),
        )

        self.assertEqual(weights_by_date[date(2024, 1, 2)]["2330"], 0.0)
        self.assertEqual(weights_by_date[date(2024, 1, 2)]["0050"], 0.0)


if __name__ == "__main__":
    unittest.main()
