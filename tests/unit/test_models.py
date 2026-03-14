from __future__ import annotations

from datetime import date
from pathlib import Path
import sys
import unittest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from tw_quant.core.models import (
    BacktestConfig,
    BacktestEngineConfig,
    BacktestResult,
    DataPaths,
    PerformanceMetrics,
    PortfolioConfig,
    RiskControlConfig,
    TradingCosts,
    WalkForwardConfig,
)


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
            portfolio=PortfolioConfig(
                tradable_symbols=("2330", "0050"),
                benchmark="TAIEX",
                rebalance_frequency="monthly",
                weighting="equal",
                min_signal_score=0.0,
                max_positions=2,
                max_weight=1.0,
                hold_cash_when_inactive=True,
            ),
            risk_controls=RiskControlConfig(
                benchmark_filter_enabled=False,
                benchmark_ma_window=200,
                defensive_mode="cash",
                defensive_gross_exposure=0.5,
                rebalance_cadence_months=1,
            ),
            backtest=BacktestEngineConfig(
                initial_nav=1.0,
                bar_input_dir=PROJECT_ROOT / "data" / "processed" / "market_data" / "daily",
                signal_input_path=PROJECT_ROOT / "data" / "processed" / "signals" / "daily" / "signal_panel.csv",
                output_dir=PROJECT_ROOT / "data" / "processed" / "backtests",
                nav_file="daily_nav.csv",
                weights_file="daily_weights.csv",
            ),
            walkforward=WalkForwardConfig(
                enabled=True,
                window_type="expanding",
                train_window_days=252,
                test_window_days=63,
                minimum_history_days=252,
            ),
        )

        self.assertEqual(config.date_range_label(), "2021-01-01 to 2021-12-31")

    def test_backtest_result_summary_text(self) -> None:
        result = BacktestResult(
            project_name="demo",
            market="TW cash equities",
            universe="demo_universe",
            benchmark="TAIEX",
            tradable_symbols=("2330", "0050"),
            rebalance_frequency="monthly",
            rebalance_cadence_months=1,
            trading_costs=TradingCosts(
                commission_bps=14.25,
                tax_bps=30.0,
                slippage_bps=5.0,
            ),
            hold_cash_when_inactive=True,
            benchmark_filter_enabled=True,
            benchmark_ma_window=200,
            defensive_mode="cash",
            defensive_gross_exposure=0.5,
            start_date=date(2021, 1, 1),
            end_date=date(2021, 12, 31),
            report_path=Path("data/processed/reports/demo/backtest_summary.md"),
            nav_path=Path("data/processed/backtests/demo/daily_nav.csv"),
            weights_path=Path("data/processed/backtests/demo/daily_weights.csv"),
            equity_curve_path=Path("data/processed/reports/demo/equity_curve.svg"),
            drawdown_path=Path("data/processed/reports/demo/drawdown.svg"),
            comparison_path=Path("data/processed/backtests/demo/risk_comparison.csv"),
            metrics=PerformanceMetrics(
                cumulative_return=0.12,
                annualized_return=0.11,
                annualized_volatility=0.2,
                max_drawdown=-0.08,
                sharpe_ratio=0.55,
                turnover=1.3,
            ),
            final_nav=1.12,
            benchmark_final_nav=1.08,
            status="local-data backtest completed",
            notes=("First note.", "Second note."),
        )

        summary = result.summary_text()
        summary_zh = result.summary_text_zh()

        self.assertIn("Project: demo", summary)
        self.assertIn("Tradable Symbols: 2330, 0050", summary)
        self.assertIn("Benchmark Filter Enabled: True", summary)
        self.assertIn("Status: local-data backtest completed", summary)
        self.assertIn("- First note.", summary)
        self.assertIn("回測完成", summary_zh)
        self.assertIn("Benchmark regime filter: 開啟", summary_zh)
        self.assertIn("累積報酬: 12.00%", summary_zh)
        self.assertIn("權益曲線圖:", summary_zh)


if __name__ == "__main__":
    unittest.main()
