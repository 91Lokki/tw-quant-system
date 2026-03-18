from __future__ import annotations

from datetime import date
from pathlib import Path
import sys
import tempfile
import unittest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from tw_quant.core.models import BacktestResult, PerformanceMetrics, TradingCosts
from tw_quant.reporting.report import build_report


class ReportingTests(unittest.TestCase):
    def test_build_report_writes_markdown_and_svg_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            nav_path = temp_root / "daily_nav.csv"
            nav_path.write_text(
                "\n".join(
                    [
                        "date,nav,daily_return,gross_return,benchmark_nav,benchmark_return,turnover,transaction_cost,cash_weight",
                        "2024-01-02,1.0,0.0,0.0,1.0,0.0,0.0,0.0,1.0",
                        "2024-01-03,1.05,0.05,0.05,1.01,0.01,0.5,0.0,0.0",
                        "2024-01-04,1.02,-0.0285714286,-0.0285714286,1.0,-0.00990099,0.0,0.0,0.0",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            report_dir = temp_root / "reports" / "demo"
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
                execution_delay_days=1,
                portfolio_max_weight=0.08,
                start_date=date(2024, 1, 2),
                end_date=date(2024, 1, 4),
                report_path=report_dir / "backtest_summary.md",
                nav_path=nav_path,
                weights_path=temp_root / "daily_weights.csv",
                equity_curve_path=report_dir / "equity_curve.svg",
                drawdown_path=report_dir / "drawdown.svg",
                comparison_path=report_dir / "risk_comparison.csv",
                metrics=PerformanceMetrics(
                    cumulative_return=0.02,
                    annualized_return=0.18,
                    annualized_volatility=0.16,
                    max_drawdown=-0.0285714286,
                    sharpe_ratio=1.125,
                    turnover=0.5,
                ),
                final_nav=1.02,
                benchmark_final_nav=1.0,
                status="local-data backtest completed",
                notes=("Sample note.",),
            )

            output_path = build_report(result)

            self.assertEqual(output_path, str(result.report_path))
            self.assertTrue(result.report_path.exists())
            self.assertTrue(result.equity_curve_path.exists())
            self.assertTrue(result.drawdown_path.exists())

            report_text = result.report_path.read_text(encoding="utf-8")
            self.assertIn("# demo Backtest Summary", report_text)
            self.assertIn("- Tradable Symbols: 2330, 0050", report_text)
            self.assertIn("- Rebalance Frequency: monthly", report_text)
            self.assertIn("- Benchmark Regime Filter: enabled", report_text)
            self.assertIn("- Defensive Gross Exposure: 50%", report_text)
            self.assertIn("- Execution Delay Days: 1", report_text)
            self.assertIn("- Portfolio Max Weight: 8%", report_text)
            self.assertIn("- Risk Comparison CSV:", report_text)
            self.assertIn(
                "Operational Mainline: `risk_controlled_3m_half_exposure_exp60_delay1`",
                report_text,
            )
            self.assertIn("Conservative Appendix: `risk_controlled_3m_half_exposure_exp60_w08`", report_text)
            self.assertIn("![Equity Curve](equity_curve.svg)", report_text)
            self.assertIn("![Drawdown](drawdown.svg)", report_text)
            self.assertIn("## Known Limitations", report_text)

            equity_svg = result.equity_curve_path.read_text(encoding="utf-8")
            drawdown_svg = result.drawdown_path.read_text(encoding="utf-8")
            self.assertIn("Equity Curve", equity_svg)
            self.assertIn("Drawdown", drawdown_svg)


if __name__ == "__main__":
    unittest.main()
