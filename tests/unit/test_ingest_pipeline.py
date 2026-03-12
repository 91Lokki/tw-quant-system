from __future__ import annotations

from datetime import date
from pathlib import Path
import sys
import tempfile
import textwrap
import unittest
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from tw_quant.data.providers import ProviderPayload
from tw_quant.pipelines.ingest import execute_ingest


class FakeProvider:
    name = "finmind"

    def fetch_security_daily(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
    ) -> ProviderPayload:
        return ProviderPayload(
            dataset="TaiwanStockPrice",
            symbol=symbol,
            rows=[
                {
                    "date": start_date.isoformat(),
                    "stock_id": symbol,
                    "open": 100,
                    "max": 110,
                    "min": 95,
                    "close": 108,
                    "Trading_Volume": 1000,
                },
                {
                    "date": end_date.isoformat(),
                    "stock_id": symbol,
                    "open": 109,
                    "max": 112,
                    "min": 107,
                    "close": 111,
                    "Trading_Volume": 2000,
                },
            ],
            raw_payload={"status": 200, "data": [{"stock_id": symbol}]},
        )

    def fetch_benchmark_daily(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
    ) -> ProviderPayload:
        return ProviderPayload(
            dataset="TaiwanStockTotalReturnIndex",
            symbol=symbol,
            rows=[
                {"date": start_date.isoformat(), "stock_id": symbol, "price": 17000},
                {"date": end_date.isoformat(), "stock_id": symbol, "price": 18000},
            ],
            raw_payload={"status": 200, "data": [{"stock_id": symbol}]},
        )


class CrossSectionalTwseProvider:
    name = "twse"

    def fetch_security_daily(self, symbol: str, start_date: date, end_date: date) -> ProviderPayload:
        raise AssertionError("per-symbol security fetch should not be used for the TWSE cross-sectional branch")

    def fetch_benchmark_daily(self, symbol: str, start_date: date, end_date: date) -> ProviderPayload:
        raise AssertionError("single-range benchmark fetch should not be used for the TWSE cross-sectional branch")

    def fetch_stock_info(self) -> ProviderPayload:
        raise AssertionError("stock-info fetch should not be used for the TWSE cross-sectional branch")

    def fetch_benchmark_month(self, symbol: str, month_anchor: date) -> ProviderPayload:
        return ProviderPayload(
            dataset="TWSE_TAIEX_HISTORY",
            symbol=symbol,
            rows=[
                {"date": "2024-01-02", "stock_id": symbol, "price": 17000},
                {"date": "2024-01-03", "stock_id": symbol, "price": 17100},
                {"date": "2024-01-31", "stock_id": symbol, "price": 17200},
            ],
            raw_payload={"stat": "OK", "data": []},
        )

    def fetch_market_snapshot(self, trading_date: date) -> ProviderPayload:
        rows_by_date = {
            "2024-01-02": [
                _twse_row("1101", "Taiwan Cement", "2024-01-02", 50, 51, 49, 50.5, 1000, 50000),
                _twse_row("2330", "TSMC", "2024-01-02", 600, 603, 598, 602, 2000, 1204000),
                _twse_row("0050", "Taiwan 50 ETF", "2024-01-02", 140, 141, 139, 140.5, 3000, 420000),
            ],
            "2024-01-03": [
                _twse_row("1101", "Taiwan Cement", "2024-01-03", 50.5, 51.5, 50, 51, 1100, 56100),
                _twse_row("2330", "TSMC", "2024-01-03", 602, 605, 601, 604, 2100, 1268400),
                _twse_row("0050", "Taiwan 50 ETF", "2024-01-03", 140.5, 141.5, 140, 141, 2900, 408900),
            ],
            "2024-01-31": [
                _twse_row("1101", "Taiwan Cement", "2024-01-31", 52, 53, 51, 52.5, 1200, 63000),
                _twse_row("2330", "TSMC", "2024-01-31", 620, 625, 618, 623, 1900, 1183700),
            ],
        }
        row_date = trading_date.isoformat()
        return ProviderPayload(
            dataset="TWSE_MI_INDEX",
            symbol=row_date,
            rows=rows_by_date[row_date],
            raw_payload={"stat": "OK", "data": []},
        )


class CrossSectionalSmallUniverseTwseProvider(CrossSectionalTwseProvider):
    def fetch_market_snapshot(self, trading_date: date) -> ProviderPayload:
        row_date = trading_date.isoformat()
        rows_by_date = {
            "2024-01-02": [
                _twse_row("1101", "Taiwan Cement", "2024-01-02", 50, 51, 49, 50.5, 1000, 50000),
                _twse_row("0050", "Taiwan 50 ETF", "2024-01-02", 140, 141, 139, 140.5, 3000, 420000),
            ],
            "2024-01-03": [
                _twse_row("1101", "Taiwan Cement", "2024-01-03", 50.5, 51.5, 50, 51, 1100, 56100),
            ],
            "2024-01-31": [
                _twse_row("1101", "Taiwan Cement", "2024-01-31", 52, 53, 51, 52.5, 1200, 63000),
            ],
        }
        return ProviderPayload(
            dataset="TWSE_MI_INDEX",
            symbol=row_date,
            rows=rows_by_date[row_date],
            raw_payload={"stat": "OK", "data": []},
        )


class FailOnFetchProvider:
    name = "finmind"

    def fetch_security_daily(self, symbol: str, start_date: date, end_date: date) -> ProviderPayload:
        raise AssertionError(f"security fetch should not be called for {symbol}")

    def fetch_benchmark_daily(self, symbol: str, start_date: date, end_date: date) -> ProviderPayload:
        raise AssertionError(f"benchmark fetch should not be called for {symbol}")


def _twse_row(
    stock_id: str,
    stock_name: str,
    row_date: str,
    open_price: float,
    high_price: float,
    low_price: float,
    close_price: float,
    volume: int,
    traded_value: int,
) -> dict[str, object]:
    return {
        "date": row_date,
        "stock_id": stock_id,
        "stock_name": stock_name,
        "open": open_price,
        "max": high_price,
        "min": low_price,
        "close": close_price,
        "Trading_Volume": volume,
        "Trading_money": traded_value,
    }


class IngestPipelineTests(unittest.TestCase):
    def test_execute_ingest_writes_files_and_uses_cache(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            config_path = temp_root / "settings.toml"
            config_path.write_text(
                textwrap.dedent(
                    """
                    project_name = "ingest_test"
                    market = "TW cash equities"
                    universe = "unit_test_universe"
                    benchmark = "TAIEX"
                    start_date = "2024-01-02"
                    end_date = "2024-01-31"

                    [paths]
                    project_root = "."
                    raw = "data/raw"
                    processed = "data/processed"
                    reports = "data/processed/reports"

                    [costs]
                    commission_bps = 10.0
                    tax_bps = 30.0
                    slippage_bps = 5.0

                    [ingest]
                    provider = "finmind"
                    symbols = ["2330", "0050"]
                    refresh = false
                    storage_format = "csv"
                    token_env_var = "FINMIND_API_TOKEN"
                    raw_cache_subdir = "finmind"
                    normalized_subdir = "market_data/daily"

                    [signals]
                    enabled_symbols = ["2330", "0050"]
                    benchmark = "TAIEX"
                    ma_fast_window = 20
                    ma_slow_window = 60
                    momentum_window = 20
                    volatility_window = 20
                    volatility_cap = 0.35
                    align_by_date = true
                    input_subdir = "market_data/daily"
                    output_subdir = "signals/daily"
                    output_file = "signal_panel.csv"

                    [portfolio]
                    tradable_symbols = ["2330", "0050"]
                    benchmark = "TAIEX"
                    rebalance_frequency = "monthly"
                    weighting = "equal"
                    min_signal_score = 0.0
                    max_positions = 2
                    max_weight = 1.0
                    hold_cash_when_inactive = true

                    [backtest]
                    initial_nav = 1.0
                    bar_input_subdir = "market_data/daily"
                    signal_input_subdir = "signals/daily"
                    signal_input_file = "signal_panel.csv"
                    output_subdir = "backtests"
                    nav_file = "daily_nav.csv"
                    weights_file = "daily_weights.csv"
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            with patch("tw_quant.pipelines.ingest.build_provider", return_value=FakeProvider()):
                first_result = execute_ingest(config_path)

            normalized_dir = temp_root / "data" / "processed" / "market_data" / "daily"
            raw_dir = temp_root / "data" / "raw" / "finmind"

            self.assertEqual(len(first_result.datasets), 3)
            self.assertTrue((normalized_dir / "2330.csv").exists())
            self.assertTrue((normalized_dir / "0050.csv").exists())
            self.assertTrue((normalized_dir / "TAIEX.csv").exists())
            self.assertTrue((raw_dir / "TaiwanStockPrice" / "2330_2024-01-02_2024-01-31.json").exists())
            self.assertTrue((raw_dir / "TaiwanStockPrice" / "0050_2024-01-02_2024-01-31.json").exists())
            self.assertTrue(
                (raw_dir / "TaiwanStockTotalReturnIndex" / "TAIEX_2024-01-02_2024-01-31.json").exists()
            )
            self.assertTrue(all(not dataset.from_cache for dataset in first_result.datasets))

            with patch("tw_quant.pipelines.ingest.build_provider", return_value=FailOnFetchProvider()):
                second_result = execute_ingest(config_path)

            self.assertTrue(all(dataset.from_cache for dataset in second_result.datasets))
            self.assertIn("TAIEX", second_result.summary_text_zh())

    def test_execute_ingest_cross_sectional_writes_metadata_and_candidate_bars(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            config_path = temp_root / "cross_sectional.toml"
            config_path.write_text(
                textwrap.dedent(
                    """
                    project_name = "tw_top50_liquidity_v1"
                    market = "TW cash equities"
                    universe = "twse_top50_liquidity"
                    benchmark = "TAIEX"
                    start_date = "2024-01-02"
                    end_date = "2024-01-31"

                    [research]
                    branch = "tw_top50_liquidity_cross_sectional"

                    [paths]
                    project_root = "."
                    raw = "data/raw"
                    processed = "data/processed"
                    reports = "data/processed/reports"

                    [costs]
                    commission_bps = 10.0
                    tax_bps = 30.0
                    slippage_bps = 5.0

                    [ingest]
                    provider = "twse"
                    symbols = []
                    refresh = false
                    storage_format = "csv"
                    raw_cache_subdir = "twse"
                    normalized_subdir = "market_data/daily"

                    [universe_selection]
                    candidate_market = "twse"
                    selection_rule = "top_liquidity"
                    liquidity_lookback_days = 60
                    top_n = 2
                    reconstitution_frequency = "monthly"
                    metadata_output_subdir = "metadata"
                    membership_output_subdir = "universe"
                    membership_file = "tw_top50_liquidity_membership.csv"

                    [signals]
                    mode = "cross_sectional_vol_adj_momentum"
                    enabled_symbols = []
                    benchmark = "TAIEX"
                    ma_fast_window = 20
                    ma_slow_window = 60
                    momentum_window = 126
                    volatility_window = 20
                    volatility_cap = 0.35
                    align_by_date = false
                    input_subdir = "market_data/daily"
                    output_subdir = "signals/monthly"
                    output_file = "cross_sectional_signal_panel.csv"

                    [portfolio]
                    tradable_symbols = []
                    benchmark = "TAIEX"
                    rebalance_frequency = "monthly"
                    weighting = "equal"
                    min_signal_score = 0.0
                    max_positions = 10
                    max_weight = 0.1
                    hold_cash_when_inactive = true

                    [backtest]
                    initial_nav = 1.0
                    bar_input_subdir = "market_data/daily"
                    signal_input_subdir = "signals/monthly"
                    signal_input_file = "cross_sectional_signal_panel.csv"
                    output_subdir = "backtests"
                    nav_file = "daily_nav.csv"
                    weights_file = "daily_weights.csv"

                    [walkforward]
                    enabled = false
                    window_type = "expanding"
                    train_window_days = 252
                    test_window_days = 63
                    minimum_history_days = 252
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            with patch("tw_quant.pipelines.ingest.build_provider", return_value=CrossSectionalTwseProvider()):
                result = execute_ingest(config_path)

            processed_root = temp_root / "data" / "processed"
            metadata_path = processed_root / "metadata" / "twse_stock_info.csv"
            normalized_dir = processed_root / "market_data" / "daily"
            self.assertEqual(result.research_branch, "tw_top50_liquidity_cross_sectional")
            self.assertEqual(result.candidate_symbol_count, 2)
            self.assertEqual(result.provider, "twse")
            self.assertEqual(result.metadata_path.resolve(), metadata_path.resolve())
            self.assertTrue(metadata_path.exists())
            self.assertTrue((normalized_dir / "1101.csv").exists())
            self.assertTrue((normalized_dir / "2330.csv").exists())
            self.assertTrue((normalized_dir / "TAIEX.csv").exists())
            self.assertEqual(len(result.datasets), 3)
            self.assertTrue(
                (
                    temp_root
                    / "data"
                    / "raw"
                    / "twse"
                    / "TWSE_MI_INDEX"
                    / "2024-01-02_2024-01-02_2024-01-02.json"
                ).exists()
            )
            metadata_text = metadata_path.read_text(encoding="utf-8")
            self.assertIn("1101", metadata_text)
            self.assertIn("2330", metadata_text)
            self.assertNotIn("0050", metadata_text)
            normalized_text = (normalized_dir / "2330.csv").read_text(encoding="utf-8")
            self.assertIn("traded_value", normalized_text.splitlines()[0])
            self.assertIn("Metadata 檔案", result.summary_text_zh())
            self.assertIn("TWSE 官方每日全市場收盤資料", result.summary_text_zh())

    def test_execute_ingest_cross_sectional_reuses_cached_twse_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            config_path = temp_root / "cross_sectional_skip.toml"
            config_path.write_text(
                textwrap.dedent(
                    """
                    project_name = "tw_top50_liquidity_v1"
                    market = "TW cash equities"
                    universe = "twse_top50_liquidity"
                    benchmark = "TAIEX"
                    start_date = "2024-01-02"
                    end_date = "2024-01-31"

                    [research]
                    branch = "tw_top50_liquidity_cross_sectional"

                    [paths]
                    project_root = "."
                    raw = "data/raw"
                    processed = "data/processed"
                    reports = "data/processed/reports"

                    [costs]
                    commission_bps = 10.0
                    tax_bps = 30.0
                    slippage_bps = 5.0

                    [ingest]
                    provider = "twse"
                    symbols = []
                    refresh = false
                    storage_format = "csv"
                    raw_cache_subdir = "twse"
                    normalized_subdir = "market_data/daily"

                    [universe_selection]
                    candidate_market = "twse"
                    selection_rule = "top_liquidity"
                    liquidity_lookback_days = 60
                    top_n = 1
                    reconstitution_frequency = "monthly"
                    metadata_output_subdir = "metadata"
                    membership_output_subdir = "universe"
                    membership_file = "tw_top50_liquidity_membership.csv"

                    [signals]
                    mode = "cross_sectional_vol_adj_momentum"
                    enabled_symbols = []
                    benchmark = "TAIEX"
                    ma_fast_window = 20
                    ma_slow_window = 60
                    momentum_window = 126
                    volatility_window = 20
                    volatility_cap = 0.35
                    align_by_date = false
                    input_subdir = "market_data/daily"
                    output_subdir = "signals/monthly"
                    output_file = "cross_sectional_signal_panel.csv"

                    [portfolio]
                    tradable_symbols = []
                    benchmark = "TAIEX"
                    rebalance_frequency = "monthly"
                    weighting = "equal"
                    min_signal_score = 0.0
                    max_positions = 10
                    max_weight = 0.1
                    hold_cash_when_inactive = true

                    [backtest]
                    initial_nav = 1.0
                    bar_input_subdir = "market_data/daily"
                    signal_input_subdir = "signals/monthly"
                    signal_input_file = "cross_sectional_signal_panel.csv"
                    output_subdir = "backtests"
                    nav_file = "daily_nav.csv"
                    weights_file = "daily_weights.csv"

                    [walkforward]
                    enabled = false
                    window_type = "expanding"
                    train_window_days = 252
                    test_window_days = 63
                    minimum_history_days = 252
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            with patch("tw_quant.pipelines.ingest.build_provider", return_value=CrossSectionalTwseProvider()):
                first_result = execute_ingest(config_path)

            self.assertTrue(first_result.usable_metadata_path is not None)
            self.assertTrue(first_result.availability_path is not None)
            with patch("tw_quant.pipelines.ingest.build_provider", return_value=FailOnFetchProvider()):
                cached_result = execute_ingest(config_path)

            self.assertTrue(all(dataset.from_cache for dataset in cached_result.datasets))
            self.assertEqual(cached_result.provider, "twse")
            self.assertIn("本地 TWSE normalized bars", cached_result.summary_text_zh())

    def test_execute_ingest_cross_sectional_fails_when_effective_candidate_pool_is_too_small(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            config_path = temp_root / "cross_sectional_fail.toml"
            config_path.write_text(
                textwrap.dedent(
                    """
                    project_name = "tw_top50_liquidity_v1"
                    market = "TW cash equities"
                    universe = "twse_top50_liquidity"
                    benchmark = "TAIEX"
                    start_date = "2024-01-02"
                    end_date = "2024-01-31"

                    [research]
                    branch = "tw_top50_liquidity_cross_sectional"

                    [paths]
                    project_root = "."
                    raw = "data/raw"
                    processed = "data/processed"
                    reports = "data/processed/reports"

                    [costs]
                    commission_bps = 10.0
                    tax_bps = 30.0
                    slippage_bps = 5.0

                    [ingest]
                    provider = "twse"
                    symbols = []
                    refresh = false
                    storage_format = "csv"
                    raw_cache_subdir = "twse"
                    normalized_subdir = "market_data/daily"

                    [universe_selection]
                    candidate_market = "twse"
                    selection_rule = "top_liquidity"
                    liquidity_lookback_days = 60
                    top_n = 50
                    reconstitution_frequency = "monthly"
                    metadata_output_subdir = "metadata"
                    membership_output_subdir = "universe"
                    membership_file = "tw_top50_liquidity_membership.csv"

                    [signals]
                    mode = "cross_sectional_vol_adj_momentum"
                    enabled_symbols = []
                    benchmark = "TAIEX"
                    ma_fast_window = 20
                    ma_slow_window = 60
                    momentum_window = 126
                    volatility_window = 20
                    volatility_cap = 0.35
                    align_by_date = false
                    input_subdir = "market_data/daily"
                    output_subdir = "signals/monthly"
                    output_file = "cross_sectional_signal_panel.csv"

                    [portfolio]
                    tradable_symbols = []
                    benchmark = "TAIEX"
                    rebalance_frequency = "monthly"
                    weighting = "equal"
                    min_signal_score = 0.0
                    max_positions = 10
                    max_weight = 0.1
                    hold_cash_when_inactive = true

                    [backtest]
                    initial_nav = 1.0
                    bar_input_subdir = "market_data/daily"
                    signal_input_subdir = "signals/monthly"
                    signal_input_file = "cross_sectional_signal_panel.csv"
                    output_subdir = "backtests"
                    nav_file = "daily_nav.csv"
                    weights_file = "daily_weights.csv"

                    [walkforward]
                    enabled = false
                    window_type = "expanding"
                    train_window_days = 252
                    test_window_days = 63
                    minimum_history_days = 252
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            with patch("tw_quant.pipelines.ingest.build_provider", return_value=CrossSectionalSmallUniverseTwseProvider()):
                with self.assertRaisesRegex(ValueError, "effective candidate pool is too small"):
                    execute_ingest(config_path)

            processed_root = temp_root / "data" / "processed" / "metadata"
            self.assertTrue((processed_root / "twse_price_availability.csv").exists())
            self.assertTrue((processed_root / "twse_usable_stock_info.csv").exists())


if __name__ == "__main__":
    unittest.main()
