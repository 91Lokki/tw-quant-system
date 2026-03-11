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


class FailOnFetchProvider:
    name = "finmind"

    def fetch_security_daily(self, symbol: str, start_date: date, end_date: date) -> ProviderPayload:
        raise AssertionError(f"security fetch should not be called for {symbol}")

    def fetch_benchmark_daily(self, symbol: str, start_date: date, end_date: date) -> ProviderPayload:
        raise AssertionError(f"benchmark fetch should not be called for {symbol}")


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


if __name__ == "__main__":
    unittest.main()
