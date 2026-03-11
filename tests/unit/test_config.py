from __future__ import annotations

from pathlib import Path
import sys
import unittest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from tw_quant.config import load_settings


class LoadSettingsTests(unittest.TestCase):
    def test_load_settings_parses_example_config(self) -> None:
        config = load_settings(PROJECT_ROOT / "configs" / "settings.example.toml")

        self.assertEqual(config.project_name, "tw_quant_v1")
        self.assertEqual(config.market, "TW cash equities")
        self.assertEqual(config.benchmark, "TAIEX")
        self.assertEqual(config.data_paths.project_root, PROJECT_ROOT)
        self.assertEqual(config.data_paths.raw_dir, PROJECT_ROOT / "data" / "raw")
        self.assertEqual(config.data_paths.reports_dir, PROJECT_ROOT / "data" / "processed" / "reports")
        self.assertAlmostEqual(config.trading_costs.commission_bps, 14.25)


if __name__ == "__main__":
    unittest.main()
