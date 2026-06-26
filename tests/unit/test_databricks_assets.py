from __future__ import annotations

import unittest
from pathlib import Path


class DatabricksAssetTests(unittest.TestCase):
    def test_pipeline_defines_medallion_tables(self) -> None:
        code = Path("databricks/pipelines/crypto_whale_pipeline.py").read_text(encoding="utf-8")

        self.assertIn('name="bronze_trades"', code)
        self.assertIn('name="silver_trades"', code)
        self.assertIn('name="gold_whale_events"', code)
        self.assertIn('name="gold_whale_metrics_1m"', code)
        self.assertIn('spark.readStream.format("kafka")', code)
        self.assertIn('@dlt.expect_or_drop("valid_price"', code)

    def test_bundle_references_pipeline_and_job(self) -> None:
        bundle = Path("databricks.yml").read_text(encoding="utf-8")

        self.assertIn("crypto_whale_medallion", bundle)
        self.assertIn("crypto_whale_lakehouse_job", bundle)
        self.assertIn("databricks/pipelines/crypto_whale_pipeline.py", bundle)
        self.assertIn("databricks/jobs/telegram_alert_task.py", bundle)

    def test_telegram_task_defaults_to_dry_run(self) -> None:
        code = Path("databricks/jobs/telegram_alert_task.py").read_text(encoding="utf-8")

        self.assertIn('parser.add_argument("--dry_run", default="true")', code)
        self.assertIn("telegram_alert_state", code)
        self.assertIn("LEFT ANTI JOIN", code)


if __name__ == "__main__":
    unittest.main()
