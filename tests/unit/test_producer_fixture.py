from __future__ import annotations

import io
import json
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from src.common.config import AppConfig
from src.producer.main import run_fixture


class ProducerFixtureTests(unittest.TestCase):
    def test_run_fixture_dry_run_prints_normalized_events(self) -> None:
        config = AppConfig(
            binance_ws_url="wss://example.test/ws",
            kafka_bootstrap_servers="localhost:9092",
            raw_trades_topic="crypto.trades.raw",
            symbols=("BTCUSDT",),
            producer_client_id="test-producer",
        )
        fixture = Path("tests/fixtures/binance_trades.ndjson")
        output = io.StringIO()

        with redirect_stdout(output):
            accepted = run_fixture(config, fixture, dry_run=True)

        lines = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual(accepted, 2)
        self.assertEqual(lines[0]["trade_id"], "123456789")
        self.assertEqual(lines[0]["notional_usd"], "162500")
        self.assertEqual(lines[1]["side_inferred"], "SELL")


if __name__ == "__main__":
    unittest.main()
