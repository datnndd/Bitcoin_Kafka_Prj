from __future__ import annotations

import json
import unittest
from datetime import UTC, datetime

from src.producer.trade_normalizer import (
    TradeNormalizationError,
    infer_active_side,
    normalize_trade,
    parse_json_message,
)


class TradeNormalizerTests(unittest.TestCase):
    def test_parse_json_message_requires_object(self) -> None:
        with self.assertRaises(TradeNormalizationError):
            parse_json_message("[]")

    def test_normalize_trade_maps_binance_trade_payload(self) -> None:
        payload = {
            "e": "trade",
            "E": 1710000000000,
            "s": "BTCUSDT",
            "t": 123456789,
            "p": "65000.00",
            "q": "2.5000",
            "T": 1710000000123,
            "m": False,
            "M": True,
        }
        ingest_time = datetime(2026, 6, 17, 1, 2, 3, tzinfo=UTC)

        normalized = normalize_trade(payload, ingest_time=ingest_time)

        self.assertEqual(normalized.symbol, "BTCUSDT")
        self.assertEqual(normalized.trade_id, "123456789")
        self.assertEqual(normalized.price, "65000")
        self.assertEqual(normalized.quantity, "2.5")
        self.assertEqual(normalized.notional_usd, "162500")
        self.assertEqual(normalized.side_inferred, "BUY")
        self.assertFalse(normalized.buyer_is_market_maker)
        self.assertEqual(normalized.ingest_time, "2026-06-17T01:02:03+00:00")
        self.assertEqual(normalized.raw_event, payload)

    def test_infer_active_side(self) -> None:
        self.assertEqual(infer_active_side(False), "BUY")
        self.assertEqual(infer_active_side(True), "SELL")

    def test_normalize_trade_rejects_missing_required_field(self) -> None:
        payload = {
            "e": "trade",
            "s": "BTCUSDT",
            "t": 123456789,
            "p": "65000.00",
            "T": 1710000000123,
            "m": False,
        }

        with self.assertRaisesRegex(TradeNormalizationError, "missing required field: q"):
            normalize_trade(payload)

    def test_normalize_trade_rejects_non_positive_quantity(self) -> None:
        payload = {
            "e": "trade",
            "s": "BTCUSDT",
            "t": 123456789,
            "p": "65000.00",
            "q": "0",
            "T": 1710000000123,
            "m": False,
        }

        with self.assertRaisesRegex(TradeNormalizationError, "field q must be positive"):
            normalize_trade(payload)

    def test_normalized_trade_serializes_as_json(self) -> None:
        payload = {
            "e": "trade",
            "s": "BTCUSDT",
            "t": 123456789,
            "p": "65000.00",
            "q": "2.5000",
            "T": 1710000000123,
            "m": False,
        }

        serialized = normalize_trade(payload).to_json()
        parsed = json.loads(serialized)

        self.assertEqual(parsed["trade_id"], "123456789")
        self.assertEqual(parsed["notional_usd"], "162500")
        self.assertEqual(parsed["raw_event"], payload)


if __name__ == "__main__":
    unittest.main()
