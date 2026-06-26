from __future__ import annotations

import argparse
import asyncio
import logging
from collections.abc import Iterable
from pathlib import Path

from src.common.config import AppConfig
from src.common.logging import configure_logging
from src.producer.binance_websocket import iter_binance_messages
from src.producer.kafka_writer import KafkaTradeWriter
from src.producer.trade_normalizer import TradeNormalizationError, normalize_trade, parse_json_message

LOGGER = logging.getLogger(__name__)


def iter_fixture_lines(path: Path) -> Iterable[str]:
    with path.open("r", encoding="utf-8-sig") as handle:
        for line in handle:
            stripped = line.strip()
            if stripped:
                yield stripped


def process_message(message: str, writer: KafkaTradeWriter | None, config: AppConfig, dry_run: bool) -> bool:
    try:
        normalized = normalize_trade(parse_json_message(message))
    except TradeNormalizationError as exc:
        LOGGER.warning("trade_normalization_failed error=%s", exc)
        return False

    payload = normalized.to_json()
    if dry_run:
        print(payload)
    else:
        if writer is None:
            raise RuntimeError("writer is required when dry_run is false")
        writer.write(config.raw_trades_topic, normalized.kafka_key(), payload)
    LOGGER.info(
        "trade_published symbol=%s trade_id=%s notional_usd=%s side=%s",
        normalized.symbol,
        normalized.trade_id,
        normalized.notional_usd,
        normalized.side_inferred,
    )
    return True


async def run_live(config: AppConfig, dry_run: bool) -> None:
    writer = None if dry_run else KafkaTradeWriter(config.kafka_bootstrap_servers, config.producer_client_id)
    try:
        async for message in iter_binance_messages(config.binance_ws_url):
            process_message(message, writer, config, dry_run)
    finally:
        if writer is not None:
            writer.flush()


def run_fixture(config: AppConfig, fixture: Path, dry_run: bool) -> int:
    writer = None if dry_run else KafkaTradeWriter(config.kafka_bootstrap_servers, config.producer_client_id)
    accepted = 0
    try:
        for message in iter_fixture_lines(fixture):
            if process_message(message, writer, config, dry_run):
                accepted += 1
    finally:
        if writer is not None:
            writer.flush()
    return accepted


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Publish Binance trade events to Kafka raw topic.")
    parser.add_argument("--fixture", type=Path, help="Replay newline-delimited JSON fixture instead of WebSocket.")
    parser.add_argument("--dry-run", action="store_true", help="Print normalized events instead of writing Kafka.")
    return parser


def main() -> None:
    configure_logging()
    args = build_parser().parse_args()
    config = AppConfig.from_env()
    if args.fixture:
        accepted = run_fixture(config, args.fixture, args.dry_run)
        LOGGER.info("fixture_replay_finished accepted=%s", accepted)
        return
    asyncio.run(run_live(config, args.dry_run))


if __name__ == "__main__":
    main()

