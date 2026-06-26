from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from confluent_kafka import Consumer, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from src.producer.main import iter_fixture_lines
from src.producer.trade_normalizer import normalize_trade, parse_json_message


def ensure_topic(bootstrap_servers: str, topic: str) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    metadata = admin.list_topics(timeout=10)
    if topic in metadata.topics:
        return
    futures = admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
    futures[topic].result(timeout=30)


def produce_fixture(bootstrap_servers: str, topic: str, fixture: Path) -> int:
    producer = Producer({"bootstrap.servers": bootstrap_servers})
    count = 0
    for line in iter_fixture_lines(fixture):
        trade = normalize_trade(parse_json_message(line))
        producer.produce(topic, key=trade.kafka_key(), value=trade.to_json())
        count += 1
    producer.flush(30)
    return count


def consume_one(bootstrap_servers: str, topic: str) -> dict[str, object]:
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"crypto-whale-smoke-{int(time.time())}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    try:
        deadline = time.time() + 20
        while time.time() < deadline:
            message = consumer.poll(1)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())
            return json.loads(message.value().decode("utf-8"))
    finally:
        consumer.close()
    raise TimeoutError("No message consumed from Kafka within 20 seconds")


def main() -> None:
    parser = argparse.ArgumentParser(description="Smoke test Kafka with normalized Binance fixture records.")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="crypto.trades.raw")
    parser.add_argument("--fixture", type=Path, default=Path("tests/fixtures/binance_trades.ndjson"))
    args = parser.parse_args()

    ensure_topic(args.bootstrap_servers, args.topic)
    produced = produce_fixture(args.bootstrap_servers, args.topic, args.fixture)
    consumed = consume_one(args.bootstrap_servers, args.topic)
    print(json.dumps({"produced": produced, "consumed_trade_id": consumed.get("trade_id"), "consumed_symbol": consumed.get("symbol"), "consumed_notional_usd": consumed.get("notional_usd")}, indent=2))


if __name__ == "__main__":
    main()
