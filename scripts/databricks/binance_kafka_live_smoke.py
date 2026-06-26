from __future__ import annotations

import argparse
import asyncio
import json
import time
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from confluent_kafka import Consumer, KafkaException, Producer
from src.producer.trade_normalizer import normalize_trade, parse_json_message


async def collect_trades(ws_url: str, count: int):
    import websockets

    trades = []
    async with websockets.connect(ws_url, ping_interval=20, ping_timeout=20) as websocket:
        while len(trades) < count:
            message = await asyncio.wait_for(websocket.recv(), timeout=20)
            trades.append(normalize_trade(parse_json_message(message)))
    return trades


def produce_trades(bootstrap_servers: str, topic: str, trades) -> None:
    producer = Producer({"bootstrap.servers": bootstrap_servers})
    for trade in trades:
        producer.produce(topic, key=trade.kafka_key(), value=trade.to_json())
    producer.flush(30)


def consume_matching(bootstrap_servers: str, topic: str, trade_ids: set[str]) -> dict[str, dict[str, object]]:
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"binance-live-smoke-{int(time.time())}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    found: dict[str, dict[str, object]] = {}
    deadline = time.time() + 30
    try:
        while time.time() < deadline and trade_ids - set(found):
            message = consumer.poll(1)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())
            payload = json.loads(message.value().decode("utf-8"))
            if payload.get("trade_id") in trade_ids:
                found[payload["trade_id"]] = payload
    finally:
        consumer.close()
    return found


def main() -> None:
    parser = argparse.ArgumentParser(description="Live smoke test: Binance WebSocket -> Kafka -> Kafka consumer.")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="crypto.trades.raw")
    parser.add_argument("--ws-url", default="wss://stream.binance.com:9443/ws/btcusdt@trade")
    parser.add_argument("--count", type=int, default=3)
    args = parser.parse_args()

    trades = asyncio.run(collect_trades(args.ws_url, args.count))
    produce_trades(args.bootstrap_servers, args.topic, trades)
    found = consume_matching(args.bootstrap_servers, args.topic, {trade.trade_id for trade in trades})
    result = {
        "produced_count": len(trades),
        "consumed_matching_count": len(found),
        "trade_ids": sorted(found),
        "sample": next(iter(found.values())) if found else None,
    }
    print(json.dumps(result, indent=2, ensure_ascii=False))
    if len(found) != len(trades):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
