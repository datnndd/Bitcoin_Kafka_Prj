from __future__ import annotations

import logging
from collections.abc import Callable

LOGGER = logging.getLogger(__name__)
DeliveryCallback = Callable[[Exception | None, object], None]


class KafkaTradeWriter:
    def __init__(self, bootstrap_servers: str, client_id: str) -> None:
        try:
            from confluent_kafka import Producer
        except ImportError as exc:
            raise RuntimeError(
                "Missing dependency `confluent-kafka`. Install with `pip install -r requirements.txt`."
            ) from exc

        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": client_id,
                "enable.idempotence": True,
                "acks": "all",
            }
        )

    def write(self, topic: str, key: str, value: str) -> None:
        self._producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=value.encode("utf-8"),
            callback=self._delivery_report,
        )
        self._producer.poll(0)

    def flush(self) -> None:
        self._producer.flush()

    @staticmethod
    def _delivery_report(error: Exception | None, message: object) -> None:
        if error is not None:
            LOGGER.error("kafka_delivery_failed error=%s", error)
            return
        LOGGER.debug("kafka_delivery_succeeded message=%s", message)
