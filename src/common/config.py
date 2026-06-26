from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class AppConfig:
    binance_ws_url: str
    kafka_bootstrap_servers: str
    raw_trades_topic: str
    symbols: tuple[str, ...]
    producer_client_id: str

    @classmethod
    def from_env(cls) -> "AppConfig":
        symbols = tuple(
            symbol.strip().upper()
            for symbol in os.getenv("SYMBOLS", "BTCUSDT").split(",")
            if symbol.strip()
        )
        return cls(
            binance_ws_url=os.getenv(
                "BINANCE_WS_URL",
                "wss://stream.binance.com:9443/ws/btcusdt@trade",
            ),
            kafka_bootstrap_servers=os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS",
                "localhost:9092",
            ),
            raw_trades_topic=os.getenv("RAW_TRADES_TOPIC", "crypto.trades.raw"),
            symbols=symbols or ("BTCUSDT",),
            producer_client_id=os.getenv("PRODUCER_CLIENT_ID", "binance-trade-producer"),
        )
