from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator

LOGGER = logging.getLogger(__name__)


async def iter_binance_messages(ws_url: str) -> AsyncIterator[str]:
    try:
        import websockets
    except ImportError as exc:
        raise RuntimeError(
            "Missing dependency `websockets`. Install with `pip install -r requirements.txt`."
        ) from exc

    while True:
        try:
            async with websockets.connect(ws_url, ping_interval=20, ping_timeout=20) as websocket:
                LOGGER.info("binance_websocket_connected url=%s", ws_url)
                async for message in websocket:
                    yield message
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            LOGGER.warning("binance_websocket_reconnect error=%s", exc)
            await asyncio.sleep(5)
