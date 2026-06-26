from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any


class TradeNormalizationError(ValueError):
    pass


@dataclass(frozen=True)
class NormalizedTrade:
    symbol: str
    trade_id: str
    price: str
    quantity: str
    notional_usd: str
    side_inferred: str
    buyer_is_market_maker: bool
    trade_time: str
    ingest_time: str
    raw_event: dict[str, Any]
    source: str
    schema_version: str

    def kafka_key(self) -> str:
        return self.trade_id

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False, separators=(",", ":"))


def parse_json_message(message: str) -> dict[str, Any]:
    try:
        value = json.loads(message)
    except json.JSONDecodeError as exc:
        raise TradeNormalizationError(f"invalid json: {exc.msg}") from exc
    if not isinstance(value, dict):
        raise TradeNormalizationError("payload must be a JSON object")
    return value


def normalize_trade(payload: dict[str, Any], ingest_time: datetime | None = None) -> NormalizedTrade:
    symbol = _required_str(payload, "s").upper()
    trade_id = str(_required(payload, "t"))
    price = _positive_decimal(payload, "p")
    quantity = _positive_decimal(payload, "q")
    trade_time_ms = _required_int(payload, "T")
    buyer_is_market_maker = _required_bool(payload, "m")

    event_name = payload.get("e")
    if event_name not in (None, "trade"):
        raise TradeNormalizationError(f"unsupported event type: {event_name}")

    notional = price * quantity
    trade_time = datetime.fromtimestamp(trade_time_ms / 1000, tz=UTC)
    ingest_timestamp = ingest_time or datetime.now(UTC)

    return NormalizedTrade(
        symbol=symbol,
        trade_id=trade_id,
        price=_decimal_to_string(price),
        quantity=_decimal_to_string(quantity),
        notional_usd=_decimal_to_string(notional),
        side_inferred=infer_active_side(buyer_is_market_maker),
        buyer_is_market_maker=buyer_is_market_maker,
        trade_time=trade_time.isoformat(),
        ingest_time=ingest_timestamp.isoformat(),
        raw_event=payload,
        source="binance_spot_trade",
        schema_version="1.0",
    )


def infer_active_side(buyer_is_market_maker: bool) -> str:
    return "SELL" if buyer_is_market_maker else "BUY"


def _required(payload: dict[str, Any], field: str) -> Any:
    if field not in payload or payload[field] is None:
        raise TradeNormalizationError(f"missing required field: {field}")
    return payload[field]


def _required_str(payload: dict[str, Any], field: str) -> str:
    value = _required(payload, field)
    if not isinstance(value, str) or not value.strip():
        raise TradeNormalizationError(f"field {field} must be a non-empty string")
    return value


def _required_int(payload: dict[str, Any], field: str) -> int:
    value = _required(payload, field)
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise TradeNormalizationError(f"field {field} must be an integer") from exc


def _required_bool(payload: dict[str, Any], field: str) -> bool:
    value = _required(payload, field)
    if not isinstance(value, bool):
        raise TradeNormalizationError(f"field {field} must be a boolean")
    return value


def _positive_decimal(payload: dict[str, Any], field: str) -> Decimal:
    value = _required(payload, field)
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise TradeNormalizationError(f"field {field} must be decimal") from exc
    if parsed <= 0:
        raise TradeNormalizationError(f"field {field} must be positive")
    return parsed


def _decimal_to_string(value: Decimal) -> str:
    return format(value.normalize(), "f")
