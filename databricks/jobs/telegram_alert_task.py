# Telegram alert task for Lakeflow Jobs.

import argparse
import json
import urllib.parse
import urllib.request
from datetime import datetime, timezone


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="main")
    parser.add_argument("--schema", default="crypto_whale")
    parser.add_argument("--dry_run", default="true")
    parser.add_argument("--telegram_bot_token", default="")
    parser.add_argument("--telegram_chat_id", default="")
    return parser.parse_args()


args = parse_args()
catalog = args.catalog
schema = args.schema
dry_run = args.dry_run.lower() == "true"
bot_token = args.telegram_bot_token
chat_id = args.telegram_chat_id
source_table = f"{catalog}.{schema}.gold_whale_events"
alert_state_table = f"{catalog}.{schema}.telegram_alert_state"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {alert_state_table} (
  trade_id STRING,
  alerted_at TIMESTAMP
) USING DELTA
""")

new_events = spark.sql(f"""
SELECT g.*
FROM {source_table} g
LEFT ANTI JOIN {alert_state_table} s
  ON g.trade_id = s.trade_id
ORDER BY g.trade_timestamp DESC
LIMIT 50
""").collect()


def format_alert(row) -> str:
    return "\n".join([
        "Whale Trade Detected",
        f"Symbol: {row['symbol']}",
        f"Side: {row['side_inferred']}",
        f"Quantity: {row['quantity']} BTC",
        f"Price: {row['price']} USDT",
        f"Notional: {row['notional_usd']} USDT",
        f"Trade Time: {row['trade_time']}",
        f"Trade ID: {row['trade_id']}",
    ])


def send_telegram(text: str) -> None:
    if dry_run:
        print(text)
        return
    if not bot_token or not chat_id:
        raise ValueError("telegram_bot_token and telegram_chat_id are required when dry_run=false")
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = urllib.parse.urlencode({"chat_id": chat_id, "text": text}).encode("utf-8")
    request = urllib.request.Request(url, data=payload, method="POST")
    with urllib.request.urlopen(request, timeout=10) as response:
        body = json.loads(response.read().decode("utf-8"))
    if not body.get("ok"):
        raise RuntimeError("Telegram sendMessage rejected request")


alerted_ids = []
for event in new_events:
    send_telegram(format_alert(event))
    alerted_ids.append(event["trade_id"])

if alerted_ids:
    values = ",".join(f"('{trade_id}', current_timestamp())" for trade_id in alerted_ids)
    spark.sql(f"INSERT INTO {alert_state_table} VALUES {values}")

print(f"alerted_count={len(alerted_ids)} run_at={datetime.now(timezone.utc).isoformat()}")
