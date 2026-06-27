# Kafka Smoke Test

## Purpose

Test the real ingestion path before VM/Databricks deployment:

```text
Binance fixture/producer -> Kafka-compatible broker -> Kafka consumer
```

## Start Local Kafka-Compatible Broker

This repo uses Redpanda for local/VM smoke tests because it is Kafka API-compatible and runs as one container.

```powershell
$env:REDPANDA_IMAGE="redpandadata/redpanda:v26.1.11"
docker compose -f docker-compose.kafka.yml up -d
```

Created topics:

- `crypto.trades.raw`
- `crypto.whales.detected`

Check topics:

```powershell
docker compose -f docker-compose.kafka.yml exec redpanda rpk topic list --brokers localhost:9092
```

## Run Smoke Test

```powershell
python scripts\databricks\kafka_smoke_test.py --bootstrap-servers localhost:9092 --topic crypto.trades.raw
```

Expected output:

```json
{
  "produced": 2,
  "consumed_trade_id": "...",
  "consumed_symbol": "BTCUSDT",
  "consumed_notional_usd": "..."
}
```

## Live Binance Smoke Test

After broker is up, test real Binance data through Kafka and consume matching trade IDs:

```powershell
python scripts\databricks\binance_kafka_live_smoke.py --bootstrap-servers localhost:9092 --topic crypto.trades.raw --count 3
```

This proves: Binance WebSocket -> normalized producer payload -> Redpanda/Kafka -> Kafka consumer.

## Real Binance Producer To Local Kafka

After broker is up:

```powershell
$env:KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
python -m src.producer.main
```

Stop with `Ctrl+C` after enough events.

## VM Notes

On the VM, run this compose file with the public address advertised:

```powershell
$env:PUBLIC_KAFKA_HOST="<vm-public-ip-or-domain>"
$env:REDPANDA_IMAGE="redpandadata/redpanda:v26.1.11"
docker compose -f docker-compose.kafka.yml up -d
```

The compose file expands that value into:

```yaml
--advertise-kafka-addr PLAINTEXT://<vm-public-ip-or-domain>:9092
```

From your laptop, test the VM broker:

```powershell
python scripts\databricks\kafka_smoke_test.py --bootstrap-servers <vm-public-ip-or-domain>:9092 --topic crypto.trades.raw
python scripts\databricks\binance_kafka_live_smoke.py --bootstrap-servers <vm-public-ip-or-domain>:9092 --topic crypto.trades.raw --count 3
```

Open firewall/security group for `9092` only from trusted Databricks egress or your IP.

## Current Local Caveat

Previous Docker image pulls failed with CloudFront EOF on this machine. The compose file is correct, but Docker must be able to pull `redpandadata/redpanda:v26.1.11`.

## Security Note

Do not commit real Telegram tokens or Kafka credentials. Rotate any token that appeared in terminal logs.

## Image Pinning

Default image is `redpandadata/redpanda:v26.1.11`. Override when needed:

```powershell
$env:REDPANDA_IMAGE="redpandadata/redpanda:<tag>"
docker compose -f docker-compose.kafka.yml up -d
```
