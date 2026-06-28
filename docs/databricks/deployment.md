# Databricks Deployment Checklist

## Before Deploy

- Databricks workspace available.
- Workspace uses serverless compute for Lakeflow pipeline and job tasks.
- Unity Catalog catalog/schema chosen.
- Kafka/Redpanda VM endpoint reachable from Databricks, e.g. `VM_IP:9092`.
- Kafka topic `crypto.trades.raw` exists.
- Producer can write normalized JSON events to Kafka.

## Deploy

```powershell
databricks bundle validate --var="kafka_bootstrap_servers=VM_IP:9092"
databricks bundle deploy --var="kafka_bootstrap_servers=VM_IP:9092"
```

If not using Asset Bundles, create a Lakeflow Declarative Pipeline manually and point it to `databricks/pipelines/crypto_whale_pipeline.py`.

## Validate

- `bronze_trades` has incoming rows.
- `silver_trades` has clean rows.
- `gold_whale_events` has trades above threshold.
- `gold_whale_metrics_1m` has aggregate rows.
- Telegram task runs in dry-run mode.
- Databricks SQL queries return Gold data.

## Current Workspace

- Profile: `crypto-whale`
- Host: `https://dbc-942476c2-e58d.cloud.databricks.com`
- Bundle validation: passed after adding `alert_cluster` job cluster.
- Bundle now uses serverless pipeline/job compute because this workspace requires serverless.
- Deployment waits on Kafka VM bootstrap server, e.g. `VM_IP:9092`.

Validate command:

```powershell
.\scripts\databricks\dbx.ps1 bundle validate --profile crypto-whale --var="kafka_bootstrap_servers=VM_IP:9092"
```
