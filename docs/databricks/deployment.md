# Databricks Deployment Checklist

## Before Deploy

- Databricks workspace available.
- Unity Catalog catalog/schema chosen.
- Kafka endpoint reachable from Databricks.
- Kafka topic `crypto.trades.raw` exists.
- Producer can write normalized JSON events to Kafka.

## Deploy

```powershell
databricks bundle validate
databricks bundle deploy
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
- Deployment blocked until Kafka bootstrap server is configured.

Validate command:

```powershell
.\scripts\databricks\dbx.ps1 bundle validate --profile crypto-whale
```
