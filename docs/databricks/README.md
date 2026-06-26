# Databricks Implementation Guide

## Target Runtime

This repo targets Databricks Lakehouse with Kafka as streaming transport.

Use:

- Lakeflow Declarative Pipelines for Bronze/Silver/Gold ETL.
- Delta Lake tables for medallion storage.
- Lakeflow Jobs for orchestration and Telegram alert task.
- Databricks SQL for dashboard queries.

## Main Assets

| File | Purpose |
| --- | --- |
| `databricks/pipelines/crypto_whale_pipeline.py` | Bronze/Silver/Gold Lakeflow pipeline. |
| `databricks/jobs/telegram_alert_task.py` | Reads Gold events and sends Telegram alerts. |
| `databricks/sql/warehouse_queries.sql` | Dashboard starter SQL. |
| `databricks.yml` | Asset Bundle scaffold. |

## Table Names

| Layer | Table |
| --- | --- |
| Bronze | `bronze_trades` |
| Silver | `silver_trades` |
| Gold | `gold_whale_events` |
| Gold | `gold_whale_metrics_1m` |
| Alert state | `telegram_alert_state` |

## Deployment Steps

1. Create Kafka endpoint reachable from Databricks.
2. Store Kafka credentials in Databricks secrets if SASL/SSL is required.
3. Configure `databricks.yml` variables.
4. Deploy bundle or create the Lakeflow pipeline manually from `databricks/pipelines/crypto_whale_pipeline.py`.
5. Run pipeline and verify Delta tables.
6. Run Telegram alert task in dry-run mode.
7. Build Databricks SQL dashboard from `databricks/sql/warehouse_queries.sql`.
