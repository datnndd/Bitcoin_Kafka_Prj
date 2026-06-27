# DBX2 — Lakeflow Pipeline Deployment

Status: in_progress
Lane: normal
Verify: `databricks bundle validate`

## Goal

Deploy and run the Lakeflow Declarative Pipeline on Databricks using a Kafka endpoint reachable from the workspace.

## Scope

- Configure Kafka VM bootstrap address.
- Deploy `crypto_whale_pipeline.py`.
- Verify Bronze/Silver/Gold Delta tables.
- Capture pipeline event log or screenshots.

## Out of Scope

- Airflow.
- Local Docker runtime proof.

## Validation Evidence

- Databricks CLI installed and wrapper `scripts/databricks/dbx.ps1` works.
- Auth profile `crypto-whale` created for workspace `https://dbc-942476c2-e58d.cloud.databricks.com`.
- `databricks bundle validate --profile crypto-whale` passed.
- Deployment is waiting on real Kafka VM bootstrap server; current bundle placeholder is `VM_IP:9092`.
