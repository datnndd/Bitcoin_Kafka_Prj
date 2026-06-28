# DBX2 — Lakeflow Pipeline Deployment

Status: in_progress
Lane: normal
Verify: `databricks bundle validate`

## Goal

Deploy and run the Lakeflow Declarative Pipeline on Databricks using a Kafka endpoint reachable from the workspace.

## Scope

- Configure Kafka VM bootstrap address.
- Use serverless compute required by the target Databricks workspace.
- Deploy `crypto_whale_pipeline.py`.
- Verify Bronze/Silver/Gold Delta tables.
- Capture pipeline event log or screenshots.

## Out of Scope

- Airflow.
- Local Docker runtime proof.

## Validation Evidence

- Databricks CLI installed and wrapper `scripts/databricks/dbx.ps1` works.
- Auth profile `crypto-whale` created for workspace `https://dbc-942476c2-e58d.cloud.databricks.com`.
- `databricks bundle validate --profile crypto-whale` passed before serverless-only deployment error.
- Classic job cluster deploy failed because workspace requires serverless compute.
- Pipeline source was imported as a Databricks notebook path without `.py`; remove notebook marker so bundle sync uploads a plain Python file.
- Serverless alert task failed with unsupported `client: "1"`; use serverless `environment_version: "2"`.
- Bundle deployed with Kafka VM bootstrap `159.223.82.182:9092`.
- Pipeline update `ea9b17b7-15a3-4ada-ac0c-55784addc663` completed and created Bronze/Silver/Gold streaming tables.
- Full job run `946369154796199` succeeded; pipeline task and dry-run Telegram alert task both terminated successfully.
