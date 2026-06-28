# DBX3 — Databricks Job and Dashboard

Status: implemented
Lane: normal
Verify: `databricks bundle validate`

## Goal

Create Lakeflow Job orchestration and Databricks SQL dashboard outputs for portfolio review.

## Scope

- Run pipeline task.
- Run Telegram alert dry-run task.
- Build dashboard from Gold tables.
- Capture dashboard screenshots or exported query evidence.

## Out of Scope

- Real Telegram send until secrets are configured.

## Validation Evidence

- Full job run `946369154796199` succeeded with pipeline task and dry-run Telegram task.
- Fixture replay produced one whale trade above `100000` USD into Kafka VM bootstrap `159.223.82.182:9092`.
- Pipeline update `87967f28-bbd9-4be8-b629-fffda90c3ef7` completed after fixture replay.
- Databricks SQL warehouse `08695af4dcd5355d` returned dashboard query evidence in `reports/dbx3/sql-evidence.md`.
- All four dashboard starter queries succeeded with one row each.
