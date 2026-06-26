# ADR-0009: Databricks-First Lakehouse Runtime

## Status
Accepted

## Context

Local Docker Spark/Kafka setup caused hardware and image-pull friction. The project still needs Kafka, Spark, medallion architecture, data quality, orchestration, and dashboarding for a strong Data Engineering CV story.

## Decision

Move the main runtime target to Databricks:

- Kafka remains the streaming transport.
- Lakeflow Declarative Pipelines implement Bronze/Silver/Gold ETL.
- Delta Lake tables become the analytical source of truth.
- Lakeflow Jobs orchestrate pipeline and Telegram alert task.
- Databricks SQL serves dashboard queries.
- Local code remains a test harness and fallback, not the primary platform.

## Rationale

- Reduces local hardware and Docker configuration burden.
- Better matches modern lakehouse Data Engineering workflows.
- Keeps Kafka/Spark/medallion skills visible.
- Adds Databricks, Delta Lake, Lakeflow Jobs, and SQL dashboard story.

## Trade-offs

- Requires Databricks workspace and a Kafka endpoint reachable from Databricks.
- Some code cannot be fully validated locally because it depends on Databricks runtime modules such as `dlt`.
- Kafka local at `localhost:9092` cannot be read directly by Databricks cloud clusters.

## Consequences

- M7 Docker runtime proof becomes optional/legacy.
- New DBX milestones become primary project roadmap.
- Documentation must clearly separate local validation from Databricks deployment.
