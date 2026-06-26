# Architecture Overview

## Databricks-First System Shape

```mermaid
flowchart LR
    A["Binance WebSocket"] --> B["Python Producer"]
    B --> C["Kafka crypto.trades.raw"]
    C --> D["Lakeflow Declarative Pipeline"]
    D --> E["Bronze Delta: bronze_trades"]
    E --> F["Silver Delta: silver_trades"]
    F --> G["Gold Delta: gold_whale_events"]
    F --> H["Gold Delta: gold_whale_metrics_1m"]
    G --> I["Databricks SQL Dashboard"]
    G --> J["Lakeflow Job: Telegram Alert Task"]
```

## Design Choices

- Kafka handles live transport and replayable raw topic flow.
- Lakeflow Declarative Pipelines own medallion ETL.
- Delta Lake tables are the analytical source of truth.
- Databricks SQL is the primary serving/dashboard layer.
- Telegram alerting runs as a job task after Gold tables are available.
- Local Python tests prove business logic without requiring Databricks runtime.

## Runtime Boundary

Local Docker Compose is optional. Databricks is the main runtime target. Kafka must be cloud-accessible from Databricks.
