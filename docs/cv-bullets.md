# CV Bullets

## Short Version

- Built a Kafka-to-Databricks Lakehouse pipeline for real-time crypto whale detection using Lakeflow Declarative Pipelines and Delta Lake.
- Designed Bronze/Silver/Gold medallion tables with data quality gates, deduplication, and Gold analytics for whale events and one-minute metrics.
- Orchestrated ETL and Telegram alerting with Lakeflow Jobs and served insights through Databricks SQL dashboard queries.

## Detailed Version

- Developed a Binance WebSocket producer that normalizes BTC/USDT trades and publishes replayable JSON events to Kafka.
- Implemented Databricks Lakeflow Declarative Pipeline reading Kafka into Bronze Delta, cleaning into Silver Delta, and curating Gold whale events/metrics.
- Added quality expectations for required fields, positive numeric values, valid buy/sell side inference, and duplicate trade handling.
- Built Telegram alert job that reads Gold whale events and tracks alert state to prevent duplicate notifications.
- Created Databricks SQL dashboard starter queries for largest whale trades, whale volume, buy/sell imbalance, and one-minute metrics.
- Maintained local deterministic tests and fixture-based E2E validation to prove core transform logic outside Databricks.

## Interview Talking Points

- Kafka is the streaming transport; Delta Lake is the analytical source of truth.
- Lakeflow Declarative Pipelines express medallion ETL as managed Databricks pipeline code.
- Lakeflow Jobs orchestrate pipeline execution and downstream alert tasks.
- Databricks SQL serves the reviewer-facing dashboard layer.
- Local Kafka is not enough for Databricks cloud; Kafka must expose a reachable endpoint.
