# Data Quality Rules

## Goal

Keep invalid exchange data out of Databricks Silver and Gold Delta tables while preserving quality metrics through Lakeflow expectations.

## Quality Gates

Defined in `databricks/pipelines/crypto_whale_pipeline.py` on `silver_trades`:

| Rule | Failure Condition | Action |
| --- | --- | --- |
| `valid_trade_id` | `trade_id` missing/null/empty. | Drop from Silver. |
| `valid_symbol` | `symbol` missing/null/empty. | Drop from Silver. |
| `valid_price` | `price <= 0` or non-decimal. | Drop from Silver. |
| `valid_quantity` | `quantity <= 0` or non-decimal. | Drop from Silver. |
| `valid_notional` | `notional_usd <= 0` or non-decimal. | Drop from Silver. |
| `valid_side` | `side_inferred` not `BUY` or `SELL`. | Drop from Silver. |

## Current Implementation

Lakeflow expectations use `@dlt.expect_or_drop`. This keeps Gold tables clean and exposes expectation metrics in Databricks pipeline event logs.

## Future Upgrade

If rejected records are needed, add a dedicated `quarantine_trades` table that stores failed Bronze rows plus rule names.
