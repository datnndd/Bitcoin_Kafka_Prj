-- Databricks SQL dashboard starter queries.
-- Replace catalog/schema if needed.

USE CATALOG main;
USE SCHEMA crypto_whale;

-- Largest whale trades.
SELECT trade_timestamp, symbol, side_inferred, quantity, price, notional_decimal AS notional_usd, trade_id
FROM gold_whale_events
ORDER BY notional_decimal DESC
LIMIT 20;

-- Whale volume by side per day.
SELECT event_date, symbol, side_inferred, count(*) AS trade_count,
       sum(notional_decimal) AS total_notional_usd,
       max(notional_decimal) AS max_notional_usd
FROM gold_whale_events
GROUP BY event_date, symbol, side_inferred
ORDER BY event_date DESC, total_notional_usd DESC;

-- One-minute whale metrics.
SELECT window_start, window_end, symbol, side_inferred, trade_count, total_notional_usd, max_notional_usd
FROM gold_whale_metrics_1m
ORDER BY window_start DESC;

-- Buy/sell imbalance.
SELECT event_date, symbol,
       sum(CASE WHEN side_inferred = 'BUY' THEN notional_decimal ELSE 0 END) AS buy_notional_usd,
       sum(CASE WHEN side_inferred = 'SELL' THEN notional_decimal ELSE 0 END) AS sell_notional_usd,
       sum(CASE WHEN side_inferred = 'BUY' THEN notional_decimal ELSE -notional_decimal END) AS net_buy_notional_usd
FROM gold_whale_events
GROUP BY event_date, symbol
ORDER BY event_date DESC;
