# Lakeflow Declarative Pipeline for crypto whale medallion tables.

import dlt
from pyspark.sql.functions import col, current_timestamp, expr, from_json, to_date, to_timestamp, window
from pyspark.sql.types import BooleanType, MapType, StringType, StructField, StructType

TRADE_SCHEMA = StructType([
    StructField("symbol", StringType(), False),
    StructField("trade_id", StringType(), False),
    StructField("price", StringType(), False),
    StructField("quantity", StringType(), False),
    StructField("notional_usd", StringType(), False),
    StructField("side_inferred", StringType(), False),
    StructField("buyer_is_market_maker", BooleanType(), False),
    StructField("trade_time", StringType(), False),
    StructField("ingest_time", StringType(), True),
    StructField("source", StringType(), False),
    StructField("schema_version", StringType(), False),
    StructField("raw_event", MapType(StringType(), StringType()), True),
])


def pipeline_conf(name: str, default: str) -> str:
    return spark.conf.get(name, default)


def kafka_options() -> dict[str, str]:
    options = {
        "kafka.bootstrap.servers": pipeline_conf("kafka.bootstrap.servers", "VM_IP:9092"),
        "subscribe": pipeline_conf("kafka.raw.topic", "crypto.trades.raw"),
        "startingOffsets": pipeline_conf("kafka.starting.offsets", "latest"),
    }
    security_protocol = spark.conf.get("kafka.security.protocol", None)
    if security_protocol:
        options["kafka.security.protocol"] = security_protocol
        options["kafka.sasl.mechanism"] = spark.conf.get("kafka.sasl.mechanism")
        options["kafka.sasl.jaas.config"] = spark.conf.get("kafka.sasl.jaas.config")
    return options


@dlt.table(name="bronze_trades", comment="Raw normalized Binance trade events read from Kafka.", table_properties={"quality": "bronze"})
def bronze_trades():
    return (
        spark.readStream.format("kafka")
        .options(**kafka_options())
        .load()
        .select(
            col("key").cast("string").alias("kafka_key"),
            col("timestamp").alias("kafka_timestamp"),
            from_json(col("value").cast("string"), TRADE_SCHEMA).alias("trade"),
        )
        .where(col("trade").isNotNull())
        .select("kafka_key", "kafka_timestamp", "trade.*")
        .withColumn("bronze_ingest_time", current_timestamp())
    )


@dlt.table(name="silver_trades", comment="Clean, typed, deduplicated trade events for analytics.", table_properties={"quality": "silver"})
@dlt.expect_or_drop("valid_trade_id", "trade_id IS NOT NULL AND trade_id <> ''")
@dlt.expect_or_drop("valid_symbol", "symbol IS NOT NULL AND symbol <> ''")
@dlt.expect_or_drop("valid_price", "CAST(price AS DECIMAL(38,12)) > 0")
@dlt.expect_or_drop("valid_quantity", "CAST(quantity AS DECIMAL(38,12)) > 0")
@dlt.expect_or_drop("valid_notional", "CAST(notional_usd AS DECIMAL(38,12)) > 0")
@dlt.expect_or_drop("valid_side", "side_inferred IN ('BUY', 'SELL')")
def silver_trades():
    return (
        dlt.read_stream("bronze_trades")
        .dropDuplicates(["trade_id"])
        .withColumn("price_decimal", col("price").cast("decimal(38,12)"))
        .withColumn("quantity_decimal", col("quantity").cast("decimal(38,12)"))
        .withColumn("notional_decimal", col("notional_usd").cast("decimal(38,12)"))
        .withColumn("trade_timestamp", to_timestamp(col("trade_time")))
        .withColumn("event_date", to_date(col("trade_timestamp")))
        .withColumn("silver_processed_time", current_timestamp())
    )


@dlt.table(name="gold_whale_events", comment="Business-ready whale trades above configured USD threshold.", table_properties={"quality": "gold"})
def gold_whale_events():
    threshold = pipeline_conf("whale.threshold.usd", "100000")
    return (
        dlt.read_stream("silver_trades")
        .where(col("notional_decimal") >= expr(f"CAST('{threshold}' AS DECIMAL(38,12))"))
        .withColumn("gold_processed_time", current_timestamp())
    )


@dlt.table(name="gold_whale_metrics_1m", comment="One-minute whale notional metrics by symbol and inferred side.", table_properties={"quality": "gold"})
def gold_whale_metrics_1m():
    return (
        dlt.read_stream("gold_whale_events")
        .groupBy(window(col("trade_timestamp"), "1 minute"), col("symbol"), col("side_inferred"))
        .agg(
            expr("count(*) AS trade_count"),
            expr("sum(notional_decimal) AS total_notional_usd"),
            expr("max(notional_decimal) AS max_notional_usd"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("symbol"),
            col("side_inferred"),
            col("trade_count"),
            col("total_notional_usd"),
            col("max_notional_usd"),
        )
    )
