# Kafka Connectivity for Databricks

Databricks cannot read Kafka from your laptop at `localhost:9092`. The Kafka bootstrap server must be reachable from the Databricks cluster network.

## Recommended Options

1. Confluent Cloud Kafka.
2. Redpanda/Kafka on a cloud VM.
3. Temporary tunnel for demos only.

## Required Pipeline Config

- `kafka.bootstrap.servers`
- `kafka.raw.topic`
- Optional `kafka.security.protocol`
- Optional `kafka.sasl.mechanism`
- Optional `kafka.sasl.jaas.config`

## Local Kafka Role

Local Kafka remains useful for local experiments. It is not enough for a real Databricks run unless exposed through a reachable endpoint.
