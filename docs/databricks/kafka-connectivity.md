# Kafka Connectivity for Databricks

Databricks cannot read Kafka from your laptop at `localhost:9092`. The Kafka bootstrap server must be reachable from the Databricks cluster network.

## Recommended Option

Use Redpanda/Kafka on a cloud VM. Databricks and the local producer both use the same public bootstrap address:

```text
VM_IP:9092
```

Use a DNS name instead of `VM_IP` if available.

## VM Requirements

- Redpanda/Kafka listens on `0.0.0.0:9092`.
- Redpanda/Kafka advertises `VM_IP:9092`, not `localhost:9092`.
- Firewall/security group allows TCP `9092` from Databricks egress and your development IP only.
- Topic `crypto.trades.raw` exists before the Lakeflow pipeline starts.

## Required Pipeline Config

- `kafka.bootstrap.servers`
- `kafka.raw.topic`
No SASL settings are needed for the basic VM path. Add Kafka auth later only if the VM broker is secured.

## Local Kafka Role

Local Kafka remains useful for local experiments. It is not enough for a real Databricks run unless exposed through a reachable endpoint.
