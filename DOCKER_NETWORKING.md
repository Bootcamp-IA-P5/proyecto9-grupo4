# Docker Networking Configuration

## Issue: Connecting Airflow to External Kafka

### Problem Description

Our Airflow pipeline needs to consume messages from a Kafka broker that simulates an external data source. The Kafka container is configured with the following advertised listeners:

```yaml
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:29092
```

This configuration creates two connection options:
- `kafka:9092` - Internal listener for containers on the same network
- `localhost:29092` - Host listener for applications running on the host machine

### Why `localhost:29092` Doesn't Work from Airflow Container

When the Airflow container tries to connect to Kafka via `localhost:29092`:

1. **Initial connection attempt**: The consumer connects to `localhost:29092`
2. **Kafka response**: Kafka responds with its advertised listener: `localhost:29092`
3. **Client reconnection attempt**: The Kafka client tries to reconnect to `localhost:29092`
4. **Failure**: Inside the Airflow container, `localhost` refers to the **container itself**, not the host machine
5. **Result**: `Connection refused` error

Even though the port is mapped (`29092:29092`), the advertised listener metadata causes the client to attempt connecting to the wrong `localhost`.

### Why `host.docker.internal:29092` Doesn't Work

On Windows and Mac, Docker provides `host.docker.internal` to access the host machine from containers. However:

1. When connecting to `host.docker.internal:29092`, the initial connection succeeds
2. Kafka still responds with its advertised listener: `localhost:29092`
3. The client still tries to reconnect to `localhost:29092` and fails

**The advertised listener must match how clients connect to Kafka.**

## Solution: Shared Docker Network

Since we cannot modify the Kafka configuration (it's an external data source simulation), the solution is to connect the Airflow container to Kafka's Docker network.

### Implementation

```bash
docker network connect data-engineering-educational-project_default airflow-webserver
```

This command connects the Airflow webserver container to the same network as Kafka, allowing it to use the internal listener `kafka:9092`.

### Configuration

**In `scripts/read_from_kafka.py`:**
```python
KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'  # Connect to Kafka via Docker network
```

### Why This Is Realistic

In production environments, this setup accurately simulates:

- **VPC Peering**: Cloud services in different VPCs connected via peering
- **Private Networks**: Services on the same corporate/cloud network
- **VPN Connections**: Remote data sources accessible via VPN tunnels
- **Service Mesh**: Microservices communicating within a service mesh

Real-world data pipelines typically have **network connectivity** to their data sources, just as our containers share a network.

## Alternative Solution (Requires Kafka Config Change)

If we could modify the Kafka configuration, we would change:

```yaml
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_HOST://host.docker.internal:29092
```

This would allow external containers to connect via `host.docker.internal:29092` without needing to share networks. However, this option is not available in our scenario.

## Current Architecture

```
┌─────────────────────────────────────────────────┐
│  data-engineering-educational-project_default   │
│                                                  │
│  ┌──────────┐     ┌───────────┐                │
│  │ Kafka    │◄────┤ Zookeeper │                │
│  │ :9092    │     └───────────┘                │
│  │ :29092   │                                   │
│  └────▲─────┘                                   │
│       │                                          │
│       │ kafka:9092                               │
│       │                                          │
│  ┌────┴──────────┐                              │
│  │ Airflow       │                              │
│  │ Webserver     │                              │
│  │ (standalone)  │                              │
│  └───────────────┘                              │
│                                                  │
└─────────────────────────────────────────────────┘

Port Mapping: localhost:29092 → kafka:29092
(Available on host machine, not used by Airflow)
```

## Summary

✅ **Airflow container** is connected to Kafka's network
✅ **Consumer uses** `kafka:9092` internal listener
✅ **Successfully processes** Kafka messages → MongoDB → Supabase
✅ **Simulates realistic** production network connectivity

This configuration allows the ETL pipeline to function correctly while maintaining the separation between the data generation infrastructure and the data processing pipeline.
