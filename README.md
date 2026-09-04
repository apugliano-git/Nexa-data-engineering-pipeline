# Nexa — Milestone 2

Milestone 2 implements the minimum ingestion path:

```text
Ratchet → Redpanda reservation.events.v1 → Spark Structured Streaming → Delta Lake Bronze
```

Nexa consumes the topic as an independent consumer. It stores one append-only
Bronze row per Kafka record, preserving the original JSON value and the Kafka
metadata needed for traceability.

## Requirements

- Docker and Docker Compose
- An external Docker network named `ecosistema-network`
- Redpanda reachable as `redpanda:29092` from that network

The Spark container uses the existing baseline image:

| Component | Version |
|---|---|
| Apache Spark | 3.5.8 |
| Scala | 2.12 |
| Java | 17.x |
| Python | 3.10.x |
| Delta Lake | 3.3.0 |
| Spark Kafka connector | 3.5.8 |

Delta Lake 3.3.x supports Spark 3.5.x. Spark 3.5.8 documents the Kafka
connector artifact used here, and Redpanda exposes a compatible Kafka protocol.
The dependencies are downloaded by `spark-submit` at startup and cached in
the container's temporary `/tmp/.ivy2` directory.

## Local storage permissions

The Compose file includes a one-shot `spark-bronze-init` service. It runs as
root only long enough to create `data/bronze` and
`data/checkpoint/bronze`, assign those directories to UID/GID `185`, and set
mode `0755`. It does not delete, replace, or initialize existing Bronze or
checkpoint files.

The `spark-bronze` service still runs as the image's default `spark` user. No
manual `chmod 777` or permanent root execution is required. The initializer is
rerun safely when the Compose service starts again.

## Start Redpanda

Nexa's Compose expects the external network to exist:

```bash
docker network inspect ecosistema-network >/dev/null 2>&1 || docker network create ecosistema-network
```

With the current Ratchet infrastructure Compose, start Redpanda from the
Ratchet repository and connect its running container to Nexa's network:

```bash
docker compose -f ../Ratchet-Payments-Engine/infra/docker-compose.yml up -d redpanda
docker network connect --alias redpanda ecosistema-network ratchet-redpanda
```

The second command is needed because the current Ratchet Compose does not
declare `ecosistema-network`. If the Redpanda container is recreated, repeat
the network connection. Nexa does not modify Ratchet.

Check broker availability and create the topic when setting up a local test:

```bash
docker exec ratchet-redpanda rpk cluster info
docker exec ratchet-redpanda rpk topic create reservation.events.v1 --partitions 1 --replicas 1
```

If the topic already exists, the create command can be skipped.

## Validate the Compose and Milestone 1

```bash
docker compose config --quiet
docker compose pull spark-smoke-test
docker compose up --abort-on-container-exit --exit-code-from spark-smoke-test spark-smoke-test
```

Successful output includes:

```text
NEXA_SMOKE_TEST_OK spark=3.5.8 python=3.10.x java=17.x master=local[2] count=1
```

## Run Bronze ingestion

Start the continuous Spark query in the background:

```bash
docker compose config --quiet
docker compose up -d spark-bronze
docker compose logs -f spark-bronze
```

This startup does not require a pre-existing `data/` directory. Docker
may create the bind-mount root as `root:root`; `spark-bronze-init` prepares its
permissions before Spark starts writing.

The consumer subscribes only to `reservation.events.v1`, uses `redpanda:29092`,
and starts at `latest` only when the checkpoint is new. The Spark checkpoint
then controls subsequent progress.

## Bronze layout

The host directories are bind-mounted so they survive container restarts:

```text
data/
├── bronze/                  # Delta table data and _delta_log
└── checkpoint/bronze/       # Spark Structured Streaming checkpoint
```

`data/` is generated local state and is ignored by Git.

Bronze has exactly these columns:

| Column | Type | Meaning |
|---|---|---|
| `key` | `STRING` | Original Kafka message key, normally Ratchet's `resourceId` |
| `value` | `STRING` | Original JSON payload, preserved without Silver transformations |
| `topic` | `STRING` | Kafka topic |
| `partition` | `INT` | Kafka partition |
| `offset` | `LONG` | Kafka offset within the partition |
| `timestamp` | `TIMESTAMP` | Kafka record timestamp |

No business deduplication is performed in Bronze. `eventId`, `eventType`, and
`holdId` remain inside the raw JSON value for later layers.

## Inspect Bronze

The following command reads the Delta table by path and extracts `eventId` only
for inspection; the ingestion job does not add it as a Bronze column:

```bash
docker compose exec spark-bronze \
  /opt/spark/bin/spark-sql \
  --master local[2] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,io.delta:delta-spark_2.12:3.3.0 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.ui.enabled=false \
  -e "SELECT topic, partition, offset, key, get_json_object(value, '$.eventId') AS event_id, value FROM delta.\`/opt/nexa/data/bronze\` ORDER BY partition, offset"
```

When the topic already contains unrelated records, verify a controlled test
using `topic + partition + offset` or a known `eventId`, not a global row count.

## Restart behavior

Stop and start the consumer without deleting `data/`:

```bash
docker compose stop spark-bronze
docker compose up -d spark-bronze
```

The one-shot permission initializer runs again, while the existing Delta table
and Spark checkpoint remain in place.

On the first query, `startingOffsets=latest` avoids an implicit historical
backfill. After the first successful batches, Spark resumes from the
checkpoint at `data/checkpoint/bronze`; `startingOffsets` does not reset an
existing checkpoint. The Delta data and checkpoint must be kept together.

There are three different duplicate-related cases:

1. A micro-batch can be re-read or retried after a failure. With the same
   Structured Streaming query and checkpoint, the native Delta sink uses its
   transaction log together with streaming progress to make the commit
   idempotent; a re-read is not automatically a second Bronze row. Delta
   documents exactly-once processing for its native streaming sink in its
   [streaming documentation](https://docs.delta.io/delta-streaming/). This
   session did not force a crash at that boundary, so this is a documented
   sink property, not a locally demonstrated crash guarantee.
2. A producer can publish the same business event more than once. Those are
   different Kafka records with different offsets. The Delta sink sees two
   input records, and Bronze intentionally preserves both because it performs
   no business deduplication.
3. Two different lifecycle events for the same hold can share a `holdId` while
   having different `eventId` values. They are distinct business events, not
   delivery retries.

The implementation therefore relies on the native Delta sink's commit
semantics for a repeated batch, while making no global exactly-once or
business-uniqueness promise. Bronze retains raw evidence and defers business
deduplication to a later layer.

## Tests and boundaries

The local projection test can run with the existing Spark image:

```bash
docker run --rm -v "$PWD:/workspace:ro" \
  apache/spark:3.5.8-scala2.12-java17-python3-ubuntu \
  /opt/spark/bin/spark-submit /workspace/streaming/test_bronze_ingest.py
```

Milestone 2 includes the continuous Kafka-to-Delta Bronze path, persistent
storage, checkpoint recovery, and the Milestone 1 smoke test. It does not include
Silver, business deduplication, windows, watermarks, HLL, anomaly detection,
DuckDB, LLM features, FastAPI, Cypher, or changes to Ratchet.
