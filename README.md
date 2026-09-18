# Nexa — Milestones 2 and 3

## Current verified state

The current repository is at commit bb6a6ee. Milestones 1 and 2 are
implemented. The historical Milestone 3 aggregate job is implemented and its
known limits are documented below. A durable event-level Silver table and a
quarantine table do not exist yet; they are the next learning task.

The current worktree and origin/main matched during the September 18, 2026
audit. Existing Bronze data and its checkpoint were preserved. No migration or
cutover of the aggregate Silver table was performed.

Milestones 2 and 3 implement the ingestion and first curation paths:

```text
Ratchet → Redpanda reservation.events.v1 → Spark Structured Streaming → Delta Lake Bronze → Delta Lake Silver
```

Nexa consumes the topic as an independent consumer. It stores one append-only
Bronze row per Kafka record, preserving the original JSON value and the Kafka
metadata needed for traceability. Silver reads Bronze as a separate streaming
query and writes window aggregates after basic validation; it never changes
Bronze. Window state is bounded, but the current event-ID deduplication state
is not.

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
root only long enough to create `data/bronze`, `data/silver`, and both
checkpoint directories, assign them to UID/GID `185`, and set mode `0755`. It
does not delete, replace, or initialize existing Bronze, Silver, or checkpoint
files.

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
├── silver/                  # Delta table data and _delta_log
└── checkpoint/
    ├── bronze/              # Bronze Spark checkpoint
    └── silver/              # Silver Spark checkpoint
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

## Run Silver aggregation

Start Bronze first. Silver waits for the Bronze Delta log before starting, so
the two services can also be started together after the initializer exists:

```bash
docker compose up -d spark-bronze
docker compose up -d spark-silver
docker compose logs -f spark-silver
```

Silver reads the existing Bronze snapshot and later Delta commits through
`readStream`; it does not consume Kafka and it never updates or deletes Bronze.
The same Silver checkpoint must stay with the Silver table across restarts.

Silver parses the JSON with an explicit schema and accepts `eventVersion = 1`
and the five Ratchet event types. All required top-level fields must be
present; `holdId` is required except for `RESERVATION_REJECTED`, which is a
valid Ratchet event without one. Invalid rows remain available in Bronze and
are reported by the Silver query's console warning stream. There is no
rejected-data table in this milestone.

This is basic validation, not full enforcement of the Ratchet contract: UUID
format, Kafka key/resource consistency, JSON scalar types, explicit timestamp
timezone, complete per-type payloads, and excessive future event times are not
yet checked. In particular, a far-future `occurredAt` can advance the watermark
and cause later input to be treated as late. Do not treat these aggregates as
production-ready anomaly inputs.

Both streaming queries are supervised: a failure in either the aggregate sink
or the invalid-event console stream propagates and stops the job. The console
stream has no persistent checkpoint and can repeat warnings after a restart;
Bronze, not the console output, is the durable invalid-event evidence.

The Silver output is an append-only Delta table with these columns:

| Column | Meaning |
|---|---|
| `window_start`, `window_end` | Five-minute tumbling event-time window |
| `event_type` | Valid Ratchet event type |
| `event_count` | Accepted events after checkpoint-scoped `eventId` deduplication |
| `distinct_users` | Approximate distinct `holderRef` count using HLL, `rsd = 0.05` |

The query uses `occurredAt` as event time and a five-minute watermark. A late
event inside that bound can update an open window; an event arriving after the
window has been finalized is dropped by Spark's bounded state. HLL saves memory
for the user estimate; it is not the event deduplication mechanism.

`dropDuplicates(["eventId"])` retains IDs across batches for the lifetime of
the checkpoint; the watermark does not evict those IDs because event time is
not part of this operator's key. State therefore grows with distinct IDs even
after old windows close. This is not durable business uniqueness independent
of the checkpoint, nor does it detect conflicting payloads for the same ID.
See Spark's [streaming deduplication documentation](https://spark.apache.org/docs/3.5.8/structured-streaming-programming-guide.html#streaming-deduplication).

The initial Bronze snapshot is not guaranteed to arrive in event-time order.
The previously supplied `withEventTimeOrder` option is not implemented by the
pinned OSS Delta 3.3.0 reader; it has been removed rather than presented as a
safety guarantee. A snapshot spanning multiple batches can interact with the
watermark, so the existing small-snapshot test does not establish complete
historical aggregation. See the pinned [Delta reader options](https://github.com/delta-io/delta/blob/v3.3.0/spark/src/main/scala/org/apache/spark/sql/delta/DeltaOptions.scala).

The table groups by window and event type, not resource, and does not persist
curated individual events. Changing this layout or the stateful operators
requires an explicit migration with separate output/checkpoint paths; do not
delete or reuse the current checkpoint to make a changed query start.

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

Silver's unit and Delta streaming tests use the same Spark image. The
integration test also resolves the pinned Delta package:

```bash
docker run --rm -v "$PWD:/workspace:ro" \
  apache/spark:3.5.8-scala2.12-java17-python3-ubuntu \
  /opt/spark/bin/spark-submit --master local[2] \
  /workspace/streaming/test_silver_aggregate.py

docker run --rm -v "$PWD:/workspace:ro" \
  apache/spark:3.5.8-scala2.12-java17-python3-ubuntu \
  /opt/spark/bin/spark-submit --master local[2] \
  --packages io.delta:delta-spark_2.12:3.3.0 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  /workspace/streaming/test_silver_stream.py
```

Milestones 2 and 3 include the continuous Kafka-to-Delta Bronze path, the
validated Bronze-to-Silver Delta stream, persistent storage, checkpoint
recovery, bounded event-time aggregation, and the Milestone 1 smoke test. They
do not include deterministic anomaly detection, Gold/DuckDB, LLM features,
FastAPI, Cypher, or changes to Ratchet.
