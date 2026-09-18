# Nexa learning log

## How to use this document

This is a cumulative log. It exists so that Nexa can still be understood months
later and so that future chats can continue from the same point.

Each milestone gets its own section. Earlier explanations should not be deleted
just because a later decision becomes more technical. First record what the
user understood; then record what was implemented and verified.

This is the only versioned internal document for learning and milestone
decisions. Public operation belongs in README.md. The code and its tests are
the source of truth for what is implemented.

The learning log is now written in English. It must still reflect the user's
real explanations and must not invent personal statements.

## State on September 2, 2026

- Nexa had the minimum Milestone 1 infrastructure, but no business code.
- Milestone 1 had its first conceptual checkpoint: Docker, Compose, Redpanda,
  Docker networks, ports, and Spark.
- The Milestone 1 design was approved and its implementation was verified with
  the official Spark image.
- Milestone 2 had not started yet.
- Proyecto_Nexa.md had not been modified. It was private, locally ignored, and
  must not be copied or published.

This distinction mattered: the early sections record previous learning and
verification; section 13 records the infrastructure that was actually
implemented and verified.

## 1. Starting point

The user had used Docker Compose in other projects, but knew the practical
steps better than the complete mental model. These ideas were often mixed:

- Docker image;
- container;
- Docker Compose;
- virtual machine;
- ports and endpoints;
- communication between services.

Redpanda was known to be related to Ratchet events, but was described as an
observer or notifier. It was not yet clear that Spark is the processing engine,
or how this connects to Databricks.

The conversation separated these layers before code was written. That order is
part of the purpose of Nexa: do not add infrastructure that cannot be
explained.

## 2. Docker mental model

### Image

An image is a reusable package containing an application, its dependencies,
and the user environment it needs. It is like a prepared template, not a
program that is already running.

It is not a complete virtual machine. A container shares the host kernel,
although its process, filesystem, network, and dependencies are isolated.

### Container

A container is an image in execution. Running the same image twice creates two
separate containers, each with its own process and runtime state. What happens
inside a container does not change the base image.

The entry point to a service is normally a port. A port is a network door
through which an application communicates with Redpanda or another service.

### Docker Compose

Compose is a configuration file and an operational recipe. It describes which
services to start, which images to use, which environment variables they need,
which ports to publish, which volumes to persist, and which networks to use.

Saying that Nexa has its own Compose file does not mean that Nexa has its own
virtual machine. It means that Nexa has an independent recipe for starting and
configuring its infrastructure.

## 3. What exists in Ratchet

Ratchet's infrastructure Compose file defines:

1. PostgreSQL for transactional data.
2. Redis for expiration support and caching.
3. Redpanda for receiving and distributing events.

The four Java microservices—inventory, reservation, payment, and
notification—are not defined in that infrastructure Compose file. The Ratchet
README says they are run separately with Maven or their JAR files.

An earlier check found no running Docker containers. That described Docker at
that moment; it did not mean the Compose file was unusable.

## 4. What Redpanda does

Redpanda is an event broker and log compatible with the Kafka API. It does not
automatically know what happens inside Ratchet and does not decide whether a
reservation is valid.

Ratchet decides which business facts to publish:

    Ratchet performs an operation
        ↓
    Ratchet stores an event in its PostgreSQL Outbox
        ↓
    the publisher sends it to Redpanda
        ↓
    consumers read it

A message can say that a reservation was created or confirmed. The canonical
topic is reservation.events.v1. Notification Service and Nexa can consume the
same topic independently.

Redpanda does not replace PostgreSQL. PostgreSQL stores Ratchet's transactional
state. Redpanda transports and retains events so that other components can
react to facts that have already been published.

The precise description is:

> Redpanda is not Ratchet's observer. It is the place where Ratchet publishes
> events and where consumers can read them.

## 5. Shared Docker network

A shared Docker network is similar to a private LAN between connected
containers:

    Ratchet Redpanda ─── shared Docker network ─── Nexa Spark

It does not connect images or virtual machines. It connects running containers
and lets them find each other by name and communicate over TCP.

It also does not give Nexa automatic access to Ratchet's PostgreSQL, Redis, or
all of its microservices. The purpose is to connect Spark to Redpanda while
keeping the other components outside Nexa's scope.

A shared Docker network is not an encrypted channel. It provides local
connectivity and isolation, not complete service-to-service security.

## 6. Host ports and internal names

Ratchet uses two ways to reach the same Redpanda:

- localhost:9092 is for a program running directly on the host;
- redpanda:29092 is for a container connected to the Docker network.

Inside a container, localhost means that same container. It does not mean the
host computer or another container. Spark must therefore use redpanda:29092.

The two ports do not necessarily mean two Redpanda servers. They are listeners
for different network contexts.

## 7. What Spark does in Nexa

Spark is the processing engine. It is not the event broker, the database, or
the tool that coordinates containers.

Milestone 1 only needed to prove that Spark starts and can run a minimal check.

The Milestone 2 flow was:

    Redpanda
        ↓ JSON events
    Spark Structured Streaming
        ↓ incremental processing
    Delta Lake Bronze
        ↓ later
    Silver and later layers

Spark receives structured events that follow a contract, for example
RESERVATION_CONFIRMED with fields such as eventId, resourceId, occurredAt, and
payload.

In Milestone 2, Spark keeps the original JSON and the approved technical
columns in Bronze. It does not yet transform the event into Silver metrics.

### What incremental means

Incremental does not mean that every event is processed only once forever, or
that data can never repeat. It means that a continuous query processes new
records from the last known offset or checkpoint, normally in small
micro-batches.

Instead of reading the whole topic again whenever an event arrives, Spark
processes the new part and keeps the state needed to continue. Checkpoints and
at-least-once delivery were studied in more detail before Milestone 2.

## 8. Why Spark helps learn Databricks

Databricks is a managed platform, not simply another name for Spark.
Databricks provides the platform, compute resources, job management, and other
capabilities. Apache Spark is the processing engine used inside that ecosystem,
and Delta Lake is one of its main storage layers.

Nexa therefore practices local concepts that transfer to Databricks work:

- DataFrames and transformations;
- reading event sources;
- Structured Streaming;
- offsets and checkpoints;
- Bronze/Silver/Gold data layers;
- later aggregations and event-time processing.

The correct claim is that Nexa uses self-hosted Apache Spark Structured
Streaming and Delta Lake, which are technologies used in the Databricks
ecosystem. Nexa does not claim to use Databricks.

## 9. End-to-end flow that was understood

    Ratchet creates or changes a reservation
        ↓
    Ratchet records the event in its Outbox
        ↓
    Ratchet publishes JSON to Redpanda
        ↓
    Redpanda keeps the event in reservation.events.v1
        ↓
    Nexa Spark reads it as an independent consumer
        ↓
    Delta Bronze keeps the raw evidence
        ↓
    Spark later transforms Bronze into Silver

Nexa is a read-only consumer. It does not call Ratchet, write to Ratchet's
PostgreSQL or Redis, or publish changes to Ratchet or Cypher.

## 10. Early verification record

- Proyecto_Nexa.md was read and was not modified at that time.
- The Nexa repository was almost empty and had no project commits yet.
- No additional local instructions were found then.
- Ratchet had unrelated local changes, which were left untouched.
- Ratchet commit cb83df2 implemented the event contract in
  reservation.events.v1.
- The five canonical event types were implemented: hold created, confirmed,
  released, expired, and rejected.
- DOUBLE_BOOKING_BLOCKED was not part of that contract.

## 11. What was not implemented at that point

The conceptual and technical Milestone 2 checkpoint still needed to cover:

- topic;
- partition;
- message key;
- offset;
- consumer group;
- Spark checkpoint;
- at-least-once delivery;
- delivery duplicate versus a different business event;
- eventId versus holdId.

## 12. Recommended flow for future chats

Each new milestone chat should:

1. read this log and Proyecto_Nexa.md;
2. explain the next concept in small parts;
3. ask the user questions about each explanation;
4. correct the user's answer and ask for another explanation when needed;
5. separate learned concepts, approved decisions, and real code;
6. verify tests before changing the status;
7. record what was out of scope;
8. create a review prompt for Astra after implementation.

The log exists inside the repository so that future chats can read it and
continue from the same workspace. It is the most direct and auditable learning
context for Nexa.

## 13. Verified closure of Milestone 1

The approved minimum infrastructure was implemented. docker-compose.yml had one
one-shot service based directly on
apache/spark:3.5.8-scala2.12-java17-python3-ubuntu. The smoke script was
mounted read-only and the container was connected to the external
ecosistema-network.

The real execution created a SparkSession with local[2] and verified that
spark.range(1).count() returned 1. The observed output was:

    NEXA_SMOKE_TEST_OK spark=3.5.8 python=3.10.12 java=17.0.17 master=local[2] count=1

The exact image pull finished successfully and Compose returned code 0. The
one-shot container stopped as expected. ecosistema-network was declared
external and available, but Milestone 1 did not consume Redpanda or read
events.

Kafka/Redpanda consumers, Delta Lake, Bronze/Silver, checkpoints, DuckDB,
FastAPI, and changes to Ratchet were outside Milestone 1. The next learning
boundary was Milestone 2: understand the event contract and Redpanda without
implementing future layers early.

## 14. Personal Milestone 1 closure

Docker was previously understood as a kind of packaged virtual machine. It is
now understood as a way to package and run isolated processes: the image is the
package, the container is the running package, and Compose is the recipe for
starting and connecting those containers.

Redpanda was previously described as an observer. It is now understood that
Ratchet publishes concrete events there and that consumers read them without
accessing Ratchet's internal state.

Spark now has a clear place in the architecture: it does not observe or
coordinate services; it processes events and, in Milestone 2, keeps raw
evidence in Delta Bronze. Later it will transform that evidence into curated
data.

## 15. Milestone 2 — learning, decisions, and verification

### 15.1 How the learning happened

This milestone was worked through interactively and in short parts. The user
explained ideas in their own words, received corrections, and tried again when
something was still confused. The log should preserve that process, including
the questions that improved the mental model.

At first, a topic was understood as a place where Ratchet records transactions,
and a resourceId was thought to send each resource to its own partition. The
correction was that the key helps choose a partition, but many resources can
share one partition. Kafka preserves order within a partition. Offsets are
positions assigned by the broker inside each partition, so different resources
can be interleaved without losing per-partition order.

Redpanda was also questioned as if it needed to keep everything in memory.
The resulting model was that Redpanda persists the log and uses bounded memory
to operate. Consumers do not receive everything at once. Spark processes
limited micro-batches. If producers are faster, consumer lag can grow, but
there is no magic protocol that makes Nexa catch up instantly.

Kafka and Redpanda were separated. Redpanda is the available broker, while
Kafka is the compatible API and protocol. Spark and Notification Service are
independent consumers. They should not use the same consumer group when both
need every event, because a group distributes partitions among its members.

The conceptual flow became:

    Ratchet produces business facts
        ↓
    Redpanda, a Kafka-compatible broker
        ↓
    topic → partitions → records with key, value, and offset
        ↓
    independent consumer groups
        ↓
    Spark continuous query
        ↓
    micro-batches → Delta Bronze

The difference between a business event and a technical record became clearer.
A hold can have several events: created, confirmed, released, expired, or
rejected. eventId identifies each event; holdId identifies the hold. Therefore,
two messages with the same holdId and different eventId values can be valid
lifecycle events, not duplicates.

In Bronze, payload and complete event were initially confused. The correction
was that payload is a field inside the JSON value and depends on the event
type. Data Lake, Delta Lake, and Bronze were also separated: Data Lake
describes storage, Delta Lake provides table format and transaction history,
and Bronze is the first logical data layer on that storage.

The checkpoint was also separated from Bronze. Bronze stores data. The
checkpoint stores query progress and state so the query can continue.
startingOffsets=latest chooses the starting point for a new query. When a
checkpoint already exists, Spark resumes from it instead of resetting it with
startingOffsets.

Three different duplicate situations were identified:

1. Spark may reread or retry the same micro-batch.
2. The native Delta sink coordinates its transaction log with streaming
   progress, so the same batch with the same query and checkpoint is
   idempotent according to Delta documentation.
3. A producer may publish the same business fact twice as two Kafka records with
   different offsets. Bronze intentionally keeps both.

The third case is not solved by the sink commit. It requires business
deduplication in a later layer. A crash exactly at the commit boundary was not
forced, so that property was documented but not locally demonstrated.

The learning method that worked best was:

- first explain where the project is in the complete flow;
- use tables, hierarchies, and diagrams when relationships matter;
- move in short parts and ask the user to rebuild each concept in their own
  words;
- correct similar terms such as event, payload, eventId, and holdId;
- keep real questions in the final learning record.

### 15.2 Approved decisions

- Build only Redpanda → Spark Structured Streaming → Delta Lake Bronze.
- Create streaming/bronze_ingest.py and add spark-bronze without removing the
  smoke test.
- Consume only reservation.events.v1 from redpanda:29092.
- Reuse Spark 3.5.8, Scala 2.12, Java 17, Python 3.10, and local[2].
- Use the pinned Kafka and Delta packages already selected for the project.
- Configure Delta extensions and use a continuous append query.
- Keep exactly six Bronze columns: key, value, topic, partition, offset, and
  timestamp. value keeps the complete JSON string.
- Persist data/bronze and data/checkpoint/bronze through local bind mounts.
- Use a one-shot root initializer only to create directories and permissions.
  Spark still runs as UID 185.
- Use startingOffsets=latest only for a new query without a checkpoint.
- Do not implement business deduplication, Silver, windows, watermarking, HLL,
  anomalies, DuckDB, LLM, FastAPI, Cypher, or Ratchet changes in this milestone.
- Do not claim global exactly-once processing or business uniqueness.

### 15.3 Implementation and adjustments

The implementation stayed small. The job reads Kafka, projects the six
approved fields, writes Delta in append mode, and waits continuously. It logs
the topic, broker, Bronze path, and checkpoint path.

The first real execution found that the Spark image runs as user spark, whose
HOME is /nonexistent. Ivy could not write its package cache. The minimum fix
was spark.jars.ivy=/tmp/.ivy2. No new dependency or abstraction was added.

A second execution found a bind-mount permission problem. In a checkout without
data/, Docker creates the directory as root:root, so UID 185 cannot create the
checkpoint. The minimum fix was spark-bronze-init, a one-shot root service that
creates the directories, sets ownership to 185:185, and sets mode 0755. It does
not delete files or run Spark as root.

### 15.4 Verification evidence

The historical verification record says:

- docker compose config --quiet succeeded.
- docker compose pull spark-smoke-test succeeded.
- The Milestone 1 smoke test printed:

      NEXA_SMOKE_TEST_OK spark=3.5.8 python=3.10.12 java=17.0.17 master=local[2] count=1

- Redpanda was started from Ratchet's infrastructure Compose and connected to
  ecosistema-network without modifying Ratchet.
- rpk cluster info confirmed redpanda:29092.
- reservation.events.v1 was created with one partition.
- Three controlled events received partition 0 offsets 0, 1, and 2.
- spark-bronze started and loaded the Kafka and Delta artifacts.
- The permission failure was reproduced in a temporary checkout and then fixed
  with the initializer.
- The initializer preserved existing Delta logs and checkpoint offsets.
- A Delta query showed the controlled records with their topic, partition,
  offset, key, eventId, JSON value, and timestamp.
- A controlled fourth event received offset 3 after an ordered restart.
- The local projection test printed NEXA_BRONZE_PROJECTION_TEST_OK.
- data/ was ignored by Git.

### 15.5 Limits and pending work

The Kafka-to-Bronze test used controlled records published with rpk. It did
not verify a complete HTTP operation from Ratchet or run k6.

The restart test was an ordered stop and restart. It did not force a crash at
the commit boundary. Delta documentation supports idempotent native sink
commits, but that is not global exactly-once processing or business
uniqueness.

The test used one partition and did not demonstrate sustained load or
multi-partition balancing. Ratchet's Compose file still does not declare the
shared network, so the Redpanda connection must be repeated if that container
is recreated.

Milestone 2 was later committed as 5aaf17e. Ratchet was not modified.

## 16. Milestone 3 — aggregate experiment, decisions, and verification

### 16.1 What was understood before implementation

The first clarification was the responsibility of each layer. value is the
JSON event published by Ratchet. Bronze keeps it raw with Kafka trace data and
does not change it. Silver reads Bronze and creates curated data without
deleting or rewriting Bronze.

Three times were separated:

- occurredAt: when the business event happened;
- Kafka timestamp: when the broker recorded the message;
- processing time: when Spark observed it.

The aggregate used occurredAt because event-time windows should represent the
business event, not the moment Spark processed it.

A tumbling window divides time into consecutive blocks, such as 00–05 and
05–10. A sliding window would overlap, but was not needed. A watermark is not
a timer attached to the last record. Spark calculates it from the largest event
time seen minus the configured delay. It lets Spark close old state while still
accepting reasonably late records.

Query state, checkpoint, and Delta log were separated:

- state holds intermediate window and deduplication information;
- the checkpoint stores query progress and state for recovery;
- the Delta log records table commits.

event_count counts valid events after eventId deduplication. distinct_users is
an approximate holderRef count using HyperLogLog. HLL saves memory; it does not
deduplicate events.

### 16.2 Approved aggregate design

- Create an independent job in streaming/silver_aggregate.py.
- Read Bronze with readStream and use data/checkpoint/silver.
- Parse JSON with an explicit schema.
- Accept eventVersion 1 and the five Ratchet event types.
- Require top-level fields and require holdId except for
  RESERVATION_REJECTED.
- Keep invalid rows in Bronze and show them through a console stream.
- Use five-minute tumbling windows and a five-minute watermark.
- Deduplicate by eventId in the streaming state.
- Use approx_count_distinct(holderRef, rsd = 0.05).
- Write append-mode aggregate rows to data/silver.
- Wait for a Bronze Delta log before starting and never reuse or delete the
  existing checkpoint.

Anomalies, Gold/DuckDB, reports, LLM, FastAPI, Cypher, and Ratchet changes were
explicitly outside this milestone.

### 16.3 Implementation and adjustments

The implementation added the aggregate job, a unit test, a temporary Delta
streaming test, and the spark-silver service.

The first test showed that from_json can return a null struct for invalid JSON
without distinguishing it from missing fields. A small get_json_object check
was added so the reason invalid_json is retained.

The validation review also found that a missing eventVersion could pass through
Spark's three-valued logic. It was explicitly marked as
unsupported_event_version.

The later audit commit bb6a6ee made these changes:

- supervise both streaming queries with awaitAnyTermination();
- remove the unsupported withEventTimeOrder reader option;
- add a regression test for failure propagation;
- document the actual guarantees and limits in README.md.

### 16.4 Historical verification evidence

The repository records these checks for the aggregate milestone:

- docker compose config --quiet succeeded after adding spark-silver.
- streaming/test_silver_aggregate.py printed
  NEXA_SILVER_PROJECTION_TESTS_OK.
- streaming/test_silver_stream.py printed NEXA_SILVER_STREAM_TESTS_OK.
- The stream test covered an initial snapshot, exact windows, a late event
  inside the watermark, an event after window closure, and checkpoint
  recovery.
- The Bronze projection regression printed
  NEXA_BRONZE_PROJECTION_TEST_OK.
- The smoke test printed the expected Spark, Python, Java, and local[2] values.

This session did not rerun those tests. The statements above are historical
repository records, not new verification performed during the documentation
pass.

### 16.5 Limits of the aggregate milestone

The aggregate is not a durable event-level Silver table. It does not preserve
one curated row per event, does not create a durable quarantine table, does not
detect conflicting content for one eventId, and does not group by resourceId.

Watermark-based aggregation may discard old data from its state. The current
eventId state is checkpoint-scoped and is not durable business uniqueness.
HLL is approximate. Invalid rows remain available in Bronze but are not stored
in a quarantine table.

The tests used local temporary Delta directories. They did not demonstrate a
complete HTTP-to-Bronze path from Ratchet, sustained load, multiple
partitions, or a crash at a precise commit boundary.

Milestone 3 was committed in 7f77e5e. The audit corrections were committed in
bb6a6ee. The earlier statement that the Milestone 3 changes were still
uncommitted was stale and has been corrected by this log.

## 17. Current verified repository state

This section records the documentation audit performed on September 18, 2026.

- Nexa HEAD is bb6a6ee, and main matched origin/main.
- The Nexa worktree was clean before this documentation pass.
- Existing Bronze Delta data and data/checkpoint/bronze were present.
- data/silver_events, data/quarantine_events, and
  data/checkpoint/silver_events were unused.
- The existing aggregate Silver table and checkpoint were not changed.
- Ratchet was inspected read-only. Its unrelated local changes were preserved.
- Ratchet's current producer code and tests confirm the five supported event
  types, the event envelope, the resourceId Kafka key, at-least-once
  publication, and REJECTED without holdId.
- No durable event-level Silver implementation exists yet.
- No data migration, checkpoint reset, commit, or push was performed during
  this documentation pass.

The next learning milestone is event-level Silver quality: validation,
business deduplication, conflict handling, quarantine, idempotent retries, and
recovery. Metrics, watermarks, and anomaly detection remain later work.
