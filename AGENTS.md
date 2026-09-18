# AGENTS.md — Nexa

## Project purpose

Nexa is a self-hosted Data Engineering pipeline that processes real
reservation events published by Ratchet.

The planned architecture is:

Ratchet
→ Redpanda/Kafka
→ Spark Structured Streaming
→ Delta Lake Bronze
→ Delta Lake Silver
→ deterministic anomaly detection
→ DuckDB Gold
→ periodic report generation
→ API protected by Cypher tokens

The core of the project is Data Engineering: ingestion, streaming,
transformation, medallion architecture, fault tolerance, traceability, and
verification. The LLM layer is optional: it writes explanations, but it never
decides what is an anomaly.

Everything must run locally with permanent zero cost.

## Sources of truth

Before proposing or implementing substantial changes, inspect:

1. The current code and tests.
2. git status, the diff, and recent history.
3. docs/bitacora-nexa.md.
4. README.md.
5. Proyecto_Nexa.md, when it exists locally.

Use this order of authority:

- Code, tests, and Git: what is really implemented.
- The learning log: decisions, learning, and verified closures.
- README: current public installation, operation, and use.
- Proyecto_Nexa.md: private planning and historical intentions.

Do not repeat a claim as current when newer evidence contradicts it.

Proyecto_Nexa.md is ignored locally and is private. Do not publish it, add it
to Git, or modify it unless the task explicitly asks to review or update the
plan.

## Known baseline

These are historical references, not a replacement for inspection:

- Milestone 1 established Spark 3.5.8, Python 3.10, Java 17, and local[2].
- Milestone 2 implemented Redpanda/Kafka → Spark Structured Streaming → Delta
  Bronze.
- The Milestone 2 closing commit is 5aaf17e feat: implement Bronze streaming
  ingestion.
- Bronze keeps the original JSON and the Kafka metadata needed for traceability.
- Bronze and its checkpoints live under data/, which must not be versioned.
- The original next step was Silver, windows, watermarking, and approximate
  counts, but the roadmap must be checked before assuming it is current.

## Integrations and contracts

Ratchet is Nexa's only data source:

- Topic: reservation.events.v1.
- Kafka message key: resourceId.
- JSON contract versioned through eventVersion.
- Known types:
  - RESERVATION_HOLD_CREATED
  - RESERVATION_CONFIRMED
  - RESERVATION_RELEASED
  - RESERVATION_EXPIRED
  - RESERVATION_REJECTED
- eventId identifies an event.
- holdId identifies a held reservation and is not present in every type,
  especially RESERVATION_REJECTED.
- Ratchet publishes with at-least-once delivery; business consumers must handle
  duplicates by eventId.

Nexa is a read-only consumer. It does not call Ratchet by REST, write to its
databases, or modify Ratchet unless a separate task explicitly authorizes it.

Cypher is not part of the internal pipeline. Its future role is limited to
protecting the external API through local JWT RS256 validation against its JWKS.

## Architectural constraints

- Permanent zero-cost infrastructure.
- Fully self-hosted and reproducible with Docker Compose.
- Python/PySpark as the main language.
- Spark Structured Streaming and Delta Lake for Bronze/Silver.
- DuckDB as a Gold candidate, subject to review before implementation.
- Deterministic and auditable anomaly rules.
- The LLM may explain calculated results, but never classify them.
- Do not implement future milestones early.
- Do not add speculative services, dependencies, abstractions, or settings.
- Do not use Databricks, Snowflake, Confluent Cloud, Redpanda Cloud, or another
  paid SaaS service as a v1 dependency.
- Check compatibility and behavior against official documentation for the
  versions actually used.

## Learning and implementation method

For a new milestone or substantial concept, use this learning loop:

explanation
→ the user's answers to questions about the explanation
→ correction of doubts and another explanation by the user
→ the next small concept
→ a short design
→ one explicit approval
→ implementation
→ verification
→ diff review
→ documentation

Do not turn trivial details into lessons. Use simple English that matches the
user's current level and explain important technical decisions clearly.

Before implementing a milestone, explain in the chat:

- what will be built;
- why it is needed;
- which files will change;
- how it will be verified;
- what is explicitly out of scope.

Wait for one explicit design approval. Do not create extra documents only to
request an equivalent approval.

If the user's explanation contains an error, correct it with evidence. Do not
agree just to be polite.

At the end of a milestone:

- document only what the user actually understood or said, plus verified code,
  tests, and limits;
- review the complete diff;
- prepare a copy-paste review prompt for Astra's main chat;
- do not claim that self-review is independent review.

## Implementation and tests

- Look for existing patterns and helpers first.
- Prefer native Spark, Python, and already-installed dependencies.
- For non-trivial logic, write the smallest check that can fail before the
  implementation.
- For infrastructure, leave an executable equivalent validation.
- Test negative cases, recovery, and relevant limits, not only the happy path.
- Do not delete checkpoints or real data to make a test pass.
- Use temporary paths for destructive or recovery tests.
- Do not claim that something works without recent verification.
- Separate properties supported by documentation from behavior demonstrated by
  an experiment.
- Watermarking, checkpointing, and anomaly detection require an independent
  review before the commit.

## Documentation

Maintain only:

- README.md: public and operational documentation, in English.
- docs/bitacora-nexa.md: learning, decisions, and closures, in English.
- Proyecto_Nexa.md: private planning, ignored by Git, in English.

Do not create specs, ADRs, reports, or parallel plans without a concrete need
and explicit approval.

Do not invent text and present it as the user's personal explanation. The
learning log must reflect what the user really understood and expressed.

## Git and safety

- Preserve other people's changes.
- Do not use destructive operations to clean the workspace.
- Before committing, review git status, git diff, git diff --cached, and the
  complete final diff.
- Keep commits small and complete, with English messages.
- Do not commit or push without separate explicit authorization.
- Never version data/, checkpoints, generated Delta files, caches, secrets,
  .env, or private documentation.
- Do not start Desktop and CLI writers at the same time for the same session.

## Communication

Speak English with the user. Use simple wording and correct important English
mistakes briefly when useful.

Clearly separate:

- verified facts;
- assumptions;
- recommendations;
- pending decisions.

Be direct, educational, and critical. Prefer the smallest correct and
reproducible solution. When ambiguity would materially change the result, ask
the user instead of inventing a decision.
