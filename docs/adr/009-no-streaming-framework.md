# ADR 009: No Kafka / streaming framework

**Status:** Accepted
**Date:** 2026-04

## Context

Modern data engineering discourse treats streaming as the default. "Real-time" is
implicitly respected; batch or micro-batch is implicitly deprecated. The
instinctive architecture for GTFS-RT ingestion would be:

```
TTC feed → Kafka → consumer → Postgres
```

This feels more "real" and is a common reach for small projects wanting to look modern.

## Decision

**Do not** introduce a streaming framework. Poll the feeds on a cron, insert to Postgres, let the database be the source of truth.

## Volume analysis

- TTC vehicle positions: ~2,000 active vehicles × 1 message / 30s = **~67 messages/second**
- TTC trip updates: ~2,000 trips × 30 stops × 1 / 30s = **~2,000 stop-time-updates/second** at peak (but these are batched in a single feed message)
- Service alerts: ~dozens of messages, updated every few minutes

By any streaming framework's design targets, this is a rounding error.

## Alternatives considered

### Kafka + a consumer service
The conventional answer.

*Rejected because:*
- At this volume, Kafka adds operational surface area (brokers, zookeeper or KRaft, topic config, consumer group management) for no latency benefit.
- Our latency requirement is "users see fresh data" — a 30-second poll cadence meets it.
- Introducing Kafka here would add complexity without a justifying workload. Tools should be chosen for what they solve, not for what they look like.

### Redpanda
Same shape as Kafka, better single-node story.

*Rejected for the same reason:* a message bus is the wrong layer at this volume.

### Postgres LISTEN/NOTIFY as a pseudo-streaming layer
Interesting, but solves a problem (fan-out to multiple consumers) we don't have.

*Rejected:* no multi-consumer use case in v0.1–v0.3.

## Consequences

**Accepted costs:**
- We cannot truthfully describe QuantumLane as a "streaming architecture." Good — accuracy matters more than aesthetics.
- Slightly higher end-to-end latency than a pure streaming setup (poll interval + insert time). At 30s poll + <100ms insert, this is well within the "fresh" band for users.

**Benefits:**
- One fewer stateful service to operate, monitor, and back up.
- Simpler mental model: all state is in Postgres.
- Reduced cost (no Kafka broker memory/disk).

**When this decision should be revisited:**
- If QuantumLane ever ingests a feed that legitimately requires sub-second latency (it won't, GTFS-RT is inherently ~15-60s fresh at source).
- If a second consumer of the raw feed stream appears (e.g., an alerting system that needs the same data as the ingester does). Even then, the right first answer is probably a fan-out table in Postgres, not Kafka.
