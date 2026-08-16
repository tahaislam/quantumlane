# ADR-017 — Compute schedule-adherence delay at event-time; stop-level overwrite for live state

**Status:** Accepted

## Context

QuantumLane needs to answer two questions about delay:

1. **Live:** "How delayed is route X *right now*?" — for the API, the MCP server, and the live map. Needs to be seconds-fresh.
2. **Historical:** "How reliable is route X *typically*?" — for dashboards and trend analysis. Daily granularity is sufficient.

Both require schedule-adherence delay = `actual_time − scheduled_time`, which means both depend on the static GTFS schedule (P2.1). Two design problems surface:

- **Temporal correctness.** The static GTFS loader is full-replace, single-snapshot (the schema's PKs are not compound with `snapshot_date`, so only one schedule snapshot exists at a time). If historical delay were recomputed later from raw RT + the *current* schedule, it would compare past actuals against a schedule that may have changed at a service-board boundary — incorrect.
- **Write volume.** TTC's `trip_updates` re-reports the same predictions every poll (~10M rows/day), the vast majority of which are superseded predictions for stops a vehicle hasn't reached yet. Logging every prediction to compute delay is enormously wasteful.

(Note: TTC leaves the GTFS-RT `delay` fields NULL across all rows, so delay must be *derived* from predicted arrival vs. scheduled arrival, not read from the feed.)

## Decision

**Compute delay once, at event-time, against the then-current schedule, and persist the result. Maintain live state with a stop-level overwrite (upsert), freezing each record when the vehicle arrives.**

Concretely:

- One row per `(trip_id, stop_sequence)` in a live delay table. Each RT poll **upserts** the predicted delay for that stop (`INSERT ... ON CONFLICT (trip_id, stop_sequence) DO UPDATE`), overwriting the prior prediction.
- When the vehicle arrives at the stop (derived from `current_status` / `stop_sequence` advancing), the record is **finalized** — the delay is now actual, not predicted, and will not change.
- The delay is computed against the current schedule *at the moment it is written*. Because the value is baked into the record at write-time, the schedule can be overwritten freely afterward — the persisted delay no longer depends on it.
- **Historical reliability is an aggregation of these finalized delay records** (`GROUP BY route, hour, day`), not a recomputation from raw RT + schedule.

This makes delay a single pipeline: live state (current predictions) → finalized records (on arrival) → historical aggregates (grouped). Headway regularity remains a separate, RT-only metric (no schedule dependency).

## Alternatives considered

- **Recompute historical delay from raw RT + retained schedule history.** Rejected: requires versioning the static schedule by snapshot (compound PKs, retained history) to be correct across service-board changes, *and* races the daily schedule refresh. Computing at event-time and persisting the result dissolves both problems — no schedule history needed, no refresh-ordering dependency.
- **Log every prediction, compute delay in batch later.** Rejected: stores ~10M superseded predictions/day for no benefit. The stop-level overwrite collapses this to one row per trip×stop, finalized on arrival — the same low-cardinality collapse seen in the Parquet compression, applied at write-time.
- **Compute live delay on-read only (no persistence).** Rejected for the historical need: the live gauge alone is fine on-read, but history requires the finalized records to exist as durable facts. Persisting at event-time serves both.

This event-time pattern is applicable *because* the delay computation is a cheap, deterministic lookup (trip + stop → scheduled time). If it were expensive or required cross-event context, on-the-hot-path computation would be the wrong call — batch would win. The decision is specific to a cheap, deterministic per-event computation.

## Consequences

- **Costs:**
  - Need a defined "arrival/finalize" trigger from the RT feed (`current_status` = STOPPED_AT, or `stop_sequence` advancing past the stop). TTC feed quality determines how clean this is.
  - Need a sweep for **missed finalization**: a vehicle that vanishes from the feed (gap, cancellation) before arriving leaves a never-finalized prediction. A timeout sweep must finalize-as-last-known or mark abandoned, or stale predictions accumulate.
  - Grain keyed on `(trip_id, stop_sequence)` (not `stop_id`) because a route can serve the same `stop_id` twice (loops); this matches the `static_gtfs.stop_times` PK.
- **Benefits:**
  - Live gauge and historical aggregate come from one mechanism; no duplicate delay computation.
  - The full-replace static loader (P2.1) is correct as-is — no schedule snapshot history required.
  - Live delay table is small (one row per trip×stop vs. per trip×stop×poll).
  - Historical reliability has no dependency on the timing of the daily schedule refresh.