# Known Data Gaps

A permanent register of known gaps in QuantumLane's data. Unlike bugs or
features, these entries have **no fix** — the data is gone. This file is the
canonical record; a pinned GitHub `known-issue` points here for visibility.

Add newest gaps at the top. Each entry records the window (UTC), affected data,
cause, whether it is recoverable, and links.

---

## 2026-08-14 → 2026-08-17 — TTC realtime feeds (~72h outage)

- **Window (UTC):** `2026-08-14 ~03:57` → `2026-08-17 ~03:59`
- **Affected:** all `realtime.*` feeds — `vehicle_positions`, `trip_updates`,
  `service_alerts` — and any OLAP / parquet rollups derived from those days.
- **Partial vs. empty days:**
  - `2026-08-14` — data only up to ~03:57 UTC (partition stops mid-day)
  - `2026-08-15`, `2026-08-16` — **fully empty**
  - `2026-08-17` — resumes from ~03:59 UTC
- **Cause:** the production disk reached 100% around Aug 14 03:57. `dagster-postgres`
  crash-looped, the entire stack went down, and it stayed down **undetected**
  until manual recovery on Aug 17.
- **Recoverable?** **No.** The hot tier is disposable-by-design and there was no
  cold archive at the time (the Iceberg tier is still planned). The upstream TTC
  GTFS-RT feeds are realtime-only — past windows cannot be re-fetched.
- **Related:** monitoring gaps that let it go unnoticed — #8, #9, #10, #16;
  root-cause growers — #11, #12.
