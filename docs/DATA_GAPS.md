# Known Data Gaps

A permanent register of known gaps in QuantumLane's data. Unlike bugs or
features, these entries have **no fix** — the data is gone. This file is the
canonical record; a pinned GitHub `known-issue` points here for visibility.

Add newest gaps at the top. Each entry records the window (UTC), affected data,
cause, whether it is recoverable, and links.

---

## 2026-09-10 → 2026-10-15 — TTC realtime feeds (upstream host retired, ~35 days)

- **Window (UTC):** `2026-09-10 15:50` → `2026-10-15 05:04`
- **Affected:** all `realtime.*` feeds — `vehicle_positions`, `trip_updates`,
  `service_alerts`. `static_gtfs.*` was unaffected (different host) and kept
  refreshing. Every day from `2026-09-11` through `2026-10-14` is **fully
  empty**; Sep 10 and Oct 15 are partial.
- **Cause:** the TTC retired `bustime.ttc.ca`. It first presented as a silent
  drop of all traffic from the production box (sockets in `SYN-SENT`, ports
  80/443 and ICMP timing out), while the same URLs still returned 200 from a
  residential Canadian IP — so for weeks it looked like a network block on the
  hosting provider, and a fresh Hetzner Ashburn server on a different range was
  blocked identically. The replacement is `gtfsrt.ttc.ca`
  (`/vehicles/position`, `/trips/update`, `/alerts/all`, `?format=binary`),
  whose WAF answers **403 to generic Python User-Agents**. Not a QuantumLane bug,
  but the migration shipped with a code change: the feed URLs, a
  product-identifying User-Agent, and parser tolerance for three shapes the new
  feed emits that the old one did not (vehicles with no trip block, NEW/DELETED
  trips with negative synthetic trip_ids, NO_DATA stop updates with no time
  block). Shipped as **v0.4.1**.
- **Recoverable?** **No.** Realtime-only feeds; nothing to re-fetch. The hot
  tier rotated the last real rows (partial `2026-09-10`) out around
  `2026-09-13`; whether that partial day reached the S3 cold tier depends on the
  nightly export having run on the 11th — check the bucket before assuming.
- **Second finding — freshness metadata did not outlive retention.** Once the
  hot tier rotated, `freshness_check`'s `MAX(received_at)` went NULL for the two
  partitioned feeds and the public page read **"Last record: never"** — as if
  they had never worked — while `service_alerts` (not partitioned) still showed
  Sep 10. The page was reporting *retention*, not *history*. Fixed by
  `ops.feed_watermark` (migration 0006): a per-feed high-water mark that only
  moves forward and that the snapshot falls back to, with
  `last_record_source = 'watermark'` so the page can say so. Lesson: any
  "last seen" metric must be persisted outside the data's retention window.
- **Third finding — the dev machine could not reproduce it.** The old host kept
  answering residential IPs until the end, so dev never saw the failure. The
  runbook reachability check now targets the new host and sends the product
  User-Agent, because a bare `curl` gets a misleading 403.
- **Side effect worth knowing:** the new feed matches the static catalog far
  better. Under the old host ~39% of `trip_updates.stop_id` values had no match
  in `static_gtfs.stops`; under the new host it is ~0.3%.

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
