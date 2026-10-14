# Known Data Gaps

A permanent register of known gaps in QuantumLane's data. Unlike bugs or
features, these entries have **no fix** — the data is gone. This file is the
canonical record; a pinned GitHub `known-issue` points here for visibility.

Add newest gaps at the top. Each entry records the window (UTC), affected data,
cause, whether it is recoverable, and links.

---

## 2026-09-10 → ongoing — TTC realtime feeds (upstream block)

- **Window (UTC):** `2026-09-10 15:50` → **ongoing** (still blocked as of 2026-10-13)
- **Affected:** all `realtime.*` feeds — `vehicle_positions`, `trip_updates`,
  `service_alerts`. `static_gtfs.*` is unaffected (different host) and keeps
  refreshing.
- **Cause:** `bustime.ttc.ca` (Azure, `20.151.116.213`) silently drops all
  traffic from the production box. Sockets sit in `SYN-SENT`; ports 80/443 and
  ICMP all time out. The same URLs return 200 from a residential Canadian IP.
  A fresh Hetzner Ashburn server on a different range (`178.156.x`) is blocked
  identically, so this is a block on the hosting network, not on one IP. A TCP
  traceroute leaves Hetzner and dies several hops inside Microsoft's network —
  the drop is at the feed's edge, not in Hetzner's. Not a QuantumLane bug.
- **Recoverable?** **No.** Realtime-only feeds; nothing to re-fetch. The hot
  tier rotated the last real rows (partial `2026-09-10`) out around
  `2026-09-13`; whether that partial day reached the S3 cold tier depends on the
  nightly export having run on the 11th — check the bucket before assuming.
- **Resolution path:** egress through a non-Hetzner network for the three
  realtime URLs (home relay or a relay at another provider), or a TTC allowlist.
- **Second finding — freshness metadata did not outlive retention.** Once the
  hot tier rotated, `freshness_check`'s `MAX(received_at)` went NULL for the two
  partitioned feeds and the public page read **"Last record: never"** — as if
  they had never worked — while `service_alerts` (not partitioned) still showed
  Sep 10. The page was reporting *retention*, not *history*. Fixed by
  `ops.feed_watermark` (migration 0006): a per-feed high-water mark that only
  moves forward and that the snapshot falls back to, with
  `last_record_source = 'watermark'` so the page can say so. Lesson: any
  "last seen" metric must be persisted outside the data's retention window.

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
