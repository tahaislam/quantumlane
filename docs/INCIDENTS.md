# Incident Register

Blameless postmortems for production incidents. One entry per incident, newest
first. Written soon after the fact, while the wrong turns are still fresh —
the value of this file is in *what we believed at each step*, not just the
final answer. `docs/DATA_GAPS.md` is the companion register for the data each
incident cost; it records *what is missing*, this file records *why and what we
learned*.

Each entry: symptom → diagnosis → root cause → remedy → what was rejected and
why → lesson → data impact → what is deliberately still open. Keep it terse.
Near-misses (caught before production) and minor incidents are kept too, in
their own sections, because the pattern section at the end is built from
all of them.

---

## INCIDENT — TTC GTFS-RT ingestion outage (2026-09-10 → 2026-10-15, ~35 days)

```
Status:    resolved 2026-10-15 05:04 UTC (v0.4.1). GitHub #18, DATA_GAPS.md.
Window:    2026-09-10 15:50 UTC → 2026-10-15 05:04 UTC
Impact:    all realtime.* feeds empty for ~35 days; public page showed
           "Last record: never" for part of it. Static GTFS unaffected.
```

**Symptom:** realtime assets failing from 2026-09-10 15:50 UTC; static GTFS
(CKAN) unaffected. Every realtime run ended in `ConnectTimeout`.

**Diagnosis:** `bustime.ttc.ca` (legacy platform, Azure-hosted,
`20.151.116.213`) drops the box's address range at the network layer — SYN
timeout, `curl` exit 28, `http_code 000`, ICMP 100% loss. Residential IPs
unaffected (dev kept getting 200 the whole time). A fresh Hetzner Ashburn
server on a different range was blocked identically, and a TCP traceroute died
several hops inside Microsoft's network. IP-block hypothesis CONFIRMED for this
host's network, not just this IP.

**Root cause:** the TTC launched a replacement platform, `gtfsrt.ttc.ca`
(trip updates Nov 2025, vehicle positions Jan 2026, still being actively
released as of Oct 2026), and stopped serving our hosting network from the
legacy host. The new host is reachable from the box — but its WAF rejects
some Python User-Agents: `403` for `python-requests/*` and `Python-urllib/*`,
`200` for `python-httpx/*`, `curl/*`, an empty UA, and a descriptive product
UA (all measured from the box on 2026-10-15). Two hosts, two gates.

**Remedy (v0.4.1):**
1. Repoint the three RT URLs to `gtfsrt.ttc.ca/{vehicles/position,
   trips/update, alerts/all}?format=binary`.
2. Client UA → `quantumlane-ingestion/<version> (+https://quantumlane.io)`,
   derived from the package version. Compose no longer forwards
   `QL_HTTP_USER_AGENT`, so a stale `.env` cannot override it.
3. Parser tolerance for three shapes the new feed emits that the old one did
   not: vehicles with no trip block (~40% of entities), NEW/DELETED trips with
   negative synthetic trip_ids, NO_DATA stop updates with no time block (with
   an explicit guard so a time of 0 is never stored as 1970).

   Note on (2): because our client is httpx, the URL swap alone would have
   passed the WAF. The 403s that shaped the diagnosis came from `requests`-
   and `urllib`-based probes. The UA change is still right — it identifies
   the product, is robust to the WAF rule tightening, and stops the next
   person's probe from misdiagnosing a block — but it was not the gate for
   *our* traffic.

**Rejected:** a VPN relay through a residential connection (egress via a
residential IP). It would have
routed around the wrong host — the legacy platform the TTC was winding down —
and bought a few months at best.

**Lesson:** the diagnosis ("blocked") was right; the remedy stalled because
nobody asked whether the endpoint being unblocked was still the endpoint to
use. Check the source's current docs before building infrastructure around a
hypothesis. Third URL-rot incident in a row (CKAN static URL, bustime RT, now
the UA gate), so treat upstream URLs as things that rot, not constants.

**Also found while fixing it:**
- **`.env` overrides beat code defaults, silently.** Each host's `.env`
  carried the old feed URLs (copied from an old `.env.example`), so after the
  code change the container still polled the old host until the `.env` was
  edited too. Hit three times in one day (static URL, RT URLs, UA). The
  compose file now documents this, and the UA is no longer overridable.
- **Stopped schedules do not revive on deploy.** `ttc_realtime_every_minute`
  and `ttc_alerts_every_5_minutes` had been stopped by hand during the
  outage; a stored stop overrides `DefaultScheduleStatus.RUNNING`, so the
  deploy produced zero polls until they were started again.
- **Freshness metadata did not outlive hot-tier retention** — the page read
  "Last record: never" once the last real rows aged out. Fixed earlier in the
  incident by `ops.feed_watermark` (migration 0006).
- **The new feed is a data-quality improvement.** ~39% of `trip_updates.stop_id`
  values from the old host had no match in `static_gtfs.stops`; ~0.3% on the
  new host. Zero route_ids outside the catalog. A naive route join still shows
  ~40% "unmatched" vehicles — those are the no-trip-block entities with a
  NULL route, not catalog misses; filter `route_id IS NOT NULL` first.
- **Dev could never reproduce it.** The legacy host kept answering residential
  IPs to the end. The runbook reachability check now targets the new host and
  sends the product UA.

**Data gap:** `2026-09-10 15:50` → `2026-10-15 05:04` UTC is permanently
missing. Sep 11 – Oct 14 are fully empty; Sep 10 and Oct 15 are partial.
Whether the partial Sep 10 reached the S3 cold tier depends on the nightly
export having run on Sep 11 — check the bucket before assuming.

**Not fixed, deliberately:** `arrival.delay` / `departure.delay` are still
absent in the new feed (0 of ~20k stop updates carried one) — the P2.15
delay-derivation design is unchanged.

**Action items:**
- [x] Alert on zero realtime rows for N minutes (#8) — `GET /readyz` returns 503
      once the newest `vehicle_positions` / `trip_updates` row is older than
      `QL_READYZ_STALE_AFTER_SECONDS` (300). Shipped 2026-11-01; repoint the
      uptime monitor at it. #9 (external dead-man's switch) and #13 (per-run
      failure notifications) remain open — this outage was noticed by a human,
      not a monitor.
- [ ] Make `make deploy` print the schedule states after `up`, so a stopped
      schedule is visible at deploy time.
- [ ] Re-check the TTC developer docs quarterly for endpoint changes.

---

## NEAR-MISS — SQLAlchemy 2.1 silently breaks dagster-postgres (2026-10-13, dev only)

```
Status:    fixed 2026-10-13 (pin `sqlalchemy<2.1`). Never reached production.
Impact:    none in production. Dev: no run could execute after a fresh build.
```

**Symptom:** after a routine `make build` on dev, every Dagster run failed
instantly; event-log writes errored with `syntax error at or near "$1"`.

**Diagnosis:** the unpinned build pulled SQLAlchemy 2.1.4, under which a
plain `postgresql://` URL resolves to the psycopg **v3** driver.
`dagster-postgres` only speaks psycopg2, so its parameter style is wrong
for the driver it got.

**Root cause:** no upper bound on a transitive dependency that Dagster's
storage layer is sensitive to. Production was still on 2.0.51 only because
its images dated from June; the next rebuild there would have hit it.

**Remedy:** pin `sqlalchemy<2.1` in the Dagster image.

**Lesson:** "it works on prod" can mean "prod has not rebuilt yet". Any
production rebuild inherits every unpinned upgrade since the last one, so a
long gap between builds is itself a risk. Rebuild dev first, always.

---

## INCIDENT — Run queue wedged for 16 days (2026-09-10 → 2026-09-26)

```
Status:    resolved 2026-09-26 (`max_runtime_seconds: 1800`,
           `max_concurrent_runs: 3`). 51,807 queued runs cancelled.
Window:    2026-09-10 → 2026-09-26
Impact:    nothing dequeued for 16 days. Masked by the TTC outage that began
           the same day — the polls would have failed anyway — so no data was
           lost to *this* incident, which is also why nobody noticed it.
```

**Symptom:** Dagster UI looked healthy, daemon logs said
`10 runs are currently in progress. Maximum is 10, won't launch more`, and
the queue grew by ~2 runs a minute.

**Diagnosis:** 10 runs stuck in `STARTED` whose worker processes were long
dead — orphaned by the 2026-06-29 deploy restarts, the 2026-08-14 disk-full
outage, and a container restart on 2026-09-10. Each held a concurrency slot
forever.

**Root cause:** the June zombie fix (below) enabled `run_monitoring`, but
its dead-worker check is gated on
`DefaultRunLauncher.supports_check_run_worker_health`, which is `False` for
the subprocess launcher we use. The MonitoringDaemon logged
`Checking run <id>` every cycle for three months without ever failing one.
`start_timeout_seconds` covers only `STARTING`; `max_runtime_seconds` was
unset, so the runtime check was a no-op. Every crash or restart ratcheted
the queue down by one slot until it hit zero.

**Remedy:**
1. `run_monitoring.max_runtime_seconds: 1800` — enforced regardless of
   launcher health-check support, so it is the only setting that actually
   reaps an orphan. Sized above the slowest legitimate job (parquet export,
   404–530 s).
2. `max_concurrent_runs: 10 → 3`. Draining the queue launched 10 workers at
   once (~200–250 MB each on a 3.7 GB box with no swap): load average 50 on
   3 vCPUs, sshd stopped answering until the daemon was stopped. Ten was
   never a survivable ceiling; its only real effect was setting how many
   leaked slots it took to wedge the queue.

**Rejected:** a cron that force-cancels old `STARTED` runs — treats the
symptom, lives outside Dagster, and the project rule is that Dagster
orchestrates everything.

**Lesson:** the June fix addressed the symptom that was seen (runs stuck
in `STARTING`), not the mechanism (a slot held by a dead process in *any*
state). Read the code path a monitoring setting actually takes before
trusting that it covers your case. Also: a queue that never drains is
invisible when the thing it would run is failing anyway — two overlapping
incidents hide each other.

**Data gap:** none attributable; see the TTC outage above.

---

## INCIDENT — Disk full, stack down and undetected for ~72 h (2026-08-14 → 2026-08-17)

```
Status:    resolved 2026-08-17 (manual recovery). Follow-ups: DATA_GAPS.md,
           GitHub #17, monitoring issues #8–#16, Dagster metadata cut 2026-09-27.
Window:    2026-08-14 ~03:57 UTC → 2026-08-17 ~03:59 UTC
Impact:    all realtime.* feeds; Aug 15 and 16 fully empty, Aug 14 and 17
           partial. Permanently lost (no cold archive for those days).
```

**Symptom:** none that anyone acted on for three days. The uptime monitor
watched `/health`, a liveness probe. Whether it fired and was missed, or
stayed green because the API kept answering while ingestion was dead, was
not recorded — either way, liveness was the wrong signal (#8 exists for
this reason).

**Diagnosis:** the disk hit 100%; `dagster-postgres` crash-looped and took
the stack with it.

**Root cause:** the hot tier had retention, but Dagster's own bookkeeping
did not. `runs` and `event_logs` grow forever at the every-minute cadence;
by 2026-09-27 they held 356 k runs and 8.16 M event rows — a 12 GB
`dagster-postgres` volume on a 75 GB disk — the single largest avoidable
contributor; the 3-day hot tier (~30 GB) and Docker images and build cache
made up most of the rest.

**Remedy:** free space, restart. Then (2026-09-27) cut Dagster run history
to 30 days: 314,668 runs deleted in FK order (`asset_event_tags` →
`event_logs` → `run_tags` → `runs`), `VACUUM FULL`, 12 GB → 1.46 GB, disk
76% → 18%.

**Lesson:** "the data has retention" is not the same as "the box has
retention". Anything that writes per run needs its own cap. And a liveness
monitor that says the API is up tells you nothing about whether data is
flowing — the alert has to be on freshness (#8, #9).

**Not fixed, deliberately:** the 30-day cut was manual. The durable fix is
a Dagster-orchestrated retention asset mirroring
`daily_partition_maintenance` (#12); until it exists this will grow back.

**Data gap:** recorded in `docs/DATA_GAPS.md` and #17.

---

## INCIDENT — Static GTFS URL rot (discovered 2026-06-24)

```
Status:    fixed 2026-06-24 (CKAN direct-download URL + zip magic-byte guard).
Impact:    none visible — the static loader was still a stub when the URL died,
           so nothing was failing. First of three upstream-URL incidents.
```

**Symptom:** implementing `ttc_static_gtfs` against the configured
`opendata.toronto.ca/.../OpenData_TTC_Schedules.zip` produced a cryptic
`zipfile.BadZipFile`.

**Diagnosis:** the URL returned an HTML page with HTTP 200. Toronto had
migrated its open-data portal to CKAN; the old path was dead.

**Remedy:** point at the CKAN direct-download URL; guard on the zip magic
bytes so a future move fails with a clear message instead of a parser
error.

**Lesson:** a 200 is not a success. Validate the *shape* of what an
upstream returns, not just the status. Durable fix (resolve the URL via the
CKAN API) tracked as P3.3; see also the compose forwarding fix of
2026-10-14, when a stale `.env` kept the container on the dead URL for
months after the code default was corrected.

---

## INCIDENT — Dagster run-pool zombie pileup (2026-06-21 → 2026-06-24)

```
Status:    resolved 2026-06-24 (`run_monitoring` enabled). Later found
           insufficient — see the 16-day wedge above.
Impact:    dev: nothing executed for days, misread as a code bug.
           prod: 4 zombies, pool not saturated, cleared by hand.
```

**Symptom:** after editing `ttc.py`, every Dagster run queued and nothing
executed. Reverting the code did not help; neither did `make down`/`up`.

**Diagnosis:** the blockage lived in the database, not the code: stale
"in progress" run records from OOM reboots and ungraceful shutdowns going
back to April pinned `max_concurrent_runs`. UI "Terminate" hung on
`Canceling` forever because no live process existed to acknowledge it.

**Remedy:** dev — `dagster run wipe`. prod — age-scoped
`UPDATE runs SET status='CANCELED' WHERE status IN (...) AND create_timestamp
< now() - interval '1 day'`. Prevention — `run_monitoring: enabled: true`,
`start_timeout_seconds: 300`, `max_resume_run_attempts: 0`. The last one is
mandatory with the subprocess launcher (it cannot resume runs); a nonzero
value fails an invariant at startup and crash-loops all three Dagster
containers — hit on dev first, which is exactly why config changes are
validated on dev before prod.

**Lesson:** "everything queued, nothing running" is a state problem, not a
code problem. Look at persisted run state before touching code. The
prevention here was incomplete (it covered `STARTING` only), which cost 16
days in September.

---

## INCIDENT — Disk full #1: partition detach-without-drop leak (May 2026, resolved 2026-05-31)

```
Status:    resolved 2026-05-31 (`fix: drop expired partitions instead of
           detaching them`). Retention self-maintenance confirmed 2026-06-01.
Window:    onset not recorded; discovered when Postgres could not start
           during the v0.3.0 reset on 2026-05-31.
Impact:    production disk at 100%. All v0.1 realtime data dropped at the
           reset — deliberately; it was disposable by design.
```

**Symptom:** the 80 GB disk filled at a rate that did not match the ~10 GB/day
of ingest against the configured retention.

**Diagnosis (first, wrong):** raw data volume. Retention was tightened from
30 to 3 days on 2026-05-24. The disk kept filling.

**Diagnosis (second, right):** `daily_partition_maintenance` *detached*
partitions older than the window but never *dropped* them. The v0.1 design
was two-stage — detach, archive to R2, drop after verification — but the
R2 export was only ever a stub and the drop job was never built. Detached
partitions accumulated as orphaned tables at ~10 GB/day, reclaiming nothing.
Invisible to the obvious check: detached partitions are parentless, so a
`pg_inherits`-based listing showed a correct 3 days + today while orphans
piled up underneath.

**Remedy:** detach-then-`DROP TABLE`; find partitions by name pattern via
`pg_class`, not `pg_inherits`, so pre-existing orphans are swept on the next
run (self-healing). Verified on dev with one attached-old and one detached
orphan; first scheduled prod run swept all stragglers.

**Collateral during recovery:** `docker system prune -af` wiped the
locally-built images (they are on no registry), causing "pull access denied"
on the next `make up`; a stale network ID from the ungraceful shutdown
needed `down` + `network prune`; and the retention fix "did not work" at
first because `make up` ran the old image. All three are now in
`AGENTS.md`.

**Lesson:** the monitoring query only answers the question you asked it.
When a number keeps moving in a direction your model says it cannot, the
model is wrong, not the number. And an archive-then-drop pipeline with a
stubbed archive step is a leak, not a safety margin — when the Iceberg cold
tier lands, the drop must be wired *downstream* of a verified archive.

---

## INCIDENT — Nuremberg deployment abandoned (2026-04-28, pre-production)

```
Status:    resolved 2026-04-28 by re-provisioning in Hetzner Ashburn (CPX21, x86).
Impact:    one lost deploy day and a deleted box. No users yet.
```

**Symptom:** first production deploy on a Hetzner CAX21 (ARM, Nuremberg)
hung at "Waiting for Postgres", then once the stack was up every realtime
asset timed out.

**Diagnosis:** two unrelated faults in sequence. `postgis/postgis` ships
amd64 only, so Docker silently ran it under QEMU emulation — too slow to
pass the health check. Then `bustime.ttc.ca` refused connections from German
IP space (TCP timeouts on 80 and 443) while `www.ttc.ca`, CDN-fronted,
worked: the feed infrastructure itself was geo-blocked.

**Remedy:** swap to the multi-arch `imresamu/postgis` (kept even on x86,
for portability); delete the box; re-provision in Ashburn. ARM is
Nuremberg-only on Hetzner, which rules out the cheaper tier for North
American transit workloads.

**Lesson:** test data-source reachability from the candidate region
*before* provisioning — a `curl -m 10` costs nothing and would have saved
the day. Now the first step in `docs/RUNBOOKS.md`. The same reachability
question, asked five months later about a different host, is how the
September outage was finally understood.

---

## Minor incidents and near-misses

One line each; the lesson is the point.

- **2026-05-31 — TLS never provisioned at launch.** `curl https://quantumlane.io`
  refused on 443 because the Caddy container was six days old: `make up` did
  not recreate it (unchanged image and definition) so it never re-read the
  bind-mounted Caddyfile. Fix: `up -d --force-recreate caddy`. Lesson: a
  bind-mounted config edit needs a process bounce, not `up -d`.
- **2026-06-10 — Uptime monitor false alarms (405).** UptimeRobot sends HEAD;
  FastAPI's router does not add HEAD to GET routes the way plain Starlette
  does. Fix: `@app.api_route(methods=["GET","HEAD"])`.
- **2026-06-18 — Dev box OOM during the archival prototype.** `pd.read_sql`
  buffered a whole day client-side and forced a reboot. Fix: server-side
  cursor, chunked read, held-open `ParquetWriter`. The same lesson was applied
  pre-emptively a week later to the 200 MB `stop_times.txt` load (streamed
  into `COPY`), avoiding a third OOM.
- **2026-06-29 — Resolver bug found live in the demo.** "King" matched
  "12 Kingston Rd". Fixed with word-boundary matching; then the real catalog
  showed 304 King and 504 King are indistinguishable by the tiebreak used.
  Lesson: synthetic fixtures never have the shape real data does.
- **2026-07-01 — Caddy `/mcp*` glob swallowed `/mcp.html`.** A static page
  sharing a prefix with a proxied route was proxied to the MCP container
  (diagnosable by `server: uvicorn` on the 404). Fix: rename the page off the
  prefix. Lesson: keep static pages off proxied prefixes.
- **2026-06-23 — Postgres collation version mismatch (latent, still open).**
  The PostGIS image's glibc moved under the existing indexes; Postgres warns on
  every query and text-index ordering could drift. Procedure in
  `docs/RUNBOOKS.md` (REINDEX, then REFRESH COLLATION VERSION). Still warning
  on dev as of 2026-10-15; check prod before relying on trigram stop search.

---

## Patterns across incidents

Worth naming, because each one has now happened more than once.

1. **Stale state outlives the fix.** Old image after `make up`; old
   `.env` value over a corrected code default (three times in one day);
   old Caddy process over an edited mount; a manually stopped schedule over
   `DefaultScheduleStatus.RUNNING`. The change was right; the running thing
   never saw it. Every "my fix does nothing" starts here.
2. **The fix addressed the symptom seen, not the mechanism.** Retention
   30→3 days before finding the leak; `run_monitoring` for `STARTING` before
   understanding slot leaks in `STARTED`; a VPN relay for a host that was
   being retired.
3. **Growth invisible to the obvious query.** Detached partitions absent
   from `pg_inherits`; Dagster's metadata DB absent from any `realtime.*`
   size check; queued runs absent from a UI that looks healthy.
4. **Upstream URLs rot.** CKAN static zip, bustime realtime host, and the
   new host's User-Agent gate. Treat every upstream endpoint as something to
   re-validate (P3.3), and validate the *shape* of the response, not the
   status code.
5. **The monitor watched the wrong thing.** `/health` stayed green through a
   72-hour ingestion outage; a wedged queue logged "in progress" forever;
   dev could not reproduce a block that only applied to the production
   network. The alert has to be on the outcome — rows arriving — not on
   process liveness.
