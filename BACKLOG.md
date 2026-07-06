# BACKLOG

Deferred work, deliberately not implemented yet. Each entry says why it waits.

## Alerts / classification

- **Mode-split alert subfeeds.** The TTC also publishes per-mode alert feeds at
  `gtfsrt.ttc.ca/alerts/{bus,subway,streetcar}`. Ingesting them would give a native
  mode dimension per alert instead of inferring it from routes. Not wired up yet —
  the combined feed covers current needs; revisit if per-mode analytics land.

- **LLM route resolution fallback for alerts with empty `informed_entity`.**
  Live TTC alerts always carry `informed_entity.route_id` today, so nothing is
  built. If that changes, an LLM could resolve route mentions from alert text —
  but it needs the static GTFS route list to validate against. **Blocked on P2.1
  (static GTFS).**

- **LLM-vs-native `effect` cross-check.** The classifier deliberately does not
  infer `effect` (the feed populates it reliably). Future validation idea: have the
  LLM infer it anyway on a sample and diff against the native value — disagreement
  rates would flag prompt drift or feed quality changes. Analysis-only; inferred
  effect would never be served.
