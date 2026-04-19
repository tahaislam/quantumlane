# ADR 004: Hetzner hosting, not AWS

**Status:** Accepted
**Date:** 2026-04

## Context

The platform needs a host. AWS is the common default for small multi-service deployments.
QuantumLane is expected to run continuously and publicly for at least six months.

## Decision

Host on **Hetzner Cloud** (CAX21 ARM instance, ~€7/month ≈ CAD $11).

## Alternatives considered

### AWS (EC2 t4g + RDS + S3)
The industry default. "Real production" aesthetic.

*Rejected because:*
- Equivalent setup runs ~CAD $120–150/month. The 10x cost carries no architectural benefit at this scale.
- EC2 + RDS + S3 is not meaningfully different architecturally from Hetzner + Postgres + R2. The patterns transfer; the bill doesn't.
- The "I used AWS" label carries little weight at this scale. The architectural reasoning is what matters, and it transfers regardless of the specific host.
- Cost anxiety (accidental $300 bills) is a known distraction pattern. Defended cost ceilings matter more than defended availability zones for this project.

### Managed PaaS (Railway, Render, Fly.io)
Simple deployment, free tiers.

*Rejected because:*
- Usage-based billing at 24/7 uptime + continuous ingestion + stateful Postgres runs into limits or burns through hobby budgets quickly.
- Less operational learning. The whole point is running the thing — the box, the cron, the restart policies.
- Vendor lock-in on the PaaS surface area (even when the underlying containers are portable).

### DigitalOcean / Linode / Vultr
Comparable to Hetzner functionally. Slightly more expensive, comparable quality.

*Weakly rejected:* any of these would work. Hetzner's price/performance on ARM is currently the best by a noticeable margin, and the CAX21 spec (4 vCPU, 8 GB RAM) is sized well for v0.1.

## Consequences

**Accepted costs:**
- Hetzner has had occasional abuse-detection false positives that block outbound traffic. Known risk, mitigated by polite User-Agent and reasonable request rates.
- EU data residency (Germany/Finland) means ~100ms latency for Toronto-based users. Acceptable for a read-mostly data API; not acceptable for a real-time user-facing app (we aren't one).
- No managed backups — we roll our own (`ops/scripts/backup.sh`).

**Benefits:**
- Monthly cost well under the CAD $20 ceiling, leaving room for domain + R2 overage.
- Architectural patterns map cleanly to AWS equivalents. If migration is ever needed, the translation is mechanical.

**Watch for:**
- If v0.3+ introduces workloads that need low latency to North American users, reconsider with a lower-cost NA host (Hetzner US, DigitalOcean TOR1) before jumping to AWS.
