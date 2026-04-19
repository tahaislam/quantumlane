# ADR 003: Single VPS, not Kubernetes

**Status:** Accepted
**Date:** 2026-04

## Context

The "modern" default for any multi-service deployment is Kubernetes — either managed (EKS, GKE, AKS) or self-hosted (k3s, k0s). The reasoning usually goes: containers, declarative config, rolling updates, autoscaling, portability.

QuantumLane needs to run Postgres, Dagster (webserver, daemon, code server), FastAPI, and Caddy. Seven containers in total. The workload is predictable: steady ingestion, low-to-moderate API traffic, no autoscaling needs.

## Decision

Run everything with **Docker Compose on a single VPS.** No Kubernetes, no service mesh, no custom orchestrator.

## Alternatives considered

### Managed Kubernetes (EKS, GKE, DOKS)
"Real production" aesthetic. Declarative config. Rolling updates.

*Rejected because:*
- Control plane cost ($70+/month) alone exceeds the total cost budget.
- Workload does not justify autoscaling, multi-AZ, or the abstractions Kubernetes exists to provide.
- Operational skills required (etcd, Helm, RBAC, ingress controllers, secrets management) are pure overhead for seven containers.

### Self-hosted k3s on a single box
Kubernetes without the managed-service cost.

*Rejected because:*
- All the complexity of Kubernetes, none of the benefits. Rolling updates of a single-replica deployment is just a restart with extra steps.
- An upgrade or misconfiguration can take down the cluster, which is the box. Compose doesn't have an equivalent failure mode.

### Nomad
Lighter than Kubernetes, still a proper orchestrator.

*Rejected because:*
- Adds an operational component (Nomad agent, Consul for service discovery) without a concrete workload that needs it.
- Docker Compose is sufficient for the primitive we need: "run these services with these dependencies."

### Bare processes via systemd
The anti-container path.

*Rejected because:*
- Container boundaries give us reproducible environments and clean dependency isolation that systemd doesn't, without Kubernetes-scale complexity.
- Local dev parity with production matters; running uvicorn under systemd locally is awkward.

## Consequences

**Accepted costs:**
- No rolling updates. `docker compose up -d` on a changed service causes a brief restart. For a read-only API at this scale, seconds of downtime during deploys are acceptable.
- No horizontal scaling within the box. If traffic ever outgrows it, we scale vertically first (Hetzner makes resizing trivial) and add a second box later if absolutely needed.
- Manual failover if the host dies. This is a real risk and is accepted explicitly: data backups to R2 are the recovery mechanism, not hot standby.

**Benefits:**
- The entire production deployment is legible in one `docker-compose.yml`. A new engineer can read it in five minutes.
- Local development uses the same compose file. No "works on Kubernetes, fails on laptop" class of bug.
- Deployment is `rsync + docker compose up -d`. Reversible and debuggable.

**Watch for:**
- Container resource limits: without them, one runaway service can starve the others. Configure `mem_limit` and `cpus` on each service before production.
- Single-host single-point-of-failure. If uptime becomes a hard requirement (it isn't in v0.1–v0.4), this ADR gets revisited.
