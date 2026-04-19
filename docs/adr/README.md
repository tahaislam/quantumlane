# Architecture Decision Records

Every non-trivial architectural decision for QuantumLane is captured as an ADR:
a short document describing what we decided, what we considered, and why.

## Format

ADRs follow a minimal template:

- **Context** — what situation prompted the decision
- **Decision** — what we chose
- **Alternatives considered** — what else we looked at
- **Consequences** — what this decision costs us

We deliberately keep them short. The goal is to capture reasoning, not to write essays.

## Index

| # | Title | Status |
|---|---|---|
| [001](001-dagster-over-airflow.md) | Use Dagster as the orchestrator | Accepted |
| [002](002-single-postgres-at-this-scale.md) | Single PostgreSQL for all app data | Accepted |
| [003](003-single-vps-over-kubernetes.md) | Single VPS, not Kubernetes | Accepted |
| [004](004-hetzner-over-aws.md) | Hetzner hosting, not AWS | Accepted |
| [005](005-static-html-no-framework.md) | Plain HTML for the website | Accepted |
| [006](006-raw-sql-migrations.md) | Raw SQL migrations, not Alembic | Accepted |
| [009](009-no-streaming-framework.md) | No Kafka / streaming framework | Accepted |

### Pending ADRs

The architecture document references three additional decisions that still need their own ADR files. They are summarized in `docs/ARCHITECTURE.md §6` and will be expanded as the project evolves:

- **007** — FastAPI over Flask or Django REST Framework
- **008** — Cloudflare R2 over S3 or Backblaze B2
- **010** — Public API with no authentication

## When to write a new ADR

Write one when:
- Choosing a foundational tool or pattern
- Rejecting an obvious-seeming choice (the record of why we *didn't* do the obvious thing is often more valuable)
- Making a decision that future-you will forget the reasoning for

Don't write one for:
- Style choices (naming conventions, formatting)
- Trivial implementation details
- Temporary workarounds
