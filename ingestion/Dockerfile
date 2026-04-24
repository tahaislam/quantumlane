# Dockerfile for the QuantumLane ingestion service (Dagster code server).
# Multi-stage build keeps the final image lean.

FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# System deps: psycopg needs libpq, gtfs-realtime-bindings has no compiled deps
RUN apt-get update && apt-get install -y --no-install-recommends \
        libpq5 \
        curl \
    && rm -rf /var/lib/apt/lists/*

# -------------------- builder --------------------
FROM base AS builder
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY pyproject.toml ./
COPY src ./src
RUN pip install --prefix=/install --no-deps .
# Install deps separately so we can see them in layer caches
RUN pip install --prefix=/install \
    "dagster>=1.9.0" \
    "dagster-postgres>=0.25.0" \
    "psycopg[binary,pool]>=3.2.0" \
    "sqlalchemy>=2.0.30" \
    "httpx>=0.27.0" \
    "tenacity>=9.0.0" \
    "gtfs-realtime-bindings>=1.0.0" \
    "protobuf>=5.28.0" \
    "pandas>=2.2.0" \
    "pyarrow>=17.0.0" \
    "boto3>=1.35.0" \
    "structlog>=24.4.0" \
    "pydantic>=2.9.0" \
    "pydantic-settings>=2.5.0"

# -------------------- runtime --------------------
FROM base AS runtime
COPY --from=builder /install /usr/local

# Non-root user
RUN useradd --create-home --shell /bin/bash ql
USER ql
WORKDIR /app

RUN mkdir -p /home/ql/dagster
ENV DAGSTER_HOME=/home/ql/dagster

# Dagster gRPC port
EXPOSE 4000

CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "quantumlane_ingestion.definitions"]
