# Dockerfile for the QuantumLane API.

FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update && apt-get install -y --no-install-recommends libpq5 && rm -rf /var/lib/apt/lists/*

FROM base AS builder
RUN apt-get update && apt-get install -y --no-install-recommends build-essential libpq-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY pyproject.toml ./
COPY src ./src
RUN pip install --prefix=/install --no-deps .
RUN pip install --prefix=/install \
    "fastapi>=0.115.0" \
    "uvicorn[standard]>=0.32.0" \
    "pydantic>=2.9.0" \
    "pydantic-settings>=2.5.0" \
    "psycopg[binary,pool]>=3.2.0" \
    "slowapi>=0.1.9" \
    "structlog>=24.4.0"

FROM base AS runtime
COPY --from=builder /install /usr/local
RUN useradd --create-home --shell /bin/bash ql
USER ql
WORKDIR /app

EXPOSE 8000
CMD ["uvicorn", "quantumlane_api.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
