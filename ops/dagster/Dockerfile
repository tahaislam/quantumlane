# Shared image for Dagster webserver and daemon.
#
# Both services use the same Python environment but run different commands:
#   webserver: `dagster-webserver -h 0.0.0.0 -p 3000 -w <workspace>`
#   daemon:    `dagster-daemon run -w <workspace>`
#
# The user code server (dagster-code) has its own image because it installs
# our ingestion package. This image is for Dagster-the-tool only; it reads
# user code over gRPC from dagster-code at runtime.

FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Dagster-postgres needs libpq at runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
        libpq5 \
    && rm -rf /var/lib/apt/lists/*

# --- builder ---
FROM base AS builder
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --prefix=/install \
    "dagster>=1.9.0,<2.0" \
    "dagster-webserver>=1.9.0,<2.0" \
    "dagster-postgres>=0.25.0,<1.0"

# --- runtime ---
FROM base AS runtime
COPY --from=builder /install /usr/local

# Create a non-root user for safety. Mount-points for config and home
# must be readable/writable by this user.
RUN useradd --create-home --shell /bin/bash dagster
USER dagster
WORKDIR /home/dagster

# DAGSTER_HOME is set by the compose file via the environment block,
# but default it here for explicitness.
ENV DAGSTER_HOME=/home/dagster