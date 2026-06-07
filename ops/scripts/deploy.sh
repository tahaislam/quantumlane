#!/usr/bin/env bash
#
# Deploy QuantumLane to a Hetzner (or any Linux) host.
#
# Prereqs on the target host:
#   - Docker + docker compose v2 installed
#   - SSH key-based auth configured
#   - A user with docker group membership (used as the deploy target)
#
# Usage:
#   DEPLOY_HOST=ql@quantumlane.io make deploy
#
# What it does:
#   1. Rsync the repo to the host (excluding .env, secrets, caches)
#   2. Copy the production .env from local ./secrets/prod.env to remote
#   3. Remote: docker compose pull + build + up -d
#   4. Remote: run any pending migrations
#
# What it deliberately does NOT do:
#   - Blue/green deployment — at this scale it's overkill; a few seconds of downtime is fine
#   - Automatic rollback — if a deploy breaks something, roll forward with a fix
#   - Secrets management — ./secrets/prod.env is the truth; commit it nowhere

set -euo pipefail

HOST="${1:-${DEPLOY_HOST:-}}"
if [[ -z "$HOST" ]]; then
    echo "Usage: $0 <user@host>" >&2
    exit 2
fi

REMOTE_DIR="${REMOTE_DIR:-/home/ql/projects/quantumlane}"
LOCAL_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
PROD_ENV_FILE="${LOCAL_ROOT}/secrets/prod.env"

if [[ ! -f "$PROD_ENV_FILE" ]]; then
    echo "ERROR: ${PROD_ENV_FILE} not found." >&2
    echo "Create it from .env.example with production values. Do NOT commit it." >&2
    exit 2
fi

echo "==> Syncing code to ${HOST}:${REMOTE_DIR}"
rsync -az --delete \
    --exclude '.git' \
    --exclude '.env' \
    --exclude 'secrets' \
    --exclude '__pycache__' \
    --exclude '*.pyc' \
    --exclude '.pytest_cache' \
    --exclude '.ruff_cache' \
    --exclude '.mypy_cache' \
    --exclude 'node_modules' \
    --exclude '.venv' \
    "${LOCAL_ROOT}/" "${HOST}:${REMOTE_DIR}/"

echo "==> Uploading production .env"
scp "$PROD_ENV_FILE" "${HOST}:${REMOTE_DIR}/.env"

echo "==> Remote build + up"
ssh "$HOST" "cd ${REMOTE_DIR} && docker compose -f ops/compose/docker-compose.yml --env-file .env build && docker compose -f ops/compose/docker-compose.yml --env-file .env up -d"

echo "==> Running migrations"
ssh "$HOST" "cd ${REMOTE_DIR} && make migrate"

echo ""
echo "✓ Deploy complete."
echo "  Check status: ssh ${HOST} 'cd ${REMOTE_DIR} && docker compose -f ops/compose/docker-compose.yml ps'"
