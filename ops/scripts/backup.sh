#!/usr/bin/env bash
#
# pg_dump the main database and upload to AWS S3.
# Intended to be run daily via a cron container in v0.2. For v0.1, invoked manually via `make backup`.
#
# Tests the restore path: see docs/RUNBOOKS.md#restore-from-backup.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "$ROOT"

# Load env
if [[ -f .env ]]; then
    set -a
    # shellcheck disable=SC1091
    source .env
    set +a
fi

TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
DUMP_FILE="/tmp/quantumlane-${TIMESTAMP}.sql.gz"

echo "==> Dumping database..."
docker compose -f ops/compose/docker-compose.yml exec -T postgres \
    pg_dump -U "${POSTGRES_USER:-quantumlane}" -d "${POSTGRES_DB:-quantumlane}" --clean --if-exists \
    | gzip -9 > "$DUMP_FILE"

SIZE="$(du -h "$DUMP_FILE" | cut -f1)"
echo "  dump: ${DUMP_FILE} (${SIZE})"

if [[ -n "${QL_S3_ACCESS_KEY_ID:-}" && -n "${QL_S3_BUCKET:-}" ]]; then
    echo "==> Uploading to S3..."
    AWS_ACCESS_KEY_ID="$QL_S3_ACCESS_KEY_ID" \
    AWS_SECRET_ACCESS_KEY="$QL_S3_SECRET_ACCESS_KEY" \
    AWS_DEFAULT_REGION="${QL_S3_REGION:-us-east-1}" \
    aws s3 cp "$DUMP_FILE" "s3://${QL_S3_BUCKET}/backups/$(basename "$DUMP_FILE")"
    rm -f "$DUMP_FILE"
    echo "✓ Backup uploaded and local copy removed."
else
    echo "  (S3 not configured — dump left at ${DUMP_FILE})"
fi
