#!/usr/bin/env bash
#
# pg_dump the main database and upload to Cloudflare R2.
# Intended to be run daily via a cron container in v0.2. For v0.1, invoked manually via `make backup`.
#
# Tests the restore path: see docs/RUNBOOKS.md#restore-from-backup (to be added).

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

if [[ -n "${QL_R2_ENDPOINT_URL:-}" && -n "${QL_R2_ACCESS_KEY_ID:-}" ]]; then
    echo "==> Uploading to R2..."
    AWS_ACCESS_KEY_ID="$QL_R2_ACCESS_KEY_ID" \
    AWS_SECRET_ACCESS_KEY="$QL_R2_SECRET_ACCESS_KEY" \
    aws --endpoint-url "$QL_R2_ENDPOINT_URL" \
        s3 cp "$DUMP_FILE" "s3://${QL_R2_BUCKET:-quantumlane}/backups/$(basename "$DUMP_FILE")"
    rm -f "$DUMP_FILE"
    echo "✓ Backup uploaded and local copy removed."
else
    echo "  (R2 not configured — dump left at ${DUMP_FILE})"
fi
