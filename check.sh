#!/usr/bin/env bash
# Verify the Docker stack prerequisites before `docker compose up`:
#   * all six GEO repositories are checked out side by side
#   * GEO_E2E/.env carries the five Supabase credentials
#
# `up.sh` and `build-images.sh` call this automatically. Run it alone with:
#   ./check.sh
#
# Usage: ./check.sh [preflight.py options]
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

PYTHON_BIN="${PYTHON:-python3}"
if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
    if command -v python >/dev/null 2>&1; then
        PYTHON_BIN=python
    else
        echo "error: Python 3 is required for the preflight check (python3 not found)" >&2
        exit 2
    fi
fi

exec "$PYTHON_BIN" preflight.py "$@"
