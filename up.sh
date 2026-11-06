#!/usr/bin/env bash
# Start the GEO stack only after the preflight checks pass.
#
# The check verifies that all six repositories are present side by side and that
# GEO_E2E/.env contains the five Supabase credentials. If something is missing, it
# prints what to fix and exits before Docker starts any container.
#
# Usage: ./up.sh [docker compose up options...]
#   ./up.sh -d              # start in the background (normal use)
#   ./up.sh -d --build      # rebuild changed images and start
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

./check.sh
exec docker compose up "$@"
