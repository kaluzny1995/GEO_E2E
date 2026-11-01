#!/usr/bin/env bash
# Every Airflow container has its own writable copy of /opt/geo (the projects are baked
# into the image, not a shared volume). Refresh that container's .env files from the
# Compose environment before handing control to Airflow's official entrypoint.
set -euo pipefail

/opt/geo/bin/refresh-env.sh
exec /entrypoint "$@"
