#!/usr/bin/env bash
# Refresh the in-image .env files from the credentials supplied by Docker Compose.
#
# The Dockerfile runs setup.sh once at build time to bake in the container paths; this
# wrapper runs as airflow-init (root) on every startup so the Supabase credentials from
# the compose `environment:` section land in:
#   /opt/geo/GEO_E2E/.env   (Airflow metadata DB)
#   /opt/geo/GEO_ETL/.env   (JDBC writes)
# Nothing secret is embedded in an image layer.
set -euo pipefail

cd /opt/geo/GEO_E2E
GEO_DOCKER=1 ./setup.sh --force

# airflow-init runs as root; the service containers run as AIRFLOW_UID (the host user) with
# gid 0. setup.sh's sibling sync replaces GEO_ETL/.env via mv, so explicitly restore the
# runtime owner when this script runs as root.
if [[ "$(id -u)" -eq 0 ]]; then
    chown "${AIRFLOW_UID:-50000}:0" /opt/geo/GEO_E2E/.env /opt/geo/GEO_ETL/.env 2>/dev/null || true
    chmod 600 /opt/geo/GEO_E2E/.env /opt/geo/GEO_ETL/.env 2>/dev/null || true
fi
