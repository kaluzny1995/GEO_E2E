#!/usr/bin/env bash
# Build the four GEO images one at a time.
#
# `docker compose build` builds independent services in parallel. That is fine on a
# workstation, but the Anaconda-based images run conda's dependency solver concurrently,
# which can spike RAM and freeze a laptop. Serialising the builds keeps the peak memory
# to one solver at a time. Layer caching still makes later builds fast.
#
# Usage: ./build-images.sh [extra docker compose build arguments]
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

for service in airflow-init geo_fastapi geo_streamlit geo_react_app; do
    echo "==> docker compose build ${service}"
    docker compose build "$@" "${service}"
done

echo
echo "Images rebuilt. Start or recreate the stack with:"
echo "  docker compose up -d"
