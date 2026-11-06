#!/usr/bin/env bash
# Build the four GEO images one at a time.
#
# `docker compose build` builds independent services in parallel. That is fine on a
# workstation, but the Anaconda-based images run conda's dependency solver concurrently,
# which can spike RAM and freeze a laptop. Serialising the builds keeps the peak memory
# to one solver at a time. Layer caching still makes later builds fast.
#
# The preflight check runs first, so a missing sibling repository or an unfilled
# GEO_E2E/.env fails before an image build starts.
#
# Usage: ./build-images.sh [extra docker compose build arguments]
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

./check.sh

for service in airflow-init geo_fastapi geo_streamlit geo_react_app; do
    echo "==> docker compose build ${service}"
    docker compose build "$@" "${service}"
done

echo
echo "Images rebuilt. Start or recreate the stack with:"
echo "  ./up.sh -d"
