#!/usr/bin/env bash
# Write GEO_API/.env from the Docker Compose environment, then start uvicorn.
# Keeping the credentials in the runtime environment (instead of a build layer) means
# rebuilding the image never embeds the Supabase password.
set -euo pipefail

: "${SUPABASE_DB_USER:?SUPABASE_DB_USER must be set in docker-compose environment}"
: "${SUPABASE_DB_PASSWORD:?SUPABASE_DB_PASSWORD must be set in docker-compose environment}"
: "${SUPABASE_DB_HOST:?SUPABASE_DB_HOST must be set in docker-compose environment}"
: "${SUPABASE_DB_PORT:?SUPABASE_DB_PORT must be set in docker-compose environment}"
: "${SUPABASE_DB_NAME:?SUPABASE_DB_NAME must be set in docker-compose environment}"

ENV_FILE=/opt/geo/GEO_API/.env
umask 077
cat > "$ENV_FILE" <<EOF
# Generated at container start from the Docker Compose environment.
SUPABASE_DB_USER=$SUPABASE_DB_USER
SUPABASE_DB_PASSWORD=$SUPABASE_DB_PASSWORD
SUPABASE_DB_HOST=$SUPABASE_DB_HOST
SUPABASE_DB_PORT=$SUPABASE_DB_PORT
SUPABASE_DB_NAME=$SUPABASE_DB_NAME
EOF

exec conda run --no-capture-output -n geo_api_311 \
    python -m uvicorn main:app \
    --host 0.0.0.0 \
    --port "${GEO_API_PORT:-8000}" \
    --log-level "${GEO_API_LOG_LEVEL:-info}"
