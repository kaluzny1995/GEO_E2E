#!/usr/bin/env bash
# Write GEO_Visual/.env from the Docker Compose environment, then start Streamlit.
# Keeping the credentials in the runtime environment (instead of a build layer) means
# rebuilding the image never embeds the Supabase password.
set -euo pipefail

: "${SUPABASE_DB_USER:?SUPABASE_DB_USER must be set in docker-compose environment}"
: "${SUPABASE_DB_PASSWORD:?SUPABASE_DB_PASSWORD must be set in docker-compose environment}"
: "${SUPABASE_DB_HOST:?SUPABASE_DB_HOST must be set in docker-compose environment}"
: "${SUPABASE_DB_PORT:?SUPABASE_DB_PORT must be set in docker-compose environment}"
: "${SUPABASE_DB_NAME:?SUPABASE_DB_NAME must be set in docker-compose environment}"

ENV_FILE=/opt/geo/GEO_Visual/.env
umask 077
cat > "$ENV_FILE" <<EOF
# Generated at container start from the Docker Compose environment.
SUPABASE_DB_USER=$SUPABASE_DB_USER
SUPABASE_DB_PASSWORD=$SUPABASE_DB_PASSWORD
SUPABASE_DB_HOST=$SUPABASE_DB_HOST
SUPABASE_DB_PORT=$SUPABASE_DB_PORT
SUPABASE_DB_NAME=$SUPABASE_DB_NAME
EOF

exec conda run --no-capture-output -n geo_visual_311 \
    streamlit run app.py \
    --server.address 0.0.0.0 \
    --server.port "${GEO_VISUAL_PORT:-8501}" \
    --server.headless true \
    --server.fileWatcherType none \
    --browser.gatherUsageStats false
