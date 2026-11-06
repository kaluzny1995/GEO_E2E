#!/usr/bin/env bash
# Writes the machine-specific env file (paths, uid, Airflow UI login, Supabase credentials)
# detected from this checkout, so the stack runs wherever the repo was cloned — and mirrors the
# Supabase credentials into the sibling GEO_ETL/.env, GEO_API/.env and GEO_Visual/.env
# (setup lives here).
# Git-ignored; docker compose auto-loads .env from the project root (see README — Setup).
#
# In a Docker container it writes .env.docker instead, non-interactively (defaults only).
# Set GEO_DOCKER=1 to run in image mode: write .env (not .env.docker), fix the container
# paths (/opt/geo, /opt/conda) and take the credentials from the process environment.
# The Airflow image runs it at build time and again from airflow-init at startup.
#
# Usage: ./setup.sh [--force] [--help]
#   --force  regenerate an existing env file (keeps the Airflow login already stored there)
set -euo pipefail

in_container() {
    [[ -f /.dockerenv ]] && return 0
    [[ -n "${container:-}" ]] && return 0
    grep -qaE '(docker|containerd|kubepods|lxc|podman|libpod)' /proc/1/cgroup 2>/dev/null
}

DOCKER_MODE="${GEO_DOCKER:-0}"
IN_CONTAINER=false
if [[ "$DOCKER_MODE" == "1" ]] || in_container; then
    IN_CONTAINER=true
fi

ENV_FILE=".env"
if [[ "$IN_CONTAINER" == true && "$DOCKER_MODE" != "1" ]]; then
    ENV_FILE=".env.docker"
fi
FORCE=false

usage() {
    sed -n '2,14p' "${BASH_SOURCE[0]}" | sed -E 's/^#[[:space:]]?//'
}

for arg in "$@"; do
    case "$arg" in
        -f|--force) FORCE=true ;;
        -h|--help) usage; exit 0 ;;
        *) echo "unknown argument: $arg (try --help)" >&2; exit 2 ;;
    esac
done

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GEO_ROOT="$(dirname "$PROJECT_DIR")"

SUPABASE_KEYS=(SUPABASE_DB_USER SUPABASE_DB_PASSWORD SUPABASE_DB_HOST SUPABASE_DB_PORT SUPABASE_DB_NAME)

# GEO_ETL, GEO_API and GEO_Visual read the same five Supabase values from their own gitignored
# .env files — GEO_E2E is where they are set up, so mirror them into all three sibling files
# (upsert: other lines are preserved, a missing file is created with a pointer comment).
sync_sibling_envs() {
    local any=0 sibling target tmp key value keys_re
    for key in "${SUPABASE_KEYS[@]}"; do
        if [[ -n "${!key:-}" ]]; then
            any=1
        fi
    done
    if [[ "$any" -ne 1 ]]; then
        return 0 # nothing to mirror — the warning above already told the user
    fi
    # joined with `|` for grep (the ${arr[*]//pat/rep} form would run per element, not on the join)
    keys_re="^($(IFS='|'; echo "${SUPABASE_KEYS[*]}"))="
    for sibling in GEO_ETL GEO_API GEO_Visual; do
        target="$GEO_ROOT/$sibling/.env"
        if [[ ! -d "$GEO_ROOT/$sibling" ]]; then
            # The Airflow image contains only GEO_ETL (not GEO_API), so stay quiet there.
            [[ "$DOCKER_MODE" == "1" ]] || echo "warning: $GEO_ROOT/$sibling not found — skipped writing its .env" >&2
            continue
        fi
        tmp="$(mktemp "${target}.tmp.XXXXXX")"
        if [[ -f "$target" ]]; then
            # keep every existing line except the five keys, then re-append the current values
            grep -E -v "$keys_re" "$target" > "$tmp" || true
        else
            {
                echo "# Supabase PostgreSQL credentials — mirrored from GEO_E2E/.env by GEO_E2E's ./setup.sh"
                echo "# Set them in GEO_E2E/.env first (see GEO_E2E/.env.example); this file is gitignored."
            } > "$tmp"
        fi
        for key in "${SUPABASE_KEYS[@]}"; do
            value="${!key:-}"
            if [[ -n "$value" ]]; then
                printf '%s=%s\n' "$key" "$value" >> "$tmp"
            fi
        done
        mv "$tmp" "$target"
        echo "Mirrored the Supabase credentials into $target:"
        grep -E "^(SUPABASE_DB_USER|SUPABASE_DB_HOST|SUPABASE_DB_PORT)=" "$target" | sed 's/^/  /'
    done
}

if [[ -e "$PROJECT_DIR/$ENV_FILE" && "$FORCE" != true ]]; then
    echo "$ENV_FILE already exists in $PROJECT_DIR — nothing to do."
    echo "Edit it manually, or rerun ./setup.sh --force to regenerate it from this checkout."
    # The env file itself is untouched, but the sibling .env files are still refreshed from it
    for key in "${SUPABASE_KEYS[@]}"; do
        # `|| true`: a missing key must not fail the script (set -e + pipefail)
        value="$(grep -E "^${key}=" "$PROJECT_DIR/$ENV_FILE" | head -n1 | cut -d= -f2- || true)"
        printf -v "$key" '%s' "$value"
    done
    sync_sibling_envs
    exit 0
fi

# Keep the login of an existing env file across --force regenerations
previous_value() {
    local key="$1" value=""
    if [[ -f "$PROJECT_DIR/$ENV_FILE" ]]; then
        # `|| true`: a missing key must not fail the script (set -e + pipefail)
        value="$(grep -E "^${key}=" "$PROJECT_DIR/$ENV_FILE" | head -n1 | cut -d= -f2- || true)"
    fi
    echo "$value"
}

prompt() {
    # prompt <default> <prompt>: asks only on an interactive terminal outside containers
    local default="$1" value=""
    if [[ "$IN_CONTAINER" != true && -t 0 ]]; then
        read -r -p "$2 [$default] " value
    fi
    echo "${value:-$default}"
}

# Supabase credentials are secrets: they are never prompted (they would land in the terminal
# scrollback) — the primary place to set them is this file (.env); a previous value of
# .env or of a sibling GEO_ETL/.env, GEO_API/.env or GEO_Visual/.env is re-used, else they
# stay empty (docker compose then fails with a message naming the missing variable).
# sync_sibling_envs() mirrors the result back into all siblings, so every project runs with
# the same credentials.
supabase_value() {
    local key="$1" value="" sibling
    # Runtime/CI environment wins (Docker Compose supplies the credentials this way).
    value="${!key:-}"
    if [[ -z "$value" ]]; then
        value="$(previous_value "$key")"
    fi
    for sibling in GEO_ETL GEO_API GEO_Visual; do
        if [[ -z "$value" && -f "$GEO_ROOT/$sibling/.env" ]]; then
            # `|| true`: a missing key must not fail the script (set -e + pipefail)
            value="$(grep -E "^${key}=" "$GEO_ROOT/$sibling/.env" | head -n1 | cut -d= -f2- || true)"
        fi
    done
    echo "$value"
}

warn_missing_sibling() {
    if [[ ! -d "$GEO_ROOT/$1" && "$DOCKER_MODE" != "1" ]]; then
        echo "warning: $GEO_ROOT/$1 not found — the $1 project must be a sibling of $PROJECT_DIR" >&2
    fi
}

warn_missing_sibling GEO_Download
warn_missing_sibling GEO_ETL

if [[ "$DOCKER_MODE" == "1" ]]; then
    CONDA_ROOT=/opt/conda
else
    CONDA_ROOT="${CONDA_ROOT:-$(conda info --base 2>/dev/null || true)}"
fi
if [[ -z "$CONDA_ROOT" ]]; then
    for _candidate in "$HOME/anaconda3" "$HOME/miniconda3" "$HOME/miniforge3" \
        /opt/anaconda3 /opt/miniconda3 /opt/homebrew/anaconda3 \
        /usr/local/anaconda3 /usr/local/miniconda3; do
        if [[ -d "$_candidate" ]]; then
            CONDA_ROOT="$_candidate"
            break
        fi
    done
fi
if [[ -z "$CONDA_ROOT" ]]; then
    if [[ "$IN_CONTAINER" == true ]]; then
        CONDA_ROOT=/opt/conda
    else
        CONDA_ROOT="$(prompt "/absolute/path/to/anaconda3" "Conda base dir (CONDA_ROOT)")"
    fi
fi

if [[ "$DOCKER_MODE" == "1" ]]; then
    CONDA_COMMENT="# Anaconda installed inside the geo_airflow_spark image"
    UID_COMMENT="# uid of the airflow user inside the container"
    FILE_COMMENT="Generated inside the image; airflow-init refreshes the credentials from the Docker Compose environment at startup."
    NEXT_LINE="Wrote $PROJECT_DIR/$ENV_FILE (image mode — Docker Compose provides the credentials at runtime)"
elif [[ "$IN_CONTAINER" == true ]]; then
    CONDA_COMMENT="# Conda installation inside the container"
    UID_COMMENT="# uid of the user running this script inside the container"
    FILE_COMMENT="Regenerate with ./setup.sh --force, or edit by hand; read by the containerized solution (in-container run)."
    NEXT_LINE="Wrote $PROJECT_DIR/$ENV_FILE (in-container run — compose step not applicable)"
else
    CONDA_COMMENT="# Conda installation on the host (optional — used by local, non-container runs)"
    UID_COMMENT="# Host uid of your user"
    FILE_COMMENT="Regenerate with ./setup.sh --force, or edit by hand; docker compose auto-loads .env from this directory."
    NEXT_LINE="Next: docker compose up -d --build"
fi

if [[ ! -d "$CONDA_ROOT" ]]; then
    if [[ "$IN_CONTAINER" == true ]]; then
        echo "warning: CONDA_ROOT=$CONDA_ROOT does not exist (no conda on PATH, /opt/conda missing)" >&2
    else
        echo "warning: CONDA_ROOT=$CONDA_ROOT does not exist (used only by local, non-container runs)" >&2
    fi
fi

if [[ "$DOCKER_MODE" == "1" ]]; then
    AIRFLOW_UID="${AIRFLOW_UID:-50000}"
else
    AIRFLOW_UID="$(id -u)"
fi
GEO_DATA_DIR="$GEO_ROOT/GEO_Download/data"
# The data folder is bind-mounted into the containers, so it must exist before Compose starts.
mkdir -p "$GEO_DATA_DIR"
USERNAME="$(prompt "${_AIRFLOW_WWW_USER_USERNAME:-$(previous_value _AIRFLOW_WWW_USER_USERNAME)}" "Airflow UI login (_AIRFLOW_WWW_USER_USERNAME)")"
USERNAME="${USERNAME:-airflow_admin}"
PASSWORD="$(prompt "${_AIRFLOW_WWW_USER_PASSWORD:-$(previous_value _AIRFLOW_WWW_USER_PASSWORD)}" "Airflow UI password (_AIRFLOW_WWW_USER_PASSWORD)")"
PASSWORD="${PASSWORD:-airflow_admin}"

SUPABASE_DB_USER="$(supabase_value SUPABASE_DB_USER)"
SUPABASE_DB_PASSWORD="$(supabase_value SUPABASE_DB_PASSWORD)"
SUPABASE_DB_HOST="$(supabase_value SUPABASE_DB_HOST)"
SUPABASE_DB_PORT="$(supabase_value SUPABASE_DB_PORT)"
SUPABASE_DB_NAME="$(supabase_value SUPABASE_DB_NAME)"
AIRFLOW_DB_SCHEMA="${AIRFLOW_DB_SCHEMA:-$(previous_value AIRFLOW_DB_SCHEMA)}"
AIRFLOW_DB_SCHEMA="${AIRFLOW_DB_SCHEMA:-geo_airflow}"
if [[ -z "$SUPABASE_DB_USER" || -z "$SUPABASE_DB_PASSWORD" || -z "$SUPABASE_DB_HOST" ]]; then
    # In image mode Compose itself requires the five values before starting the container,
    # so a build-time warning would only be noise.
    [[ "$DOCKER_MODE" == "1" ]] || echo "warning: SUPABASE_DB_* not found in $ENV_FILE, $GEO_ROOT/GEO_ETL/.env, $GEO_ROOT/GEO_API/.env or $GEO_ROOT/GEO_Visual/.env — fill them in $ENV_FILE" \
        "(template: .env.example) before docker compose" >&2
fi

cat > "$PROJECT_DIR/$ENV_FILE" <<EOF
# Generated by ./setup.sh on $(date -u +%Y-%m-%dT%H:%M:%SZ) from $PROJECT_DIR
# Machine-specific values (paths, uid, Airflow UI login, Supabase credentials) — git-ignored.
# $FILE_COMMENT

# Parent dir of the sibling projects (GEO_Download, GEO_E2E, GEO_ETL)
GEO_ROOT=$GEO_ROOT
# Data folder bind-mounted into Airflow and Spark as /opt/geo/GEO_Download/data
GEO_DATA_DIR=$GEO_DATA_DIR
$CONDA_COMMENT
CONDA_ROOT=$CONDA_ROOT
$UID_COMMENT
AIRFLOW_UID=$AIRFLOW_UID

# Airflow UI login (airflow-init creates this admin user on startup)
_AIRFLOW_WWW_USER_USERNAME=$USERNAME
_AIRFLOW_WWW_USER_PASSWORD=$PASSWORD

# Supabase PostgreSQL — Airflow's metadata DB (no local Postgres container)
SUPABASE_DB_USER=$SUPABASE_DB_USER
SUPABASE_DB_PASSWORD=$SUPABASE_DB_PASSWORD
SUPABASE_DB_HOST=$SUPABASE_DB_HOST
SUPABASE_DB_PORT=$SUPABASE_DB_PORT
SUPABASE_DB_NAME=$SUPABASE_DB_NAME
# Schema of that database holding Airflow's metadata tables (created by airflow-init)
AIRFLOW_DB_SCHEMA=$AIRFLOW_DB_SCHEMA
EOF

echo "Wrote $PROJECT_DIR/$ENV_FILE:"
grep -E "^(GEO_ROOT|CONDA_ROOT|AIRFLOW_UID|_AIRFLOW_WWW_USER_USERNAME|SUPABASE_DB_USER|SUPABASE_DB_HOST|SUPABASE_DB_PORT|AIRFLOW_DB_SCHEMA)=" "$PROJECT_DIR/$ENV_FILE" | sed 's/^/  /'
echo
sync_sibling_envs
echo
echo "$NEXT_LINE"
