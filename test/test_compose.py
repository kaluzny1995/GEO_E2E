"""Tests for the stack wiring: the Supabase metadata DB in docker-compose.yaml and the env files."""

from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
_COMPOSE = (_ROOT / "docker-compose.yaml").read_text()
_ENV_EXAMPLE = (_ROOT / ".env.example").read_text()
_GITIGNORE = (_ROOT / ".gitignore").read_text()

# Same variable names as the GEO_ETL project — the password deliberately travels in
# PGPASSWORD (libpq) instead of inside the SQLAlchemy URL, so it needs no URL-encoding
SUPABASE_VARS = (
    "SUPABASE_DB_USER",
    "SUPABASE_DB_PASSWORD",
    "SUPABASE_DB_HOST",
    "SUPABASE_DB_PORT",
    "SUPABASE_DB_NAME",
)


def _conn_line() -> str:
    """The single compose line that assembles the SQLAlchemy connection URL."""
    return next(line for line in _COMPOSE.splitlines() if "SQL_ALCHEMY_CONN" in line)


def test_no_local_postgres_left_in_compose() -> None:
    """The local Postgres service, its volume and its port are gone — Supabase is the metadata DB."""
    assert "airflow-postgres" not in _COMPOSE
    assert "postgres_data" not in _COMPOSE
    assert "postgres:16" not in _COMPOSE
    assert "5472" not in _COMPOSE


def test_metadata_db_is_built_from_the_supabase_env_vars() -> None:
    """The URL is assembled from the SUPABASE_DB_* values and pins search_path to the schema."""
    conn = _conn_line()
    # every credential except the password (it is in PGPASSWORD) is interpolated into the URL
    for var in SUPABASE_VARS:
        if var != "SUPABASE_DB_PASSWORD":
            assert "${" + var in conn, var
    assert "postgresql+psycopg2://" in conn
    assert "sslmode=require" in conn
    # `search_path=geo_airflow` on every connection — the schema is what holds the Airflow tables
    assert "search_path%3D${AIRFLOW_DB_SCHEMA:-geo_airflow}" in conn


def test_password_travels_in_pgpassword_not_in_the_url() -> None:
    """The Supabase password is passed to libpq through the environment, never into the URL."""
    assert "PGPASSWORD: ${SUPABASE_DB_PASSWORD" in _COMPOSE
    assert "SUPABASE_DB_PASSWORD" not in _conn_line()


def test_airflow_init_creates_the_schema_before_the_migration() -> None:
    """create_schema.py must run before /entrypoint triggers `airflow db migrate` — Airflow
    creates its tables in search_path, which would silently fall back to the default schema."""
    assert _COMPOSE.index("create_schema.py") < _COMPOSE.index("/entrypoint airflow version")


def test_env_example_documents_the_supabase_variables() -> None:
    """The committed template carries every credential variable and the schema name."""
    for var in (*SUPABASE_VARS, "AIRFLOW_DB_SCHEMA"):
        assert f"{var}=" in _ENV_EXAMPLE, var
    assert "AIRFLOW_DB_SCHEMA=geo_airflow" in _ENV_EXAMPLE


def test_credentials_stay_out_of_git() -> None:
    """The env file holding the Supabase password is git-ignored; only the template is committed."""
    ignored = {line.strip() for line in _GITIGNORE.splitlines() if line.strip() and not line.startswith("#")}
    assert ".env" in ignored
    assert ".env.docker" in ignored
    assert ".env.example" not in ignored


def test_setup_scripts_write_the_supabase_variables() -> None:
    """setup.sh and setup.ps1 carry the credentials into the env file (from GEO_ETL/.env)."""
    for script in ("setup.sh", "setup.ps1"):
        text = (_ROOT / script).read_text()
        for var in SUPABASE_VARS:
            assert var in text, f"{script}: {var}"


def test_setup_scripts_mirror_the_credentials_into_the_sibling_projects() -> None:
    """The Supabase setup lives here: both setup scripts must also write GEO_ETL/.env and
    GEO_API/.env, so the sibling projects read the same SUPABASE_DB_* values without their own
    setup — including on a plain re-run that leaves an existing .env untouched."""
    # a call immediately followed by `exit 0` = the "env file already exists" early-exit path
    early_exit_call = {
        "setup.sh": "    sync_sibling_envs\n    exit 0\n",
        "setup.ps1": "    Sync-SiblingEnv (Get-SupabaseValueMap)\n    exit 0\n",
    }
    for script, func in (("setup.sh", "sync_sibling_envs"), ("setup.ps1", "Sync-SiblingEnv")):
        text = (_ROOT / script).read_text()
        assert func in text, f"{script}: {func}"
        for sibling in ("GEO_ETL", "GEO_API"):
            assert sibling in text, f"{script}: {sibling}"
        assert early_exit_call[script] in text, f"{script}: early-exit must still mirror"
