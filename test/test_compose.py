"""Tests for the three-image Docker stack and its environment wiring."""

from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
_COMPOSE = (_ROOT / "docker-compose.yaml").read_text()
_ENV_EXAMPLE = (_ROOT / ".env.example").read_text()
_GITIGNORE = (_ROOT / ".gitignore").read_text()
_DOCKER = _ROOT / "docker"

# Same variable names as the GEO_ETL project — the password deliberately travels in
# PGPASSWORD (libpq) instead of inside the SQLAlchemy URL, so it needs no URL-encoding
SUPABASE_VARS = (
    "SUPABASE_DB_USER",
    "SUPABASE_DB_PASSWORD",
    "SUPABASE_DB_HOST",
    "SUPABASE_DB_PORT",
    "SUPABASE_DB_NAME",
)

IMAGES = {
    "geo_airflow_spark": _DOCKER / "geo_airflow_spark" / "Dockerfile",
    "geo_fastapi": _DOCKER / "geo_fastapi" / "Dockerfile",
    "geo_react_app": _DOCKER / "geo_react_app" / "Dockerfile",
}


def _conn_line() -> str:
    """The single compose line that assembles the SQLAlchemy connection URL."""
    return next(line for line in _COMPOSE.splitlines() if "SQL_ALCHEMY_CONN" in line)


def test_three_images_and_dockerfiles_exist() -> None:
    """Exactly the three requested images have a Dockerfile in their subfolder."""
    for image, dockerfile in IMAGES.items():
        assert dockerfile.is_file(), dockerfile
        assert f"image: {image}:latest" in _COMPOSE, image
        assert f"dockerfile: GEO_E2E/docker/{image}/Dockerfile" in _COMPOSE, image


def test_no_host_bind_mounts_or_host_conda_left_in_compose() -> None:
    """The images are self-contained: no host project, host conda or host data mounts."""
    assert "network_mode: host" not in _COMPOSE
    assert "CONDA_ROOT: ${CONDA_ROOT" not in _COMPOSE
    assert "${GEO_ROOT" not in _COMPOSE
    assert "${GEO_DATA_DIR" not in _COMPOSE
    assert "geo_data:/opt/geo/GEO_Download/data" in _COMPOSE
    assert "airflow_data:/opt/airflow" in _COMPOSE


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
    """create_schema.py must run before /entrypoint triggers `airflow db migrate`."""
    assert _COMPOSE.index("create_schema.py") < _COMPOSE.index("/entrypoint airflow version")


def test_airflow_credentials_have_the_documented_defaults() -> None:
    """The Airflow login defaults to airflow_admin / airflow_admin and is overridable."""
    assert "${_AIRFLOW_WWW_USER_USERNAME:-airflow_admin}" in _COMPOSE
    assert "${_AIRFLOW_WWW_USER_PASSWORD:-airflow_admin}" in _COMPOSE
    assert "_AIRFLOW_WWW_USER_USERNAME=airflow_admin" in _ENV_EXAMPLE
    assert "_AIRFLOW_WWW_USER_PASSWORD=airflow_admin" in _ENV_EXAMPLE


def test_env_example_documents_the_supabase_variables() -> None:
    """The committed template carries every credential variable and the schema name."""
    for var in (*SUPABASE_VARS, "AIRFLOW_DB_SCHEMA"):
        assert f"{var}=" in _ENV_EXAMPLE, var
    assert "AIRFLOW_DB_SCHEMA=geo_airflow" in _ENV_EXAMPLE
    assert "VITE_API_BASE=" in _ENV_EXAMPLE


def test_credentials_stay_out_of_git() -> None:
    """The env file holding the Supabase password is git-ignored; only the template is committed."""
    ignored = {line.strip() for line in _GITIGNORE.splitlines() if line.strip() and not line.startswith("#")}
    assert ".env" in ignored
    assert ".env.docker" in ignored
    assert ".env.example" not in ignored


def test_setup_scripts_write_the_supabase_variables() -> None:
    """setup.sh and setup.ps1 carry the credentials into the env file."""
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


def test_container_startup_refreshes_env_from_compose() -> None:
    """Image builds contain paths only; airflow-init injects the Compose credentials."""
    airflow_dockerfile = IMAGES["geo_airflow_spark"].read_text()
    fastapi_dockerfile = IMAGES["geo_fastapi"].read_text()
    assert "GEO_DOCKER=1 ./setup.sh --force" in airflow_dockerfile
    assert "/opt/geo/bin/refresh-env.sh" in _COMPOSE
    assert 'entrypoint: ["/opt/geo/bin/airflow-entrypoint.sh"]' in _COMPOSE
    assert "airflow-entrypoint.sh" in airflow_dockerfile
    assert "GEO_DOCKER=1" in (_DOCKER / "geo_airflow_spark" / "refresh-env.sh").read_text()
    assert "SUPABASE_DB_PASSWORD=$SUPABASE_DB_PASSWORD" in (_DOCKER / "geo_fastapi" / "entrypoint.sh").read_text()
    assert "entrypoint.sh" in fastapi_dockerfile


def test_container_dependencies() -> None:
    """Airflow waits for Spark; React waits for the API; the API and Airflow use Supabase."""
    assert "SPARK_MASTER_URL: spark://geo-spark-master:7078" in _COMPOSE
    assert "AIRFLOW__CORE__EXECUTION_API_SERVER_URL: http://airflow-api-server:8090/execution/" in _COMPOSE
    assert "condition: service_healthy" in _COMPOSE  # Spark/API health gating
    assert "geo_fastapi:" in _COMPOSE
    assert "VITE_API_BASE: ${VITE_API_BASE:-http://localhost:8000}" in _COMPOSE


def test_images_exclude_development_files() -> None:
    """Every build context excludes docs/tests/tooling; the source folders are selectively copied."""
    for image in IMAGES:
        ignore = (_DOCKER / image / "Dockerfile.dockerignore").read_text()
        for pattern in ("**/test", "**/docs", "**/.tox", "**/node_modules", "**/*.md", "**/.env"):
            assert pattern in ignore, f"{image}: {pattern}"
    airflow_dockerfile = IMAGES["geo_airflow_spark"].read_text()
    # Only runtime entry points/packages are copied, not the whole repository trees.
    assert "GEO_E2E/test/" not in airflow_dockerfile
    assert "GEO_E2E/docs/" not in airflow_dockerfile
    assert "GEO_ETL/test/" not in airflow_dockerfile


def test_runtime_is_memory_bounded() -> None:
    """Every container has a hard limit and the Airflow executor is not left at 32 workers."""
    # Hard cgroup limits + no container swap + OOM preference over the desktop session.
    assert "mem_limit:" in _COMPOSE
    assert "memswap_limit:" in _COMPOSE
    assert "oom_score_adj: 500" in _COMPOSE
    assert "mem_limit: ${AIRFLOW_SCHEDULER_MEM_LIMIT:-4g}" in _COMPOSE
    assert "mem_limit: ${AIRFLOW_INIT_MEM_LIMIT:-2g}" in _COMPOSE
    assert "mem_limit: ${SPARK_WORKER_MEM_LIMIT:-3g}" in _COMPOSE
    assert "mem_limit: ${GEO_API_MEM_LIMIT:-1g}" in _COMPOSE
    assert "mem_limit: ${GEO_APP_MEM_LIMIT:-128m}" in _COMPOSE

    # LocalExecutor workers and per-DAG concurrency are capped below Airflow's defaults.
    assert "AIRFLOW__CORE__PARALLELISM: ${AIRFLOW_PARALLELISM:-8}" in _COMPOSE
    assert "AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG: ${AIRFLOW_MAX_ACTIVE_TASKS_PER_DAG:-6}" in _COMPOSE
    assert "AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG: ${AIRFLOW_MAX_ACTIVE_RUNS_PER_DAG:-1}" in _COMPOSE

    # Spark drivers/executors are capped too, and the knobs are documented in the template.
    assert "SPARK_DRIVER_MEMORY: ${SPARK_DRIVER_MEMORY:-1g}" in _COMPOSE
    assert "SPARK_EXECUTOR_MEMORY: ${SPARK_EXECUTOR_MEMORY:-1g}" in _COMPOSE
    assert "SPARK_WORKER_MEMORY: ${SPARK_WORKER_MEMORY:-2g}" in _COMPOSE
    for var in (
        "AIRFLOW_PARALLELISM=8",
        "AIRFLOW_SCHEDULER_MEM_LIMIT=4g",
        "SPARK_WORKER_MEM_LIMIT=3g",
        "SPARK_DRIVER_MEMORY=1g",
    ):
        assert var in _ENV_EXAMPLE, var


def test_serial_build_helper_exists() -> None:
    """A serial build helper is available for machines where parallel conda builds thrash."""
    script = _ROOT / "build-images.sh"
    assert script.is_file()
    text = script.read_text()
    for service in ("airflow-init", "geo_fastapi", "geo_react_app"):
        assert service in text
    assert "docker compose build" in text
