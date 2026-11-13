← [Configuration](../configuration.md)

# Environment, Credentials & Resource Limits

The host paths, Supabase credentials and Compose resource caps live in the git-ignored
`.env` and `.env.example`.

## Data folder and runtime UID (`.env`)

`GEO_DATA_DIR` is the host folder bind-mounted at `/opt/geo/GEO_Download/data` in the Airflow and Spark containers. GEO_ETL publishes the CSV inventory from the Spark side to Supabase, and GEO_API reads that table. `./setup.sh` writes the absolute path into `.env`; the manual `.env.example` uses the relative `../GEO_Download/data`. This folder is the single source of truth for the downloaded CSVs and reference lookups, so keep it when editing `.env` by hand.

`AIRFLOW_UID` is the host UID the Airflow containers run as. Downloads therefore create files owned by that user, and the same UID is used by `airflow-init` when it prepares `/opt/airflow` and the bind-mounted data folder. `./setup.sh` detects it automatically; the manual template uses `1000`.

## Supabase metadata DB (`.env`)

Airflow's metadata DB, GEO_ETL's JDBC writes, GEO_API's queries and GEO_Visual's read-only queries all use the **Supabase** Postgres of the GEO_ETL project — there is no local Postgres container. `docker-compose.yaml` assembles Airflow's connection from the five variables in the git-ignored `.env` (template: `.env.example`).

| Variable | Shipped value | Description |
|----------|---------------|-------------|
| `SUPABASE_DB_USER` | `postgres.<project-ref>` | Database user — `postgres.` followed by the Supabase project ref |
| `SUPABASE_DB_PASSWORD` | — | Database password (secret — never committed, never prompted) |
| `SUPABASE_DB_HOST` | `<pooler host>` | Transaction-pooler hostname (Supabase dashboard → Project → Connect) |
| `SUPABASE_DB_PORT` | `6543` | Transaction-pooler port (the direct primary listener uses `5432`) |
| `SUPABASE_DB_NAME` | `postgres` | Database name |
| `AIRFLOW_DB_SCHEMA` | `geo_airflow` | Optional override of the schema holding the Airflow tables |

Compose turns them into `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`:

```
postgresql+psycopg2://<user>@<host>:6543/postgres?sslmode=require&options=-c%20search_path%3Dgeo_airflow
```

- `sslmode=require` — Supabase terminates TLS on the pooler.
- `search_path=geo_airflow` — every connection resolves unqualified table names in that schema, so all Airflow tables live in **`geo_airflow`** and nothing lands in `public`.
- The **password is not part of the URL** — compose passes it as `PGPASSWORD`, which libpq reads at connect time, so no character needs URL-encoding (the same separation GEO_ETL uses for its JDBC properties).
- The schema itself is created by `docker/geo_airflow_spark/create_schema.py`, which `airflow-init` runs *before* `airflow db migrate` — Postgres does not create schemas on its own, and without the step the migration would silently fall back to `public`. The script is idempotent, so every `docker compose up` re-checks it.

### Credentials are injected at runtime

The images are built without secrets:

- Every Airflow service (`scheduler`, `api-server`, `dag-processor`) starts through `docker/geo_airflow_spark/airflow-entrypoint.sh`, which runs `refresh-env.sh` — `setup.sh` with `GEO_DOCKER=1` — and writes the Compose-provided values into that container's `/opt/geo/GEO_E2E/.env` and `/opt/geo/GEO_ETL/.env`. Each container has its own writable copy of the baked-in projects, so these files must be refreshed per container.
- `airflow-init` runs the same refresh before creating the schema and running the migration.
- `geo_fastapi`'s entrypoint writes `/opt/geo/GEO_API/.env` from its Compose `environment:` block.
- `geo_streamlit`'s entrypoint writes `/opt/geo/GEO_Visual/.env` from its Compose `environment:` block.

The sibling `.env` files therefore always match the Compose credentials, but rebuilding an image never embeds the Supabase password.

### Preflight check before `docker compose up`

`./up.sh -d` runs `./check.sh` before starting anything, and `./build-images.sh` runs the same check before building. It verifies the two hard requirements:

1. **All six repositories are present** — `GEO_API`, `GEO_APP`, `GEO_Download`, `GEO_E2E`, `GEO_ETL` and `GEO_Visual` must be directories in the same parent folder, each with its expected runtime entry point.
2. **The Supabase credentials are configured** — `.env` exists, all five `SUPABASE_DB_*` keys are non-empty, no value still contains an `<...>` placeholder from `.env.example`, and `SUPABASE_DB_PORT` is a number in `1..65535`.

Run the check alone with `./check.sh` (or `python3 preflight.py`); on Windows use `.\check.ps1`. A failure lists every missing repository/variable and the remediation, exits non-zero, and does not start Docker. Exported environment variables are accepted with the same precedence as Docker Compose, but the `.env` file itself must still exist.

### Step by step: filling in `.env`

For a fresh Docker-only checkout, **only the five `SUPABASE_DB_*` values are required**. Airflow defaults to `airflow_admin` / `airflow_admin`.

1. **Create the env file**:

   ```bash
   cp .env.example .env
   ```

   or let `./setup.sh` generate it (it imports existing credentials from `.env`, `../GEO_ETL/.env`, `../GEO_API/.env` or `../GEO_Visual/.env` when present).
2. **Airflow UI login** — `_AIRFLOW_WWW_USER_USERNAME` / `_AIRFLOW_WWW_USER_PASSWORD`: `airflow_admin` / `airflow_admin` are the defaults; change them only if you want a different admin login. `airflow-init` creates that user on first start.
3. **Supabase host, port, database, user** — open the [Supabase dashboard](https://supabase.com/dashboard) and select the project → **Connect** → *Connection pooling → Transaction pooler* → copy the four values into `.env`:

   | Variable | Comes from | Typical value |
   |----------|------------|---------------|
   | `SUPABASE_DB_HOST` | *Transaction pooler* → **Host** | `<region>.pooler.supabase.com` |
   | `SUPABASE_DB_PORT` | *Transaction pooler* → **Port** | `6543` |
   | `SUPABASE_DB_NAME` | *Transaction pooler* → **Database** | `postgres` |
   | `SUPABASE_DB_USER` | *Transaction pooler* → **User** | `postgres.<project-ref>` |

   > `6543` is the **transaction pooler**; the direct primary listener uses `5432`. Airflow holds long-lived pooled connections, so the pooler (with the defaults `sql_alchemy_pool_pre_ping = true`, `sql_alchemy_pool_recycle = 1800`) is the right port — the same one GEO_ETL uses.
4. **Supabase password** — `SUPABASE_DB_PASSWORD` is **never displayed** in the dashboard: use the value you saved when the project was created, or go to **Project Settings → Database → Generate password** (formerly *Reset database password*). That rotation invalidates the previous password immediately. See [GEO_ETL — Supabase Credentials](../../../GEO_ETL/docs/configuration/supabase-credentials.md) for the walkthrough, API/PAT rotation and a least-privilege role.
5. **Schema** — leave `AIRFLOW_DB_SCHEMA=geo_airflow`. On the first `docker compose up`, `airflow-init` creates it and runs the migration into it.
6. **Optional dashboard URLs/ports** — `VITE_API_BASE` is compiled into `geo_react_app`; keep `http://localhost:8000` when the browser runs on the Docker host. `GEO_VISUAL_HOST_PORT` (default `8501`) publishes the Streamlit dashboard.
7. **Check** — `./check.sh` verifies the six repositories and the five Supabase values before any container starts; `docker compose config --quiet` additionally renders the full Compose file and fails with the name of any missing key. To verify the metadata connection and schema:

   ```bash
   docker compose exec airflow-scheduler python -c "import os; from sqlalchemy import create_engine, text; e=create_engine(os.environ['AIRFLOW__DATABASE__SQL_ALCHEMY_CONN']); print(e.connect().execute(text('select current_schema()')).scalar())"
   ```

   It prints `geo_airflow` when the connection and the schema are right.

`./check.sh` catches a missing, empty or placeholder value before Docker starts; without it, Compose still stops early with a message naming the key (`Set SUPABASE_DB_HOST in .env`). `.env` is git-ignored; only `.env.example` (placeholders) is committed. The legacy `.env.docker` name is still ignored by git but is not used by the Docker images (they write `.env`).

## Resource tuning (`.env`)

The stack is bounded by default. These optional variables override the Compose defaults and are documented in `.env.example`:

| Variable | Default | Effect |
|----------|---------|--------|
| `AIRFLOW_PARALLELISM` | `8` | Number of `LocalExecutor` worker processes in the scheduler (Airflow's default is 32) |
| `AIRFLOW_MAX_ACTIVE_TASKS_PER_DAG` | `6` | Per-DAG task cap (pools still limit real concurrency to 3 downloads + 2 ETL jobs) |
| `AIRFLOW_MAX_ACTIVE_RUNS_PER_DAG` | `1` | Prevents overlapping scheduled runs |
| `AIRFLOW_MEM_LIMIT` | `1g` | Common hard limit for one-shot Airflow services |
| `AIRFLOW_SCHEDULER_MEM_LIMIT` | `4g` | Scheduler + all task subprocesses + PySpark drivers |
| `AIRFLOW_INIT_MEM_LIMIT` / `AIRFLOW_API_MEM_LIMIT` / `AIRFLOW_DAG_PROCESSOR_MEM_LIMIT` | `2g` / `1g` / `1g` | One-shot init, API server, DAG processor |
| `SPARK_MASTER_MEM_LIMIT` / `SPARK_WORKER_MEM_LIMIT` | `1g` / `3g` | Spark master and each worker container |
| `SPARK_WORKER_MEMORY` / `SPARK_WORKER_CORES` | `2g` / `2` | Resources advertised by each Spark worker |
| `SPARK_DRIVER_MEMORY` / `SPARK_EXECUTOR_MEMORY` | `1g` / `1g` | PySpark heap caps applied by `GEO_ETL/config.json` |
| `SPARK_EXECUTOR_CORES` / `SPARK_SHUFFLE_PARTITIONS` / `SPARK_MAX_RESULT_SIZE` | `1` / `8` / `256m` | Executor cores, shuffle partitions and driver result cap |
| `GEO_API_MEM_LIMIT` / `GEO_APP_MEM_LIMIT` / `GEO_VISUAL_MEM_LIMIT` | `1g` / `128m` / `1g` | FastAPI, nginx and Streamlit |

Every service also has `memswap_limit` equal to its memory limit, so containers cannot push the host into swap; `oom_score_adj: 500` makes a container the kernel's first OOM victim. Use `docker stats --no-stream` to see actual usage, and `docker compose down` to release everything when the stack is not needed.

The first build is the other memory peak: Anaconda's solver runs in three Anaconda-based images. `./build-images.sh` builds the four images serially; `docker compose build` may run independent services in parallel.
