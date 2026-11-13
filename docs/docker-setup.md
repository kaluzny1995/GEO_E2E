← [README](../README.md)

# Docker Setup

## Dockerizing the solution (recommended)

### 1. Configure the checkout and credentials — mandatory before `docker compose up`

All six repositories (`GEO_API`, `GEO_APP`, `GEO_Download`, `GEO_E2E`, `GEO_ETL`, `GEO_Visual`) must be cloned side by side first; the preflight check at the end of this section verifies that and the credentials below. Create the git-ignored `.env` from the template:

```bash
cp .env.example .env
```

Then fill in the five **Supabase** values:

```env
SUPABASE_DB_USER=postgres.<project-ref>
SUPABASE_DB_PASSWORD=<supabase_db_password>
SUPABASE_DB_HOST=<pooler host>
SUPABASE_DB_PORT=6543
SUPABASE_DB_NAME=postgres
```

Airflow has usable defaults and does **not** need to be configured unless you want a different login:

```env
_AIRFLOW_WWW_USER_USERNAME=airflow_admin
_AIRFLOW_WWW_USER_PASSWORD=airflow_admin
```

`./setup.sh` can also generate `.env` and imports existing Supabase credentials from `../GEO_ETL/.env`, `../GEO_API/.env` or `../GEO_Visual/.env` when present. Manual `cp .env.example .env` remains the simplest path for a fresh Docker-only checkout. On macOS use `./setup.command`; on Windows use `.\setup.ps1`, then `.\check.ps1` / `.\up.ps1` (the PowerShell twins of the shell helpers).

> **Security:** the Supabase credentials are injected into the containers at **runtime** from the Compose `environment:` section. Every long-running Airflow service (scheduler, API server, DAG processor) refreshes its own in-image `.env` through `airflow-entrypoint.sh`; `airflow-init` does the same before the migration, the FastAPI entrypoint writes `GEO_API/.env`, and the Streamlit entrypoint writes `GEO_Visual/.env`. No credential is ever baked into an image layer. `.env` is git-ignored.

`docker compose` loads `.env` automatically from this directory. It fails before starting with a message naming any missing required Supabase variable.

#### Preflight check — repositories and credentials

`./up.sh -d` runs `./check.sh` before every start. Run the check alone with:

```bash
./check.sh
# or, equivalently:
python3 preflight.py
```

It verifies both hard requirements:

1. **All six repositories** — `GEO_API`, `GEO_APP`, `GEO_Download`, `GEO_E2E`, `GEO_ETL` and `GEO_Visual` are checked out in the same folder (the parent of `GEO_E2E`).
2. **Supabase credentials** — `.env` exists and all five `SUPABASE_DB_*` values are non-empty, are not the `<...>` placeholders from `.env.example`, and the port is a valid number.

If something is missing, the check prints the exact repository or variable to fix, the `cp .env.example .env` command and the Supabase dashboard path, then exits non-zero — no container is started. On Windows use `.\check.ps1` / `.\up.ps1`; both wrap the same Python checker.

Where the values come from:

1. `SUPABASE_DB_USER`, `SUPABASE_DB_HOST`, `SUPABASE_DB_PORT`, `SUPABASE_DB_NAME` — [Supabase dashboard](https://supabase.com/dashboard) → project → **Connect** → *Connection pooling* → **Transaction pooler**.
2. `SUPABASE_DB_PASSWORD` — the password saved at project creation, or **Project Settings → Database → Generate password** (rotation invalidates the old one immediately).
3. `AIRFLOW_DB_SCHEMA` — leave `geo_airflow`; `airflow-init` creates it before the migration.
4. `VITE_API_BASE` — optional; the API URL compiled into the React app. Keep `http://localhost:8000` when the browser runs on the Docker host.

Full walkthrough and URL anatomy: [Configuration → Supabase metadata DB](configuration/environment.md#supabase-metadata-db-env).

### 2. Build and start the stack

```bash
./build-images.sh      # serial builds; runs the preflight first
./up.sh -d             # preflight + docker compose up -d
```

`./up.sh` is the supported start command: it runs `./check.sh` and only then calls
`docker compose up`. Raw `docker compose up -d` still works, but it skips the friendly
preflight (Compose itself only reports missing variables, not missing repositories or
leftover placeholders). `./up.sh -d --build` works too, but builds independent images in
parallel and can spike RAM on the first run. The first build is long because Anaconda and
the `geo_*_311` environments are installed into the images; later rebuilds reuse the Docker
layer cache.

| Service | Ports | Purpose |
|---------|-------|---------|
| `airflow-init` | – | One-shot: refresh `.env`, create the Supabase schema, migrate the metadata DB, create the admin user, seed pools |
| `airflow-dag-processor` | – | Parses `geo_pipeline.py` into tasks |
| `airflow-scheduler` | – | Triggers the DAG and runs its tasks (LocalExecutor); waits for Spark |
| `airflow-api-server` | **8090** | Airflow REST API + web UI |
| `airflow-dag-activate` | – | One-shot: ensures `geo_pipeline` is unpaused after startup |
| `geo-spark-master` | **7078**, **8083** | Spark master RPC + web UI |
| `geo-spark-worker-1` | **8084** | Spark worker (2 cores, 2g advertised, 3g container limit) |
| `geo-spark-worker-2` | **8085** | Spark worker (2 cores, 2g advertised, 3g container limit) |
| `geo_fastapi` | **8000** | GEO_API (Swagger UI at `/docs`) |
| `geo_react_app` | **3000** | React dashboard served by nginx |
| `geo_streamlit` | **8501** | GEO_Visual Streamlit dashboard |

**Airflow UI:** http://localhost:8090 — default login `airflow_admin` / `airflow_admin` (from `_AIRFLOW_WWW_USER_*`).
**Spark UI:** http://localhost:8083 — worker UIs at http://localhost:8084 and http://localhost:8085.
**GEO_API:** http://localhost:8000 — health at `/health`, Swagger at `/docs`.
**GEO_APP:** http://localhost:3000.
**GEO_Visual:** http://localhost:8501 — Streamlit dashboard.

**Healthchecks:** every long-running service reports health — the scheduler and DAG processor through `airflow jobs check` (`SchedulerJob` / `DagProcessorJob`), the API server through `/api/v2/monitor/health`, the Spark master/workers through TCP probes, GEO_API through `/health`, nginx through `/`, and Streamlit through `/_stcore/health`. The one-shot `airflow-init`/`airflow-dag-activate` and the debug-only `airflow-cli` are gated by `service_completed_successfully`/profile instead.

If a host port is already in use, override it in `.env` without changing the container port:
`AIRFLOW_HOST_PORT`, `SPARK_MASTER_PORT_HOST`, `SPARK_MASTER_UI_HOST_PORT`,
`SPARK_WORKER_1_UI_HOST_PORT`, `SPARK_WORKER_2_UI_HOST_PORT`, `GEO_API_HOST_PORT`,
`GEO_APP_HOST_PORT`, `GEO_VISUAL_HOST_PORT` (see `.env.example`). Remember to update `VITE_API_BASE` and rebuild
`geo_react_app` when the API host port changes.

### 3. Memory and resource limits

The stack is deliberately bounded so a bad task cannot freeze the host:

| Limit | Default | Why |
|-------|---------|-----|
| `AIRFLOW__CORE__PARALLELISM` | `8` | Airflow's `LocalExecutor` default is **32 worker processes**; the pools only need 3 downloads + 2 ETL jobs at a time |
| `AIRFLOW_SCHEDULER_MEM_LIMIT` | `4g` | The scheduler container also runs every task subprocess and its PySpark driver |
| `AIRFLOW_MEM_LIMIT` / `AIRFLOW_INIT_MEM_LIMIT` | `1g` / `2g` | API server, DAG processor and the one-shot init |
| `SPARK_WORKER_MEM_LIMIT` | `3g` per worker | Worker JVM + one executor JVM |
| `SPARK_DRIVER_MEMORY` / `SPARK_EXECUTOR_MEMORY` | `1g` each | PySpark heap caps written into every ETL `SparkSession` |
| `GEO_API_MEM_LIMIT` / `GEO_APP_MEM_LIMIT` / `GEO_VISUAL_MEM_LIMIT` | `1g` / `128m` / `1g` | FastAPI, nginx and Streamlit |

Every service gets `mem_limit` + `memswap_limit` (equal values, so containers cannot push
the host into swap) and `oom_score_adj: 500`, which makes the kernel kill a container
before the desktop session if the host is still short on memory.

Tune them in `.env` (the defaults are in `.env.example`). For a small laptop:

```env
AIRFLOW_PARALLELISM=4
AIRFLOW_MAX_ACTIVE_TASKS_PER_DAG=4
AIRFLOW_SCHEDULER_MEM_LIMIT=3g
SPARK_WORKER_MEMORY=1g
SPARK_WORKER_MEM_LIMIT=2g
SPARK_DRIVER_MEMORY=512m
SPARK_EXECUTOR_MEMORY=512m
```

**Building safely** — the first build runs Anaconda's dependency solver inside three Anaconda-based images.
`docker compose build` builds independent services in parallel, which can spike RAM. Use:

```bash
./build-images.sh          # builds airflow-init, geo_fastapi, geo_streamlit and geo_react_app serially
```

and avoid running the old stack while building for the first time. Check what is actually
running with `docker stats --no-stream`; stop unused workers with
`docker compose stop geo-spark-worker-2` when you are not running ETL jobs. `docker compose down`
releases all container memory (and `restart: unless-stopped` will otherwise start the
stack again after a reboot). `docker compose build --memory` is ignored by BuildKit, so a
serial build is the supported way to cap the build peak.

> The conda definitions use the `libmamba` solver and `PIP_NO_CACHE_DIR=1`; both reduce the
> build's peak memory and disk churn.

### 4. Images and container dependencies

| Image | Contents | Published ports | Dockerfile |
|-------|----------|-----------------|------------|
| `geo_airflow_spark` | Airflow 3.3.2, Spark 3.5.5, Anaconda + `geo_e2e_311` / `geo_download_311` / `geo_etl_311`, GEO_E2E + GEO_Download + GEO_ETL under `/opt/geo` | 8090, 7078, 8083, 8084, 8085 | `docker/geo_airflow_spark/Dockerfile` |
| `geo_fastapi` | GEO_API under `/opt/geo/GEO_API`, Anaconda + `geo_api_311` | 8000 | `docker/geo_fastapi/Dockerfile` |
| `geo_react_app` | compiled Vite `dist` served by nginx (no source or test tooling) | 3000 | `docker/geo_react_app/Dockerfile` |
| `geo_streamlit` | GEO_Visual under `/opt/geo/GEO_Visual`, Anaconda + `geo_visual_311` | 8501 | `docker/geo_streamlit/Dockerfile` |

Container dependencies:

- `airflow-init`, `airflow-scheduler`, `airflow-dag-processor` and `airflow-api-server` → **Supabase** (metadata DB).
- `airflow-scheduler` → `airflow-init` completed + Spark master healthy + both workers started.
- Spark workers → Spark master.
- Airflow + Spark → the host `GEO_Download/data` folder bind-mounted at `/opt/geo/GEO_Download/data`; Airflow downloads CSV files, Spark executors read them.
- Airflow scheduler + Spark workers → the host `GEO_ETL/ml_models` folder bind-mounted at `/opt/geo/GEO_ETL/ml_models`; PREDICTION reads and writes its persisted MLlib models there.
- `geo_fastapi` → **Supabase** (read-only queries).
- `geo_react_app` → `geo_fastapi` healthy. The browser then calls the API at `VITE_API_BASE` (default `http://localhost:8000`).
- `geo_streamlit` → **Supabase** dashboard views (read-only). It deliberately does not depend on `geo_fastapi`.

### 5. Rebuilding after repository changes

The images copy the source at build time and use **no source bind mounts** (only the data and model directories are mounted), so every repository change is picked up by a rebuild:

```bash
docker compose build                 # rebuild every image whose inputs changed
./up.sh -d                           # preflight + recreate the containers
```

Or in one command:

```bash
./up.sh -d --build
```

Individual images:

```bash
docker compose build airflow-init   # builds geo_airflow_spark (GEO_E2E/GEO_Download/GEO_ETL)
docker compose build geo_fastapi    # builds geo_fastapi (GEO_API)
docker compose build geo_streamlit  # builds geo_streamlit (GEO_Visual)
docker compose build geo_react_app  # builds geo_react_app (GEO_APP)
./up.sh -d
```

On a memory-constrained laptop use `./build-images.sh` instead — it runs exactly these
four builds serially, keeping only one conda solver alive at a time, and runs the preflight
check before the first build.

`airflow-init` is the Compose service that owns the shared `geo_airflow_spark` build; the scheduler, API server, DAG processor and Spark services reference the same image.

- `docker compose restart` **does not** rebuild an image; use `build` first.
- `docker compose build --no-cache <service>` forces fresh base-image and package resolution.
- The React API URL is inlined by Vite, so changing `VITE_API_BASE` also requires rebuilding `geo_react_app`.
- Base images are pinned to Airflow 3.3.2, Spark 3.5.5, Anaconda 2024.10-1, Node 20 and nginx 1.27.

### 6. Data, logs and volumes

| Mount | Host path / volume | Purpose |
|-------|--------------------|---------|
| `/opt/geo/GEO_Download/data` | `GEO_DATA_DIR` (`../GEO_Download/data` by default) | downloaded CSV files and reference lookups |
| `/opt/geo/GEO_ETL/ml_models` | `../GEO_ETL/ml_models` | persisted PySpark MLlib models and metadata (survive rebuilds) |
| `airflow_data` | named volume | Airflow logs and runtime data |
| `geo_api_logs` | named volume | per-endpoint API logs |

`GEO_Download/data` is the single source of truth: it is bind-mounted into the Airflow and
Spark containers, so a CSV written by a download task is immediately visible on the host and
to every Spark executor. `GEO_ETL/ml_models` is writable by the Airflow/worker users and lets
the PREDICTION job reuse models across container recreations; delete a model directory there
to force a retrain. The directory is git-ignored, so a fresh checkout starts with no models
and the first PREDICTION run recreates them. GEO_ETL's DATA_SOURCE_FILE job publishes the CSV inventory from the Spark
side to `geo.data_source_files`; GEO_API reads that table, so the API container has no data
mount. `./setup.sh` writes the absolute `GEO_DATA_DIR` into `.env`; the
manual `.env.example` uses the relative `../GEO_Download/data`. The old `geo_data` named
volume is no longer used and is left untouched (not deleted); `docker compose down -v`
removes only the remaining named volumes, never the host data folder.

### 7. Pools and schedule are automatic

- `airflow-init` seeds the `download` (3 slots) and `etl` (2 slots) pools on every start.
- `dags_are_paused_at_creation=false` creates `geo_pipeline` unpaused; `airflow-dag-activate` also repairs an already-paused DAG after startup.
- The DAG runs daily at **08:00 Europe/Warsaw**, defined by the scheduled jobs in `jobs.json`.

Verify:

```bash
docker compose run --rm airflow-cli pools list
docker compose run --rm airflow-cli dags list
```

### 8. Local development without Docker (optional)

Host conda is only needed for running the DAG code, tests or tooling outside containers:

```bash
conda env create -f environment.yml
conda env create -f ../GEO_Download/environment.yml   # geo_download_311
conda env create -f ../GEO_ETL/environment.yml        # geo_etl_311
./setup.sh                                            # writes host paths + credentials into .env
```

`GEO_DATA_DIR` and `AIRFLOW_UID` are read by Docker Compose (the data bind mount and the runtime UID); `GEO_ROOT` and `CONDA_ROOT` are used only by local runs. Tests and linting stay local:

```bash
tox
ruff check .
mypy
```

### 9. Project layout

All six projects remain siblings under `GeographicData/`. Their runtime files are copied to `/opt/geo/<project>` inside the images, so the relative `../GEO_*` lookups keep working without host mounts.

```text
GeographicData/
├── GEO_Download/           # Data downloader (runtime files baked into geo_airflow_spark)
│   ├── run_download.py
│   ├── config.json
│   ├── downloader/ model/
│   └── data/               # bind-mounted into Airflow + Spark (GEO_DATA_DIR)
├── GEO_E2E/                # orchestrator + Compose + image definitions
│   ├── docker-compose.yaml
│   ├── .env.example        # Docker credentials template (committed)
│   ├── .env                # local credentials (git-ignored)
│   ├── setup.sh            # host + image-mode bootstrap (GEO_DOCKER=1)
│   ├── setup.ps1 / check.ps1 / up.ps1  # Windows twins of the shell helpers
│   ├── build-images.sh     # serial image builds for memory-constrained machines
│   ├── preflight.py        # checks all six repos + SUPABASE_DB_* in .env
│   ├── check.sh            # preflight wrapper (used by up.sh and build-images.sh)
│   ├── up.sh               # preflight + docker compose up
│   ├── config.json / jobs.json
│   ├── model.py / geo_dag.py / geo_pipeline.py / util.py
│   ├── docker/
│   │   ├── geo_airflow_spark/
│   │   │   ├── Dockerfile
│   │   │   ├── Dockerfile.dockerignore
│   │   │   ├── environment/        # runtime-only conda env definitions
│   │   │   ├── jars/               # PostgreSQL JDBC + spark-monitor
│   │   │   ├── create_schema.py
│   │   │   ├── seed_pools.py
│   │   │   ├── refresh-env.sh
│   │   │   └── airflow-entrypoint.sh
│   │   ├── geo_fastapi/
│   │   │   ├── Dockerfile
│   │   │   ├── environment.yml
│   │   │   └── entrypoint.sh
│   │   ├── geo_react_app/
│   │   │   ├── Dockerfile
│   │   │   └── nginx.conf
│   │   └── geo_streamlit/
│   │       ├── Dockerfile
│   │       ├── environment.yml
│   │       └── entrypoint.sh
│   └── docs/ test/
├── GEO_ETL/                # PySpark ETL (runtime files baked into geo_airflow_spark)
├── GEO_API/                # FastAPI service (runtime files baked into geo_fastapi)
├── GEO_APP/                # React dashboard (compiled into geo_react_app)
└── GEO_Visual/             # Streamlit dashboard (runtime files baked into geo_streamlit)
```
