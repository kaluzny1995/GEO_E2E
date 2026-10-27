← [README](../README.md)

# Architecture & Project Layout

## Project Structure

```
GEO_E2E/
├── README.md                 # Overview, setup and quick start
├── docker-compose.yaml       # Airflow stack (init, scheduler, API server, DAG processor → Supabase) + Spark cluster
├── .env.example              # Template for the machine-specific .env.local (committed)
├── .env.local                # Machine-specific paths (written by ./setup.sh, not in git)
├── setup.sh                  # Bootstrap: writes .env.local (+ login) from this checkout; in a container writes .env.docker
├── setup.ps1                 # Windows twin of setup.sh — same detection, same file names
├── setup.command             # macOS double-click entry point — thin wrapper that runs setup.sh
├── .gitignore                # Excludes .env, .env.local, .env.docker, logs/, __pycache__, .tox, IDE files
├── config.json               # Job config (command templates per job type)
├── jobs.json                 # Flat job registry: one object per job, next_jobs = name references
├── model.py                  # Pydantic models: JobConfig, Job, TaskSpec, build_task_specs()
├── geo_dag.py                # JobsSubDAG + GEO_DAG: task specs grouped per source and rendered into the Airflow DAG
├── geo_pipeline.py           # The Airflow DAG — the @dag wrapper around geo_dag.GEO_DAG
├── util.py                   # Memory logging helpers (Airflow task callbacks)
├── .airflowignore            # Airflow scans the project root: only geo_pipeline.py counts as a DAG file
├── environment.yml           # Conda environment definition (Python, ruff/tox/mypy, pinned Airflow for the DAG file)
├── pyproject.toml            # Ruff rules, formatter style, mypy files and pytest options
├── tox.ini                   # lint / test / typecheck / format / warnings environments
├── docker/
│   └── airflow/
│       ├── Dockerfile        # apache/airflow:3.3.2 + providers-standard, psutil, pydantic, conda symlink
│       └── seed_pools.py     # seeds the Airflow pools from config.json (dag.pool_slots) on startup
├── docs/                     # Extended documentation (this file lives here)
└── test/                     # Unit tests
    ├── __init__.py
    ├── test_dags.py          # build_task_specs(): task ids, wiring, union fan-in
    ├── test_geo_pipeline.py  # JobsSubDAG / GEO_DAG: subDAGs, the rendered mega-DAG, the union fan-in
    ├── test_jobs.py          # Job chains from jobs.json + command building
    ├── test_models.py        # JobTypeConfig, JobConfig, get_job_config()
    └── test_util.py          # memory helpers
```

| File | Responsibility |
|------|----------------|
| `geo_pipeline.py` | The `geo_pipeline` DAG: the `@dag` wrapper around `geo_dag.GEO_DAG` — supplies the DAG keywords (`geo_dag.GEO_DAG.dag_kwargs()`) and renders the tasks (`geo_dag.GEO_DAG.to_airflow()`); the cron comes from the scheduled jobs of `jobs.json`, the runtime knobs (retries, timeout, pools) from the `dag` block of `config.json` |
| `geo_dag.py` | `JobsSubDAG` (one `jobs.json` chain — a source or the union — as `BashOperator`s, wired internally) and `GEO_DAG` (groups the specs by source into the nine source subDAGs + the union subDAG, wires the cross-subDAG union fan-in and carries the `@dag` keywords) |
| `model.py` | Pydantic models: `JobTypeConfig`, `JobConfig`, `JobTypeConfig`, `Job`, `EJobType`, `TaskSpec`, plus `get_job_config()`, `load_chains()` and `build_task_specs()` |
| `util.py` | `get_memory_info()` and `log_memory_usage()` — used as `on_success_callback` / `on_failure_callback` of every task |
| `config.json` | Command templates for `DOWNLOAD`/`INGESTION`/`DATA_QUALITY`/`SINGLE_ETL`/`VIEW` (folder, script, conda env) |
| `jobs.json` | Flat job list — one object per job (`name`, `job_type`, `next_jobs` as name references) — plus the `Union` job |
| `docker-compose.yaml` | Airflow services (`network_mode: host`, UI on `8090`, metadata DB = Supabase Postgres from `.env.local`) plus the Spark cluster |
| `setup.sh` | Writes the git-ignored `.env.local` once per checkout — `GEO_ROOT`/`GEO_DATA_DIR` from the clone's location, `CONDA_ROOT` from `conda info --base`, `AIRFLOW_UID` from `id -u`, the Airflow UI login (`--force` keeps the existing one) and the `SUPABASE_DB_*` credentials (re-used from an existing file or from `../GEO_ETL/.env`); compose reads it via `--env-file .env.local` |
| `docker/airflow/Dockerfile` | Extends `apache/airflow:3.3.2-python3.11` with `apache-airflow-providers-standard`, `psutil`, `pydantic` (pinned to the Airflow constraints) and a `conda` symlink to the host conda (bind-mounted at runtime) |
| `docker/airflow/create_schema.py` | Creates the `geo_airflow` schema on Supabase — `airflow-init` runs it before `airflow db migrate`, so the migration's tables land in the schema the connection's `search_path` points at |

## How it works

1. The Airflow scheduler triggers the `geo_pipeline` DAG on the cron declared by the **scheduled jobs** in `jobs.json` (the nine `DOWNLOAD` roots, all `0 8 * * *` — `model.resolve_schedule()` requires them to agree, since Airflow schedules whole DAGs; `catchup`/`max_active_runs`/timezone come from the `dag` block of `config.json`). Downstreamed jobs (no `schedule`) start when their parent finishes. The DAG is created unpaused on first parse (`dags_are_paused_at_creation=false`), and the `download`/`etl` pools are seeded by `airflow-init` from `dag.pool_slots` (`docker/airflow/seed_pools.py`) on every `docker compose up`. The DAGs folder is the whole `GEO_E2E` directory (`AIRFLOW__CORE__DAGS_FOLDER` in `docker-compose.yaml`) and `.airflowignore` excludes everything else from the scan, so `geo_pipeline.py` is the only file parsed as a DAG. Airflow's metadata lives in **Supabase Postgres**: `.env.local` supplies the `SUPABASE_DB_*` credentials, `docker-compose.yaml` builds `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` from them with `sslmode=require` and `search_path=geo_airflow`, and `airflow-init` creates that schema (`docker/airflow/create_schema.py`) before running the migration — no local Postgres container is involved.
2. At DAG-parse time, `geo_pipeline.py` calls `model.build_task_specs()`, which loads every chain from `jobs.json` and flattens it into `TaskSpec`s — one per job, with the task id taken from the job's `task_id` field in the registry (`NIFC.download`, `NIFC.single_etl`, `NIFC_etl.data_quality`, `View.metrics_flood_vw`, …) and the shell command built from the `job` block of `config.json` — for `DOWNLOAD`: `cd ..;cd GEO_Download;conda run --no-capture-output -n geo_download_311 python run_download.py <source_name>`.
3. `geo_pipeline.py` delegates the conversion to `geo_dag.GEO_DAG`: it groups the specs by `source_name` into ten `geo_dag.JobsSubDAG`s (the nine source chains in `jobs.json` order, then the union chain), every subDAG creates one `BashOperator` per spec (`cwd` = the GEO_E2E directory, so the `cd ..;cd GEO_*` commands resolve the sibling projects, pool by job type, trigger rule from the spec) and wires the dependencies **inside** itself; `GEO_DAG.to_airflow()` then wires the dependencies **between** subDAGs — the union fan-in and the verdict task's edges. Tasks with `next_jobs` use the default `all_success` trigger rule; the union root uses `all_done_min_one_success`, the verdict `all_done`.
4. At run time the scheduler (LocalExecutor) executes each task as a subprocess inside the scheduler container: `conda` is on the image's `PATH` (symlink to the bind-mounted host conda), and the `geo_download_311` / `geo_etl_311` envs are found at their absolute host paths.
5. Concurrency is capped by pools: `DOWNLOAD` tasks go to the `download` pool (3 slots), every other job type to the `etl` pool (2 slots) — so at most 3 downloads and 2 Spark ETL jobs run at once.
6. Each task's stdout/stderr is captured as the Airflow task log (viewable in the web UI). On success **and** failure the memory callback appends `NIFC.download Memory: X out of Y GiB (Z %) used`.
7. If a task exits non-zero it is marked **failed**, and everything reachable through its `next_jobs` is **skipped** (`all_success` trigger rule) — the DAG semantics of the shipped `jobs.json`'s `is_next_skipped_if_error: true` (documentation only). Skipping is per branch: a failed plain-table `DATA_QUALITY` does not stop its sibling `SINGLE_ETL`.
8. A successful `INGESTION` releases both siblings: the plain-table `DATA_QUALITY` job and the `SINGLE_ETL` job (which itself chains a `DATA_QUALITY` job on the `_etl` table). The recursion continues to any depth, so extra steps can be appended to `jobs.json` without touching code.
9. A hanging task is killed after 30 minutes (`execution_timeout` in the DAG's `default_args`); tasks are never retried (`retries=0`).
10. The union root task (`Union.single_etl`) waits for the `SINGLE_ETL` task of all nine sources (`NIFC.single_etl`, … — the nine `*_etl` producers, 9 upstreams) with the `all_done_min_one_success` trigger rule, so it starts the moment the last single-ETL reached a terminal state, whatever the chains' results — but only if at least one of them succeeded (when all nine failed, the scheduler marks it `upstream_failed` without running it). The DATA_QUALITY jobs are terminal steps of `jobs.json` — nothing chains from them, like in the old Celery chains (where they simply ended the chain); only the verdict gate (next item) waits on them. It then runs `cd ..;cd GEO_ETL;conda run --no-capture-output -n geo_etl_311 python run_etl.py SINGLE_ETL union_etl`, stacking the nine `*_etl` tables into `geo.union_etl`. Chained under the same task (siblings of the union `DATA_QUALITY` job, `all_success`, `etl` pool) are the seventeen `VIEW` jobs — `run_etl.py VIEW <view_name>` — each (re)creating one view over the freshly stacked `geo.union_etl`.
11. The synthesized verdict task (`Union.verdict`) has all 64 registry tasks as upstreams and waits for them with `all_done`; its Jinja command then reads `Union.single_etl`'s state of the run via `ti.get_task_states()` (served by the supervisor through the execution API) and exits 0 only when it is `success`. It has no successors — as the DAG's only leaf it alone decides the run's state (see [Execution model](#execution-model)).
12. There are no completion markers, no polling and no deadline fallback — `logs/progress/`, `union.poll_seconds` and `union.max_attempts` are gone with the Celery setup.

## Execution model

The end-to-end diagram lives in the [README — Workflow](../README.md#workflow). At runtime Airflow sees 65 tasks of one DAG: 9 sources × 5 chain tasks + the union chain (`Union.single_etl`, its `Union_etl.data_quality` child and the seventeen `View.*` tasks) + the synthesized `Union.verdict` task, built from ten code-level subDAGs. A failed task shows up red in the grid, its descendants grey/skipped, and the union task still runs (`all_done_min_one_success`) as long as at least one single-ETL succeeded. The **run's** state comes from the leaf tasks only, and the verdict is the sole leaf: it exits 0 iff `Union.single_etl` succeeded, so the run succeeds with the union stack — a red `DATA_QUALITY`/`VIEW`/other branch does not fail the run, while a failed download or source single-ETL does (the union root gets `upstream_failed` → the verdict exits 1). Manual re-runs are done in the web UI (**Clear** the failed task), not via marker files.

## Failure handling

| Situation | Task state | Downstream tasks |
|-----------|------------|------------------|
| Command exits non-zero (e.g. `DOWNLOAD` wrote no CSV files, `INGESTION` found nothing new, ETL error) | FAILED (red in the UI) | every task chained below it is SKIPPED; sibling branches still run |
| Task exceeds `execution_timeout` (30 min) | FAILED (killed) | same as above |
| Config error — unknown `source_name`, a non-`DOWNLOAD` job without `table_name`, jobs.json without the `Union` chain or declaring a `VERDICT` job | DAG import error (visible in the UI under **Import Errors**) | DAG does not run at all |
| A source chain fails entirely | its tasks FAILED/SKIPPED | the union task still runs once every chain reached a terminal state — provided at least one single-ETL succeeded (`all_done_min_one_success`); with zero successes it is marked `upstream_failed` |
| Union job itself fails (`SINGLE_ETL union_etl` returns non-zero) | FAILED (red) | nothing downstream — the failure is visible and can be re-run from the UI |
| `Union.verdict` reads a non-`success` union state (failed, `upstream_failed`, or missing) | verdict FAILED → **the whole run is FAILED** | the other 64 tasks keep their own states — **Clear** just the verdict to re-rate the run after fixing the union task |

See [Configuration Reference](configuration.md) for the `jobs.json` field reference and the DAG settings.
