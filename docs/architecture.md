← [README](../README.md)

# Architecture & Project Layout

## Project Structure

```
GEO_E2E/
├── README.md                 # Overview, setup and quick start
├── docker-compose.yaml       # Airflow stack (Postgres, init, scheduler, API server, DAG processor) + Spark cluster
├── .env.example              # Template for the machine-specific .env (committed)
├── .env                      # Machine-specific paths (not in git)
├── .gitignore                # Excludes .env, logs/, __pycache__, .tox, IDE files
├── config.json               # Job config (command templates per job type)
├── jobs.json                 # Flat job registry: one object per job, next_jobs = name references
├── model.py                  # Pydantic models: JobConfig, Job, TaskSpec, build_task_specs()
├── util.py                   # Memory logging helpers (Airflow task callbacks)
├── environment.yml           # Conda environment definition (Python, ruff/tox/mypy, pinned Airflow for dags/)
├── pyproject.toml            # Ruff rules, formatter style, mypy files and pytest options
├── tox.ini                   # lint / test / typecheck / format / warnings environments
├── dags/
│   └── geo_pipeline.py       # The Airflow DAG — maps TaskSpecs to BashOperators
├── docker/
│   └── airflow/
│       ├── Dockerfile        # apache/airflow:3.3.2 + providers-standard, psutil, pydantic, conda symlink
│       └── seed_pools.py     # seeds the Airflow pools from config.json (dag.pool_slots) on startup
├── docs/                     # Extended documentation (this file lives here)
└── test/                     # Unit tests
    ├── __init__.py
    ├── test_dags.py          # build_task_specs(): task ids, wiring, union fan-in
    ├── test_jobs.py          # Job chains from jobs.json + command building
    ├── test_models.py        # JobTypeConfig, JobConfig, get_job_config()
    └── test_util.py          # memory helpers
```

| File | Responsibility |
|------|----------------|
| `dags/geo_pipeline.py` | The `geo_pipeline` DAG: builds one `BashOperator` per `TaskSpec` and wires the upstream dependencies; the cron comes from the scheduled jobs of `jobs.json`, the runtime knobs (retries, timeout, pools) from the `dag` block of `config.json` |
| `model.py` | Pydantic models: `JobTypeConfig`, `JobConfig`, `JobTypeConfig`, `Job`, `EJobType`, `TaskSpec`, plus `get_job_config()`, `load_chains()` and `build_task_specs()` |
| `util.py` | `get_memory_info()` and `log_memory_usage()` — used as `on_success_callback` / `on_failure_callback` of every task |
| `config.json` | Command templates for `DOWNLOAD`/`INGESTION`/`DATA_QUALITY`/`SINGLE_ETL` (folder, script, conda env) |
| `jobs.json` | Flat job list — one object per job (`name`, `job_type`, `next_jobs` as name references) — plus the `Union` job |
| `docker-compose.yaml` | Airflow services (`network_mode: host`, Postgres on host port `5472`, UI on `8090`) plus the Spark cluster |
| `docker/airflow/Dockerfile` | Extends `apache/airflow:3.3.2-python3.11` with `apache-airflow-providers-standard`, `psutil`, `pydantic` (pinned to the Airflow constraints) and a `conda` symlink to the host conda (bind-mounted at runtime) |

## How it works

1. The Airflow scheduler triggers the `geo_pipeline` DAG on the cron declared by the **scheduled jobs** in `jobs.json` (the nine `DOWNLOAD` roots, all `0 2 * * *` — `model.resolve_schedule()` requires them to agree, since Airflow schedules whole DAGs; `catchup`/`max_active_runs`/timezone come from the `dag` block of `config.json`). Downstreamed jobs (no `schedule`) start when their parent finishes. The DAG is created unpaused on first parse (`dags_are_paused_at_creation=false`), and the `download`/`etl` pools are seeded by `airflow-init` from `dag.pool_slots` (`docker/airflow/seed_pools.py`) on every `docker compose up`.
2. At DAG-parse time, `dags/geo_pipeline.py` calls `model.build_task_specs()`, which loads every chain from `jobs.json` and flattens it into `TaskSpec`s — one per job, with the task id taken from the job-type path inside the chain (`NIFC.download`, `NIFC.ingestion`, `NIFC.ingestion.single_etl.data_quality`, …) and the shell command built from the `job` block of `config.json` — for `DOWNLOAD`: `cd ..;cd GEO_Download;conda run --no-capture-output -n geo_download_311 python run_download.py <source_name>`.
3. The DAG file maps every spec to a `BashOperator` (`cwd` = the GEO_E2E directory, so the `cd ..;cd GEO_*` commands resolve the sibling projects) and wires each spec's `upstream` task ids via `set_upstream`. Tasks with `next_jobs` use the default `all_success` trigger rule; the union root uses `all_done_min_one_success`.
4. At run time the scheduler (LocalExecutor) executes each task as a subprocess inside the scheduler container: `conda` is on the image's `PATH` (symlink to the bind-mounted host conda), and the `geo_download_311` / `geo_etl_311` envs are found at their absolute host paths.
5. Concurrency is capped by pools: `DOWNLOAD` tasks go to the `download` pool (3 slots), every other job type to the `etl` pool (2 slots) — so at most 3 downloads and 2 Spark ETL jobs run at once.
6. Each task's stdout/stderr is captured as the Airflow task log (viewable in the web UI). On success **and** failure the memory callback appends `NIFC.download Memory: X out of Y GiB (Z %) used`.
7. If a task exits non-zero it is marked **failed**, and everything reachable through its `next_jobs` is **skipped** (`all_success` trigger rule) — the DAG semantics of the shipped `jobs.json`'s `is_next_skipped_if_error: true` (documentation only). Skipping is per branch: a failed plain-table `DATA_QUALITY` does not stop its sibling `SINGLE_ETL`.
8. A successful `INGESTION` releases both siblings: the plain-table `DATA_QUALITY` job and the `SINGLE_ETL` job (which itself chains a `DATA_QUALITY` job on the `_etl` table). The recursion continues to any depth, so extra steps can be appended to `jobs.json` without touching code.
9. A hanging task is killed after 30 minutes (`execution_timeout` in the DAG's `default_args`); tasks are never retried (`retries=0`).
10. The union root task (`Union.single_etl`) waits for the `ingestion.single_etl` task of all nine sources — the nine `*_etl` producers, 9 upstreams — with the `all_done_min_one_success` trigger rule, so it starts the moment the last single-ETL reached a terminal state, whatever the chains' results — but only if at least one of them succeeded (when all nine failed, the scheduler marks it `upstream_failed` without running it). The DATA_QUALITY jobs are terminal steps with no successors, like in the old Celery chains (where they simply ended the chain). It then runs `cd ..;cd GEO_ETL;conda run --no-capture-output -n geo_etl_311 python run_etl.py SINGLE_ETL union_etl`, stacking the nine `*_etl` tables into `geo.union_etl`.
11. There are no completion markers, no polling and no deadline fallback — `logs/progress/`, `union.poll_seconds` and `union.max_attempts` are gone with the Celery setup.

## Execution model

The end-to-end diagram lives in the [README — Workflow](../README.md#workflow). At runtime Airflow sees 46 tasks of one DAG: 9 sources × 5 chain tasks + 1 union fan-in task. A failed task shows up red in the grid, its descendants grey/skipped, and the union task still runs (`all_done_min_one_success`) as long as at least one single-ETL succeeded. Manual re-runs are done in the web UI (**Clear** the failed task), not via marker files.

## Failure handling

| Situation | Task state | Downstream tasks |
|-----------|------------|------------------|
| Command exits non-zero (e.g. `DOWNLOAD` wrote no CSV files, `INGESTION` found nothing new, ETL error) | FAILED (red in the UI) | every task chained below it is SKIPPED; sibling branches still run |
| Task exceeds `execution_timeout` (30 min) | FAILED (killed) | same as above |
| Config error — unknown `source_name`, an `INGESTION`/`DATA_QUALITY`/`SINGLE_ETL` job without `table_name`, jobs.json without the `Union` chain | DAG import error (visible in the UI under **Import Errors**) | DAG does not run at all |
| A source chain fails entirely | its tasks FAILED/SKIPPED | the union task still runs once every chain reached a terminal state — provided at least one single-ETL succeeded (`all_done_min_one_success`); with zero successes it is marked `upstream_failed` |
| Union job itself fails (`SINGLE_ETL union_etl` returns non-zero) | FAILED (red) | nothing downstream — the failure is visible and can be re-run from the UI |

See [Configuration Reference](configuration.md) for the `jobs.json` field reference and the DAG settings.
