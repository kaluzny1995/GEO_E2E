← [README](../README.md)

# Architecture & Project Layout

## Project Structure

```
GEO_E2E/
├── README.md             # Overview, setup and quick start
├── docker-compose.yaml   # RabbitMQ + Spark cluster services
├── .env                  # Machine-specific paths (not in git)
├── .gitignore            # Excludes .env, logs/, __pycache__, .tox, IDE files
├── app.py                # Celery app definition, beat schedule, geo_e2e + union_etl tasks
├── config.json           # Celery config (broker, tasks, union) and job config (commands)
├── jobs.json             # Job chains per data source: job_type -> next_jobs
├── model.py              # Pydantic models: CeleryAppConfig, CeleryTask, UnionTask, JobConfig, Job
├── util.py               # Logger setup, timezone conversion, beat cleanup, run_command, completion markers
├── environment.yml       # Conda environment definition (Python + ruff/tox/mypy)
├── pyproject.toml        # Ruff rules, formatter style and pytest options
├── tox.ini               # lint / test / typecheck / format / warnings environments
├── docs/                 # Extended documentation (this file lives here)
├── logs/                 # Timestamped log files per source + union progress markers
│   ├── USGS_Earthquake/
│   ├── Union/
│   └── progress/<date>/  # completion markers (one file per finished source)
└── test/                 # Unit tests
    ├── __init__.py
    ├── test_app.py
    ├── test_jobs.py
    ├── test_models.py
    └── test_util.py
```

| File | Responsibility |
|------|----------------|
| `app.py` | Celery application, beat schedule built from `config.json`, the `geo_e2e` and `union_etl` tasks |
| `model.py` | Pydantic models: `CeleryAppConfig`, `CeleryTask`, `UnionTask`, `CelerySettings`, `JobConfig`, `JobTypeConfig`, `Job`, `EJobType`, plus `get_job_config()` |
| `util.py` | `run_command()` (subprocess → log file), logger setup, timezone conversion, beat cleanup, memory logging, union completion markers |
| `config.json` | Broker credentials, per-source schedules, the union fan-in schedule, command templates for `DOWNLOAD`/`INGESTION`/`DATA_QUALITY`/`SINGLE_ETL` |
| `jobs.json` | Per-source job chains plus the `Union` job: `job_type` + `next_jobs` |

## How it works

1. Celery Beat triggers the `geo_e2e` task at the configured schedule (converted to UTC automatically); the beat entry is named `geo_e2e_<source_name>`.
2. The worker picks up the task from the RabbitMQ queue and loads the source's chain root from `jobs.json` (`Job.from_config(source_name)`).
3. `Job.run()` builds the shell command for the current `job_type` from the `job` block of `config.json` — for `DOWNLOAD`: `cd ..;cd GEO_Download;conda run --no-capture-output -n geo_download_311 python run_download.py <source_name>`.
4. A subprocess executes the command, with output streamed directly to the log file (no in-memory buffering).
5. Output of the **whole chain** goes to `logs/<source>/celery_<source>_<timestamp>.log`.
6. If the job exits non-zero, its two flags in `jobs.json` decide what happens (checked in this order): `is_failed_if_error: true` → ERROR logged and `CalledProcessError` raised, Celery marks the task FAILURE; otherwise `is_stop_if_error: true` → WARNING logged and `next_jobs` skipped (task still SUCCESS); both `false` → WARNING logged and the chain continues. The shipped `jobs.json` sets `is_failed_if_error: false` + `is_stop_if_error: true` on every job, so a failure is a WARNING, the chain stops and the task still succeeds. A `DOWNLOAD` job raises `RuntimeError` when it saved no CSV files, which ends its subprocess with exit code `1` (see [GEO_Download — Exit codes](../../GEO_Download/docs/architecture.md#exit-codes)), so a lagging upstream feed stops the chain instead of triggering an ingestion of unchanged data.
7. If the job succeeded, `Job.run()` recursively runs its `next_jobs` — for `DOWNLOAD` that is `INGESTION`: `cd ..;cd GEO_ETL;conda run --no-capture-output -n geo_etl_311 python run_etl.py INGESTION <table_name>`.
8. A successful `INGESTION` runs its two `next_jobs` in order: the chained `DATA_QUALITY` job on the plain table — `cd ..;cd GEO_ETL;conda run --no-capture-output -n geo_etl_311 python run_etl.py DATA_QUALITY <table_name>` — quality metrics are therefore measured only on freshly ingested data; a failed ingestion skips it (`INGESTION` itself exits `1` when there was nothing new to ingest, no file newer than the table's `file_date` — see [GEO_ETL — Ingestion job](../../GEO_ETL/docs/etl_jobs/ingestion.md), so an unchanged source stops the chain here too). Because the two jobs are siblings, a failed plain-table `DATA_QUALITY` only ends its own branch — `SINGLE_ETL` still runs.
9. The second `next_jobs` entry is `SINGLE_ETL`: `cd ..;cd GEO_ETL;conda run --no-capture-output -n geo_etl_311 python run_etl.py SINGLE_ETL <table_name>_etl` — it rebuilds the analytics-ready `_etl` table from the plain table (auto `HISTORICAL` / `INCREMENTAL` detection inside GEO_ETL, see [GEO_ETL — Single ETL job](../../GEO_ETL/docs/etl_jobs/single-etl.md)).
10. `SINGLE_ETL` in turn chains a `DATA_QUALITY` job on `<table_name>_etl`, so the etl table gets its own per-`file_date` metrics only after it was rebuilt.
11. The recursion continues to any depth, so extra steps can be appended to `jobs.json` without touching code.
12. If a job hangs, it is killed after 30 minutes (`task_time_limit` in `config.json`) — the limit covers the whole chain.
13. When a chain ends — success, failure or config error — `geo_e2e` writes a completion marker `logs/progress/<date>/<source_name>` in a `finally` block, so the result never matters to the fan-in (a task killed by `task_time_limit` never reaches it and is covered by step 15's deadline).
14. Beat triggers `union_etl` after the source window (the `union` block in `config.json` — 04:15 Europe/Warsaw, after the last source starts at 04:00). The task first removes marker directories of previous days, then compares today's markers against the sources listed in `tasks`.
15. While any source is pending the task logs which ones and re-queues itself (`union_etl.apply_async(countdown=poll_seconds)`), up to `max_attempts` times; after the deadline it logs `Deadline reached — … running the union job anyway` and proceeds regardless, so one stuck or missing source cannot block the daily union.
16. With every source finished (or at the deadline) it runs the `Union` chain from `jobs.json` — `cd ..;cd GEO_ETL;conda run --no-capture-output -n geo_etl_311 python run_etl.py SINGLE_ETL union_etl` — stacking the nine `*_etl` tables into `geo.union_etl` (GEO_ETL exits `1` when there is nothing newer to stack; the shipped flags turn that into a WARNING with a SUCCESS task).

## Execution model

The end-to-end diagram (config → beat → worker → download → ingestion → data quality / single ETL → etl data quality) lives in the [README — Workflow](../README.md#workflow). At runtime the worker only ever sees two tasks: `geo_e2e(source_name)`, which expands to the chain from `jobs.json` and runs each step as a subprocess, and `union_etl(attempt)`, the fan-in that waits for the completion markers and then runs the `Union` chain.

## Failure handling

| Job returns non-zero with… | Log | Celery task | `next_jobs` |
|-----------------------------|-----|-------------|-------------|
| `is_failed_if_error: true` | ERROR (the raise itself stops the chain) | FAILURE | never executed |
| `is_failed_if_error: false`, `is_stop_if_error: true` | WARNING `… — next jobs will not be executed` | SUCCESS | never executed (sibling jobs still run) |
| both flags `false` | WARNING `… — continuing with next jobs` | SUCCESS | executed |
| `DOWNLOAD` wrote no CSV files (upstream feed has nothing new) | WARNING `… — next jobs will not be executed` (shipped flags) | SUCCESS | never executed |
| `INGESTION` found nothing to ingest (table already up to date) | WARNING `… — next jobs will not be executed` (shipped flags) | SUCCESS | plain-table `DATA_QUALITY` and `SINGLE_ETL` never executed |
| plain-table `DATA_QUALITY` fails | WARNING `… — next jobs will not be executed` (shipped flags) | SUCCESS | that branch ends; sibling `SINGLE_ETL` and its etl-table `DATA_QUALITY` still executed |
| `SINGLE_ETL` fails | WARNING `… — next jobs will not be executed` (shipped flags) | SUCCESS | etl-table `DATA_QUALITY` never executed |
| Config error — unknown `source_name` in `jobs.json`, or an `INGESTION`/`DATA_QUALITY`/`SINGLE_ETL` job without `table_name` | traceback raised by `Job.from_config()` / `Job.get_command()` before any subprocess runs | FAILURE | chain never starts (the two failure flags do not apply) |
| Job exceeds `task_time_limit` | task killed after 30 min (covers the whole chain) | killed | never executed |

See [Configuration Reference](configuration.md) for the `jobs.json` field reference.

**Union fan-in:** a pending source never fails the `union_etl` task — it logs `Waiting for N source chain(s) to finish: …` at INFO and re-queues itself with a `poll_seconds` countdown, and after `max_attempts` it logs the `Deadline reached — …` warning and runs the union job anyway. A chain killed by `task_time_limit` simply misses its marker and lands in that deadline path, and a non-zero `Union` job follows the same two flags as every other job (shipped: WARNING, task SUCCESS).
