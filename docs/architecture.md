← [README](../README.md)

# Architecture & Project Layout

## Project Structure

```
GEO_E2E/
├── README.md             # Overview, setup and quick start
├── docker-compose.yaml   # RabbitMQ + Spark cluster services
├── .env                  # Machine-specific paths (not in git)
├── app.py                # Celery app definition, beat schedule, geo_e2e task
├── config.json           # Celery config (broker, tasks) and job config (commands)
├── jobs.json             # Job chains per data source: job_type -> next_jobs
├── model.py              # Pydantic models: CeleryAppConfig, CeleryTask, JobConfig, Job
├── util.py               # Logger setup, timezone conversion, beat cleanup, run_command
├── environment.yml       # Conda environment definition (Python + ruff/tox/mypy)
├── pyproject.toml        # Ruff rules, formatter style and pytest options
├── tox.ini               # lint / test / typecheck / format / warnings environments
├── docs/                 # Extended documentation (this file lives here)
├── logs/                 # Timestamped log files per source
│   └── USGS_Earthquake/
└── test/                 # Unit tests
    ├── __init__.py
    ├── test_app.py
    ├── test_jobs.py
    ├── test_models.py
    └── test_util.py
```

| File | Responsibility |
|------|----------------|
| `app.py` | Celery application, beat schedule built from `config.json`, the `geo_e2e` task |
| `model.py` | Pydantic models: `CeleryAppConfig`, `CeleryTask`, `JobConfig`, `JobTypeConfig`, `Job`, `EJobType` |
| `util.py` | `run_command()` (subprocess → log file), logger setup, timezone conversion, beat cleanup, memory logging |
| `config.json` | Broker credentials, per-source schedules, command templates for `DOWNLOAD`/`INGESTION`/`DATA_QUALITY` |
| `jobs.json` | Per-source job chains: `job_type` + `next_jobs` |

## How it works

1. Celery Beat triggers the `geo_e2e` task at the configured schedule (converted to UTC automatically); the beat entry is named `geo_e2e_<source_name>`.
2. The worker picks up the task from the RabbitMQ queue and loads the source's chain root from `jobs.json` (`Job.from_config(source_name)`).
3. `Job.run()` builds the shell command for the current `job_type` from the `job` block of `config.json` — for `DOWNLOAD`: `cd ../GEO_Download; conda run --no-capture-output -n geo_download_311 python run_download.py <source_name>`.
4. A subprocess executes the command, with output streamed directly to the log file (no in-memory buffering).
5. Output of the **whole chain** goes to `logs/<source>/celery_<source>_<timestamp>.log`.
6. If the job exits non-zero, its two flags in `jobs.json` decide what happens (checked in this order): `is_failed_if_error: true` → ERROR logged and `CalledProcessError` raised, Celery marks the task FAILURE; otherwise `is_stop_if_error: true` → WARNING logged and `next_jobs` skipped (task still SUCCESS); both `false` → WARNING logged and the chain continues. The shipped `jobs.json` sets `is_failed_if_error: false` + `is_stop_if_error: true` on every job, so a failure is a WARNING, the chain stops and the task still succeeds. A `DOWNLOAD` job raises `RuntimeError` when it saved no CSV files, which ends its subprocess with exit code `1` (see [GEO_Download — Exit codes](../../GEO_Download/docs/architecture.md#exit-codes)), so a lagging upstream feed stops the chain instead of triggering an ingestion of unchanged data.
7. If the job succeeded, `Job.run()` recursively runs its `next_jobs` — for `DOWNLOAD` that is `INGESTION`: `cd ../GEO_ETL; conda run --no-capture-output -n geo_etl_311 python run_etl.py INGESTION <table_name>`.
8. A successful `INGESTION` in turn runs the chained `DATA_QUALITY` job: `cd ../GEO_ETL; conda run --no-capture-output -n geo_etl_311 python run_etl.py DATA_QUALITY <table_name>` — quality metrics are therefore measured only on freshly ingested data. A failed ingestion skips it; `INGESTION` itself exits `1` when there was nothing new to ingest (no file newer than the table's `file_date` — see [GEO_ETL — ETL jobs](../../GEO_ETL/docs/etl-jobs.md#ingestion-job)), so an unchanged source stops the chain here too.
9. The recursion continues to any depth, so extra steps can be appended to `jobs.json` without touching code.
10. If a job hangs, it is killed after 30 minutes (`task_time_limit` in `config.json`) — the limit covers the whole chain.

## Execution model

The end-to-end diagram (config → beat → worker → download → ingestion → data quality) lives in the [README — Workflow](../README.md#workflow). At runtime the worker only ever sees one task: `geo_e2e(source_name)`, which expands to the chain from `jobs.json` and runs each step as a subprocess.

## Failure handling

| Job returns non-zero with… | Log | Celery task | `next_jobs` |
|-----------------------------|-----|-------------|-------------|
| `is_failed_if_error: true` | ERROR (the raise itself stops the chain) | FAILURE | never executed |
| `is_failed_if_error: false`, `is_stop_if_error: true` | WARNING `… — next jobs will not be executed` | SUCCESS | never executed |
| both flags `false` | WARNING `… — continuing with next jobs` | SUCCESS | executed |
| `DOWNLOAD` wrote no CSV files (upstream feed has nothing new) | WARNING `… — next jobs will not be executed` (shipped flags) | SUCCESS | never executed |
| `INGESTION` found nothing to ingest (table already up to date) | WARNING `… — next jobs will not be executed` (shipped flags) | SUCCESS | `DATA_QUALITY` never executed |
| Job exceeds `task_time_limit` | task killed after 30 min (covers the whole chain) | killed | never executed |

See [Configuration Reference](configuration.md) for the `jobs.json` field reference.
