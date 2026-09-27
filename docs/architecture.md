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
6. If the job exits non-zero, the chain **stops** — `next_jobs` are never executed. With `is_failed_if_error: false` a warning is logged; with `true` a `CalledProcessError` is raised and Celery marks the task as FAILURE.
7. If the job succeeded, `Job.run()` recursively runs its `next_jobs` — for `DOWNLOAD` that is `INGESTION`: `cd ../GEO_ETL; conda run --no-capture-output -n geo_etl_311 python run_etl.py INGESTION <table_name>`.
8. A successful `INGESTION` in turn runs the chained `DATA_QUALITY` job: `cd ../GEO_ETL; conda run --no-capture-output -n geo_etl_311 python run_etl.py DATA_QUALITY <table_name>` — quality metrics are therefore measured only on freshly ingested data. A failed ingestion skips it.
9. The recursion continues to any depth, so extra steps can be appended to `jobs.json` without touching code.
10. If a job hangs, it is killed after 30 minutes (`task_time_limit` in `config.json`) — the limit covers the whole chain.

## Execution model

The end-to-end diagram (config → beat → worker → download → ingestion → data quality) lives in the [README — Workflow](../README.md#workflow). At runtime the worker only ever sees one task: `geo_e2e(source_name)`, which expands to the chain from `jobs.json` and runs each step as a subprocess.

## Failure handling

| Situation | `is_failed_if_error: false` | `is_failed_if_error: true` |
|-----------|-----------------------------|----------------------------|
| Job returns non-zero | WARNING logged, chain stops | `CalledProcessError` raised → Celery task FAILURE |
| Chain stopped | `next_jobs` are never executed | `next_jobs` are never executed |
| Job exceeds `task_time_limit` | task killed after 30 min (whole chain) | task killed after 30 min (whole chain) |

See [Configuration Reference](configuration.md) for the `jobs.json` field reference.
