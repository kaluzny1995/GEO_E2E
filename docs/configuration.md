← [README](../README.md)

# Configuration Reference

All settings are in `config.json` (abridged — the `tasks` array holds one entry per source, see [Modifying the schedule](#modifying-the-schedule)):

```json
{
  "celery_app": {
    "name": "geo_download_app",
    "username": "geo_mq_admin",
    "password": "geo_mq_admin",
    "host": "localhost",
    "port": 5673,
    "celery_settings": {
      "task_time_limit": 1800,
      "task_soft_time_limit": 1500,
      "worker_max_tasks_per_child": 20,
      "worker_max_memory_per_child": 400000,
      "worker_prefetch_multiplier": 1,
      "task_acks_late": true
    },
    "tasks": [
      {
        "source_name": "USGS_Earthquake",
        "month": "*",
        "day": "*",
        "hour": 2,
        "minute": 0,
        "timezone": "Europe/Warsaw",
        "name": "geo_e2e_USGS_Earthquake"
      }
    ]
  },
  "job": {
    "download": {
      "folder_name": "GEO_Download",
      "python_script": "run_download.py",
      "conda_env": "geo_download_311"
    },
    "ingestion": {
      "folder_name": "GEO_ETL",
      "python_script": "run_etl.py",
      "conda_env": "geo_etl_311"
    }
  }
}
```

## Celery App Config

| Field      | Description                                              |
|------------|----------------------------------------------------------|
| `name`     | Celery application name                                  |
| `username` | RabbitMQ username                                         |
| `password` | RabbitMQ password                                         |
| `host`     | RabbitMQ host                                            |
| `port`     | RabbitMQ port (RabbitMQ container maps `5673` → `5672`)  |
| `tasks`    | List of scheduled job chains (one entry per data source)      |

### Celery Settings

Nested under `celery_app.celery_settings` (as set in `config.json`; fallback defaults come from `CelerySettings` in `model.py`):

| Field | Description | Config value |
|-------|-------------|--------------|
| `task_time_limit` | Hard kill after N seconds — covers the **whole chain** (model default `600`) | `1800` |
| `task_soft_time_limit` | Soft timeout (raises `SoftTimeLimitExceeded`) after N seconds (model default `480`) | `1500` |
| `worker_max_tasks_per_child` | Recycle the worker child after N tasks — clears leaked memory | `20` |
| `worker_max_memory_per_child` | Kill a worker child above N kilobytes | `400000` |
| `worker_prefetch_multiplier` | Only grab 1 task at a time so downloads don't pile up | `1` |
| `task_acks_late` | Requeue tasks if the worker crashes mid-download | `true` |

## Task Config

| Field         | Description                                                   |
|---------------|---------------------------------------------------------------|
| `source_name` | Must match a source key in `jobs.json` and in GEO_Download's `config.json` |
| `month`       | Cron month (`*` = every month, `1`-`12`)                      |
| `day`         | Cron day of month (`*` = every day, `1`-`31`)                 |
| `hour`        | Hour of day as an integer `0`-`23` in local timezone (converted to UTC automatically; `*` is not supported) |
| `minute`      | Minute (`0`-`59`)                                             |
| `timezone`    | IANA timezone string (e.g. `Europe/Warsaw`, `UTC`)            |
| `name`        | Unique Celery Beat task name, conventionally `geo_e2e_<source_name>` (e.g. `geo_e2e_NIFC`) |

## Job Config

The `job` block defines **how** each job type is executed (command, folder, conda env). It is referenced by every job in a chain:

| Block        | `folder_name`  | `python_script` | `conda_env`       |
|--------------|----------------|-----------------|-------------------|
| `download`   | `GEO_Download` | `run_download.py` | `geo_download_311` |
| `ingestion`  | `GEO_ETL`      | `run_etl.py`    | `geo_etl_311`     |

| Field           | Description                                                        |
|-----------------|--------------------------------------------------------------------|
| `folder_name`   | Name of the sibling project directory                              |
| `python_script` | Python script to execute (e.g. `run_download.py`)                  |
| `conda_env`     | Optional. Conda env to run in (`conda run --no-capture-output -n <env>`); each step runs in its own env, so the worker's env does not need PySpark or the downloader's dependencies |

## Job Chains (`jobs.json`)

The chain for each data source — which jobs run, in what order, and what happens on failure. Keyed by `source_name`, exactly like E2EReddit's `jobs.json`:

```json
{
  "NIFC": {
    "source_name": "NIFC",
    "job_type": "DOWNLOAD",
    "is_failed_if_error": false,
    "next_jobs": [
      {
        "source_name": "NIFC",
        "job_type": "INGESTION",
        "table_name": "nifc_fire",
        "is_failed_if_error": false,
        "next_jobs": []
      }
    ]
  }
}
```

| Field               | Description                                                                 |
|---------------------|-----------------------------------------------------------------------------|
| `source_name`       | Data source this job belongs to (must match the key and `config.json` task) |
| `job_type`          | `DOWNLOAD` or `INGESTION` — selects the matching `job.*` command block      |
| `table_name`        | Required for `INGESTION` — table passed to `run_etl.py INGESTION <table>`   |
| `is_failed_if_error`| `true` → non-zero return code raises `CalledProcessError` (Celery task FAILURE); `false` → warning is logged |
| `next_jobs`         | Jobs to run **after this job succeeded**, recursively (same list shape, nests to any depth) |

> A non-zero return code **stops the chain** in both cases — `next_jobs` never run after a failed job.

### Source → table mapping

| Source | Table | Source | Table |
|--------|-------|--------|-------|
| `Avalanche_ORG` | `avalanche_org` | `NIFC` | `nifc_fire` |
| `EONET` | `eonet_event` | `OpenMeteo_Flood` | `openmeteo_flood` |
| `GDACS` | `gdacs_event` | `USGS_Earthquake` | `usgs_earthquake` |
| `IBTrACS` | `ibtracs_cyclone` | `USGS_NWIS` | `usgs_nwis` |
| `USGS_Volcano` | `usgs_volcano` | | |

## Modifying the schedule

Edit `config.json` — change `hour` and `minute` in the task config. The schedule uses your local timezone (e.g. `Europe/Warsaw`), converted to UTC automatically. Restart the beat process after changes.

```json
{
  "tasks": [
    {
      "source_name": "USGS_Earthquake",
      "hour": 2,
      "minute": 0,
      "timezone": "Europe/Warsaw"
    }
  ]
}
```

### Adding a new data source

1. Add a task entry to `celery_app.tasks` in `config.json` (`source_name`, schedule, `name`).
2. Add a chain to `jobs.json` under the same `source_name` — `DOWNLOAD` plus a chained `INGESTION` job with its `table_name`.
3. Make sure the source exists in GEO_Download's `config.json` and that the table exists in GEO_ETL.
4. Restart the worker and beat.
