← [README](../README.md)

# Configuration Reference

`config.json` defines **how** each job type is executed; `jobs.json` defines **which** jobs each source runs and how they depend on each other. Schedule, pools and timeouts live in the DAG file `dags/geo_pipeline.py`.

## Job Config (`config.json`)

```json
{
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
    },
    "data_quality": {
      "folder_name": "GEO_ETL",
      "python_script": "run_etl.py",
      "conda_env": "geo_etl_311"
    },
    "single_etl": {
      "folder_name": "GEO_ETL",
      "python_script": "run_etl.py",
      "conda_env": "geo_etl_311"
    }
  }
}
```

The `job` block defines **how** each job type is executed (command, folder, conda env). It is referenced by every job in a chain:

| Block         | `folder_name`  | `python_script` | `conda_env`       |
|---------------|----------------|-----------------|-------------------|
| `download`    | `GEO_Download` | `run_download.py` | `geo_download_311` |
| `ingestion`   | `GEO_ETL`      | `run_etl.py`    | `geo_etl_311`     |
| `data_quality` | `GEO_ETL`      | `run_etl.py`    | `geo_etl_311`     |
| `single_etl`  | `GEO_ETL`      | `run_etl.py`    | `geo_etl_311`     |

| Field           | Description                                                        |
|-----------------|--------------------------------------------------------------------|
| `folder_name`   | Name of the sibling project directory                              |
| `python_script` | Python script to execute (e.g. `run_download.py`)                  |
| `conda_env`     | Optional. Conda env to run in (`conda run --no-capture-output -n <env>`); each sibling project runs in its own env (`GEO_Download` → `geo_download_311`, every `run_etl.py` step → `geo_etl_311`), so the Airflow image does not need PySpark or the downloader's dependencies |

## Job Chains (`jobs.json`)

The chain for each data source — which jobs run, in what order, and what depends on what. Keyed by `source_name`, exactly like E2EReddit's `jobs.json`:

```json
{
  "NIFC": {
    "source_name": "NIFC",
    "job_type": "DOWNLOAD",
    "is_failed_if_error": false,
    "is_stop_if_error": true,
    "next_jobs": [
      {
        "source_name": "NIFC",
        "job_type": "INGESTION",
        "table_name": "nifc_fire",
        "is_failed_if_error": false,
        "is_stop_if_error": true,
        "next_jobs": [
          {
            "source_name": "NIFC",
            "job_type": "DATA_QUALITY",
            "table_name": "nifc_fire",
            "is_failed_if_error": false,
            "is_stop_if_error": true,
            "next_jobs": []
          },
          {
            "source_name": "NIFC",
            "job_type": "SINGLE_ETL",
            "table_name": "nifc_fire_etl",
            "is_failed_if_error": false,
            "is_stop_if_error": true,
            "next_jobs": [
              {
                "source_name": "NIFC",
                "job_type": "DATA_QUALITY",
                "table_name": "nifc_fire_etl",
                "is_failed_if_error": false,
                "is_stop_if_error": true,
                "next_jobs": []
              }
            ]
          }
        ]
      }
    ]
  }
}
```

| Field               | Description                                                                       |
|---------------------|-----------------------------------------------------------------------------------|
| `source_name`       | Data source this job belongs to (must match the key)                              |
| `job_type`          | `DOWNLOAD`, `INGESTION`, `DATA_QUALITY` or `SINGLE_ETL` — selects the matching `job.*` command block |
| `table_name`        | Required for `INGESTION`, `DATA_QUALITY` and `SINGLE_ETL` — argument passed to `run_etl.py <JOB_TYPE> <table>` (plain table name for `INGESTION`/`DATA_QUALITY`, `<table>_etl` for `SINGLE_ETL`; GEO_ETL adds `_raw` itself only for `INGESTION`) |
| `is_failed_if_error`| **Documentation only** (kept from the Celery setup) — not consulted by the DAG   |
| `is_stop_if_error`  | **Documentation only** (kept from the Celery setup) — the DAG always uses the `all_success` trigger rule below, which skips a failed job's `next_jobs` |
| `next_jobs`         | Jobs that depend on this job — turned into downstream Airflow tasks (nests to any depth) |

> Every non-union job waits for its parent with Airflow's default `all_success` trigger rule: a failed parent skips everything chained below it (grey/skipped in the UI), while sibling branches still run — e.g. a failed plain-table `DATA_QUALITY` does not stop its sibling `SINGLE_ETL`. The shipped `jobs.json` sets `is_failed_if_error: false` + `is_stop_if_error: true` on every job to document that this is the intended behavior; the flags themselves are no longer read at runtime.

### Task ids

Each job becomes one Airflow task whose id follows the job-type path inside the chain (the `DOWNLOAD` prefix is dropped after the root):

| Job | Task id |
|-----|---------|
| chain root `DOWNLOAD` | `NIFC.download` |
| `INGESTION` under it | `NIFC.ingestion` |
| plain-table `DATA_QUALITY` | `NIFC.ingestion.data_quality` |
| `SINGLE_ETL` | `NIFC.ingestion.single_etl` |
| etl-table `DATA_QUALITY` | `NIFC.ingestion.single_etl.data_quality` |
| union fan-in | `Union.single_etl` |

### Union job (`Union`)

Besides the nine source chains, `jobs.json` holds a bare `Union` entry — the fan-in job that stacks every `*_etl` table into `geo.union_etl`:

```json
"Union": {
  "source_name": "Union",
  "job_type": "SINGLE_ETL",
  "table_name": "union_etl",
  "is_failed_if_error": false,
  "is_stop_if_error": true,
  "next_jobs": []
}
```

`SINGLE_ETL union_etl` stacks the nine `*_etl` tables into `geo.union_etl` (see [GEO_ETL — union_etl](../../GEO_ETL/docs/etl_jobs/single-etl.md#union_etl)); a non-zero exit — e.g. nothing newer to stack — fails the task (visible in the UI, re-runnable). The DAG gives the union root the `all_done` trigger rule and the nine `ingestion.single_etl` tasks as upstreams, so it starts as soon as the last single-ETL finished, whatever the results — no markers, no polling, no deadline. The DATA_QUALITY jobs are not part of the fan-in: they stay terminal steps with no successors, like in the old Celery chains. A `DATA_QUALITY union_etl` step can be chained via `next_jobs` to measure the union table after it was rebuilt.

### Data quality step

Every table in the mapping below has **two** `DATA_QUALITY` jobs: one chained directly under its `INGESTION` job (measuring the plain table), and one under the `SINGLE_ETL` job (measuring the `<table>_etl` table). The plain-table metrics are therefore computed **only after the ingestion for that table finished successfully** — a failed or skipped ingestion means `run_etl.py DATA_QUALITY <table>` never runs (the metrics would otherwise be measured on stale data), and the etl-table metrics only after the `_etl` table was rebuilt. Since each job is the last step of its branch, a failure there stops that branch without affecting anything downstream.

Both jobs run in the same conda env and folder as ingestion (see `job.data_quality` in `config.json`); GEO_ETL additionally writes its own `logs/data_quality/data_quality_<table>_<timestamp>.log` per run (see [Usage — Logs](usage.md#logs)). GEO_ETL picks its load type (`HISTORICAL` / `INCREMENTAL`) from the state of `geo.data_quality` — see [GEO_ETL — Data Quality job](../../GEO_ETL/docs/etl_jobs/data-quality.md).

### Single ETL step

`SINGLE_ETL` is chained under the same `INGESTION` job as the plain-table `DATA_QUALITY` job (its sibling), with `table_name` set to the etl script name `<table>_etl`. It rebuilds `geo.<table>_etl` from the plain table and chains its own `DATA_QUALITY` job on that etl table — see [GEO_ETL — Single ETL job](../../GEO_ETL/docs/etl_jobs/single-etl.md).

### Source → table mapping

| Source | Table | Source | Table |
|--------|-------|--------|-------|
| `Avalanche_ORG` | `avalanche_org` | `NIFC` | `nifc_fire` |
| `EONET` | `eonet_event` | `OpenMeteo_Flood` | `openmeteo_flood` |
| `GDACS` | `gdacs_event` | `USGS_Earthquake` | `usgs_earthquake` |
| `IBTrACS` | `ibtracs_cyclone` | `USGS_NWIS` | `usgs_nwis` |
| `USGS_Volcano` | `usgs_volcano` | | |

Each source additionally uses the etl table `<table>_etl` (e.g. `nifc_fire_etl`) for its `SINGLE_ETL` job and the `DATA_QUALITY` job chained under it.

## Constructing the DAG

You never write Airflow code to add or reshape work — you edit `jobs.json`. On every parse, `model.build_task_specs()` flattens the chains into one task spec per job, and `dags/geo_pipeline.py` turns each spec into an operator:

1. **`jobs.json`** — the source of truth: which jobs a source runs, and which job waits on which (nesting inside `next_jobs`).
2. **`build_task_specs()`** — produces one `TaskSpec` per job: the task id (job-type path), the shell command (from `config.json`), the upstream list (the parent job) and the trigger rule (`all_success`; `all_done` only for the union root).
3. **`dags/geo_pipeline.py`** — a `BashOperator` per spec (pool by job type, 30-minute timeout, no retries, memory callbacks) plus `set_upstream()` for every entry in `upstream`.

### Placing a job

- Each top-level key of `jobs.json` is one chain; its root job is the source's `DOWNLOAD` — the only job whose command argument is the `source_name` instead of a `table_name`.
- A job's dependents go into its `next_jobs`, nested to any depth. `next_jobs` order is irrelevant: jobs sharing a parent are siblings and run in parallel (throttled by the pools).
- `job_type` selects the command template from the `job` block of `config.json`; `INGESTION`, `DATA_QUALITY` and `SINGLE_ETL` additionally need `table_name` (passed as the script argument).
- Chains are independent of each other: a job can only depend on its own ancestors. The one cross-chain dependency is the automatic union fan-in below.

> **Task ids must stay unique along each path.** The id is the job-type path inside the chain (DOWNLOAD dropped after the root — see [Task ids](#task-ids)), so two sibling jobs of the same type under one parent would collide (e.g. two `DATA_QUALITY` jobs directly under one `INGESTION` both become `<source>.ingestion.data_quality`). The DAG keeps only one of them — re-check the ids when reshaping a chain; the generation tests in `test/test_dags.py` assert uniqueness.

### Creating dependencies

| You want | Do |
|----------|-----|
| B runs after A, and is skipped when A fails | Nest B inside A's `next_jobs` — B waits for A with `all_success` |
| Two jobs run in parallel | Put them as siblings in the same `next_jobs` list |
| A job runs after a whole branch | Chain it under the branch's last job (nesting depth is free) |
| A job waits for a job of another source | Not possible per-job — only the union root fans in across chains |

Failure semantics follow the trigger rules: a failed (or skipped) job skips everything nested below it (grey/skipped in the UI) while its siblings still run — e.g. a failed plain-table `DATA_QUALITY` never stops the sibling `SINGLE_ETL`. Nothing retries (`retries: 0`); failures stay visible and are re-run manually from the UI.

### The union fan-in (the one special chain)

The `Union` chain is not a normal source. Its root (`Union.single_etl`) is the only task with the `all_done` trigger rule, and `build_task_specs()` gives it — automatically — the `ingestion.single_etl` task of every other chain as upstreams:

- Adding or removing a source chain needs no edit to `Union`; the fan-in follows the chains in `jobs.json`.
- The union starts as soon as the last single-ETL reaches a terminal state, whatever the results. The `DATA_QUALITY` jobs are terminal steps with no successors and are deliberately not part of the fan-in.
- Jobs nested under the union root (none today) wait for it with the normal `all_success` rule.
- `jobs.json` must always contain a `Union` chain — `build_task_specs()` raises `ValueError` at parse time otherwise, which surfaces as a DAG import error in the UI until the chain is back.

### Worked example: an extra step under the union

To measure the union table after it was rebuilt, chain a `DATA_QUALITY` job on `union_etl` under the union root:

```json
"Union": {
  "source_name": "Union",
  "job_type": "SINGLE_ETL",
  "table_name": "union_etl",
  "is_failed_if_error": false,
  "is_stop_if_error": true,
  "next_jobs": [
    {
      "source_name": "Union",
      "job_type": "DATA_QUALITY",
      "table_name": "union_etl",
      "is_failed_if_error": false,
      "is_stop_if_error": true,
      "next_jobs": []
    }
  ]
}
```

This adds the task `Union.single_etl.data_quality`, waits for `Union.single_etl` with `all_success` and runs in the `etl` pool — no change to `dags/geo_pipeline.py` or `model.py` needed.

### Validating a change

1. `conda run --no-capture-output -n geo_e2e_311 pytest` — the DAG generation tests (`test/test_dags.py`) assert task counts, ids, wiring, trigger rules and commands.
2. Save `jobs.json` — the DAG processor re-parses within ~30 seconds (or refresh via the UI); a valid file leaves `dags list-import-errors` empty.
3. Open the **Graph** view of a manual run — the new tasks and edges show up where you nested them.

## DAG settings (`dags/geo_pipeline.py`)

| Setting | Value | Description |
|---------|-------|-------------|
| `schedule` | `0 2 * * *` | Daily at 02:00 |
| `start_date` / timezone | 2026-01-01, `Europe/Warsaw` | Schedule timezone (the old Celery staggering of 15 minutes per source is gone — all downloads start at 02:00) |
| `catchup` | `False` | No backfill of missed runs |
| `max_active_runs` | `1` | One pipeline run at a time |
| `retries` | `0` | No automatic retries (parity with Celery; failures show up red instead of silently re-running) |
| `execution_timeout` | 30 min | Per task (parity with the old `task_time_limit`) |
| pool `download` | 3 slots | All `DOWNLOAD` tasks |
| pool `etl` | 2 slots | All `INGESTION` / `DATA_QUALITY` / `SINGLE_ETL` tasks |

The pools are seeded automatically by `airflow-init` on every `docker compose up` (`airflow pools set download 3 0` / `airflow pools set etl 2 0` — idempotent), so no UI setup is needed; you can still inspect or change them under **Admin → Pools** or via `docker compose run --rm airflow-cli pools list`. The DAG itself is created unpaused (`dags_are_paused_at_creation=false` in `docker-compose.yaml`).

## Modifying the schedule

Edit the `schedule` argument of the `@dag` decorator in `dags/geo_pipeline.py` (cron syntax, evaluated in `Europe/Warsaw`). The DAG processor picks the change up automatically within ~30 seconds — no restart needed.

### Adding a new data source

1. Add a chain to `jobs.json` under the new `source_name` — `DOWNLOAD`, then a chained `INGESTION` job with its `table_name`, whose `next_jobs` hold a `DATA_QUALITY` job on the same table and a `SINGLE_ETL` job on `<table_name>_etl` (itself chaining a `DATA_QUALITY` job on `<table_name>_etl`). The placement and dependency rules are spelled out in [Constructing the DAG](#constructing-the-dag).
2. Make sure the source exists in GEO_Download's `config.json` and that the table exists in GEO_ETL.
3. Wait for the DAG to re-parse (or trigger a refresh in the UI) — the new tasks appear automatically.

The union fan-in needs no change: `build_task_specs()` gives the union root the `ingestion.single_etl` task of **every** non-union chain as upstreams, so the new source is covered automatically.
