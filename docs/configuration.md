← [README](../README.md)

# Configuration Reference

`config.json` defines **how** each job type is executed (the `job` block) and holds the DAG-level runtime knobs (the `dag` block: retries, pools, ...); `jobs.json` defines **which** jobs each source runs, how they depend on each other, and which of them are scheduled (each scheduled job carries its own `schedule`). The DAG file `geo_pipeline.py` only turns those configs into Airflow tasks.

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

## Job Registry (`jobs.json`)

The flat registry of every job the pipeline runs — a JSON **array** of job objects, one per job, keyed relationships via `name` instead of nesting. Each source contributes a chain of five jobs (`DOWNLOAD` → `INGESTION` → `DATA_QUALITY` + `SINGLE_ETL` → etl-table `DATA_QUALITY`); the `Union` job stacks the results. The first source's jobs:

```json
[
  {
    "name": "DOWNLOAD-USGS_Earthquake",
    "source_name": "USGS_Earthquake",
    "job_type": "DOWNLOAD",
    "schedule": "0 2 * * *",
    "is_next_skipped_if_error": true,
    "next_jobs": ["INGESTION-usgs_earthquake"]
  },
  {
    "name": "INGESTION-usgs_earthquake",
    "source_name": "USGS_Earthquake",
    "job_type": "INGESTION",
    "table_name": "usgs_earthquake",
    "is_next_skipped_if_error": true,
    "next_jobs": ["DATA_QUALITY-usgs_earthquake", "SINGLE_ETL-usgs_earthquake_etl"]
  },
  {
    "name": "DATA_QUALITY-usgs_earthquake",
    "source_name": "USGS_Earthquake",
    "job_type": "DATA_QUALITY",
    "table_name": "usgs_earthquake",
    "is_next_skipped_if_error": true,
    "next_jobs": []
  },
  {
    "name": "SINGLE_ETL-usgs_earthquake_etl",
    "source_name": "USGS_Earthquake",
    "job_type": "SINGLE_ETL",
    "table_name": "usgs_earthquake_etl",
    "is_next_skipped_if_error": true,
    "next_jobs": ["DATA_QUALITY-usgs_earthquake_etl"]
  },
  {
    "name": "DATA_QUALITY-usgs_earthquake_etl",
    "source_name": "USGS_Earthquake",
    "job_type": "DATA_QUALITY",
    "table_name": "usgs_earthquake_etl",
    "is_next_skipped_if_error": true,
    "next_jobs": []
  }
]
```

| Field                     | Description                                                                                       |
|---------------------------|---------------------------------------------------------------------------------------------------|
| `name`                    | Unique job id — the reference key other jobs use in `next_jobs` (convention: `<job_type>-<table_name>`, `DOWNLOAD-<source_name>` for downloads) |
| `source_name`             | Data source this job belongs to; the chain root's value names the source (`NIFC`) and prefixes its task ids |
| `job_type`                | `DOWNLOAD`, `INGESTION`, `DATA_QUALITY` or `SINGLE_ETL` — selects the matching `job.*` command block |
| `table_name`              | Required for `INGESTION`, `DATA_QUALITY` and `SINGLE_ETL` — argument passed to `run_etl.py <JOB_TYPE> <table>` (plain table name for `INGESTION`/`DATA_QUALITY`, `<table>_etl` for `SINGLE_ETL`; GEO_ETL adds `_raw` itself only for `INGESTION`); omitted for `DOWNLOAD` (the command takes the `source_name` instead) |
| `schedule`                | Cron for a **scheduled** job — a pipeline root that starts on this cron (shipped: the nine `DOWNLOAD` jobs, all `0 2 * * *`). **Omit the field for downstreamed jobs** — they start when the job referencing them in `next_jobs` finishes |
| `is_next_skipped_if_error`| **Documentation only** — marks that a failure skips the jobs named in `next_jobs`; the DAG always uses the `all_success` trigger rule below, which does exactly that |
| `next_jobs`               | Names of the jobs that depend on this job — turned into downstream Airflow tasks (chains via references to any depth) |

> **How jobs are launched.** There are exactly three launch paths:
>
> 1. **Scheduled** — a job with a `schedule` (shipped: the nine `DOWNLOAD` roots) starts on its own cron at the beginning of the run and must not be referenced by any other job.
> 2. **Downstream** — a job without a `schedule` starts when the job referencing it in `next_jobs` finishes, with Airflow's `all_success` trigger rule: a failed parent skips everything reachable through `next_jobs` (grey/skipped in the UI), while sibling branches still run — e.g. a failed plain-table `DATA_QUALITY` does not stop its sibling `SINGLE_ETL`.
> 3. **Union fan-in** — the one exception to both rules: the union job is downstreamed on **all nine** chains at once (see [The union fan-in](#the-union-fan-in-the-one-special-job)). It waits until the `ingestion.single_etl` task of every source reached a terminal state — whatever the chains' results — but **runs only if at least one of them succeeded**; when all nine failed or were skipped, the scheduler marks it `upstream_failed` without running it (nothing fresh to stack).
>
> The shipped `jobs.json` sets `is_next_skipped_if_error: true` on every job to document the skip behavior of path 2; the flag itself is not read at runtime.

The registry is validated on every DAG parse (`model.build_task_specs()` / `model.resolve_schedule()`): names must be unique, every `next_jobs` entry must resolve to an existing job, every job may be referenced by **exactly one** parent (task ids derive from a single chain path), the references must not form cycles, every non-union root must be scheduled, the union job must not be scheduled, and all scheduled jobs must share the same cron (Airflow schedules whole DAGs, not single tasks — the DAG's schedule is derived from them). A violation raises `ValueError` and surfaces as a DAG import error in the UI until fixed.

### Task ids

Each job becomes one Airflow task whose id follows the job-type path walked from the chain root (the `DOWNLOAD` prefix is dropped after the root) — not the job's `name`, which is only the reference key inside `jobs.json`:

| Job | Task id |
|-----|---------|
| chain root `DOWNLOAD` | `NIFC.download` |
| `INGESTION` under it | `NIFC.ingestion` |
| plain-table `DATA_QUALITY` | `NIFC.ingestion.data_quality` |
| `SINGLE_ETL` | `NIFC.ingestion.single_etl` |
| etl-table `DATA_QUALITY` | `NIFC.ingestion.single_etl.data_quality` |
| union fan-in | `Union.single_etl` |
| union data quality | `Union.single_etl.data_quality` |

### Union job (`Union`)

Besides the nine source chains, `jobs.json` holds a `Union` entry — the fan-in job that stacks every `*_etl` table into `geo.union_etl`, followed by its `DATA_QUALITY` job:

```json
[
  {
    "name": "SINGLE_ETL-union_etl",
    "source_name": "Union",
    "job_type": "SINGLE_ETL",
    "table_name": "union_etl",
    "is_next_skipped_if_error": true,
    "next_jobs": ["DATA_QUALITY-union_etl"]
  },
  {
    "name": "DATA_QUALITY-union_etl",
    "source_name": "Union",
    "job_type": "DATA_QUALITY",
    "table_name": "union_etl",
    "is_next_skipped_if_error": true,
    "next_jobs": []
  }
]
```

`SINGLE_ETL union_etl` stacks the nine `*_etl` tables into `geo.union_etl` (see [GEO_ETL — union_etl](../../GEO_ETL/docs/etl_jobs/single-etl.md#union_etl)); a non-zero exit — e.g. nothing newer to stack — fails the task (visible in the UI, re-runnable). The DAG gives the union root the `all_done_min_one_success` trigger rule and the nine `ingestion.single_etl` tasks as upstreams: it starts as soon as the last single-ETL reached a terminal state, whatever the chains' results — but only if at least one of them succeeded; when all nine failed, the scheduler marks it `upstream_failed` without running it. No markers, no polling, no deadline. It is then followed by `DATA_QUALITY union_etl`, which measures `geo.union_etl` right after the stack. The DATA_QUALITY jobs are not part of the fan-in: they stay terminal steps with no successors, like in the old Celery chains.

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

You never write Airflow code to add or reshape work — you edit `jobs.json`. On every parse, `model.build_task_specs()` resolves the flat registry into one task spec per job, and `geo_pipeline.py` turns each spec into an operator:

1. **`jobs.json`** — the source of truth: every job as one object — either *scheduled* (carries a `schedule`, starts the pipeline on that cron) or *downstreamed* (no `schedule`, starts when the job referencing it in `next_jobs` finishes).
2. **`build_task_specs()`** — validates the registry (unique names, resolvable references, one parent per job, no cycles, scheduled jobs are roots sharing one cron), then produces one `TaskSpec` per job: the task id (job-type path), the shell command (from `config.json`), the upstream list (the parent job) and the trigger rule (`all_success`; `all_done_min_one_success` only for the union root). `resolve_schedule()` returns the cron the scheduled jobs share — the DAG's schedule.
3. **`geo_dag.py` + `geo_pipeline.py`** — `GEO_DAG` groups the specs by source into ten `JobsSubDAG`s (nine source chains + the union chain), renders each one into a `BashOperator` per spec (pool by job type, trigger rule, retries/timeout from the `dag` block, memory callbacks), wires the dependencies inside each subDAG and the union fan-in between them; `geo_pipeline.py` only applies `GEO_DAG.dag_kwargs()` through `@dag(...)` and calls `GEO_DAG.to_airflow()`.

### Placing a job

- Add one object per job anywhere in the array; give it a unique `name` (convention: `<job_type>-<table_name>`, `DOWNLOAD-<source_name>` for downloads).
- Mark the job as **scheduled** or **downstreamed**: give a root job a `schedule` cron (shipped: every `DOWNLOAD`); omit `schedule` for everything else, which then must be referenced by exactly one parent. The `Union` root is the exception — downstreamed on all nine chains at once, never scheduled.
- A job with no incoming references and no `schedule` is invalid (nothing would ever start it); a scheduled job that is also referenced is invalid (it would start twice). Both are rejected at parse time.
- Each source's root is a `DOWNLOAD` — the only job whose command argument is the `source_name` instead of a `table_name`.
- `job_type` selects the command template from the `job` block of `config.json`; `INGESTION`, `DATA_QUALITY` and `SINGLE_ETL` additionally need `table_name` (passed as the script argument).
- Chains are otherwise independent: a job can only wait on its own ancestors. The one cross-chain dependency is the automatic union fan-in below.

> **Task ids are derived, not taken from `name`.** The id is the job-type path walked from the chain root (DOWNLOAD dropped after the root — see [Task ids](#task-ids)), so two sibling jobs of the same type under one parent would collide (e.g. two `DATA_QUALITY` jobs directly under one `INGESTION` both become `<source>.ingestion.data_quality`). The DAG keeps only one of them — re-check the ids when reshaping a chain; the generation tests in `test/test_dags.py` assert uniqueness.

### Creating dependencies

| You want | Do |
|----------|-----|
| B runs after A, and is skipped when A fails | Put B's `name` into A's `next_jobs` — B waits for A with `all_success` |
| Two jobs run in parallel | List both jobs' names in the same parent's `next_jobs` |
| A job runs after a whole branch | Put the new job's name into the branch's last job's `next_jobs` (chains via references to any depth) |
| A job waits for a job of another source | Not possible per-job — only the union root fans in across chains |

The order of names inside a `next_jobs` list is irrelevant: jobs sharing a parent are siblings and run in parallel (throttled by the pools).

Failure semantics follow the trigger rules: a failed (or skipped) job skips everything reachable through its `next_jobs` (grey/skipped in the UI) while its siblings still run — e.g. a failed plain-table `DATA_QUALITY` never stops the sibling `SINGLE_ETL`. Nothing retries (`retries: 0`); failures stay visible and are re-run manually from the UI.

### The union fan-in (the one special job)

The `Union` job is not a normal source. Its root (`Union.single_etl`) is the only task with the `all_done_min_one_success` trigger rule, and `build_task_specs()` gives it — automatically — the `ingestion.single_etl` task of every other chain as upstreams:

- Adding or removing a source chain needs no edit to `SINGLE_ETL-union_etl`; the fan-in follows the chains in `jobs.json`.
- The union starts as soon as the last single-ETL reached a terminal state, whatever the chains' results — **but only if at least one of them succeeded**. When all nine failed, the scheduler marks it `upstream_failed` without running it (nothing fresh to stack); when all nine are skipped, it is skipped as well. The `DATA_QUALITY` jobs are terminal steps with no successors and are deliberately not part of the fan-in.
- Jobs referenced from the union root wait for it with the normal `all_success` rule (shipped: `DATA_QUALITY-union_etl`, the union chain's last job).
- `jobs.json` must always contain the `Union` job — `build_task_specs()` raises `ValueError` at parse time otherwise, which surfaces as a DAG import error in the UI until it is back.

### Adding a step under the union

The shipped registry already chains a `DATA_QUALITY` job under the union root (`DATA_QUALITY-union_etl`, referenced from `SINGLE_ETL-union_etl.next_jobs` → the task `Union.single_etl.data_quality`). Further steps follow the same recipe: add the job object and reference it from the union job's `next_jobs` — the new task waits for its parent with `all_success` and runs in the `etl` pool, no change to `geo_pipeline.py`, `geo_dag.py` or `model.py` needed.

### Validating a change

1. `conda run --no-capture-output -n geo_e2e_311 pytest` — the registry and DAG generation tests (`test/test_jobs.py`, `test/test_dags.py`) assert the reference rules, task counts, ids, wiring, trigger rules and commands.
2. Save `jobs.json` — the DAG processor re-parses within ~30 seconds (or refresh via the UI); a valid file leaves `dags list-import-errors` empty, an invalid one shows the `ValueError` there.
3. Open the **Graph** view of a manual run — the new tasks and edges show up where you wired them.

## DAG settings (`config.json` → `dag` block)

The runtime knobs live in the `dag` block of `config.json`, loaded via pydantic (`model.DagSettings` / `get_dag_settings()`); `geo_pipeline.py` consumes it and hardcodes nothing. The cron itself is **not** here — it lives on the scheduled jobs in `jobs.json` (see [Modifying the schedule](#modifying-the-schedule)):

```json
"dag": {
  "dag_id": "geo_pipeline",
  "start_date": "2026-01-01",
  "timezone": "Europe/Warsaw",
  "catchup": false,
  "max_active_runs": 1,
  "tags": ["geo", "e2e"],
  "retries": 0,
  "execution_timeout_minutes": 30,
  "union_source": "Union",
  "pools": {
    "download": "download",
    "ingestion": "etl",
    "data_quality": "etl",
    "single_etl": "etl"
  },
  "pool_slots": {
    "download": 3,
    "etl": 2
  }
}
```

| Field | Shipped value | Description |
|-------|---------------|-------------|
| `dag_id` | `geo_pipeline` | Airflow DAG id |
| `start_date` | `2026-01-01` | Schedule anchor date (midnight in `timezone`; the old Celery staggering of 15 minutes per source is gone — all downloads start at the scheduled time) |
| `timezone` | `Europe/Warsaw` | IANA timezone the cron is evaluated in — validated at config load, unknown zones raise |
| `catchup` | `false` | No backfill of missed runs |
| `max_active_runs` | `1` | One pipeline run at a time |
| `tags` | `["geo", "e2e"]` | Tags shown in the UI |
| `retries` | `0` | No automatic retries (parity with Celery; failures show up red instead of silently re-running) |
| `execution_timeout_minutes` | `30` | Per-task timeout (parity with the old `task_time_limit`) |
| `union_source` | `Union` | `source_name` of the fan-in job in `jobs.json` — must match exactly one root job there |
| `pools` | download/etl | Airflow pool per job type (`DOWNLOAD` → `download`, everything else → `etl`) |
| `pool_slots` | 3 / 2 | Slot count per pool — seeded by `airflow-init` on every `docker compose up` via `docker/airflow/seed_pools.py` (idempotent) |

You can inspect the pools under **Admin → Pools** or via `docker compose --env-file .env.local run --rm airflow-cli pools list`. The DAG itself is created unpaused (`dags_are_paused_at_creation=false` in `docker-compose.yaml`).

## Modifying the schedule

Edit the `schedule` field on the scheduled jobs in `jobs.json` — the nine `DOWNLOAD` objects (they must all carry the **same** cron: Airflow schedules the whole DAG, and `model.resolve_schedule()` rejects differing crons). The `timezone` stays in the `dag` block of `config.json`. The DAG processor picks the change up automatically within ~30 seconds — no restart needed.

### Adding a new data source

1. Add five job objects to the `jobs.json` array for the new `source_name`: `DOWNLOAD-<Source>` — carrying the same `schedule` cron as the other downloads — referencing `INGESTION-<table>`, which references both `DATA_QUALITY-<table>` and `SINGLE_ETL-<table>_etl` — the latter referencing `DATA_QUALITY-<table>_etl` (all downstreamed, so no `schedule` field). The placement and dependency rules are spelled out in [Constructing the DAG](#constructing-the-dag).
2. Make sure the source exists in GEO_Download's `config.json` and that the table exists in GEO_ETL.
3. Wait for the DAG to re-parse (or trigger a refresh in the UI) — the new tasks appear automatically.

The union fan-in needs no change: `build_task_specs()` gives the union root the `ingestion.single_etl` task of **every** non-union chain as upstreams, so the new source is covered automatically.
