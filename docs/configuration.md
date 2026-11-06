← [README](../README.md)

# Configuration Reference

`config.json` defines **how** each job type is executed (the `job` block) and holds the DAG-level runtime knobs (the `dag` block: retries, pools, ...); `jobs.json` defines **which** jobs each source runs, how they depend on each other, and which of them are scheduled (each scheduled job carries its own `schedule`). The DAG file `geo_pipeline.py` only turns those configs into Airflow tasks. Runtime values — the Airflow UI login and the Supabase credentials of Airflow's metadata DB — live outside both, in the git-ignored `.env`; the Docker containers receive them from the Compose `environment:` section (see [Supabase metadata DB](#supabase-metadata-db-env)).

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
    "data_source_file": {
      "folder_name": "GEO_ETL",
      "python_script": "run_etl.py",
      "conda_env": "geo_etl_311"
    },
    "single_etl": {
      "folder_name": "GEO_ETL",
      "python_script": "run_etl.py",
      "conda_env": "geo_etl_311"
    },
    "view": {
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
| `data_source_file` | `GEO_ETL`  | `run_etl.py`    | `geo_etl_311`     |
| `single_etl`  | `GEO_ETL`      | `run_etl.py`    | `geo_etl_311`     |
| `view`        | `GEO_ETL`      | `run_etl.py`    | `geo_etl_311`     |

| Field           | Description                                                        |
|-----------------|--------------------------------------------------------------------|
| `folder_name`   | Name of the sibling project directory                              |
| `python_script` | Python script to execute (e.g. `run_download.py`)                  |
| `conda_env`     | Optional. Conda env to run in (`conda run --no-capture-output -n <env>`); each sibling project runs in its own env (`GEO_Download` → `geo_download_311`, every `run_etl.py` step → `geo_etl_311`), so the Airflow image does not need PySpark or the downloader's dependencies |

## Job Registry (`jobs.json`)

The flat registry of every job the pipeline runs — a JSON **array** of job objects, one per job, keyed relationships via `name` instead of nesting. Each source contributes a chain of six jobs (`DOWNLOAD` → `INGESTION` → `DATA_QUALITY` + `SINGLE_ETL` → etl-table `DATA_QUALITY`, plus `DATA_SOURCE_FILE` released by the download); the `Union` entry stacks the results and builds the views on top of them. The first source's jobs:

```json
[
  {
    "name": "DOWNLOAD-USGS_Earthquake",
    "task_id": "USGS_Earthquake.download",
    "source_name": "USGS_Earthquake",
    "job_type": "DOWNLOAD",
    "schedule": "0 8 * * *",
    "is_next_skipped_if_error": true,
    "next_jobs": ["INGESTION-usgs_earthquake", "DATA_SOURCE_FILE-USGS_Earthquake"]
  },
  {
    "name": "INGESTION-usgs_earthquake",
    "task_id": "USGS_Earthquake.ingestion",
    "source_name": "USGS_Earthquake",
    "job_type": "INGESTION",
    "table_name": "usgs_earthquake",
    "is_next_skipped_if_error": true,
    "next_jobs": ["DATA_QUALITY-usgs_earthquake", "SINGLE_ETL-usgs_earthquake_etl"]
  },
  {
    "name": "DATA_QUALITY-usgs_earthquake",
    "task_id": "USGS_Earthquake.data_quality",
    "source_name": "USGS_Earthquake",
    "job_type": "DATA_QUALITY",
    "table_name": "usgs_earthquake",
    "is_next_skipped_if_error": true,
    "next_jobs": []
  },
  {
    "name": "SINGLE_ETL-usgs_earthquake_etl",
    "task_id": "USGS_Earthquake.single_etl",
    "source_name": "USGS_Earthquake",
    "job_type": "SINGLE_ETL",
    "table_name": "usgs_earthquake_etl",
    "is_next_skipped_if_error": true,
    "next_jobs": ["DATA_QUALITY-usgs_earthquake_etl"]
  },
  {
    "name": "DATA_QUALITY-usgs_earthquake_etl",
    "task_id": "USGS_Earthquake_etl.data_quality",
    "source_name": "USGS_Earthquake",
    "job_type": "DATA_QUALITY",
    "table_name": "usgs_earthquake_etl",
    "is_next_skipped_if_error": true,
    "next_jobs": []
  },
  {
    "name": "DATA_SOURCE_FILE-USGS_Earthquake",
    "task_id": "USGS_Earthquake.data_source_file",
    "source_name": "USGS_Earthquake",
    "job_type": "DATA_SOURCE_FILE",
    "table_name": "USGS_Earthquake",
    "is_next_skipped_if_error": true,
    "next_jobs": []
  }
]
```

| Field                     | Description                                                                                       |
|---------------------------|---------------------------------------------------------------------------------------------------|
| `name`                    | Unique job id — the reference key other jobs use in `next_jobs` (convention: `<job_type>-<table_name>`, `DOWNLOAD-<source_name>` for downloads); **not** the Airflow task id |
| `task_id`                 | Unique Airflow task id this job becomes (convention: `<source>.<step>`, e.g. `NIFC.single_etl`, `NIFC_etl.data_quality`, `View.<view_name>`); dot-separated segments of letters, digits and underscores — see [Task ids](#task-ids) |
| `source_name`             | Data source this job belongs to; the chain root's value names the source (`NIFC`) and groups its tasks in the UI |
| `job_type`                | `DOWNLOAD`, `INGESTION`, `DATA_QUALITY`, `DATA_SOURCE_FILE`, `SINGLE_ETL` or `VIEW` — selects the matching `job.*` command block (`VERDICT` is not declarable: `build_task_specs()` synthesizes that task itself) |
| `table_name`              | Required for every job type except `DOWNLOAD` — argument passed to `run_etl.py <JOB_TYPE> <table>` (plain table name for `INGESTION`/`DATA_QUALITY`, the source folder name for `DATA_SOURCE_FILE`, `<table>_etl` for `SINGLE_ETL`, the view script name for `VIEW`); omitted for `DOWNLOAD` (the command takes the `source_name` instead) |
| `schedule`                | Cron for a **scheduled** job — a pipeline root that starts on this cron (shipped: the nine `DOWNLOAD` jobs, all `0 8 * * *`). **Omit the field for downstreamed jobs** — they start when the job referencing them in `next_jobs` finishes |
| `is_next_skipped_if_error`| **Documentation only** — marks that a failure skips the jobs named in `next_jobs`; the DAG always uses the `all_success` trigger rule below, which does exactly that |
| `next_jobs`               | Names of the jobs that depend on this job — turned into downstream Airflow tasks (chains via references to any depth) |

> **How jobs are launched.** There are exactly three launch paths:
>
> 1. **Scheduled** — a job with a `schedule` (shipped: the nine `DOWNLOAD` roots) starts on its own cron at the beginning of the run and must not be referenced by any other job.
> 2. **Downstream** — a job without a `schedule` starts when the job referencing it in `next_jobs` finishes, with Airflow's `all_success` trigger rule: a failed parent skips everything reachable through `next_jobs` (grey/skipped in the UI), while sibling branches still run — e.g. a failed plain-table `DATA_QUALITY` does not stop its sibling `SINGLE_ETL`.
> 3. **Union fan-in** — the one exception to both rules: the union job is downstreamed on **all nine** chains at once (see [The union fan-in](#the-union-fan-in-the-one-special-job)). It waits until the `SINGLE_ETL` task of every source (`NIFC.single_etl`, …) reached a terminal state — whatever the chains' results — but **runs only if at least one of them succeeded**; when all nine failed or were skipped, the scheduler marks it `upstream_failed` without running it (nothing fresh to stack).
>
> The shipped `jobs.json` sets `is_next_skipped_if_error: true` on every job to document the skip behavior of path 2; the flag itself is not read at runtime.

The registry is validated on every DAG parse (`model.build_task_specs()` / `model.resolve_schedule()`): names must be unique, every `next_jobs` entry must resolve to an existing job, every job may be referenced by **exactly one** parent, `task_id`s must be unique and well-formed, the references must not form cycles, every non-union root must be scheduled, the union job must not be scheduled, no `VERDICT` job may be declared (the verdict task is synthesized), and all scheduled jobs must share the same cron (Airflow schedules whole DAGs, not single tasks — the DAG's schedule is derived from them). A violation raises `ValueError` and surfaces as a DAG import error in the UI until fixed.

### Task ids

Each job becomes one Airflow task whose id is its own `task_id` field in `jobs.json` — the id is *data*, declared per job (not derived from the chain shape, and not the job's `name`, which is only the reference key inside `jobs.json`):

| Job | Task id |
|-----|---------|
| chain root `DOWNLOAD` | `NIFC.download` |
| `INGESTION` under it | `NIFC.ingestion` |
| plain-table `DATA_QUALITY` | `NIFC.data_quality` |
| `DATA_SOURCE_FILE` | `NIFC.data_source_file` |
| `SINGLE_ETL` | `NIFC.single_etl` |
| etl-table `DATA_QUALITY` | `NIFC_etl.data_quality` |
| union fan-in | `Union.single_etl` |
| union data quality | `Union_etl.data_quality` |
| `VIEW` jobs | `View.<view_name>` — e.g. `View.metrics_flood_vw`; the seventeen views are siblings, so each id carries its script name |
| verdict (no job) | `Union.verdict` — synthesized by `build_task_specs()`, not present in `jobs.json`; waits for all other tasks and decides the run's state |

The id must be dot-separated segments of letters, digits and underscores (`[A-Za-z0-9_]+(\.[A-Za-z0-9_]+)*`) — validated by `model.Job` at load; `Union.verdict` is reserved for the synthesized verdict task.

### Union job (`Union`)

Besides the nine source chains, `jobs.json` holds the `Union` entry — the fan-in job that stacks every `*_etl` table into `geo.union_etl`, followed by its `DATA_QUALITY` job and the `VIEW` jobs that build the views on top of the stacked table (example below shows one view for brevity — the shipped registry chains all seventeen):

```json
[
  {
    "name": "SINGLE_ETL-union_etl",
    "task_id": "Union.single_etl",
    "source_name": "Union",
    "job_type": "SINGLE_ETL",
    "table_name": "union_etl",
    "is_next_skipped_if_error": true,
    "next_jobs": ["DATA_QUALITY-union_etl", "VIEW-metrics_avalanche_vw"]
  },
  {
    "name": "DATA_QUALITY-union_etl",
    "task_id": "Union_etl.data_quality",
    "source_name": "Union",
    "job_type": "DATA_QUALITY",
    "table_name": "union_etl",
    "is_next_skipped_if_error": true,
    "next_jobs": []
  },
  {
    "name": "VIEW-metrics_avalanche_vw",
    "task_id": "View.metrics_avalanche_vw",
    "source_name": "Union",
    "job_type": "VIEW",
    "table_name": "metrics_avalanche_vw",
    "is_next_skipped_if_error": true,
    "next_jobs": []
  }
]
```

`SINGLE_ETL union_etl` stacks the nine `*_etl` tables into `geo.union_etl` (see [GEO_ETL — union_etl](../../GEO_ETL/docs/etl_jobs/single-etl/union-etl.md)); a non-zero exit — e.g. nothing newer to stack — fails the task (visible in the UI, re-runnable). The DAG gives the union root the `all_done_min_one_success` trigger rule and the nine `SINGLE_ETL` tasks (`NIFC.single_etl`, …) as upstreams: it starts as soon as the last single-ETL reached a terminal state, whatever the chains' results — but only if at least one of them succeeded; when all nine failed, the scheduler marks it `upstream_failed` without running it. No markers, no polling, no deadline. It is then followed by `DATA_QUALITY union_etl`, which measures `geo.union_etl` right after the stack. The DATA_QUALITY jobs are not part of the fan-in: they stay terminal steps of `jobs.json` (nothing chains from them — only the verdict gate waits), like in the old Celery chains.

### View step

Every `VIEW` job is chained under the union root (`SINGLE_ETL-union_etl.next_jobs`) (the four single-ETL tasks that share the `state_province` transform via `utils.column_helpers.state_province` are `openmeteo_flood_etl`, `usgs_nwis_etl`, `ibtracs_cyclone_etl` and `gdacs_event_etl` — see GEO_ETL's `utils.column_helpers`), with `table_name` set to the view script name — so the shipped registry has seventeen of them (`metrics_*_vw` and `phenomenons_*_vw`, all reading `geo.union_etl`). They are siblings of `DATA_QUALITY-union_etl`: each waits for the stack with the normal `all_success` rule, runs `run_etl.py VIEW <view_name>` in the `etl` pool and (re)creates its view on the freshly stacked table (see [GEO_ETL — View job](../../GEO_ETL/docs/etl_jobs/view.md)). Like the DATA_QUALITY jobs they are terminal steps of `jobs.json` (only the verdict gate waits on them); because they are siblings, a failed data-quality run does not skip the views and vice versa.

### Data quality step

Every table in the mapping below has **two** `DATA_QUALITY` jobs: one chained directly under its `INGESTION` job (measuring the plain table), and one under the `SINGLE_ETL` job (measuring the `<table>_etl` table). The plain-table metrics are therefore computed **only after the ingestion for that table finished successfully** — a failed or skipped ingestion means `run_etl.py DATA_QUALITY <table>` never runs (the metrics would otherwise be measured on stale data), and the etl-table metrics only after the `_etl` table was rebuilt. Since each job is the last step of its branch, a failure there stops that branch without affecting anything downstream.

Both jobs run in the same conda env and folder as ingestion (see `job.data_quality` in `config.json`); GEO_ETL additionally writes its own `logs/data_quality/data_quality_<table>_<timestamp>.log` per run (see [Usage — Logs](usage.md#logs)). GEO_ETL picks its load type (`HISTORICAL` / `INCREMENTAL`) from the state of `geo.data_quality` — see [GEO_ETL — Data Quality job](../../GEO_ETL/docs/etl_jobs/data-quality.md). Each normal `DATA_QUALITY` run also refreshes that table's whole-table `overall` snapshot in `geo.data_quality`, so the pipelines carry no separate `--overall` jobs and the aggregate stays in step with the dated rows automatically (see [GEO_ETL — Overall refresh](../../GEO_ETL/docs/etl_jobs/data-quality.md#overall-refresh)).

### Data source file step

Every source also has a `DATA_SOURCE_FILE` job released directly by its `DOWNLOAD` job (a sibling of `INGESTION`), with `table_name` set to the source folder name (e.g. `NIFC`). It publishes that source's CSV inventory to `geo.data_source_files` for GEO_API; GEO_ETL picks `HISTORICAL` or `INCREMENTAL` from the state of that table. It has no successor, so like the `DATA_QUALITY` jobs it is a terminal branch that only the verdict gate waits on — see [GEO_ETL — DATA_SOURCE_FILE job](../../GEO_ETL/docs/etl_jobs/data-source-file.md).

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
2. **`build_task_specs()`** — validates the registry (unique names, unique well-formed `task_id`s, resolvable references, one parent per job, no cycles, scheduled jobs are roots sharing one cron), then produces one `TaskSpec` per job: the task id (the job's `task_id`), the shell command (from `config.json`), the upstream list (the parent job) and the trigger rule (`all_success`; `all_done_min_one_success` only for the union root). `resolve_schedule()` returns the cron the scheduled jobs share — the DAG's schedule.
3. **`geo_dag.py` + `geo_pipeline.py`** — `GEO_DAG` groups the specs by source into ten `JobsSubDAG`s (nine source chains + the union chain), renders each one into a `BashOperator` per spec (pool by job type, trigger rule, retries/timeout from the `dag` block, memory callbacks), wires the dependencies inside each subDAG and the union fan-in between them; `geo_pipeline.py` only applies `GEO_DAG.dag_kwargs()` through `@dag(...)` and calls `GEO_DAG.to_airflow()`.

### Placing a job

- Add one object per job anywhere in the array; give it a unique `name` (convention: `<job_type>-<table_name>`, `DOWNLOAD-<source_name>` for downloads) and a unique `task_id` (convention: `<source>.<step>`).
- Mark the job as **scheduled** or **downstreamed**: give a root job a `schedule` cron (shipped: every `DOWNLOAD`); omit `schedule` for everything else, which then must be referenced by exactly one parent. The `Union` root is the exception — downstreamed on all nine chains at once, never scheduled.
- A job with no incoming references and no `schedule` is invalid (nothing would ever start it); a scheduled job that is also referenced is invalid (it would start twice). Both are rejected at parse time.
- Each source's root is a `DOWNLOAD` — the only job whose command argument is the `source_name` instead of a `table_name`.
- `job_type` selects the command template from the `job` block of `config.json`; everything except `DOWNLOAD` additionally needs `table_name` (passed as the script argument).
- Chains are otherwise independent: a job can only wait on its own ancestors. The one cross-chain dependency is the automatic union fan-in below.

> **`task_id`s are data — declared, not derived.** Each job carries its own `task_id` (see [Task ids](#task-ids)), so two sibling jobs of the same type under one parent no longer collide by construction: give them distinct ids (e.g. `NIFC.data_quality` and `NIFC_etl.data_quality`) and keep them unique — `build_task_specs()` raises `ValueError` on a duplicate or malformed id, and the generation tests in `test/test_dags.py` assert uniqueness across the shipped registry.

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

The `Union` job is not a normal source. Its root (`Union.single_etl`) is the only task with the `all_done_min_one_success` trigger rule, and `build_task_specs()` gives it — automatically — the `SINGLE_ETL` task of every other chain (`NIFC.single_etl`, `GDACS.single_etl`, …) as upstreams:

- Adding or removing a source chain needs no edit to `SINGLE_ETL-union_etl`; the fan-in follows the chains in `jobs.json`.
- The union starts as soon as the last single-ETL reached a terminal state, whatever the chains' results — **but only if at least one of them succeeded**. When all nine failed, the scheduler marks it `upstream_failed` without running it (nothing fresh to stack); when all nine are skipped, it is skipped as well. The `DATA_QUALITY` and `DATA_SOURCE_FILE` jobs are terminal steps of `jobs.json` (only the verdict gate waits on them) and are deliberately not part of the fan-in.
- Jobs referenced from the union root wait for it with the normal `all_success` rule (shipped: `DATA_QUALITY-union_etl` and the seventeen `VIEW-<view_name>` jobs).
- `jobs.json` must always contain the `Union` job — `build_task_specs()` raises `ValueError` at parse time otherwise, which surfaces as a DAG import error in the UI until it is back.

### Adding a step under the union

The shipped registry already chains a `DATA_QUALITY` job and the seventeen `VIEW` jobs under the union root (`DATA_QUALITY-union_etl` and `VIEW-<view_name>`, all referenced from `SINGLE_ETL-union_etl.next_jobs` → the tasks `Union_etl.data_quality` and `View.<view_name>`). Further steps follow the same recipe: add the job object and reference it from the union job's `next_jobs` — the new task waits for its parent with `all_success` and runs in the `etl` pool, no change to `geo_pipeline.py`, `geo_dag.py` or `model.py` needed (a new job *type* additionally needs an `EJobType` member and its `config.json` blocks — see `VIEW`).

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
    "data_source_file": "etl",
    "single_etl": "etl",
    "view": "etl",
    "verdict": "etl"
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
| `pools` | download/etl | Airflow pool per job type (`DOWNLOAD` → `download`, everything else — `VIEW` and the synthesized `VERDICT` task included — → `etl`) |
| `pool_slots` | 3 / 2 | Slot count per pool — seeded by `airflow-init` on every `docker compose up` via `docker/geo_airflow_spark/seed_pools.py` (idempotent) |

You can inspect the pools under **Admin → Pools** or via `docker compose run --rm airflow-cli pools list`. The DAG itself is created unpaused (`dags_are_paused_at_creation=false` in `docker-compose.yaml`; `airflow-dag-activate` also repairs an already-paused DAG).

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
4. **Supabase password** — `SUPABASE_DB_PASSWORD` is **never displayed** in the dashboard: use the value you saved when the project was created, or go to **Project Settings → Database → Generate password** (formerly *Reset database password*). That rotation invalidates the previous password immediately. See [GEO_ETL — Supabase Credentials](../../GEO_ETL/docs/configuration/supabase-credentials.md) for the walkthrough, API/PAT rotation and a least-privilege role.
5. **Schema** — leave `AIRFLOW_DB_SCHEMA=geo_airflow`. On the first `docker compose up`, `airflow-init` creates it and runs the migration into it.
6. **Optional dashboard URLs/ports** — `VITE_API_BASE` is compiled into `geo_react_app`; keep `http://localhost:8000` when the browser runs on the Docker host. `GEO_VISUAL_HOST_PORT` (default `8501`) publishes the Streamlit dashboard.
7. **Check** — `docker compose config --quiet` renders the file and fails with the name of any missing key. To verify the metadata connection and schema:

   ```bash
   docker compose exec airflow-scheduler python -c "import os; from sqlalchemy import create_engine, text; e=create_engine(os.environ['AIRFLOW__DATABASE__SQL_ALCHEMY_CONN']); print(e.connect().execute(text('select current_schema()')).scalar())"
   ```

   It prints `geo_airflow` when the connection and the schema are right.

A missing variable stops compose early with a message naming the key (`Set SUPABASE_DB_HOST in .env`). `.env` is git-ignored; only `.env.example` (placeholders) is committed. The legacy `.env.docker` name is still ignored by git but is not used by the Docker images (they write `.env`).

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

## Modifying the schedule

Edit the `schedule` field on the scheduled jobs in `jobs.json` — the nine `DOWNLOAD` objects (they must all carry the **same** cron: Airflow schedules the whole DAG, and `model.resolve_schedule()` rejects differing crons). The `timezone` stays in the `dag` block of `config.json`. The DAG processor picks the change up automatically within ~30 seconds — no restart needed.

### Adding a new data source

1. Add six job objects to the `jobs.json` array for the new `source_name`: `DOWNLOAD-<Source>` — carrying the same `schedule` cron as the other downloads — referencing both `INGESTION-<table>` and `DATA_SOURCE_FILE-<Source>`; `INGESTION` references both `DATA_QUALITY-<table>` and `SINGLE_ETL-<table>_etl` — the latter referencing `DATA_QUALITY-<table>_etl` (all downstreamed, so no `schedule` field; `DATA_SOURCE_FILE` has no successor). The placement and dependency rules are spelled out in [Constructing the DAG](#constructing-the-dag).
2. Make sure the source exists in GEO_Download's `config.json` and that the table exists in GEO_ETL.
3. Wait for the DAG to re-parse (or trigger a refresh in the UI) — the new tasks appear automatically.

The union fan-in needs no change: `build_task_specs()` gives the union root the `SINGLE_ETL` task (`<source>.single_etl`) of **every** non-union chain as upstreams, so the new source is covered automatically.
