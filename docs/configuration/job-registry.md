← [Configuration](../configuration.md)

# Job Registry & Task Dependencies

`jobs.json` is the source of truth for which jobs run, how they depend on each other and
what each Airflow task is called. The `config.json` `job` block supplies the command template.

## Job Registry (`jobs.json`)

The flat registry of every job the pipeline runs — a JSON **array** of job objects, one per job, keyed relationships via `name` instead of nesting. Each source contributes a chain of six jobs (`DOWNLOAD` → `INGESTION` → `DATA_QUALITY` + `SINGLE_ETL` → etl-table `DATA_QUALITY`, plus `DATA_SOURCE_FILE` released by the download); the `Union` entry stacks the results, builds the observed/map views, runs the MLlib prediction and then the combined metric views. The first source's jobs:

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
| `job_type`                | `DOWNLOAD`, `INGESTION`, `DATA_QUALITY`, `DATA_SOURCE_FILE`, `SINGLE_ETL`, `VIEW` or `PREDICTION` — selects the matching `job.*` command block (`VERDICT` is not declarable: `build_task_specs()` synthesizes that task itself) |
| `table_name`              | Required for every job type except `DOWNLOAD` — argument passed to `run_etl.py <JOB_TYPE> <table>` (plain table name for `INGESTION`/`DATA_QUALITY`, the source folder name for `DATA_SOURCE_FILE`, `<table>_etl` for `SINGLE_ETL`, the view script name for `VIEW`, `prediction_etl` for `PREDICTION`); omitted for `DOWNLOAD` (the command takes the `source_name` instead) |
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
| prediction | `Union.prediction` |
| `VIEW` jobs | `View.<view_name>` — e.g. `View.metrics_flood_vw`; the eighteen views are keyed by script name |
| verdict (no job) | `Union.verdict` — synthesized by `build_task_specs()`, not present in `jobs.json`; waits for all other tasks and decides the run's state |

The id must be dot-separated segments of letters, digits and underscores (`[A-Za-z0-9_]+(\.[A-Za-z0-9_]+)*`) — validated by `model.Job` at load; `Union.verdict` is reserved for the synthesized verdict task.

### Union job (`Union`)

Besides the nine source chains, `jobs.json` holds the `Union` entry — the fan-in job that stacks every `*_etl` table into `geo.union_etl`, followed by its `DATA_QUALITY` job, the observed/phenomenon views, the MLlib prediction and the eight combined metric views (the example below is abridged — the shipped registry chains all ten first-stage views and all eight combined metric views):

```json
[
  {
    "name": "SINGLE_ETL-union_etl",
    "task_id": "Union.single_etl",
    "source_name": "Union",
    "job_type": "SINGLE_ETL",
    "table_name": "union_etl",
    "is_next_skipped_if_error": true,
    "next_jobs": ["DATA_QUALITY-union_etl", "VIEW-metrics_observed_vw", "VIEW-phenomenons_us_vw"]
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
    "name": "VIEW-metrics_observed_vw",
    "task_id": "View.metrics_observed_vw",
    "source_name": "Union",
    "job_type": "VIEW",
    "table_name": "metrics_observed_vw",
    "is_next_skipped_if_error": true,
    "next_jobs": ["PREDICTION-prediction_etl"]
  },
  {
    "name": "PREDICTION-prediction_etl",
    "task_id": "Union.prediction",
    "source_name": "Union",
    "job_type": "PREDICTION",
    "table_name": "prediction_etl",
    "is_next_skipped_if_error": true,
    "next_jobs": ["VIEW-metrics_avalanche_vw"]
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

`SINGLE_ETL union_etl` stacks the nine `*_etl` tables into `geo.union_etl` (see [GEO_ETL — union_etl](../../../GEO_ETL/docs/etl_jobs/single-etl/union-etl.md)); a non-zero exit — e.g. nothing newer to stack — fails the task (visible in the UI, re-runnable). The DAG gives the union root the `all_done_min_one_success` trigger rule and the nine `SINGLE_ETL` tasks (`NIFC.single_etl`, …) as upstreams: it starts as soon as the last single-ETL reached a terminal state, whatever the chains' results — but only if at least one of them succeeded; when all nine failed, the scheduler marks it `upstream_failed` without running it. No markers, no polling, no deadline. It is then followed by `DATA_QUALITY union_etl`, which measures `geo.union_etl` right after the stack. The DATA_QUALITY jobs are not part of the fan-in: they stay terminal steps of `jobs.json` (nothing chains from them — only the verdict gate waits), like in the old Celery chains.

### View, prediction and metric-view steps

Ten first-stage `VIEW` jobs hang off the union root (`SINGLE_ETL-union_etl.next_jobs`): `VIEW-metrics_observed_vw` and the nine `VIEW-phenomenons_*_vw` map views (the four single-ETL tasks that share the `state_province` transform via `utils.column_helpers.state_province` are `openmeteo_flood_etl`, `usgs_nwis_etl`, `ibtracs_cyclone_etl` and `gdacs_event_etl` — see GEO_ETL's `utils.column_helpers`), with `table_name` set to the view script name. They are siblings of `DATA_QUALITY-union_etl`: each waits for the stack with the normal `all_success` rule, runs `run_etl.py VIEW <view_name>` in the `etl` pool and (re)creates its view on the freshly stacked table (see [GEO_ETL — View job](../../../GEO_ETL/docs/etl_jobs/view.md)).

`VIEW-metrics_observed_vw` then releases `PREDICTION-prediction_etl` (`Union.prediction`, `run_etl.py PREDICTION prediction_etl`), which rebuilds `geo.prediction_etl` with PySpark MLlib estimates for missing metric dates (see [GEO_ETL — PREDICTION job](../../../GEO_ETL/docs/etl_jobs/prediction.md)). The eight combined `VIEW-metrics_*_vw` jobs are chained under the prediction task, so they always rebuild over the refreshed predictions; they are the only views that read `geo.prediction_etl`. The phenomenon/map views stay independent of predictions and run in parallel with the prediction branch.

The combined metric views and phenomenon views are terminal steps (only the verdict gate waits on them). A failed data-quality run does not skip the views, and a failed phenomenon view does not stop the metric branch. Because the prediction branch gates the combined views and the verdict, a failed observed view or prediction does fail the DAG run — the metric views would otherwise silently lose their predicted rows.

### Data quality step

Every table in the mapping below has **two** `DATA_QUALITY` jobs: one chained directly under its `INGESTION` job (measuring the plain table), and one under the `SINGLE_ETL` job (measuring the `<table>_etl` table). The plain-table metrics are therefore computed **only after the ingestion for that table finished successfully** — a failed or skipped ingestion means `run_etl.py DATA_QUALITY <table>` never runs (the metrics would otherwise be measured on stale data), and the etl-table metrics only after the `_etl` table was rebuilt. Since each job is the last step of its branch, a failure there stops that branch without affecting anything downstream.

Both jobs run in the same conda env and folder as ingestion (see `job.data_quality` in `config.json`); GEO_ETL additionally writes its own `logs/data_quality/data_quality_<table>_<timestamp>.log` per run (see [Usage — Logs](../usage.md#logs)). GEO_ETL picks its load type (`HISTORICAL` / `INCREMENTAL`) from the state of `geo.data_quality` — see [GEO_ETL — Data Quality job](../../../GEO_ETL/docs/etl_jobs/data-quality.md). Each normal `DATA_QUALITY` run also refreshes that table's whole-table `overall` snapshot in `geo.data_quality`, so the pipelines carry no separate `--overall` jobs and the aggregate stays in step with the dated rows automatically (see [GEO_ETL — Overall refresh](../../../GEO_ETL/docs/etl_jobs/data-quality.md#overall-refresh)).

### Data source file step

Every source also has a `DATA_SOURCE_FILE` job released directly by its `DOWNLOAD` job (a sibling of `INGESTION`), with `table_name` set to the source folder name (e.g. `NIFC`). It publishes that source's CSV inventory to `geo.data_source_files` for GEO_API; GEO_ETL picks `HISTORICAL` or `INCREMENTAL` from the state of that table. It has no successor, so like the `DATA_QUALITY` jobs it is a terminal branch that only the verdict gate waits on — see [GEO_ETL — DATA_SOURCE_FILE job](../../../GEO_ETL/docs/etl_jobs/data-source-file.md).

### Single ETL step

`SINGLE_ETL` is chained under the same `INGESTION` job as the plain-table `DATA_QUALITY` job (its sibling), with `table_name` set to the etl script name `<table>_etl`. It rebuilds `geo.<table>_etl` from the plain table and chains its own `DATA_QUALITY` job on that etl table — see [GEO_ETL — Single ETL job](../../../GEO_ETL/docs/etl_jobs/single-etl.md).

### Source → table mapping

| Source | Table | Source | Table |
|--------|-------|--------|-------|
| `Avalanche_ORG` | `avalanche_org` | `NIFC` | `nifc_fire` |
| `EONET` | `eonet_event` | `OpenMeteo_Flood` | `openmeteo_flood` |
| `GDACS` | `gdacs_event` | `USGS_Earthquake` | `usgs_earthquake` |
| `IBTrACS` | `ibtracs_cyclone` | `USGS_NWIS` | `usgs_nwis` |
| `USGS_Volcano` | `usgs_volcano` | | |

Each source additionally uses the etl table `<table>_etl` (e.g. `nifc_fire_etl`) for its `SINGLE_ETL` job and the `DATA_QUALITY` job chained under it.
