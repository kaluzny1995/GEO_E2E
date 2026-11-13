← [Configuration](../configuration.md)

# DAG Construction & Settings

This page covers the registry-to-Airflow pipeline, the `dag` block of `config.json`, the
schedule and how to add a new source chain.

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

> **`task_id`s are data — declared, not derived.** Each job carries its own `task_id` (see [Task ids](job-registry.md#task-ids)), so two sibling jobs of the same type under one parent no longer collide by construction: give them distinct ids (e.g. `NIFC.data_quality` and `NIFC_etl.data_quality`) and keep them unique — `build_task_specs()` raises `ValueError` on a duplicate or malformed id, and the generation tests in `test/test_dags.py` assert uniqueness across the shipped registry.

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
- Jobs referenced from the union root wait for it with the normal `all_success` rule (shipped: `DATA_QUALITY-union_etl`, `VIEW-metrics_observed_vw`, the nine `VIEW-phenomenons_*_vw` jobs, and the downstream `PREDICTION-prediction_etl` → eight `VIEW-metrics_*_vw` chain).
- `jobs.json` must always contain the `Union` job — `build_task_specs()` raises `ValueError` at parse time otherwise, which surfaces as a DAG import error in the UI until it is back.

### Adding a step under the union

The shipped registry already chains a `DATA_QUALITY` job, the observed/phenomenon views and the prediction/metric-view branch under the union root (`DATA_QUALITY-union_etl`, `VIEW-metrics_observed_vw`, `VIEW-phenomenons_*_vw` and the downstream `PREDICTION-prediction_etl` → `VIEW-metrics_*_vw` chain). Further steps follow the same recipe: add the job object and reference it from the union job's `next_jobs` — the new task waits for its parent with `all_success` and runs in the `etl` pool, no change to `geo_pipeline.py`, `geo_dag.py` or `model.py` needed (a new job *type* additionally needs an `EJobType` member and its `config.json` blocks — see `VIEW`).

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
| `pools` | download/etl | Airflow pool per job type (`DOWNLOAD` → `download`; `INGESTION`, `DATA_QUALITY`, `DATA_SOURCE_FILE`, `SINGLE_ETL`, `VIEW`, `PREDICTION` and the synthesized `VERDICT` task → `etl`) |
| `pool_slots` | 3 / 2 | Slot count per pool — seeded by `airflow-init` on every `docker compose up` via `docker/geo_airflow_spark/seed_pools.py` (idempotent) |

You can inspect the pools under **Admin → Pools** or via `docker compose run --rm airflow-cli pools list`. The DAG itself is created unpaused (`dags_are_paused_at_creation=false` in `docker-compose.yaml`; `airflow-dag-activate` also repairs an already-paused DAG).

## Modifying the schedule

Edit the `schedule` field on the scheduled jobs in `jobs.json` — the nine `DOWNLOAD` objects (they must all carry the **same** cron: Airflow schedules the whole DAG, and `model.resolve_schedule()` rejects differing crons). The `timezone` stays in the `dag` block of `config.json`. The DAG processor picks the change up automatically within ~30 seconds — no restart needed.

### Adding a new data source

1. Add six job objects to the `jobs.json` array for the new `source_name`: `DOWNLOAD-<Source>` — carrying the same `schedule` cron as the other downloads — referencing both `INGESTION-<table>` and `DATA_SOURCE_FILE-<Source>`; `INGESTION` references both `DATA_QUALITY-<table>` and `SINGLE_ETL-<table>_etl` — the latter referencing `DATA_QUALITY-<table>_etl` (all downstreamed, so no `schedule` field; `DATA_SOURCE_FILE` has no successor). The placement and dependency rules are spelled out in [Constructing the DAG](#constructing-the-dag).
2. Make sure the source exists in GEO_Download's `config.json` and that the table exists in GEO_ETL.
3. Wait for the DAG to re-parse (or trigger a refresh in the UI) — the new tasks appear automatically.

The union fan-in needs no change: `build_task_specs()` gives the union root the `SINGLE_ETL` task (`<source>.single_etl`) of **every** non-union chain as upstreams, so the new source is covered automatically.
