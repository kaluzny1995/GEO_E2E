← [README](../README.md)

# Usage

## Start the Airflow stack

> **Prerequisite:** the `.env` file must exist in the project root — `docker compose` fails fast with `required variable GEO_ROOT is missing` otherwise. Create it once per machine: `cp .env.example .env` and adjust the six values, including the Airflow UI login (see [README — Setup](../README.md#2-configure-environment-variables-mandatory)).

Everything runs inside Docker Compose — no local worker/beat processes:

```bash
docker compose up -d --build
```

This builds the Airflow image (first run only) and starts Postgres, the one-shot `airflow-init` (DB migration + admin user), the DAG processor, the scheduler and the API server, plus the Spark cluster. Follow the scheduler to watch task execution:

```bash
docker compose logs -f airflow-scheduler
```

> The Airflow containers use `network_mode: host`, so task subprocesses reach Spark at `spark://localhost:7078` and your host conda envs via their bind-mounted absolute paths. See [README — Setup](../README.md#3-start-services-with-docker-compose).

## The web UI

Open **http://localhost:8090** and log in with `airflow_admin` / `airflow_admin` (the `_AIRFLOW_WWW_USER_*` values from `.env`):

| Page | Use it for |
|------|------------|
| **DAGs** | `geo_pipeline` is unpaused from the start (`dags_are_paused_at_creation=false`); see next/last run |
| **Grid** view of a run | per-task state: green = success, red = failed, grey = skipped (downstream of a failure), light green = running |
| **Graph** view | the full task graph — 46 tasks: 9 chains of 5 + the union fan-in |
| **Gantt** view | task durations over the run |
| Task log | the full stdout/stderr of the task's `conda run` subprocess (also includes the memory callback line) |
| **Trigger DAG** | run the pipeline manually, without waiting for the 02:00 schedule |
| **Active/Paused** toggle | turn the daily 02:00 schedule on/off (manual triggers still work while paused) |
| Failed task → **Clear** | re-run just that task (and its downstream) after a fix — no marker files to clean up |
| **Admin → Pools** | inspect the `download` (3 slots) and `etl` (2 slots) pools — seeded automatically by `airflow-init` |

## Run the pipeline manually

**Whole DAG** — **Trigger DAG** button in the UI (top right of the DAG details), or via the CLI container:

```bash
docker compose run --rm airflow-cli dags trigger geo_pipeline
```

**A single job** — two options:

- **Re-run inside an existing run** — open the run in the UI, click the task and press **Clear**: only that task (plus the downstream tasks cleared with it) runs again; its upstreams stay untouched. This is the normal way to retry one failed job after a fix (see also [Logs](#logs) below).
- **Standalone, without recording a run** — `tasks test` executes one task once in a temporary run, keeping nothing in the metadata DB (useful for debugging a job in isolation; the memory callback still writes to the output):

```bash
docker compose run --rm airflow-cli tasks test geo_pipeline NIFC.download
```

## Pause / unpause the schedule

The **Active/Paused** toggle next to `geo_pipeline` on the **DAGs** page turns the daily 02:00 schedule off/on. Same via the CLI container:

```bash
docker compose run --rm airflow-cli dags pause geo_pipeline     # schedule off
docker compose run --rm airflow-cli dags unpause geo_pipeline   # schedule on
```

Pausing stops only *scheduled* runs — a manual **Trigger DAG** still works while paused, and already-running runs finish normally.

Useful debugging commands (the `airflow-cli` service is behind the `debug` compose profile):

```bash
docker compose run --rm airflow-cli dags list                  # confirm geo_pipeline is discovered
docker compose run --rm airflow-cli dags list-import-errors    # empty = the DAG file parses cleanly
docker compose run --rm airflow-cli tasks states-for-dag-run geo_pipeline <run_id>
docker compose run --rm airflow-cli pools list
```

## Logs

Every task's output is the Airflow task log (web UI → task → **Log**), stored on the shared `airflow_data` volume under `logs/dag_id=geo_pipeline/...`. Each step additionally writes its own log in the sibling project:

```
<Airflow UI> → geo_pipeline → <run> → <task> → Log        # GEO_E2E — subprocess output + memory callback
../GEO_Download/logs/<source>/download_<source>_<timestamp>.log      # GEO_Download — DOWNLOAD step
../GEO_ETL/logs/ingestion/ingestion_<table>_<timestamp>.log          # GEO_ETL — INGESTION step
../GEO_ETL/logs/data_quality/data_quality_<table>_<timestamp>.log    # GEO_ETL — DATA_QUALITY step (plain and _etl table)
../GEO_ETL/logs/single_etl/single_etl_<table>_etl_<timestamp>.log    # GEO_ETL — SINGLE_ETL step
```

### Example log output

Excerpt of the `NIFC.ingestion` task log (the subprocess output of the step is streamed into the same log):

```
[2026-10-04 20:31:11] {bash.py:300} INFO - Running command: cd ..;cd GEO_ETL;conda run --no-capture-output -n geo_etl_311 python run_etl.py INGESTION nifc_fire
[2026-10-04 20:31:11] {bash.py:301} INFO - cwd: <GEO_ROOT>/GEO_E2E
...
2026-10-04 20:31:15,642 INFO ---- Ingestion job: nifc_fire ----
...
[2026-10-04 20:32:03] {bash.py:310} INFO - Command exited with return code 0
[2026-10-04 20:32:03] {taskinstance.py:2811} INFO - NIFC.ingestion Memory: 21.57 out of 62.50 GiB (34.50 %) used
```

A non-zero return code fails the task (`Command exited with return code 1` + red state in the UI), and everything chained below it is skipped — e.g. a failed `NIFC.download` skips `NIFC.ingestion` and its whole subtree, while `GDACS.download` is unaffected. The union task still runs once every chain reached a terminal state (`all_done`).

After a fix, open the failed task in the UI and press **Clear** — only that task and its downstream re-run.
