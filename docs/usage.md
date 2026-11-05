← [README](../README.md)

# Usage

## Start the stack

> **Prerequisite:** `.env` must exist in the project root with the five `SUPABASE_DB_*` values. Airflow defaults to `airflow_admin` / `airflow_admin`; override `_AIRFLOW_WWW_USER_USERNAME` / `_AIRFLOW_WWW_USER_PASSWORD` only if needed. Create `.env` with `cp .env.example .env` (or `./setup.sh`) — see [README — Dockerizing the solution](../README.md#1-configure-credentials--mandatory-before-docker-compose-up).

Everything runs inside Docker Compose — no local worker/beat processes:

```bash
./build-images.sh     # serial builds; safer on a laptop
docker compose up -d
```

`docker compose up -d --build` works too, but builds independent images in parallel and can spike RAM on the first run.

This builds the three images (first run only) and starts the one-shot `airflow-init` (Supabase schema creation, DB migration, admin user, pool seeding, `.env` refresh), the DAG processor, the scheduler, the API server, the DAG activation job, the Spark cluster, GEO_API and GEO_APP. Follow the scheduler to watch task execution:

```bash
docker compose logs -f airflow-scheduler
docker compose ps
docker stats --no-stream     # every container stays inside its memory limit
```

> On a laptop build the images serially with `./build-images.sh` instead of `docker compose build`; the Anaconda-based images can otherwise run two dependency solvers at once. See [README — Memory and resource limits](../README.md#3-memory-and-resource-limits).

> The images are self-contained. Airflow task subprocesses use the image's `/opt/conda` environments and reach Spark at `spark://geo-spark-master:7078` over the Compose bridge network. The Spark UI is published at 8083; the Airflow UI at 8090. The host `GEO_Download/data` folder is bind-mounted at `/opt/geo/GEO_Download/data`, so downloaded CSVs are immediately visible in the local repo. See [README — Dockerizing the solution](../README.md#2-build-and-start-the-stack).

### Rebuild after a repository change

```bash
docker compose build                 # rebuild changed images
docker compose up -d                 # recreate containers
```

Use `./build-images.sh` when RAM is tight (serial builds). `docker compose restart` does not rebuild an image. See [README — Rebuilding](../README.md#5-rebuilding-after-repository-changes).

> **Troubleshooting — services crash-looping with `ModuleNotFoundError: No module named 'airflow'`:** this comes from the entrypoint's `airflow db check` when `docker-compose.yaml` sets `HOME: /opt/airflow`. The `airflow` console script resolves its packages through the *HOME* user-site, while the image installs Airflow in `/home/airflow/.local`; the entrypoint only self-corrects `HOME` on a container's **first** start (when its uid is still missing from `/etc/passwd`), so every *restarted* service dies. Do not add a `HOME` override to the compose services — `AIRFLOW_HOME: /opt/airflow` alone keeps data/logs on the volume (see the comment in `docker-compose.yaml`).

> **Troubleshooting — the host froze or became unresponsive:** this stack used to run Airflow's `LocalExecutor` at its default 32 workers and had no container memory limits, so a PySpark driver or conda task could exhaust host RAM. The current Compose file caps every service and sets `memswap_limit` equal to `mem_limit`. If it still happens: run `docker stats --no-stream` to find the container, lower `AIRFLOW_SCHEDULER_MEM_LIMIT` / `SPARK_WORKER_MEM_LIMIT` / `AIRFLOW_PARALLELISM` in `.env`, and `docker compose down` when the pipeline is idle. The first Anaconda build is the other peak — use `./build-images.sh`.

## The web UI

Open **http://localhost:8090** and log in with `airflow_admin` / `airflow_admin` (the `_AIRFLOW_WWW_USER_*` values from `.env`). The API is at **http://localhost:8000** (`/docs` for Swagger) and the dashboard at **http://localhost:3000**:

| Page | Use it for |
|------|------------|
| **DAGs** | `geo_pipeline` is unpaused from the start (`dags_are_paused_at_creation=false`); see next/last run |
| **Grid** view of a run | per-task state: green = success, red = failed, grey = skipped (downstream of a failure), light green = running |
| **Graph** view | the full task graph — 74 tasks: 9 chains of 6 + the union chain (fan-in + data quality + 17 views) + the `Union.verdict` task |
| **Gantt** view | task durations over the run |
| Task log | the full stdout/stderr of the task's `conda run` subprocess (also includes the memory callback line) |
| **Trigger DAG** | run the pipeline manually, without waiting for the 08:00 schedule |
| **Active/Paused** toggle | turn the daily 08:00 schedule on/off (manual triggers still work while paused) |
| Failed task → **Clear** | re-run just that task (and its downstream) after a fix — no marker files to clean up |
| **Admin → Pools** | inspect the `download` (3 slots) and `etl` (2 slots) pools — seeded automatically by `airflow-init` |

## Run the pipeline manually

**Whole DAG** — **Trigger DAG** button in the UI (top right of the DAG details), or via the CLI container:

```bash
docker compose run --rm airflow-cli dags trigger geo_pipeline
```

The run's own status is decided by `Union.verdict`, the last task of the graph: once every
other task reached a terminal state it reads `Union.single_etl`'s state of the run and exits 0
only when the stack finished `success`. So a run is green iff the union stack succeeded — a red
`DATA_QUALITY`/`VIEW` branch stays red as a task but does not fail the run, while a failed
download or source single-ETL does. To re-rate a run after fixing the union task, **Clear**
`Union.verdict` in the grid. (Mind that `tasks test geo_pipeline Union.verdict` exits 1 unless
the tested run already holds a *successful* `Union.single_etl` task instance — it reads that
state from the metadata DB.)

**A single job** — two options:

- **Re-run inside an existing run** — open the run in the UI, click the task and press **Clear**: only that task (plus the downstream tasks cleared with it) runs again; its upstreams stay untouched. This is the normal way to retry one failed job after a fix (see also [Logs](#logs) below).
- **Standalone, without recording a run** — `tasks test` executes one task once in a temporary run, keeping nothing in the metadata DB (useful for debugging a job in isolation; the memory callback still writes to the output):

```bash
docker compose exec airflow-scheduler airflow tasks test geo_pipeline NIFC.download
```

Run task tests in the **scheduler** container rather than the `airflow-cli` debug service. `airflow-cli` overrides the image entrypoint, so its runtime UID has no passwd entry; Spark's Hadoop login then fails with `LoginException: invalid null input: name` for every ETL task. Downloads work either way, but the scheduler command is the one that covers both.

## Pause / unpause the schedule

The **Active/Paused** toggle next to `geo_pipeline` on the **DAGs** page turns the daily 08:00 schedule off/on. Same via the CLI container:

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
../GEO_ETL/logs/data_source_file/data_source_file_<source>_<timestamp>.log # GEO_ETL — DATA_SOURCE_FILE step
../GEO_ETL/logs/single_etl/single_etl_<table>_etl_<timestamp>.log    # GEO_ETL — SINGLE_ETL step
```

### Example log output

Excerpt of the `NIFC.ingestion` task log. The step's own logger writes to its `GEO_ETL` log file **and** mirrors to stdout, so the job output (`---- Ingestion job: nifc_fire ----`, `Connected to Spark`, …) is streamed live into the same Airflow task log:

```
Running command: cd ..;cd GEO_ETL;conda run --no-capture-output -n geo_etl_311 python run_etl.py INGESTION nifc_fire
cwd: /opt/geo/GEO_E2E
Output:
2026-10-10 20:58:21,855 INFO ---- Ingestion job: nifc_fire ----
2026-10-10 20:58:25,922 INFO Connected to Spark: spark://geo-spark-master:7078
2026-10-10 20:58:25,924 INFO Spark version: 3.5.5
...
Command exited with return code 0
NIFC.ingestion Memory: 21.57 out of 62.50 GiB (34.50 %) used
```

For ETL steps, the JVM's `[spark]` stderr lines and the stage-memory lines are appended to stdout after the job finishes (they only exist once the log merge runs), so they appear at the end of the task log; the `GEO_ETL` `.log` file keeps the fully merged chronological view.

A non-zero return code fails the task (`Command exited with return code 1` + red state in the UI), and everything chained below it is skipped — e.g. a failed `NIFC.download` skips `NIFC.ingestion`, `NIFC.data_source_file` and their whole subtrees, while `GDACS.download` is unaffected. The union task still runs once every chain reached a terminal state — provided at least one single-ETL succeeded; when all nine failed, it is marked `upstream_failed` without running. A source whose upstream has no new data yet (e.g. the IBTrACS `last3years` file trails the present by 1–2 days) exits non-zero and shows red by design — it is not masked.

After a fix, open the failed task in the UI and press **Clear** — only that task and its downstream re-run.
