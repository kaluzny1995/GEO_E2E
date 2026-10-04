← [README](../README.md)

# Usage

## Start the Celery worker and beat scheduler

Both processes must run simultaneously in **separate terminals**. The worker executes tasks; the beat sends them on schedule.

**Terminal 1 — Worker** (executes the job chains):

```bash
conda run -n geo_e2e_311 celery -A app.geo_app worker --loglevel=info --concurrency=2
```

> `--concurrency=2` limits the worker to 2 simultaneous pipelines (default is # of CPU cores). Combined with `worker_max_memory_per_child` and `task_time_limit` configured in `config.json`, this prevents memory exhaustion from piled-up jobs.

**Terminal 2 — Beat** (sends tasks on schedule):

```bash
conda run -n geo_e2e_311 celery -A app.geo_app beat --loglevel=info
```

Or run both in one command (worker in background, beat in foreground):

```bash
conda run -n geo_e2e_311 bash -c "celery -A app.geo_app worker --loglevel=info --concurrency=2 & celery -A app.geo_app beat --loglevel=info"
```

> **Important:** Always start the worker **before** the beat, or at the same time. If the beat fires with no worker online, the task will be queued in RabbitMQ and executed once a worker connects.

## Run a chain manually

Useful for testing a single source without waiting for the schedule:

```bash
conda run -n geo_e2e_311 python -c "from app import geo_e2e; geo_e2e.delay('NIFC')"
```

The chain runs in the worker (which must be running) and writes to `logs/NIFC/celery_NIFC_<timestamp>.log`.

> Run it from the project directory: importing `app` also runs `celerybeat_cleanup()`, which deletes the `celerybeat-schedule*` files, so prefer this while the beat is stopped (otherwise just wait for the next scheduled run).

## Logs

Each chain writes one timestamped file in this project, and each step writes its own in the sibling project:

```
logs/<source>/celery_<source>_<timestamp>.log                        # GEO_E2E — whole chain (step output streamed in)
../GEO_Download/logs/<source>/download_<source>_<timestamp>.log      # GEO_Download — DOWNLOAD step
../GEO_ETL/logs/ingestion/ingestion_<table>_<timestamp>.log          # GEO_ETL — INGESTION step
../GEO_ETL/logs/data_quality/data_quality_<table>_<timestamp>.log    # GEO_ETL — DATA_QUALITY step (plain and _etl table)
../GEO_ETL/logs/single_etl/single_etl_<table>_etl_<timestamp>.log    # GEO_ETL — SINGLE_ETL step
```

### Example log output

Excerpt from `logs/NIFC/celery_NIFC_2026-10-04T20:31:07.324061.log` (the subprocess output of each step is streamed into the same file):

```
2026-10-04 20:31:07,324 INFO ---- Celery triggered E2E job chain for NIFC ----
2026-10-04 20:31:07,324 INFO Worker Memory: 21.57 out of 62.50 GiB (34.50 %) used
2026-10-04 20:31:07,325 INFO Starting DOWNLOAD job for "NIFC".
2026-10-04 20:31:07,325 INFO Executing command: cd ..;cd GEO_Download;conda run --no-capture-output -n geo_download_311 python run_download.py NIFC
---- NIFC Downloader ----
...
2026-10-04 20:31:11,836 INFO Finished DOWNLOAD job for "NIFC". Return code: 0.
2026-10-04 20:31:11,836 INFO Starting INGESTION job for "NIFC".
2026-10-04 20:31:11,836 INFO Executing command: cd ..;cd GEO_ETL;conda run --no-capture-output -n geo_etl_311 python run_etl.py INGESTION nifc_fire
...
2026-10-04 20:32:03,342 INFO Finished INGESTION job for "NIFC". Return code: 0.
2026-10-04 20:32:03,342 INFO Starting DATA_QUALITY job for "NIFC".
2026-10-04 20:32:03,342 INFO Executing command: cd ..;cd GEO_ETL;conda run --no-capture-output -n geo_etl_311 python run_etl.py DATA_QUALITY nifc_fire
...
2026-10-04 20:33:08,509 INFO Finished DATA_QUALITY job for "NIFC". Return code: 0.
2026-10-04 20:33:08,509 INFO Starting SINGLE_ETL job for "NIFC".
2026-10-04 20:33:08,509 INFO Executing command: cd ..;cd GEO_ETL;conda run --no-capture-output -n geo_etl_311 python run_etl.py SINGLE_ETL nifc_fire_etl
...
2026-10-04 20:33:43,018 INFO Finished SINGLE_ETL job for "NIFC". Return code: 0.
2026-10-04 20:33:43,019 INFO Starting DATA_QUALITY job for "NIFC".
2026-10-04 20:33:43,019 INFO Executing command: cd ..;cd GEO_ETL;conda run --no-capture-output -n geo_etl_311 python run_etl.py DATA_QUALITY nifc_fire_etl
...
2026-10-04 20:34:47,667 INFO Finished DATA_QUALITY job for "NIFC". Return code: 0.
2026-10-04 20:34:47,668 INFO Worker Memory: 21.87 out of 62.50 GiB (35.00 %) used
```

The plain-table `DATA_QUALITY` job and the `SINGLE_ETL` job are siblings under `INGESTION`, so skipping is per job: if `DATA_QUALITY nifc_fire` fails, only its own branch is skipped and `SINGLE_ETL nifc_fire_etl` still runs.

A non-zero return code is logged as a warning instead of `Finished ... Return code: 0.`, and the `next_jobs` are skipped. Every job in the shipped `jobs.json` has `is_failed_if_error: false` **and** `is_stop_if_error: true`, so the Celery task still finishes as SUCCESS:

```
2026-09-26 15:03:59,854 WARNING DOWNLOAD job for "NIFC" exited with return code 1 — next jobs will not be executed
```

With `is_failed_if_error: true` the same failure logs `ERROR DOWNLOAD job for "NIFC" exited with return code 1` and raises `CalledProcessError`, so the Celery task ends as FAILURE; with both flags `false` the warning reads `WARNING … — continuing with next jobs` and the chain proceeds.
