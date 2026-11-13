← [README](../README.md)

# Configuration Reference

`config.json` defines **how** each job type is executed (the `job` block) and holds the DAG-level runtime knobs (the `dag` block: retries, pools, ...); `jobs.json` defines **which** jobs each source runs, how they depend on each other, and which of them are scheduled (each scheduled job carries its own `schedule`). The DAG file `geo_pipeline.py` only turns those configs into Airflow tasks. Runtime values — the Airflow UI login and the Supabase credentials of Airflow's metadata DB — live outside both, in the git-ignored `.env`; the Docker containers receive them from the Compose `environment:` section (see [Supabase metadata DB](configuration/environment.md#supabase-metadata-db-env)).

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
| `prediction`  | `GEO_ETL`      | `run_etl.py`    | `geo_etl_311`     |

| Field           | Description                                                        |
|-----------------|--------------------------------------------------------------------|
| `folder_name`   | Name of the sibling project directory                              |
| `python_script` | Python script to execute (e.g. `run_download.py`)                  |
| `conda_env`     | Optional. Conda env to run in (`conda run --no-capture-output -n <env>`); each sibling project runs in its own env (`GEO_Download` → `geo_download_311`, every `run_etl.py` step → `geo_etl_311`), so the Airflow image does not need PySpark or the downloader's dependencies |

## Detailed references

| Page | Contents |
|------|----------|
| [Job registry & task dependencies](configuration/job-registry.md) | `jobs.json` fields, task ids, union fan-in, view/prediction steps, data-quality/source steps and the source → table mapping. |
| [DAG construction & settings](configuration/dag.md) | How jobs become Airflow tasks, dependency rules, the `dag` block, schedule changes and adding a source. |
| [Environment, credentials & resource limits](configuration/environment.md) | `GEO_DATA_DIR`, `AIRFLOW_UID`, Supabase metadata DB, preflight checks, `.env` walkthrough and memory limits. |
