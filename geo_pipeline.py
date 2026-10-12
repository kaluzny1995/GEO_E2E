"""Airflow DAG `geo_pipeline`: the E2E mega-DAG built by `geo_dag.GEO_DAG` — nine source
subDAGs (download -> ingestion + data quality -> single_etl + data quality) plus the union
subDAG (single_etl + data quality), wired together by the union fan-in.
The cron comes from the scheduled jobs of jobs.json; every other knob (timezone, retries,
timeout, pools) from the `dag` block of config.json."""

from __future__ import annotations

import sys
from pathlib import Path

_PROJECT_DIR = Path(__file__).resolve().parent
# Make the sibling modules (geo_dag, model, util) importable even when this file is parsed
# from outside the DAGs folder
if str(_PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(_PROJECT_DIR))

from airflow.sdk import dag

from geo_dag import GEO_DAG

# Built once at import (parses jobs.json + config.json); the tasks are rendered on the @dag call
_GEO_DAG = GEO_DAG(project_dir=_PROJECT_DIR)


@dag(**_GEO_DAG.dag_kwargs())
def geo_pipeline() -> None:
    """Render every subDAG of `jobs.json` into this DAG and wire the union fan-in."""
    _GEO_DAG.to_airflow()


# Calling the decorated function creates the DAG — this module-level call is what Airflow discovers
geo_pipeline()
