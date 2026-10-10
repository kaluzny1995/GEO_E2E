"""Airflow DAG `geo_pipeline`: every source chain (download -> ingestion -> data quality /
single ETL) from jobs.json in one DAG run, with the union job fanning in after all chains."""

from __future__ import annotations

import logging
import sys
from datetime import timedelta
from pathlib import Path

_PROJECT_DIR = Path(__file__).resolve().parents[1]
if str(_PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(_PROJECT_DIR))

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag
from airflow.task.trigger_rule import TriggerRule

from model import EJobType, TaskSpec, build_task_specs
from util import log_memory_usage

_TRIGGER_RULES = {
    "all_success": TriggerRule.ALL_SUCCESS,
    "all_done": TriggerRule.ALL_DONE,
}
# Downloads are throttled by the "download" pool (3 slots); everything else runs in "etl" (2 slots)
_POOLS = {EJobType.DOWNLOAD: "download"}
# Callbacks receive a RuntimeTaskInstance without a .log attribute — log into the
# task-configured "airflow.task" logger instead (its handler writes the task log)
_TASK_LOGGER = logging.getLogger("airflow.task")


def _log_memory(context) -> None:
    """Log system memory usage into the Airflow task log (on success and failure alike)."""
    task_instance = context["ti"]
    log_memory_usage(_TASK_LOGGER, task_instance.task_id)


def _make_task(spec: TaskSpec) -> BashOperator:
    """Build one BashOperator from a TaskSpec: cwd of the commands, pool by job type,
    trigger rule from the spec and the memory callbacks."""
    return BashOperator(
        task_id=spec.task_id,
        bash_command=spec.command,
        cwd=str(_PROJECT_DIR),
        pool=_POOLS.get(spec.job_type, "etl"),
        trigger_rule=_TRIGGER_RULES[spec.trigger_rule],
        on_success_callback=_log_memory,
        on_failure_callback=_log_memory,
    )


@dag(
    dag_id="geo_pipeline",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Warsaw"),
    catchup=False,
    max_active_runs=1,
    tags=["geo", "e2e"],
    default_args={"retries": 0, "execution_timeout": timedelta(minutes=30)},
    doc_md="""
## geo_pipeline (E2E mega-DAG)

Nine source chains from `jobs.json` run as one DAG:

- Every chain starts with `DOWNLOAD` at 02:00 (Europe/Warsaw); the `download` pool (3 slots)
  and `etl` pool (2 slots) throttle concurrency.
- A failed task skips its downstream jobs (`all_success` trigger rule) — same effect as the
  old `is_stop_if_error` chains, but visible as skipped in the UI.
- The union job (`Union.single_etl`) waits for the `ingestion.single_etl` task of **every**
  source with `all_done`, so it stacks the fresh `*_etl` tables as soon as the last single-ETL
  finished, whatever the chains' results — the DATA_QUALITY jobs are terminal steps with no
  successors (like in the old Celery chains).
- Tasks have no retries and a 30 minute execution timeout (parity with the old Celery time limit).

Tasks are generated from `jobs.json` via `model.build_task_specs()`; the shell commands come
from the `job` block of `config.json` and run in the conda environments `geo_download_311` /
`geo_etl_311`.
""",
)
def geo_pipeline() -> None:
    """Build every job of jobs.json as a BashOperator and wire the chain dependencies."""
    specs = build_task_specs()
    tasks = {spec.task_id: _make_task(spec) for spec in specs}
    for spec in specs:
        for upstream_id in spec.upstream:
            tasks[spec.task_id].set_upstream(tasks[upstream_id])


geo_pipeline()
