"""Airflow DAG `geo_pipeline`: every source chain (download -> ingestion -> data quality /
single ETL) from jobs.json in one DAG run, with the union job fanning in after all chains.
The cron comes from the scheduled jobs of jobs.json; every other knob (timezone, retries,
timeout, pools) from the `dag` block of config.json."""

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

from model import TaskSpec, build_task_specs, get_dag_settings, resolve_schedule
from util import log_memory_usage

_DAG_SETTINGS = get_dag_settings()
# The DAG's cron comes from the scheduled jobs of jobs.json (every one of them declares it)
_SCHEDULE = resolve_schedule()
_START = _DAG_SETTINGS.start_date
_TRIGGER_RULES = {
    "all_success": TriggerRule.ALL_SUCCESS,
    "all_done_min_one_success": TriggerRule.ALL_DONE_MIN_ONE_SUCCESS,
}
# Callbacks receive a RuntimeTaskInstance without a .log attribute — log into the
# task-configured "airflow.task" logger instead (its handler writes the task log)
_TASK_LOGGER = logging.getLogger("airflow.task")


def _log_memory(context) -> None:
    """Log system memory usage into the Airflow task log (on success and failure alike)."""
    task_instance = context["ti"]
    log_memory_usage(_TASK_LOGGER, task_instance.task_id)


def _make_task(spec: TaskSpec) -> BashOperator:
    """Build one BashOperator from a TaskSpec: cwd of the commands, pool by job type
    (config.json `dag.pools`), trigger rule from the spec and the memory callbacks."""
    return BashOperator(
        task_id=spec.task_id,
        bash_command=spec.command,
        cwd=str(_PROJECT_DIR),
        pool=_DAG_SETTINGS.pool_for(spec.job_type),
        trigger_rule=_TRIGGER_RULES[spec.trigger_rule],
        on_success_callback=_log_memory,
        on_failure_callback=_log_memory,
    )


@dag(
    dag_id=_DAG_SETTINGS.dag_id,
    schedule=_SCHEDULE,
    start_date=pendulum.datetime(_START.year, _START.month, _START.day, tz=_DAG_SETTINGS.timezone),
    catchup=_DAG_SETTINGS.catchup,
    max_active_runs=_DAG_SETTINGS.max_active_runs,
    tags=_DAG_SETTINGS.tags,
    default_args={
        "retries": _DAG_SETTINGS.retries,
        "execution_timeout": timedelta(minutes=_DAG_SETTINGS.execution_timeout_minutes),
    },
    doc_md="""
## geo_pipeline (E2E mega-DAG)

Nine source chains from `jobs.json` run as one DAG. The cron comes from the scheduled jobs
in `jobs.json` (each `DOWNLOAD` root declares it; all must share one cron — Airflow schedules
whole DAGs). Every other knob — timezone, retries, execution timeout, the download/etl pools
and the union source name — comes from the `dag` block of `config.json`; the pool slot counts
are seeded by `airflow-init` on every `docker compose up` (`docker/airflow/seed_pools.py`).

- A failed task skips its downstream jobs (`all_success` trigger rule) — same effect as the
  old `is_stop_if_error` chains, but visible as skipped in the UI.
- The union job (`Union.single_etl`) waits for the `ingestion.single_etl` task of **every**
  source with `all_done_min_one_success`: it starts as soon as the last single-ETL reached a
  terminal state, whatever the chains' results — **but only if at least one of them
  succeeded**. When all nine failed, the scheduler marks it `upstream_failed` (nothing fresh
  to stack); when all nine are skipped, it is skipped too. The DATA_QUALITY jobs are terminal
  steps with no successors (like in the old Celery chains).

Tasks are generated from the flat job registry in `jobs.json` via `model.build_task_specs()`;
the shell commands come from the `job` block of `config.json` and run in the conda environments
`geo_download_311` / `geo_etl_311`.
""",
)
def geo_pipeline() -> None:
    """Build every job of jobs.json as a BashOperator and wire the chain dependencies."""
    specs = build_task_specs(union_source=_DAG_SETTINGS.union_source)
    tasks = {spec.task_id: _make_task(spec) for spec in specs}
    for spec in specs:
        for upstream_id in spec.upstream:
            tasks[spec.task_id].set_upstream(tasks[upstream_id])


geo_pipeline()
