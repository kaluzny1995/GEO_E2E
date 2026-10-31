"""Airflow rendering of the job registry: `JobsSubDAG` (one chain) and `GEO_DAG` (the mega-DAG).

The flat registry of `jobs.json` is resolved by `model.build_task_specs()`; this module groups
the resulting task specs into subDAGs — one per data source plus the union chain — and converts
them into Apache Airflow structures: operators and dependencies (`JobsSubDAG.to_airflow()` /
`GEO_DAG.to_airflow()`) and the DAG-level keywords (`GEO_DAG.dag_kwargs()`).

The subDAGs are code-level groupings only: each one renders its tasks into the single
`geo_pipeline` DAG (Airflow 3 dropped `SubDagOperator`), so task ids and the task graph are
exactly the ones of the flat spec list."""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from datetime import timedelta
from functools import cached_property
from pathlib import Path
from typing import Any

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.task.trigger_rule import TriggerRule

from model import DagSettings, EJobType, TaskSpec, build_task_specs, get_dag_settings, resolve_schedule
from util import log_memory_usage

# Trigger-rule names of a TaskSpec -> Airflow enum, so model.py needs no Airflow imports
_TRIGGER_RULES = {
    "all_success": TriggerRule.ALL_SUCCESS,
    "all_done_min_one_success": TriggerRule.ALL_DONE_MIN_ONE_SUCCESS,
    "all_done": TriggerRule.ALL_DONE,
}
# Callbacks receive a RuntimeTaskInstance without a .log attribute — log into the
# task-configured "airflow.task" logger instead (its handler writes the task log)
_TASK_LOGGER = logging.getLogger("airflow.task")

_DOC_MD = """
## geo_pipeline (E2E mega-DAG)

One Airflow DAG built from ten code-level subDAGs (`geo_dag.JobsSubDAG`): the nine source
chains of `jobs.json` (download -> ingestion + data quality -> single ETL + data quality) and
the union chain. The cron comes from the scheduled jobs in `jobs.json` (each `DOWNLOAD` root
declares it; all must share one cron — Airflow schedules whole DAGs). Every other knob —
timezone, retries, execution timeout, the download/etl pools and the union source name — comes
from the `dag` block of `config.json`; the pool slot counts are seeded by `airflow-init` on
every `docker compose up` (`docker/airflow/seed_pools.py`).

- The subDAGs organize the code only: every one renders its tasks into this same DAG, so the
  grid still shows the flat chains of `jobs.json`.
- A failed task skips its downstream jobs (`all_success` trigger rule) — same effect as the
  old `is_stop_if_error` chains, but visible as skipped in the UI.
- The dependencies *between* subDAGs are the union fan-in and the verdict task's edges. The
  union fan-in: the union root (`Union.single_etl`)
  waits for the `single_etl` task — the last job — of **every** source subDAG with
  `all_done_min_one_success`: it starts as soon as the last single-ETL reached a terminal
  state, whatever the chains' results — **but only if at least one of them succeeded**. When
  all nine failed, the scheduler marks it `upstream_failed` (nothing fresh to stack); when all
  nine are skipped, it is skipped too. It stacks the nine `*_etl` tables into `geo.union_etl`,
  then runs `Union.single_etl.data_quality` — the union chain's terminal DATA_QUALITY step,
  like every other DATA_QUALITY job of the registry — plus the seventeen `Union.single_etl.view.*`
  tasks (siblings of that step, `all_success`, `etl` pool), each `run_etl.py VIEW <view_name>`
  refreshing one view over the freshly stacked `geo.union_etl`.
- The run's state is decided by one task: the synthesized verdict task (`Union.verdict`). It
  waits for every other task with `all_done`, reads `Union.single_etl`'s state of the run via
  `ti.get_task_states()` and exits 0 only when it finished `success`. As the DAG's **only leaf**
  (Airflow derives a run's success/failure from its leaf tasks) it alone turns the grid
  green/red: a failed download or source single-ETL still fails the run (the union root gets
  `upstream_failed`), a failed DATA_QUALITY, VIEW or other non-fan-in branch does not.

Tasks come from the flat job registry in `jobs.json` via `model.build_task_specs()`; the shell
commands come from the `job` block of `config.json` and run in the conda environments
`geo_download_311` / `geo_etl_311`.
"""


def _log_memory(context) -> None:
    """Log system memory usage into the Airflow task log (on success and failure alike)."""
    task_instance = context["ti"]
    log_memory_usage(_TASK_LOGGER, task_instance.task_id)


class JobsSubDAG:
    """One chain of `jobs.json` — a single data source's jobs or the union chain — as Airflow tasks.

    A subDAG holds a *partial* view of the flat registry: the `TaskSpec`s of exactly one
    `source_name`, converted into `BashOperator`s and wired to their parents (`to_airflow()`).
    Dependencies that leave the subDAG — the union fan-in and the verdict task's edges — are
    wired afterwards by `wire_upstreams_from()`, once every subDAG of the mega-DAG exists.

    Rendering must happen inside a DAG context (the `@dag` body of `geo_pipeline.py`):
    operators attach to the current DAG on creation, which is also what makes the
    `set_upstream()` calls legal.
    """

    def __init__(self, specs: Sequence[TaskSpec], *, dag_settings: DagSettings, project_dir: Path) -> None:
        if not specs:
            raise ValueError("a JobsSubDAG needs at least one task spec")
        source_names = {spec.source_name for spec in specs}
        if len(source_names) != 1:
            raise ValueError(f"a JobsSubDAG holds the tasks of one source only, got: {', '.join(sorted(source_names))}")
        task_ids = [spec.task_id for spec in specs]
        if len(task_ids) != len(set(task_ids)):
            raise ValueError(f'a JobsSubDAG of "{next(iter(source_names))}" has duplicate task ids')
        self._specs = tuple(specs)
        self._dag_settings = dag_settings
        self._project_dir = project_dir
        self._task_ids = frozenset(task_ids)

    @property
    def source_name(self) -> str:
        """The `source_name` shared by every job of this subDAG (`NIFC`, ..., `Union`)."""
        return self._specs[0].source_name

    @property
    def specs(self) -> tuple[TaskSpec, ...]:
        """The task specs this subDAG renders (jobs.json file order)."""
        return self._specs

    @property
    def task_ids(self) -> frozenset[str]:
        """The task ids rendered by this subDAG."""
        return self._task_ids

    @property
    def single_etl_task_ids(self) -> tuple[str, ...]:
        """The subDAG's SINGLE_ETL task ids — its "last job", the hook the union fan-in waits on."""
        return tuple(spec.task_id for spec in self._specs if spec.job_type == EJobType.SINGLE_ETL)

    def to_airflow(self) -> dict[str, BashOperator]:
        """Build one BashOperator per task spec and wire the dependencies inside this subDAG.

        Upstreams rendered by *another* subDAG are left to `wire_upstreams_from()`.
        """
        tasks = {spec.task_id: self._make_task(spec) for spec in self._specs}
        # Wire only the parents rendered inside this subDAG — foreign ids are wired by wire_upstreams_from()
        for spec in self._specs:
            upstreams = [tasks[upstream_id] for upstream_id in spec.upstream if upstream_id in self._task_ids]
            if upstreams:
                tasks[spec.task_id].set_upstream(upstreams)
        return tasks

    def wire_upstreams_from(self, tasks: Mapping[str, BashOperator]) -> None:
        """Wire this subDAG's tasks to parents rendered by other subDAGs (fan-in and verdict).

        Every upstream id of a task spec belongs either to this subDAG or to another one; the
        foreign ones are connected here, keeping the trigger rule of their spec — the union
        root's `all_done_min_one_success` makes it run only when at least one of the source
        `single_etl` tasks succeeded, the verdict task's `all_done` waits for everything.

        Raises:
            ValueError: When a foreign upstream id is not rendered by any subDAG.
        """
        for spec in self._specs:
            # Upstreams outside this subDAG = the cross-subDAG edges (today: the union fan-in)
            foreign = [upstream_id for upstream_id in spec.upstream if upstream_id not in self._task_ids]
            missing = [upstream_id for upstream_id in foreign if upstream_id not in tasks]
            if missing:
                raise ValueError(f'task "{spec.task_id}" waits for unknown task(s): {", ".join(missing)}')
            if foreign:
                tasks[spec.task_id].set_upstream([tasks[upstream_id] for upstream_id in foreign])

    def _make_task(self, spec: TaskSpec) -> BashOperator:
        """Build one BashOperator from a TaskSpec: cwd of the commands, pool by job type
        (config.json `dag.pools`), trigger rule from the spec and the memory callbacks."""
        return BashOperator(
            task_id=spec.task_id,
            bash_command=spec.command,
            # cwd = the project dir, so `cd ..;cd GEO_*` reaches the sibling projects
            cwd=str(self._project_dir),
            pool=self._dag_settings.pool_for(spec.job_type),
            trigger_rule=_TRIGGER_RULES[spec.trigger_rule],
            on_success_callback=_log_memory,
            on_failure_callback=_log_memory,
        )


class GEO_DAG:
    """The `geo_pipeline` mega-DAG: one `JobsSubDAG` per data source plus the union subDAG.

    The flat task specs of `jobs.json` (`model.build_task_specs()`) are grouped by
    `source_name` — the source chains in jobs.json order (nine today), then the union chain —
    and every subDAG renders its own tasks into the single Airflow DAG.

    Between the subDAGs there are two dependencies: the union subDAG is downstreamed on
    all the others, its root (`Union.single_etl`) waiting for the `single_etl` task of every
    source subDAG, and the verdict task (`Union.verdict`) waiting for every task of the DAG
    (see `JobsSubDAG.wire_upstreams_from()`).
    """

    def __init__(
        self,
        *,
        dag_settings: DagSettings | None = None,
        project_dir: Path | None = None,
        specs: Sequence[TaskSpec] | None = None,
    ) -> None:
        self._dag_settings = dag_settings if dag_settings is not None else get_dag_settings()
        self._project_dir = project_dir if project_dir is not None else Path(__file__).resolve().parent
        if specs is None:
            specs = build_task_specs(union_source=self._dag_settings.union_source)
        # Group the flat specs by source_name — the dict keeps jobs.json order
        groups: dict[str, list[TaskSpec]] = {}
        for spec in specs:
            groups.setdefault(spec.source_name, []).append(spec)
        self._subdags = tuple(
            JobsSubDAG(group, dag_settings=self._dag_settings, project_dir=self._project_dir)
            for group in groups.values()
        )

    @property
    def subdags(self) -> tuple[JobsSubDAG, ...]:
        """Every subDAG of the mega-DAG: the source chains (jobs.json order) and the union chain."""
        return self._subdags

    @cached_property
    def source_subdags(self) -> tuple[JobsSubDAG, ...]:
        """The data-source subDAGs the union fan-in waits for (everything but the union chain)."""
        return tuple(subdag for subdag in self._subdags if subdag.source_name != self._dag_settings.union_source)

    @property
    def union_subdag(self) -> JobsSubDAG:
        """The union subDAG (its root fans in on every `source_subdags` single-ETL task).

        Raises:
            ValueError: When the spec list carries no chain of `dag.union_source`.
        """
        for subdag in self._subdags:
            if subdag.source_name == self._dag_settings.union_source:
                return subdag
        raise ValueError(f'jobs.json has no "{self._dag_settings.union_source}" chain')

    def dag_kwargs(self) -> dict[str, Any]:
        """The Airflow structure of the DAG itself — the arguments of its `@dag(...)` decorator:
        id, cron (the scheduled jobs' shared one), start date, run limits, tags, the default
        task args (retries, execution timeout) and the DAG documentation."""
        start = self._dag_settings.start_date
        return {
            "dag_id": self._dag_settings.dag_id,
            # The DAG's cron comes from the scheduled jobs of jobs.json (every one of them declares it)
            "schedule": resolve_schedule(),
            "start_date": pendulum.datetime(start.year, start.month, start.day, tz=self._dag_settings.timezone),
            "catchup": self._dag_settings.catchup,
            "max_active_runs": self._dag_settings.max_active_runs,
            "tags": self._dag_settings.tags,
            "default_args": {
                "retries": self._dag_settings.retries,
                "execution_timeout": timedelta(minutes=self._dag_settings.execution_timeout_minutes),
            },
            "doc_md": _DOC_MD,
        }

    def to_airflow(self) -> dict[str, BashOperator]:
        """Render every subDAG into the Airflow DAG and wire the dependencies between them.

        Must run inside a DAG context (the `@dag` body): operators attach to the current DAG on
        creation, which is also what makes the cross-subDAG `set_upstream()` calls legal.

        Raises:
            ValueError: When two subDAGs would render the same task id, or when a subDAG waits
                for a task no subDAG renders.
        """
        tasks: dict[str, BashOperator] = {}
        # Phase 1: render every subDAG's tasks (duplicate task ids across subDAGs are rejected)
        for subdag in self._subdags:
            duplicates = subdag.task_ids.intersection(tasks)
            if duplicates:
                raise ValueError(f"duplicate task ids: {', '.join(sorted(duplicates))}")
            tasks.update(subdag.to_airflow())
        # Phase 2: wire dependencies now that every task exists (cross-subDAG edges included)
        for subdag in self._subdags:
            subdag.wire_upstreams_from(tasks)
        return tasks
