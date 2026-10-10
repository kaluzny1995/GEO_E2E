"""Config models for the job commands (config.json), the E2E job chains (jobs.json)
and the Airflow task specs derived from them."""

import json
import os
from datetime import date
from enum import StrEnum
from typing import Literal
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, field_validator

_PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
_CONFIG_PATH = os.path.join(_PROJECT_DIR, "config.json")
_JOBS_PATH = os.path.join(_PROJECT_DIR, "jobs.json")


class EJobType(StrEnum):
    """Kind of pipeline step a single job in the chain performs."""

    DOWNLOAD = "DOWNLOAD"
    INGESTION = "INGESTION"
    DATA_QUALITY = "DATA_QUALITY"
    SINGLE_ETL = "SINGLE_ETL"


class JobTypeConfig(BaseModel):
    """Shell command template for one job type: folder, script and optional conda env."""

    folder_name: str
    python_script: str
    conda_env: str | None = None

    model_config = ConfigDict(frozen=True)

    def get_command_pattern_string(self) -> str:
        """Build the command without arguments, e.g. `cd ..;cd GEO_ETL;conda run ... python run_etl.py`."""
        conda_part = f"conda run --no-capture-output -n {self.conda_env} " if self.conda_env else ""
        return f"cd ..;cd {self.folder_name};{conda_part}python {self.python_script}"

    def get_command_string(self, *args: str) -> str:
        """Build the command with space-separated arguments appended, e.g. `... run_etl.py INGESTION nifc_fire`."""
        return f"{self.get_command_pattern_string()} {' '.join(args)}"


class JobConfig(BaseModel):
    """The `job` block of config.json — command templates for DOWNLOAD, INGESTION, DATA_QUALITY and SINGLE_ETL."""

    download: JobTypeConfig
    ingestion: JobTypeConfig
    data_quality: JobTypeConfig
    single_etl: JobTypeConfig

    model_config = ConfigDict(frozen=True)


def get_job_config() -> JobConfig:
    """Load the `job` block of config.json (DOWNLOAD, INGESTION, DATA_QUALITY and SINGLE_ETL command templates)."""
    with open(_CONFIG_PATH) as f:
        return JobConfig(**json.load(f)["job"])


class PoolConfig(BaseModel):
    """Airflow pool per job type (the `dag.pools` block of config.json)."""

    download: str
    ingestion: str
    data_quality: str
    single_etl: str

    model_config = ConfigDict(frozen=True)


class DagSettings(BaseModel):
    """Runtime knobs of the mega-DAG (the `dag` block of config.json; the schedule itself
    lives on the scheduled jobs of jobs.json — see `resolve_schedule`)."""

    dag_id: str = "geo_pipeline"
    start_date: date
    timezone: str
    catchup: bool = False
    max_active_runs: int = 1
    tags: list[str] = []
    retries: int = 0
    execution_timeout_minutes: int = 30
    union_source: str = "Union"
    pools: PoolConfig
    pool_slots: dict[str, int]

    model_config = ConfigDict(frozen=True)

    @field_validator("timezone")
    @classmethod
    def _unknown_timezone(cls, value: str) -> str:
        """Reject unknown IANA timezones at config load instead of at DAG parse."""
        try:
            ZoneInfo(value)
        except (KeyError, ValueError) as exc:
            raise ValueError(f"unknown IANA timezone: {value}") from exc
        return value

    def pool_for(self, job_type: EJobType) -> str:
        """Name of the Airflow pool a task of the given job type runs in."""
        return {
            EJobType.DOWNLOAD: self.pools.download,
            EJobType.INGESTION: self.pools.ingestion,
            EJobType.DATA_QUALITY: self.pools.data_quality,
            EJobType.SINGLE_ETL: self.pools.single_etl,
        }[job_type]


def get_dag_settings() -> DagSettings:
    """Load the `dag` block of config.json (retries, timeout, pools, ...)."""
    with open(_CONFIG_PATH) as f:
        return DagSettings(**json.load(f)["dag"])


class Job(BaseModel):
    """A single job of the flat jobs.json registry — referenced by other jobs via its `name`.

    A job either carries a `schedule` (a *scheduled* job: a pipeline root that starts on that
    cron) or omits it (a *downstreamed* job: it starts when the job referencing it in
    `next_jobs` finishes). `is_next_skipped_if_error` is kept for documentation (the DAG maps
    it to Airflow's `all_success` trigger rule, so it is not consulted at runtime)."""

    name: str
    source_name: str
    job_type: EJobType
    table_name: str | None = None
    schedule: str | None = None
    is_next_skipped_if_error: bool = True
    next_jobs: list[str] = []

    model_config = ConfigDict(frozen=True)

    def get_command(self) -> str:
        """Build the shell command of this job from the `job` block of config.json.

        Raises:
            ValueError: When an INGESTION, DATA_QUALITY or SINGLE_ETL job has no table_name
                configured in jobs.json.
        """
        job_config = get_job_config()

        if self.job_type == EJobType.DOWNLOAD:
            return job_config.download.get_command_string(self.source_name)

        if self.table_name is None:
            raise ValueError(
                f'{self.job_type.value} job for "{self.source_name}" has no table_name configured in jobs.json'
            )

        if self.job_type == EJobType.INGESTION:
            return job_config.ingestion.get_command_string(self.job_type.value, self.table_name)

        if self.job_type == EJobType.SINGLE_ETL:
            return job_config.single_etl.get_command_string(self.job_type.value, self.table_name)

        return job_config.data_quality.get_command_string(self.job_type.value, self.table_name)


class TaskSpec(BaseModel):
    """One Airflow task: the shell command of a single job plus the task ids it must wait for."""

    task_id: str
    source_name: str
    job_type: EJobType
    command: str
    # Task ids of the parent jobs (empty for a chain's root job)
    upstream: tuple[str, ...] = ()
    # all_success for source-chain jobs; all_done_min_one_success for the union root only
    trigger_rule: Literal["all_success", "all_done_min_one_success"] = "all_success"

    model_config = ConfigDict(frozen=True)


def load_jobs() -> list[Job]:
    """Load the flat job list of jobs.json, preserving file order."""
    with open(_JOBS_PATH) as f:
        return [Job(**raw) for raw in json.load(f)]


def _index_jobs(jobs: list[Job]) -> tuple[dict[str, Job], list[Job]]:
    """Index the flat jobs by name and validate the reference graph.

    Returns the name -> job index and the root jobs (the ones no other job references),
    in file order. Raises ValueError on duplicate names, unknown `next_jobs` references,
    jobs referenced by more than one parent, dependency cycles, or a scheduled job that
    also has a parent (scheduled jobs start on their own cron, not from another job).
    """
    jobs_by_name: dict[str, Job] = {}
    for job in jobs:
        if job.name in jobs_by_name:
            raise ValueError(f'jobs.json has duplicate job name "{job.name}"')
        jobs_by_name[job.name] = job

    parent_of: dict[str, str] = {}
    for job in jobs:
        for child_name in job.next_jobs:
            if child_name not in jobs_by_name:
                raise ValueError(f'job "{job.name}" references unknown job "{child_name}"')
            if child_name in parent_of:
                raise ValueError(
                    f'job "{child_name}" is referenced by both "{parent_of[child_name]}" and '
                    f'"{job.name}" — every job needs exactly one parent job'
                )
            parent_of[child_name] = job.name

    for job in jobs:
        if job.schedule is not None and job.name in parent_of:
            raise ValueError(
                f'scheduled job "{job.name}" is referenced by "{parent_of[job.name]}" — a scheduled '
                "job starts on its own schedule and must not have a parent"
            )

    # Every job has at most one parent, so a simple walk from the roots reaches all
    # non-cyclic jobs; whatever stays unvisited forms dependency cycles.
    reached: set[str] = set()
    queue = [job.name for job in jobs if job.name not in parent_of]
    while queue:
        name = queue.pop()
        reached.add(name)
        queue.extend(jobs_by_name[name].next_jobs)
    cyclic = [name for name in jobs_by_name if name not in reached]
    if cyclic:
        raise ValueError(f"jobs.json has a dependency cycle among: {', '.join(cyclic)}")

    return jobs_by_name, [job for job in jobs if job.name not in parent_of]


def _walk_chain(jobs_by_name: dict[str, Job], job: Job, parent_id: str | None, path: tuple[str, ...]) -> list[TaskSpec]:
    """Depth-first walk of one chain of the flat job registry.

    Returns the task specs of the visited subtree. Task ids follow the
    job-type path inside the chain, skipping the DOWNLOAD prefix (`NIFC.ingestion.single_etl`).
    """
    # The chain root's DOWNLOAD is not part of its children's id prefix (NIFC.ingestion, not NIFC.download.ingestion)
    id_path = tuple(job_type for job_type in path if job_type != EJobType.DOWNLOAD.lower())
    task_path = (*id_path, job.job_type.lower())
    task_id = ".".join((job.source_name, *task_path))
    spec = TaskSpec(
        task_id=task_id,
        source_name=job.source_name,
        job_type=job.job_type,
        command=job.get_command(),
        upstream=(parent_id,) if parent_id else (),
    )

    specs = [spec]
    for child_name in job.next_jobs:
        specs.extend(_walk_chain(jobs_by_name, jobs_by_name[child_name], task_id, task_path))
    return specs


def resolve_schedule() -> str:
    """The cron every scheduled job of jobs.json shares — the DAG's single schedule.

    Airflow schedules whole DAGs, not single tasks: every job carrying a `schedule` must
    declare the same cron, and at least one job must be scheduled.

    Raises:
        ValueError: When jobs.json has no scheduled job or more than one distinct schedule.
    """
    schedules = sorted({job.schedule for job in load_jobs() if job.schedule is not None})
    if not schedules:
        raise ValueError("jobs.json has no scheduled job — at least one job needs a schedule")
    if len(schedules) > 1:
        raise ValueError(
            f"jobs.json has different schedules ({', '.join(schedules)}) — Airflow supports one "
            "schedule per DAG, so scheduled jobs must share the same cron"
        )
    return schedules[0]


def build_task_specs(union_source: str = "Union") -> list[TaskSpec]:
    """Flatten the flat job registry of jobs.json into the Airflow task specs of the mega-DAG.

    Scheduled jobs (those carrying a `schedule`) become the DAG's roots and start on the cron
    (see `resolve_schedule`); every downstreamed job waits for its parent with the default
    `all_success` trigger rule (failed downloads skip their downstream jobs, like the old
    `is_stop_if_error` chains). The union chain's root is neither scheduled nor a simple child:
    it waits for the SINGLE_ETL task of every source chain with `all_done_min_one_success` —
    it starts whatever the chains' results, but only when at least one single-ETL succeeded
    (with none, the scheduler marks it `upstream_failed` — nothing fresh to stack). The
    DATA_QUALITY jobs stay terminal steps with no successors, like in the old Celery chains.

    Raises:
        ValueError: When jobs.json has no single root job for *union_source*, a non-union root
            is not scheduled, the union job itself is scheduled, or the reference graph is
            invalid (see `_index_jobs`).
    """
    jobs_by_name, roots = _index_jobs(load_jobs())

    union_roots = [job for job in roots if job.source_name == union_source]
    if not union_roots:
        raise ValueError(f'jobs.json has no "{union_source}" chain')
    if len(union_roots) > 1:
        raise ValueError(f'jobs.json has {len(union_roots)} "{union_source}" chains — expected exactly one')
    union_names = {job.name for job in union_roots}

    for root in roots:
        if root.name in union_names:
            if root.schedule is not None:
                raise ValueError(
                    f'union job "{root.name}" must not be scheduled — it waits for the fan-in '
                    "of every source's single_etl task"
                )
        elif root.schedule is None:
            raise ValueError(
                f'root job "{root.name}" is neither scheduled nor the union job — give it a '
                '"schedule" or reference it from its parent job\'s next_jobs'
            )

    specs: list[TaskSpec] = []
    union_specs: list[TaskSpec] = []
    single_etl_ids: list[str] = []

    for root in roots:
        walked = _walk_chain(jobs_by_name, root, parent_id=None, path=())
        if root.name in union_names:
            union_specs = walked
        else:
            specs.extend(walked)
            single_etl_ids.extend(spec.task_id for spec in walked if spec.job_type == EJobType.SINGLE_ETL)

    fan_in = tuple(single_etl_ids)
    union_specs = [
        spec.model_copy(update={"upstream": fan_in, "trigger_rule": "all_done_min_one_success"})
        if not spec.upstream
        else spec
        for spec in union_specs
    ]
    return specs + union_specs
