"""Config models for the job commands (config.json), the E2E job chains (jobs.json)
and the Airflow task specs derived from them."""

import json
import os
import re
from datetime import date
from enum import StrEnum
from functools import lru_cache
from typing import Literal
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, field_validator

# Config lives next to this module — the paths are resolved once at import time
_PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
_CONFIG_PATH = os.path.join(_PROJECT_DIR, "config.json")
_JOBS_PATH = os.path.join(_PROJECT_DIR, "jobs.json")


class EJobType(StrEnum):
    """Kind of pipeline step a single job in the chain performs."""

    DOWNLOAD = "DOWNLOAD"
    INGESTION = "INGESTION"
    DATA_QUALITY = "DATA_QUALITY"
    SINGLE_ETL = "SINGLE_ETL"
    VIEW = "VIEW"
    # Not a jobs.json type — build_task_specs() synthesizes the verdict task with it
    VERDICT = "VERDICT"


class JobTypeConfig(BaseModel):
    """Shell command template for one job type: folder, script and optional conda env."""

    folder_name: str
    python_script: str
    conda_env: str | None = None

    model_config = ConfigDict(frozen=True)

    def get_command_pattern_string(self) -> str:
        """Build the command without arguments, e.g. `cd ..;cd GEO_ETL;conda run ... python run_etl.py`."""
        conda_part = f"conda run --no-capture-output -n {self.conda_env} " if self.conda_env else ""
        # The cd ..;cd <folder> hop assumes cwd = GEO_E2E — the BashOperator sets it (see geo_dag)
        return f"cd ..;cd {self.folder_name};{conda_part}python {self.python_script}"

    def get_command_string(self, *args: str) -> str:
        """Build the command with space-separated arguments appended, e.g. `... run_etl.py INGESTION nifc_fire`."""
        return f"{self.get_command_pattern_string()} {' '.join(args)}"


class JobConfig(BaseModel):
    """The `job` block of config.json — command templates for every job type."""

    download: JobTypeConfig
    ingestion: JobTypeConfig
    data_quality: JobTypeConfig
    single_etl: JobTypeConfig
    view: JobTypeConfig

    model_config = ConfigDict(frozen=True)


@lru_cache(maxsize=1)
def _read_job_config(config_path: str) -> JobConfig:
    """Read and parse the `job` block of the given config.json — cached per path.

    The cache key is the path itself, so tests patching `_CONFIG_PATH` with their own tmp
    files keep working (each patched path misses the cache). `JobConfig` is frozen, so the
    shared instance cannot be mutated by a caller.
    """
    with open(config_path) as f:
        return JobConfig(**json.load(f)["job"])


def get_job_config() -> JobConfig:
    """Load the `job` block of config.json (the command template of every job type)."""
    return _read_job_config(_CONFIG_PATH)


class PoolConfig(BaseModel):
    """Airflow pool per job type (the `dag.pools` block of config.json)."""

    download: str
    ingestion: str
    data_quality: str
    single_etl: str
    view: str
    verdict: str

    model_config = ConfigDict(frozen=True)


class DagSettings(BaseModel):
    """Runtime knobs of the mega-DAG (the `dag` block of config.json; the schedule itself
    lives on the scheduled jobs of jobs.json — see `resolve_schedule`)."""

    dag_id: str = "geo_pipeline"
    start_date: date
    timezone: str
    catchup: bool = False
    max_active_runs: int = 1
    tags: list[str] = Field(default_factory=list)
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
            EJobType.VIEW: self.pools.view,
            EJobType.VERDICT: self.pools.verdict,
        }[job_type]


@lru_cache(maxsize=1)
def _read_dag_settings(config_path: str) -> DagSettings:
    """Read and parse the `dag` block of the given config.json — cached per path
    (same keying rules as `_read_job_config`; `DagSettings` is frozen)."""
    with open(config_path) as f:
        return DagSettings(**json.load(f)["dag"])


def get_dag_settings() -> DagSettings:
    """Load the `dag` block of config.json (retries, timeout, pools, ...)."""
    return _read_dag_settings(_CONFIG_PATH)


class Job(BaseModel):
    """A single job of the flat jobs.json registry — referenced by other jobs via its `name`.

    `task_id` is the Airflow task id this job becomes — the names are *data*, declared per job
    in jobs.json instead of derived from the chain shape (`NIFC.single_etl`, `NIFC_etl.data_quality`,
    `View.metrics_flood_vw`). A job either carries a `schedule` (a *scheduled* job: a pipeline
    root that starts on that cron) or omits it (a *downstreamed* job: it starts when the job
    referencing it in `next_jobs` finishes). `is_next_skipped_if_error` is kept for documentation
    (the DAG maps it to Airflow's `all_success` trigger rule, so it is not consulted at runtime)."""

    name: str
    task_id: str
    source_name: str
    job_type: EJobType
    table_name: str | None = None
    schedule: str | None = None
    is_next_skipped_if_error: bool = True
    next_jobs: list[str] = Field(default_factory=list)

    model_config = ConfigDict(frozen=True)

    @field_validator("task_id")
    @classmethod
    def _valid_task_id(cls, value: str) -> str:
        """Reject task ids Airflow (or the DAG's own id joins) cannot handle, at config load."""
        if not re.fullmatch(r"[A-Za-z0-9_]+(\.[A-Za-z0-9_]+)*", value):
            raise ValueError(
                f"invalid task_id {value!r} — expected dot-separated segments of letters, "
                "digits and underscores (e.g. NIFC.single_etl, View.metrics_flood_vw)"
            )
        return value

    def get_command(self, job_config: JobConfig | None = None) -> str:
        """Build the shell command of this job from the `job` block of config.json.

        `job_config` lets a caller (see `build_task_specs`) pass the config it already
        loaded instead of re-reading it per job; without it the config is fetched here
        (cached per path — see `_read_job_config`).

        Raises:
            ValueError: When a non-DOWNLOAD job (INGESTION, DATA_QUALITY, SINGLE_ETL or
                VIEW) has no table_name configured in jobs.json, or when the job is a
                VERDICT job (the verdict task is synthesized by build_task_specs()).
        """
        if self.job_type == EJobType.VERDICT:
            raise ValueError(
                "a VERDICT job has no shell command — build_task_specs() synthesizes the verdict task itself"
            )

        if job_config is None:
            job_config = get_job_config()

        # DOWNLOAD takes the source name as its only argument; the other types take (job_type, table_name)
        if self.job_type == EJobType.DOWNLOAD:
            return job_config.download.get_command_string(self.source_name)

        if self.table_name is None:
            raise ValueError(
                f'{self.job_type.value} job for "{self.source_name}" has no table_name configured in jobs.json'
            )

        # INGESTION / DATA_QUALITY / SINGLE_ETL / VIEW — every one's JobTypeConfig field of
        # JobConfig is the lowercased job type (the VERDICT and DOWNLOAD cases are handled above)
        type_config: JobTypeConfig = getattr(job_config, self.job_type.value.lower())
        return type_config.get_command_string(self.job_type.value, self.table_name)


class TaskSpec(BaseModel):
    """One Airflow task: the shell command of a single job plus the task ids it must wait for."""

    task_id: str
    source_name: str
    job_type: EJobType
    command: str
    # Task ids of the parent jobs (empty for a chain's root job)
    upstream: tuple[str, ...] = ()
    # all_success for source-chain jobs; all_done_min_one_success for the union root;
    # all_done for the verdict task (it reads the union result after every task finished)
    trigger_rule: Literal["all_success", "all_done_min_one_success", "all_done"] = "all_success"

    model_config = ConfigDict(frozen=True)


@lru_cache(maxsize=1)
def _read_jobs(jobs_path: str) -> tuple[Job, ...]:
    """Read and parse the given jobs.json (file order) — cached per path.

    Same keying rules as `_read_job_config`: tests patching `_JOBS_PATH` with their own
    tmp files miss the cache, and no test rewrites one path between reads. The cache holds
    an immutable tuple of frozen `Job`s; `load_jobs()` hands out a fresh list per call, so
    a caller can never corrupt the cached entry.
    """
    with open(jobs_path) as f:
        return tuple(Job(**raw) for raw in json.load(f))


def load_jobs() -> list[Job]:
    """Load the flat job list of jobs.json, preserving file order."""
    return list(_read_jobs(_JOBS_PATH))


def _index_jobs(jobs: list[Job]) -> tuple[dict[str, Job], list[Job]]:
    """Index the flat jobs by name and validate the reference graph.

    Returns the name -> job index and the root jobs (the ones no other job references),
    in file order. Raises ValueError on duplicate names, duplicate task ids, unknown
    `next_jobs` references, jobs referenced by more than one parent, dependency cycles,
    or a scheduled job that also has a parent (scheduled jobs start on their own cron,
    not from another job).
    """
    jobs_by_name: dict[str, Job] = {}
    task_id_owner: dict[str, str] = {}
    for job in jobs:
        if job.name in jobs_by_name:
            raise ValueError(f'jobs.json has duplicate job name "{job.name}"')
        if job.task_id in task_id_owner:
            raise ValueError(
                f'jobs.json has duplicate task_id "{job.task_id}" — claimed by '
                f'"{task_id_owner[job.task_id]}" and "{job.name}" (Airflow task ids must be unique)'
            )
        jobs_by_name[job.name] = job
        task_id_owner[job.task_id] = job.name

    # next_jobs forms a tree: every child must be claimed by exactly one parent
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


def _walk_chain(jobs_by_name: dict[str, Job], job: Job, parent_id: str | None) -> list[TaskSpec]:
    """Depth-first walk of one chain of the flat job registry.

    Returns the task specs of the visited subtree. Task ids are the `task_id` fields of
    jobs.json — the config names every task explicitly, the chain only decides the edges
    (`NIFC.download` -> `NIFC.ingestion` -> `NIFC.single_etl`).
    """
    task_id = job.task_id
    spec = TaskSpec(
        task_id=task_id,
        source_name=job.source_name,
        job_type=job.job_type,
        command=job.get_command(),
        upstream=(parent_id,) if parent_id else (),
    )

    specs = [spec]
    for child_name in job.next_jobs:
        specs.extend(_walk_chain(jobs_by_name, jobs_by_name[child_name], task_id))
    return specs


def resolve_schedule() -> str:
    """The cron every scheduled job of jobs.json shares — the DAG's single schedule.

    Airflow schedules whole DAGs, not single tasks: every job carrying a `schedule` must
    declare the same cron, and at least one job must be scheduled.

    Raises:
        ValueError: When jobs.json has no scheduled job or more than one distinct schedule.
    """
    # set() collapses the duplicate crons, sorted() keeps the error messages deterministic
    schedules = sorted({job.schedule for job in load_jobs() if job.schedule is not None})
    if not schedules:
        raise ValueError("jobs.json has no scheduled job — at least one job needs a schedule")
    if len(schedules) > 1:
        raise ValueError(
            f"jobs.json has different schedules ({', '.join(schedules)}) — Airflow supports one "
            "schedule per DAG, so scheduled jobs must share the same cron"
        )
    return schedules[0]


def _verdict_command(union_root_task_id: str) -> str:
    """Shell command of the verdict task — a Jinja template the BashOperator renders right before it runs.

    `all_done` guarantees every other task of the DAG reached a terminal state by then, so
    `ti.get_task_states()` (the Airflow task SDK — a comms call the supervisor serves in real
    runs and in `airflow tasks test`) can read the union root's state of the *current* run:
    the command exits 0 only when it is `success`, 1 otherwise (including a missing TI).
    """
    return (
        "{% set task = '" + union_root_task_id + "' %}\n"
        "{% set state = ti.get_task_states(ti.dag_id, task_ids=[task], run_ids=[run_id]).get(run_id, {})"
        ".get(task) %}\n"
        "{% if state == 'success' %}\n"
        'echo "verdict: {{ task }} = {{ state }} -> DAG run succeeds"\n'
        "{% else %}\n"
        'echo "verdict: {{ task }} = {{ state }} -> DAG run fails" >&2\n'
        "exit 1\n"
        "{% endif %}\n"
    )


def build_task_specs(union_source: str = "Union") -> list[TaskSpec]:
    """Flatten the flat job registry of jobs.json into the Airflow task specs of the mega-DAG.

    Scheduled jobs (those carrying a `schedule`) become the DAG's roots and start on the cron
    (see `resolve_schedule`); every downstreamed job waits for its parent with the default
    `all_success` trigger rule (failed downloads skip their downstream jobs, like the old
    `is_stop_if_error` chains). The union chain's root is neither scheduled nor a simple child:
    it waits for the SINGLE_ETL task of every source chain with `all_done_min_one_success` —
    it starts whatever the chains' results, but only when at least one single-ETL succeeded
    (with none, the scheduler marks it `upstream_failed` — nothing fresh to stack). The
    DATA_QUALITY jobs stay terminal steps of the registry (nothing chains from them), like in
    the old Celery chains; so do the VIEW jobs the union root chains on (they only read the
    freshly stacked table) — only the synthesized verdict task below waits on every task.

    On top of the registry's specs the function appends the synthesized verdict task
    (`<union_source>.verdict`, job type VERDICT): it waits for **every** registry task with
    `all_done` and exits 0 only when the union root finished `success`. Airflow derives a
    run's state from its *leaf* tasks — the verdict has no successors and swallows every
    former leaf, so it alone decides whether the DAG run succeeded.

    Raises:
        ValueError: When jobs.json has no single root job for *union_source*, a non-union root
            is not scheduled, the union job itself is scheduled, the registry declares a
            VERDICT job, a job claims the reserved verdict task id, or the reference graph
            is invalid (see `_index_jobs`).
    """
    jobs = load_jobs()
    if any(job.job_type == EJobType.VERDICT for job in jobs):
        raise ValueError("jobs.json must not declare a VERDICT job — build_task_specs() synthesizes the verdict task")
    verdict_id = f"{union_source}.verdict"
    reserved = next((job.name for job in jobs if job.task_id == verdict_id), None)
    if reserved is not None:
        raise ValueError(
            f'job "{reserved}" uses task_id "{verdict_id}" — that id is reserved for the synthesized verdict task'
        )
    jobs_by_name, roots = _index_jobs(jobs)

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
        walked = _walk_chain(jobs_by_name, root, parent_id=None)
        if root.name in union_names:
            union_specs = walked
        else:
            specs.extend(walked)
            single_etl_ids.extend(spec.task_id for spec in walked if spec.job_type == EJobType.SINGLE_ETL)

    # The union root — the chain's only parentless spec — waits for every source's SINGLE_ETL
    # task; its child keeps the plain parent link and the default all_success rule
    fan_in = tuple(single_etl_ids)
    union_root_id = next(spec.task_id for spec in union_specs if not spec.upstream)
    union_specs = [
        spec.model_copy(update={"upstream": fan_in, "trigger_rule": "all_done_min_one_success"})
        if not spec.upstream
        else spec
        for spec in union_specs
    ]
    registry_specs = specs + union_specs

    # The verdict task: downstream of every registry task (so it is the DAG's only leaf — and
    # only leaf tasks decide a run's state) and gated on the union root's state by its command
    verdict = TaskSpec(
        task_id=verdict_id,
        source_name=union_source,
        job_type=EJobType.VERDICT,
        command=_verdict_command(union_root_id),
        upstream=tuple(spec.task_id for spec in registry_specs),
        trigger_rule="all_done",
    )
    return [*registry_specs, verdict]
