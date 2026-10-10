"""Config models for the job commands (config.json), the E2E job chains (jobs.json)
and the Airflow task specs derived from them."""

import json
import os
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict

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


class Job(BaseModel):
    """A single job in a chain — its `next_jobs` are turned into downstream Airflow tasks.

    `is_failed_if_error` / `is_stop_if_error` are kept for documentation; the DAG maps them
    to Airflow trigger rules (all_success / skip semantics), so they are not consulted here."""

    source_name: str
    job_type: EJobType
    table_name: str | None = None
    is_failed_if_error: bool = False
    is_stop_if_error: bool = False
    next_jobs: list["Job"] = []

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

    @staticmethod
    def from_config(source_name: str) -> "Job":
        """Returns the root job of the chain for given source from jobs.json"""
        with open(_JOBS_PATH) as f:
            return Job(**json.load(f)[source_name])


class TaskSpec(BaseModel):
    """One Airflow task: the shell command of a single job plus the task ids it must wait for."""

    task_id: str
    source_name: str
    job_type: EJobType
    command: str
    # Task ids of the parent jobs (empty for a chain's root job)
    upstream: tuple[str, ...] = ()
    trigger_rule: Literal["all_success", "all_done"] = "all_success"

    model_config = ConfigDict(frozen=True)


def load_chains() -> dict[str, Job]:
    """Load every source chain of jobs.json, keyed by source name."""
    with open(_JOBS_PATH) as f:
        return {source_name: Job(**data) for source_name, data in json.load(f).items()}


def _walk_chain(source_name: str, job: Job, parent_id: str | None, path: tuple[str, ...]) -> list[TaskSpec]:
    """Depth-first walk of one chain.

    Returns the task specs of the visited subtree. Task ids follow the
    job-type path inside the chain, skipping the DOWNLOAD prefix (`NIFC.ingestion.single_etl`).
    """
    # The chain root's DOWNLOAD is not part of its children's id prefix (NIFC.ingestion, not NIFC.download.ingestion)
    id_path = tuple(job_type for job_type in path if job_type != EJobType.DOWNLOAD.lower())
    task_path = (*id_path, job.job_type.lower())
    task_id = ".".join((source_name, *task_path))
    spec = TaskSpec(
        task_id=task_id,
        source_name=source_name,
        job_type=job.job_type,
        command=job.get_command(),
        upstream=(parent_id,) if parent_id else (),
    )

    specs = [spec]
    for next_job in job.next_jobs:
        specs.extend(_walk_chain(source_name, next_job, task_id, task_path))
    return specs


def build_task_specs(union_source: str = "Union") -> list[TaskSpec]:
    """Flatten every chain in jobs.json into the Airflow task specs of the mega-DAG.

    Task ids follow the job-type path inside the chain (`NIFC.ingestion.single_etl`).
    Every non-union job waits for its parent with the default `all_success` trigger rule
    (failed downloads skip their downstream jobs, like the old `is_stop_if_error` chains).
    The union chain's root waits for the SINGLE_ETL task of every source chain with
    `all_done`, so it stacks the fresh `*_etl` tables whatever the chains' results —
    the DATA_QUALITY jobs stay terminal steps with no successors, like in the old Celery chains.

    Raises:
        ValueError: When jobs.json has no chain for *union_source*.
    """
    chains = load_chains()
    if union_source not in chains:
        raise ValueError(f'jobs.json has no "{union_source}" chain')

    specs: list[TaskSpec] = []
    union_specs: list[TaskSpec] = []
    single_etl_ids: list[str] = []

    for source_name, root in chains.items():
        walked = _walk_chain(source_name, root, parent_id=None, path=())
        if source_name == union_source:
            union_specs = walked
        else:
            specs.extend(walked)
            single_etl_ids.extend(spec.task_id for spec in walked if spec.job_type == EJobType.SINGLE_ETL)

    fan_in = tuple(single_etl_ids)
    union_specs = [
        spec if spec.upstream else spec.model_copy(update={"upstream": fan_in, "trigger_rule": "all_done"})
        for spec in union_specs
    ]
    return specs + union_specs
