"""Config models for the Celery app (config.json) and the E2E job chains (jobs.json)."""

import json
import logging
import os
import subprocess
from enum import StrEnum

from pydantic import BaseModel

from util import run_command

_PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
_CONFIG_PATH = os.path.join(_PROJECT_DIR, "config.json")
_JOBS_PATH = os.path.join(_PROJECT_DIR, "jobs.json")


class EJobType(StrEnum):
    """Kind of pipeline step a single job in the chain performs."""

    DOWNLOAD = "DOWNLOAD"
    INGESTION = "INGESTION"
    DATA_QUALITY = "DATA_QUALITY"


class CeleryTask(BaseModel):
    """One beat schedule entry — decides when a source's E2E chain is triggered."""

    source_name: str
    month: int | str
    day: int | str
    hour: int
    minute: int | str
    timezone: str
    name: str


class JobTypeConfig(BaseModel):
    """Shell command template for one job type: folder, script and optional conda env."""

    folder_name: str
    python_script: str
    conda_env: str | None = None

    class ConfigDict:
        """Makes the model immutable."""

        frozen = True

    def get_command_pattern_string(self) -> str:
        """Build the command without arguments, e.g. `cd ..;cd GEO_ETL;conda run ... python run_etl.py`."""
        conda_part = f"conda run --no-capture-output -n {self.conda_env} " if self.conda_env else ""
        return f"cd ..;cd {self.folder_name};{conda_part}python {self.python_script}"

    def get_command_string(self, *args: str) -> str:
        """Build the command with space-separated arguments appended, e.g. `... run_etl.py INGESTION nifc_fire`."""
        return f"{self.get_command_pattern_string()} {' '.join(args)}"


class JobConfig(BaseModel):
    """The `job` block of config.json — command templates for DOWNLOAD, INGESTION and DATA_QUALITY."""

    download: JobTypeConfig
    ingestion: JobTypeConfig
    data_quality: JobTypeConfig

    class ConfigDict:
        """Makes the model immutable."""

        frozen = True


class CelerySettings(BaseModel):
    """Celery worker & broker tuning — all values come from config.json."""

    # Hard kill after N seconds
    task_time_limit: int = 600
    # Soft timeout (raises SoftTimeLimitExceeded) after N seconds
    task_soft_time_limit: int = 480
    # Recycle worker after N tasks (clears leaked memory)
    worker_max_tasks_per_child: int = 20
    # Max memory per worker child in kilobytes
    worker_max_memory_per_child: int = 400000
    # Only grab 1 task at a time so downloads don't pile up
    worker_prefetch_multiplier: int = 1
    # Requeue tasks if the worker crashes mid-download
    task_acks_late: bool = True

    class ConfigDict:
        """Makes the model immutable."""

        frozen = True


class CeleryAppConfig(BaseModel):
    """The `celery_app` block of config.json — broker credentials, beat tasks and worker tuning."""

    name: str
    username: str
    password: str
    host: str
    port: int
    tasks: list[CeleryTask]
    celery_settings: CelerySettings = CelerySettings()

    class ConfigDict:
        """Makes the model immutable."""

        frozen = True

    @staticmethod
    def from_config() -> "CeleryAppConfig":
        """Load the app config from config.json, falling back to CelerySettings defaults."""
        with open(_CONFIG_PATH) as f:
            raw = json.load(f)["celery_app"]
        settings = raw.pop("celery_settings", {})
        return CeleryAppConfig(**raw, celery_settings=CelerySettings(**settings))

    def get_rabbitmq_broker_string(self) -> str:
        """Build the AMQP broker URL (`amqp://user:password@host:port//`) from the config fields."""
        return f"amqp://{self.username}:{self.password}@{self.host}:{self.port}//"


def get_job_config() -> JobConfig:
    """Load the `job` block of config.json (DOWNLOAD, INGESTION and DATA_QUALITY command templates)."""
    with open(_CONFIG_PATH) as f:
        return JobConfig(**json.load(f)["job"])


class Job(BaseModel):
    """A single job in a chain — after it finishes successfully,
    its `next_jobs` are executed recursively (same as E2EReddit's Job)."""

    source_name: str
    job_type: EJobType
    table_name: str | None = None
    is_failed_if_error: bool = False
    next_jobs: list["Job"] = []

    class ConfigDict:
        """Makes the model immutable."""

        frozen = True

    def get_command(self) -> str:
        """Build the shell command of this job from the `job` block of config.json.

        Raises:
            ValueError: When an INGESTION or DATA_QUALITY job has no table_name configured in jobs.json.
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

        return job_config.data_quality.get_command_string(self.job_type.value, self.table_name)

    def run(self, logger: logging.Logger, log_file: str) -> None:
        """Execute this job's command, then its `next_jobs` recursively (only if this job succeeded).

        A non-zero return code stops the chain: it is logged as a warning, or raises
        when `is_failed_if_error` is set.

        Raises:
            subprocess.CalledProcessError: When the command fails and `is_failed_if_error` is true.
        """
        command = self.get_command()
        logger.info(f'Starting {self.job_type.value} job for "{self.source_name}".')
        logger.info(f"Executing command: {command}")

        returncode = run_command(command, log_file)

        if returncode != 0:
            message = f'{self.job_type.value} job for "{self.source_name}" exited with return code {returncode}'
            if self.is_failed_if_error:
                logger.error(message)
                raise subprocess.CalledProcessError(returncode=returncode, cmd=command)
            logger.warning(f"{message} — next jobs will not be executed")
            return

        logger.info(f'Finished {self.job_type.value} job for "{self.source_name}". Return code: {returncode}.')

        # run the next jobs
        for next_job in self.next_jobs:
            next_job.run(logger=logger, log_file=log_file)

    @staticmethod
    def from_config(source_name: str) -> "Job":
        """Returns the root job of the chain for given source from jobs.json"""
        with open(_JOBS_PATH) as f:
            return Job(**json.load(f)[source_name])
