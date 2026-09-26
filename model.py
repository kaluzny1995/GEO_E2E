import json
import logging
import os
import subprocess
from enum import Enum
from typing import List
from pydantic import BaseModel

from util import run_command

_PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
_CONFIG_PATH = os.path.join(_PROJECT_DIR, "config.json")
_JOBS_PATH = os.path.join(_PROJECT_DIR, "jobs.json")


class EJobType(str, Enum):
    DOWNLOAD = "DOWNLOAD"
    INGESTION = "INGESTION"


class CeleryTask(BaseModel):
    source_name: str
    month: int | str
    day: int | str
    hour: int | str
    minute: int | str
    timezone: str
    name: str


class JobTypeConfig(BaseModel):
    folder_name: str
    python_script: str
    conda_env: str | None = None

    class ConfigDict:
        frozen = True

    def get_command_pattern_string(self) -> str:
        conda_part = f"conda run --no-capture-output -n {self.conda_env} " if self.conda_env else ""
        return f"cd ..;cd {self.folder_name};{conda_part}python {self.python_script}"

    def get_command_string(self, *args: str) -> str:
        return f"{self.get_command_pattern_string()} {' '.join(args)}"


class JobConfig(BaseModel):
    download: JobTypeConfig
    ingestion: JobTypeConfig

    class ConfigDict:
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
        frozen = True


class CeleryAppConfig(BaseModel):
    name: str
    username: str
    password: str
    host: str
    port: int
    tasks: List[CeleryTask]
    celery_settings: CelerySettings = CelerySettings()

    class ConfigDict:
        frozen = True

    @staticmethod
    def from_config():
        with open(_CONFIG_PATH, "r") as f:
            raw = json.load(f)['celery_app']
        settings = raw.pop('celery_settings', {})
        return CeleryAppConfig(**raw, celery_settings=CelerySettings(**settings))

    def get_rabbitmq_broker_string(self) -> str:
        return f"amqp://{self.username}:{self.password}@{self.host}:{self.port}//"


def get_job_config() -> JobConfig:
    with open(_CONFIG_PATH, "r") as f:
        return JobConfig(**json.load(f)['job'])


class Job(BaseModel):
    """ A single job in a chain — after it finishes successfully,
        its `next_jobs` are executed recursively (same as E2EReddit's Job). """
    source_name: str
    job_type: EJobType
    table_name: str | None = None
    is_failed_if_error: bool = False
    next_jobs: List['Job'] = []

    class ConfigDict:
        frozen = True

    def get_command(self) -> str:
        """ Build the shell command of this job from the `job` block of config.json """
        job_config = get_job_config()

        if self.job_type == EJobType.DOWNLOAD:
            return job_config.download.get_command_string(self.source_name)

        if self.table_name is None:
            raise ValueError(f'INGESTION job for "{self.source_name}" has no table_name configured in jobs.json')

        return job_config.ingestion.get_command_string(self.job_type.value, self.table_name)

    def run(self, logger: logging.Logger, log_file: str) -> None:
        """ Executes the job command and then its next jobs (only if this job succeeded) """
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
    def from_config(source_name: str) -> 'Job':
        """ Returns the root job of the chain for given source from jobs.json """
        with open(_JOBS_PATH, "r") as f:
            return Job(**json.load(f)[source_name])
