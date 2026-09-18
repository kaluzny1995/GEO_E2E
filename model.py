import json
import os
from typing import List
from pydantic import BaseModel

_PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
_CONFIG_PATH = os.path.join(_PROJECT_DIR, "config.json")


class CeleryTask(BaseModel):
    source_name: str
    month: int | str
    day: int | str
    hour: int | str
    minute: int | str
    timezone: str
    name: str


class JobConfig(BaseModel):
    folder_name: str
    python_script: str

    class ConfigDict:
        frozen = True

    def get_command_pattern_string(self) -> str:
        return f"cd ..;cd {self.folder_name};python {self.python_script}"


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
