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


class CeleryAppConfig(BaseModel):
    name: str
    username: str
    password: str
    host: str
    port: int
    tasks: List[CeleryTask]

    class ConfigDict:
        frozen = True

    @staticmethod
    def from_config():
        with open(_CONFIG_PATH, "r") as f:
            return CeleryAppConfig(**json.load(f)['celery_app'])

    def get_rabbitmq_broker_string(self) -> str:
        return f"amqp://{self.username}:{self.password}@{self.host}:{self.port}//"


def get_job_config() -> JobConfig:
    with open(_CONFIG_PATH, "r") as f:
        return JobConfig(**json.load(f)['job'])
