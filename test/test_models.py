import json
import pytest
from unittest.mock import patch

from model import CeleryTask, CelerySettings, CeleryAppConfig, JobConfig, get_job_config


def test_celery_task_creation():
    # Arrange
    # Act
    task = CeleryTask(
        source_name="USGS_Earthquake",
        month="*",
        day="*",
        hour=2,
        minute=0,
        timezone="Europe/Warsaw",
        name="download_USGS_Earthquake"
    )

    # Assert
    assert task.source_name == "USGS_Earthquake"
    assert task.hour == 2
    assert task.minute == 0
    assert task.timezone == "Europe/Warsaw"


def test_celery_task_int_fields():
    # Arrange
    # Act
    task = CeleryTask(
        source_name="Test",
        month=1,
        day=15,
        hour=10,
        minute=30,
        timezone="UTC",
        name="test_task"
    )

    # Assert
    assert task.month == 1
    assert task.day == 15
    assert task.hour == 10
    assert task.minute == 30


def test_celery_settings_defaults():
    # Arrange
    # Act
    settings = CelerySettings()

    # Assert
    assert settings.task_time_limit == 600
    assert settings.task_soft_time_limit == 480
    assert settings.worker_max_tasks_per_child == 20
    assert settings.worker_max_memory_per_child == 400000
    assert settings.worker_prefetch_multiplier == 1
    assert settings.task_acks_late is True


def test_celery_settings_custom():
    # Arrange
    # Act
    settings = CelerySettings(
        task_time_limit=300,
        task_soft_time_limit=240,
        worker_max_tasks_per_child=10,
        worker_max_memory_per_child=200000,
        worker_prefetch_multiplier=2,
        task_acks_late=False,
    )

    # Assert
    assert settings.task_time_limit == 300
    assert settings.task_soft_time_limit == 240
    assert settings.worker_max_tasks_per_child == 10
    assert settings.worker_max_memory_per_child == 200000
    assert settings.worker_prefetch_multiplier == 2
    assert settings.task_acks_late is False


def test_job_config_creation():
    # Arrange
    # Act
    config = JobConfig(
        folder_name="GEO_Download",
        python_script="run_download.py"
    )

    # Assert
    assert config.folder_name == "GEO_Download"
    assert config.python_script == "run_download.py"


def test_job_config_get_command_pattern_string():
    # Arrange
    config = JobConfig(
        folder_name="GEO_Download",
        python_script="run_download.py"
    )

    # Act
    result = config.get_command_pattern_string()

    # Assert
    assert result == "cd ..;cd GEO_Download;python run_download.py"


def test_celery_app_config_get_rabbitmq_broker_string():
    # Arrange
    config = CeleryAppConfig(
        name="test_app",
        username="guest",
        password="guest",
        host="localhost",
        port=5672,
        tasks=[]
    )

    # Act
    result = config.get_rabbitmq_broker_string()

    # Assert
    assert result == "amqp://guest:guest@localhost:5672//"


def test_get_job_config(tmp_path):
    # Arrange
    config_file = tmp_path / "config.json"
    config_file.write_text(json.dumps({
        "job": {
            "folder_name": "TestFolder",
            "python_script": "test_script.py"
        }
    }))

    # Act
    with patch("model._CONFIG_PATH", str(config_file)):
        result = get_job_config()

    # Assert
    assert result.folder_name == "TestFolder"
    assert result.python_script == "test_script.py"


def test_celery_app_config_from_config(tmp_path):
    # Arrange
    config_file = tmp_path / "config.json"
    config_file.write_text(json.dumps({
        "celery_app": {
            "name": "test_app",
            "username": "user",
            "password": "pass",
            "host": "127.0.0.1",
            "port": 5672,
            "celery_settings": {
                "task_time_limit": 300,
                "task_soft_time_limit": 240,
                "worker_max_tasks_per_child": 10,
                "worker_max_memory_per_child": 200000,
                "worker_prefetch_multiplier": 2,
                "task_acks_late": False,
            },
            "tasks": [
                {
                    "source_name": "TestSource",
                    "month": "*",
                    "day": "*",
                    "hour": 3,
                    "minute": 15,
                    "timezone": "UTC",
                    "name": "test_task"
                }
            ]
        }
    }))

    # Act
    with patch("model._CONFIG_PATH", str(config_file)):
        result = CeleryAppConfig.from_config()

    # Assert
    assert result.name == "test_app"
    assert result.username == "user"
    assert len(result.tasks) == 1
    assert result.tasks[0].source_name == "TestSource"
    assert result.tasks[0].hour == 3
    assert result.celery_settings.task_time_limit == 300
    assert result.celery_settings.task_soft_time_limit == 240
    assert result.celery_settings.worker_max_tasks_per_child == 10
    assert result.celery_settings.worker_max_memory_per_child == 200000
    assert result.celery_settings.worker_prefetch_multiplier == 2
    assert result.celery_settings.task_acks_late is False
