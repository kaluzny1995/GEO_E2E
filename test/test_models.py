"""Unit tests for the pydantic config models defined in model.py."""

import json
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from model import EJobType, JobConfig, JobTypeConfig, get_dag_settings, get_job_config
from test._helpers import ETL_CMD_PREFIX


def test_job_type_config_creation() -> None:
    """JobTypeConfig defaults conda_env to None when it is not configured."""
    # Arrange
    # Act
    config = JobTypeConfig(folder_name="GEO_Download", python_script="run_download.py")

    # Assert
    assert config.folder_name == "GEO_Download"
    assert config.python_script == "run_download.py"
    assert config.conda_env is None


@pytest.mark.parametrize(
    ("config", "expected"),
    [
        pytest.param(
            JobTypeConfig(folder_name="GEO_Download", python_script="run_download.py"),
            "cd ..;cd GEO_Download;python run_download.py",
            id="without-conda-env",
        ),
        pytest.param(
            JobTypeConfig(folder_name="GEO_ETL", python_script="run_etl.py", conda_env="geo_etl_311"),
            ETL_CMD_PREFIX,
            id="with-conda-env",
        ),
    ],
)
def test_job_type_config_get_command_pattern_string(config: JobTypeConfig, expected: str) -> None:
    """Without conda_env the command runs the bare python script, with conda_env it is wrapped in conda run."""
    # Act
    result = config.get_command_pattern_string()

    # Assert
    assert result == expected


def test_job_type_config_get_command_string() -> None:
    """get_command_string() appends the given arguments to the pattern."""
    # Arrange
    config = JobTypeConfig(folder_name="GEO_ETL", python_script="run_etl.py", conda_env="geo_etl_311")

    # Act
    result = config.get_command_string("INGESTION", "nifc_fire")

    # Assert
    assert result == f"{ETL_CMD_PREFIX} INGESTION nifc_fire"


def test_job_config_creation() -> None:
    """JobConfig exposes a command template per job type (download, ingestion, data_quality,
    single_etl and view)."""
    # Arrange
    # Act
    config = JobConfig(
        download=JobTypeConfig(folder_name="GEO_Download", python_script="run_download.py"),
        ingestion=JobTypeConfig(folder_name="GEO_ETL", python_script="run_etl.py", conda_env="geo_etl_311"),
        data_quality=JobTypeConfig(folder_name="GEO_ETL", python_script="run_etl.py", conda_env="geo_etl_311"),
        single_etl=JobTypeConfig(folder_name="GEO_ETL", python_script="run_etl.py", conda_env="geo_etl_311"),
        view=JobTypeConfig(folder_name="GEO_ETL", python_script="run_etl.py", conda_env="geo_etl_311"),
    )

    # Assert
    assert config.download.folder_name == "GEO_Download"
    assert config.download.python_script == "run_download.py"
    assert config.ingestion.folder_name == "GEO_ETL"
    assert config.ingestion.python_script == "run_etl.py"
    assert config.ingestion.conda_env == "geo_etl_311"
    assert config.data_quality.folder_name == "GEO_ETL"
    assert config.data_quality.python_script == "run_etl.py"
    assert config.data_quality.conda_env == "geo_etl_311"
    assert config.single_etl.folder_name == "GEO_ETL"
    assert config.single_etl.python_script == "run_etl.py"
    assert config.single_etl.conda_env == "geo_etl_311"
    assert config.view.folder_name == "GEO_ETL"
    assert config.view.python_script == "run_etl.py"
    assert config.view.conda_env == "geo_etl_311"


def test_get_job_config(tmp_path: Path) -> None:
    """get_job_config() reads the `job` block of config.json."""
    # Arrange
    config_file = tmp_path / "config.json"
    config_file.write_text(
        json.dumps(
            {
                "job": {
                    "download": {
                        "folder_name": "TestFolder",
                        "python_script": "test_script.py",
                        "conda_env": "test_download_env",
                    },
                    "ingestion": {
                        "folder_name": "TestEtlFolder",
                        "python_script": "test_etl_script.py",
                        "conda_env": "test_env",
                    },
                    "data_quality": {
                        "folder_name": "TestEtlFolder",
                        "python_script": "test_etl_script.py",
                        "conda_env": "test_dq_env",
                    },
                    "single_etl": {
                        "folder_name": "TestEtlFolder",
                        "python_script": "test_etl_script.py",
                        "conda_env": "test_single_etl_env",
                    },
                    "view": {
                        "folder_name": "TestEtlFolder",
                        "python_script": "test_etl_script.py",
                        "conda_env": "test_view_env",
                    },
                }
            }
        )
    )

    # Act
    with patch("model._CONFIG_PATH", str(config_file)):
        result = get_job_config()

    # Assert
    assert result.download.folder_name == "TestFolder"
    assert result.download.python_script == "test_script.py"
    assert result.download.conda_env == "test_download_env"
    assert result.ingestion.folder_name == "TestEtlFolder"
    assert result.ingestion.python_script == "test_etl_script.py"
    assert result.ingestion.conda_env == "test_env"
    assert result.data_quality.folder_name == "TestEtlFolder"
    assert result.data_quality.python_script == "test_etl_script.py"
    assert result.data_quality.conda_env == "test_dq_env"
    assert result.single_etl.folder_name == "TestEtlFolder"
    assert result.single_etl.python_script == "test_etl_script.py"
    assert result.single_etl.conda_env == "test_single_etl_env"
    assert result.view.folder_name == "TestEtlFolder"
    assert result.view.python_script == "test_etl_script.py"
    assert result.view.conda_env == "test_view_env"


def _dag_block() -> dict:
    """The minimal required fields of a `dag` config block (everything else has defaults)."""
    return {
        "start_date": "2026-01-01",
        "timezone": "Europe/Warsaw",
        "pools": {
            "download": "download",
            "ingestion": "etl",
            "data_quality": "etl",
            "single_etl": "etl",
            "view": "etl",
            "verdict": "etl",
        },
        "pool_slots": {"download": 3, "etl": 2},
    }


def test_dag_settings_defaults() -> None:
    """DagSettings falls back to the shipped defaults for every optional knob."""
    # Arrange & Act
    settings = get_dag_settings()

    # Assert
    assert settings.dag_id == "geo_pipeline"
    assert settings.start_date == date(2026, 1, 1)
    assert settings.timezone == "Europe/Warsaw"
    assert settings.catchup is False
    assert settings.max_active_runs == 1
    assert settings.tags == ["geo", "e2e"]
    assert settings.retries == 0
    assert settings.execution_timeout_minutes == 30
    assert settings.union_source == "Union"
    assert settings.pool_slots == {"download": 3, "etl": 2}


@pytest.mark.parametrize(
    ("job_type", "pool"),
    [
        pytest.param(EJobType.DOWNLOAD, "download", id="download"),
        pytest.param(EJobType.INGESTION, "etl", id="ingestion"),
        pytest.param(EJobType.DATA_QUALITY, "etl", id="data-quality"),
        pytest.param(EJobType.SINGLE_ETL, "etl", id="single-etl"),
        pytest.param(EJobType.VIEW, "etl", id="view"),
        pytest.param(EJobType.VERDICT, "etl", id="verdict"),
    ],
)
def test_dag_settings_pool_for_maps_every_job_type(job_type: EJobType, pool: str) -> None:
    """pool_for() resolves the configured pool name for each EJobType."""
    # Act & Assert
    assert get_dag_settings().pool_for(job_type) == pool


def test_dag_settings_unknown_timezone_raises(tmp_path: Path) -> None:
    """An unknown IANA timezone is rejected at config load."""
    # Arrange
    block = _dag_block()
    block["timezone"] = "Mars/Olympus_Mons"
    config_file = tmp_path / "config.json"
    config_file.write_text(json.dumps({"dag": block}))

    # Act & Assert
    with patch("model._CONFIG_PATH", str(config_file)), pytest.raises(ValidationError, match="unknown IANA"):
        get_dag_settings()


def test_get_dag_settings_reads_dag_block(tmp_path: Path) -> None:
    """get_dag_settings() parses the `dag` block of config.json."""
    # Arrange
    config_file = tmp_path / "config.json"
    config_file.write_text(json.dumps({"dag": _dag_block()}))

    # Act
    with patch("model._CONFIG_PATH", str(config_file)):
        settings = get_dag_settings()

    # Assert
    assert settings.start_date == date(2026, 1, 1)
    assert settings.timezone == "Europe/Warsaw"
    assert settings.pools.download == "download"
    assert settings.pools.single_etl == "etl"
    assert settings.pool_slots == {"download": 3, "etl": 2}
