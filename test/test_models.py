"""Unit tests for the pydantic config models defined in model.py."""

import json
from pathlib import Path
from unittest.mock import patch

from model import JobConfig, JobTypeConfig, get_job_config


def test_job_type_config_creation() -> None:
    """JobTypeConfig defaults conda_env to None when it is not configured."""
    # Arrange
    # Act
    config = JobTypeConfig(folder_name="GEO_Download", python_script="run_download.py")

    # Assert
    assert config.folder_name == "GEO_Download"
    assert config.python_script == "run_download.py"
    assert config.conda_env is None


def test_job_type_config_get_command_pattern_string_without_conda_env() -> None:
    """Without conda_env the command runs the bare python script."""
    # Arrange
    config = JobTypeConfig(folder_name="GEO_Download", python_script="run_download.py")

    # Act
    result = config.get_command_pattern_string()

    # Assert
    assert result == "cd ..;cd GEO_Download;python run_download.py"


def test_job_type_config_get_command_pattern_string_with_conda_env() -> None:
    """With conda_env the command is wrapped in conda run."""
    # Arrange
    config = JobTypeConfig(folder_name="GEO_ETL", python_script="run_etl.py", conda_env="geo_etl_311")

    # Act
    result = config.get_command_pattern_string()

    # Assert
    assert result == "cd ..;cd GEO_ETL;conda run --no-capture-output -n geo_etl_311 python run_etl.py"


def test_job_type_config_get_command_string() -> None:
    """get_command_string() appends the given arguments to the pattern."""
    # Arrange
    config = JobTypeConfig(folder_name="GEO_ETL", python_script="run_etl.py", conda_env="geo_etl_311")

    # Act
    result = config.get_command_string("INGESTION", "nifc_fire")

    # Assert
    assert result == (
        "cd ..;cd GEO_ETL;conda run --no-capture-output -n geo_etl_311 python run_etl.py INGESTION nifc_fire"
    )


def test_job_config_creation() -> None:
    """JobConfig exposes the download, ingestion, data_quality and single_etl command templates."""
    # Arrange
    # Act
    config = JobConfig(
        download=JobTypeConfig(folder_name="GEO_Download", python_script="run_download.py"),
        ingestion=JobTypeConfig(folder_name="GEO_ETL", python_script="run_etl.py", conda_env="geo_etl_311"),
        data_quality=JobTypeConfig(folder_name="GEO_ETL", python_script="run_etl.py", conda_env="geo_etl_311"),
        single_etl=JobTypeConfig(folder_name="GEO_ETL", python_script="run_etl.py", conda_env="geo_etl_311"),
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
