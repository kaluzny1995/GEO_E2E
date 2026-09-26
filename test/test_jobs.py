import io
import json
import logging
import subprocess

import pytest

from model import EJobType, Job


class CommandRunner:
    """ Fake for model.run_command — records commands and returns queued return codes. """

    def __init__(self, monkeypatch):
        self.calls = []
        self.results = []
        monkeypatch.setattr("model.run_command", self._run)

    @property
    def commands(self):
        return [command for command, _ in self.calls]

    def _run(self, command, log_file):
        self.calls.append((command, log_file))
        return self.results.pop(0) if self.results else 0


@pytest.fixture()
def runner(monkeypatch):
    return CommandRunner(monkeypatch)


@pytest.fixture()
def logger():
    log = logging.getLogger("test_jobs")
    log.handlers.clear()
    log.propagate = False
    log.setLevel(logging.INFO)
    stream = io.StringIO()
    log.addHandler(logging.StreamHandler(stream))
    return log, stream


@pytest.fixture()
def jobs_path(tmp_path, monkeypatch):
    """ Point Job.from_config() at a temporary jobs.json """
    path = tmp_path / "jobs.json"
    monkeypatch.setattr("model._JOBS_PATH", str(path))
    return path


def _chain() -> dict:
    return {
        "NIFC": {
            "source_name": "NIFC",
            "job_type": "DOWNLOAD",
            "is_failed_if_error": False,
            "next_jobs": [
                {
                    "source_name": "NIFC",
                    "job_type": "INGESTION",
                    "table_name": "nifc_fire",
                    "is_failed_if_error": False,
                    "next_jobs": []
                }
            ]
        }
    }


def test_job_from_config_returns_chain_root(jobs_path):
    # Arrange
    jobs_path.write_text(json.dumps(_chain()))

    # Act
    job = Job.from_config("NIFC")

    # Assert
    assert job.source_name == "NIFC"
    assert job.job_type == EJobType.DOWNLOAD
    assert len(job.next_jobs) == 1
    assert job.next_jobs[0].job_type == EJobType.INGESTION
    assert job.next_jobs[0].table_name == "nifc_fire"
    assert job.next_jobs[0].next_jobs == []


def test_job_from_config_unknown_source(jobs_path):
    # Arrange
    jobs_path.write_text(json.dumps(_chain()))

    # Act & Assert
    with pytest.raises(KeyError):
        Job.from_config("UNKNOWN")


def test_job_get_command_download():
    # Arrange
    job = Job(source_name="NIFC", job_type=EJobType.DOWNLOAD)

    # Act
    result = job.get_command()

    # Assert
    assert result == "cd ..;cd GEO_Download;conda run --no-capture-output -n geo_download_311 python run_download.py NIFC"


def test_job_get_command_ingestion():
    # Arrange
    job = Job(source_name="NIFC", job_type=EJobType.INGESTION, table_name="nifc_fire")

    # Act
    result = job.get_command()

    # Assert
    assert result == "cd ..;cd GEO_ETL;conda run --no-capture-output -n geo_etl_311 python run_etl.py INGESTION nifc_fire"


def test_job_get_command_ingestion_without_table_name_raises():
    # Arrange
    job = Job(source_name="NIFC", job_type=EJobType.INGESTION)

    # Act & Assert
    with pytest.raises(ValueError, match="no table_name"):
        job.get_command()


def test_run_executes_next_jobs_after_success(runner, logger):
    # Arrange
    job = Job(
        source_name="NIFC",
        job_type=EJobType.DOWNLOAD,
        next_jobs=[Job(source_name="NIFC", job_type=EJobType.INGESTION, table_name="nifc_fire")],
    )
    log, _ = logger
    runner.results = [0, 0]

    # Act
    job.run(logger=log, log_file="logs/NIFC/test.log")

    # Assert
    assert runner.commands == [
        "cd ..;cd GEO_Download;conda run --no-capture-output -n geo_download_311 python run_download.py NIFC",
        "cd ..;cd GEO_ETL;conda run --no-capture-output -n geo_etl_311 python run_etl.py INGESTION nifc_fire",
    ]
    assert {log_file for _, log_file in runner.calls} == {"logs/NIFC/test.log"}


def test_run_stops_chain_when_job_fails(runner, logger):
    # Arrange
    job = Job(
        source_name="NIFC",
        job_type=EJobType.DOWNLOAD,
        next_jobs=[Job(source_name="NIFC", job_type=EJobType.INGESTION, table_name="nifc_fire")],
    )
    log, stream = logger
    runner.results = [1]

    # Act
    job.run(logger=log, log_file="logs/NIFC/test.log")

    # Assert
    assert runner.commands == [
        "cd ..;cd GEO_Download;conda run --no-capture-output -n geo_download_311 python run_download.py NIFC"
    ]
    assert "next jobs will not be executed" in stream.getvalue()


def test_run_does_not_raise_when_is_failed_if_error_is_false(runner, logger):
    # Arrange
    job = Job(
        source_name="NIFC",
        job_type=EJobType.INGESTION,
        table_name="nifc_fire",
        is_failed_if_error=False,
    )
    log, _ = logger
    runner.results = [1]

    # Act
    job.run(logger=log, log_file="logs/NIFC/test.log")

    # Assert
    assert len(runner.commands) == 1


def test_run_raises_when_is_failed_if_error_is_true(runner, logger):
    # Arrange
    job = Job(
        source_name="NIFC",
        job_type=EJobType.INGESTION,
        table_name="nifc_fire",
        is_failed_if_error=True,
    )
    log, _ = logger
    runner.results = [1]

    # Act & Assert
    with pytest.raises(subprocess.CalledProcessError):
        job.run(logger=log, log_file="logs/NIFC/test.log")
    assert len(runner.commands) == 1
