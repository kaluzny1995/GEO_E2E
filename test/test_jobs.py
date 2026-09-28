"""Tests for Job chains: loading from jobs.json, command building and recursive execution."""

import io
import json
import logging
import subprocess
from pathlib import Path

import pytest

from model import EJobType, Job

LogCapture = tuple[logging.Logger, io.StringIO]


class CommandRunner:
    """Fake for model.run_command — records commands and returns queued return codes."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Install this fake as model.run_command and prepare the call/result queues."""
        self.calls = []
        self.results = []
        monkeypatch.setattr("model.run_command", self._run)

    @property
    def commands(self) -> list[str]:
        """Commands recorded so far (without their log files)."""
        return [command for command, _ in self.calls]

    def _run(self, command: str, log_file: str) -> int:
        """Record the call and pop the next queued return code (0 when none left)."""
        self.calls.append((command, log_file))
        return self.results.pop(0) if self.results else 0


@pytest.fixture()
def runner(monkeypatch: pytest.MonkeyPatch) -> CommandRunner:
    """CommandRunner installed as model.run_command."""
    return CommandRunner(monkeypatch)


@pytest.fixture()
def logger() -> LogCapture:
    """Logger writing to an in-memory stream, returned as (logger, stream)."""
    log = logging.getLogger("test_jobs")
    log.handlers.clear()
    log.propagate = False
    log.setLevel(logging.INFO)
    stream = io.StringIO()
    log.addHandler(logging.StreamHandler(stream))
    return log, stream


@pytest.fixture()
def jobs_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point Job.from_config() at a temporary jobs.json"""
    path = tmp_path / "jobs.json"
    monkeypatch.setattr("model._JOBS_PATH", str(path))
    return path


def _chain() -> dict:
    """A minimal DOWNLOAD -> INGESTION -> DATA_QUALITY chain for NIFC, ready for json.dumps()."""
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
                    "next_jobs": [
                        {
                            "source_name": "NIFC",
                            "job_type": "DATA_QUALITY",
                            "table_name": "nifc_fire",
                            "is_failed_if_error": False,
                            "next_jobs": [],
                        }
                    ],
                }
            ],
        }
    }


def test_job_from_config_returns_chain_root(jobs_path: Path) -> None:
    """Job.from_config() rebuilds the whole chain starting at the root DOWNLOAD job."""
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
    assert len(job.next_jobs[0].next_jobs) == 1
    assert job.next_jobs[0].next_jobs[0].job_type == EJobType.DATA_QUALITY
    assert job.next_jobs[0].next_jobs[0].table_name == "nifc_fire"
    assert job.next_jobs[0].next_jobs[0].next_jobs == []


def test_job_from_config_unknown_source(jobs_path: Path) -> None:
    """Job.from_config() raises KeyError for a source missing from jobs.json."""
    # Arrange
    jobs_path.write_text(json.dumps(_chain()))

    # Act & Assert
    with pytest.raises(KeyError):
        Job.from_config("UNKNOWN")


def test_jobs_json_chains_data_quality_after_ingestion() -> None:
    """Every source in jobs.json runs DATA_QUALITY on its table right after a successful INGESTION."""
    # Arrange
    with open(Path(__file__).resolve().parents[1] / "jobs.json") as f:
        sources = json.load(f)

    # Act & Assert
    for source_name, raw in sources.items():
        ingestion = raw["next_jobs"][0]

        assert ingestion["job_type"] == "INGESTION", source_name
        assert len(ingestion["next_jobs"]) == 1, source_name

        data_quality = ingestion["next_jobs"][0]

        assert data_quality["job_type"] == "DATA_QUALITY", source_name
        assert data_quality["table_name"] == ingestion["table_name"], source_name
        assert data_quality["next_jobs"] == [], source_name


def test_jobs_json_every_job_stops_chain_without_failing_task() -> None:
    """Every job in jobs.json sets is_failed_if_error=false and is_stop_if_error=true,
    so a failure ends the chain with a warning while the Celery task still succeeds."""
    # Arrange
    with open(Path(__file__).resolve().parents[1] / "jobs.json") as f:
        sources = json.load(f)

    # Act & Assert
    for source_name, raw in sources.items():
        stack = [raw]
        while stack:
            job = stack.pop()
            location = f"{source_name}/{job['job_type']}"
            assert job["is_failed_if_error"] is False, location
            assert job["is_stop_if_error"] is True, location
            stack.extend(job["next_jobs"])


def test_job_get_command_download() -> None:
    """A DOWNLOAD job builds the run_download.py command with the source name."""
    # Arrange
    job = Job(source_name="NIFC", job_type=EJobType.DOWNLOAD)

    # Act
    result = job.get_command()

    # Assert
    assert result == (
        "cd ..;cd GEO_Download;conda run --no-capture-output -n geo_download_311 python run_download.py NIFC"
    )


def test_job_get_command_ingestion() -> None:
    """An INGESTION job builds the run_etl.py command with the table name."""
    # Arrange
    job = Job(source_name="NIFC", job_type=EJobType.INGESTION, table_name="nifc_fire")

    # Act
    result = job.get_command()

    # Assert
    assert result == (
        "cd ..;cd GEO_ETL;conda run --no-capture-output -n geo_etl_311 python run_etl.py INGESTION nifc_fire"
    )


def test_job_get_command_ingestion_without_table_name_raises() -> None:
    """An INGESTION job without table_name raises ValueError."""
    # Arrange
    job = Job(source_name="NIFC", job_type=EJobType.INGESTION)

    # Act & Assert
    with pytest.raises(ValueError, match="no table_name"):
        job.get_command()


def test_job_get_command_data_quality() -> None:
    """A DATA_QUALITY job builds the run_etl.py command with the plain table name."""
    # Arrange
    job = Job(source_name="NIFC", job_type=EJobType.DATA_QUALITY, table_name="nifc_fire")

    # Act
    result = job.get_command()

    # Assert
    assert result == (
        "cd ..;cd GEO_ETL;conda run --no-capture-output -n geo_etl_311 python run_etl.py DATA_QUALITY nifc_fire"
    )


def test_job_get_command_data_quality_without_table_name_raises() -> None:
    """A DATA_QUALITY job without table_name raises ValueError."""
    # Arrange
    job = Job(source_name="NIFC", job_type=EJobType.DATA_QUALITY)

    # Act & Assert
    with pytest.raises(ValueError, match="no table_name"):
        job.get_command()


def test_run_executes_next_jobs_after_success(runner: CommandRunner, logger: LogCapture) -> None:
    """After a successful job the chained jobs run in order with the same log file."""
    # Arrange
    job = Job(
        source_name="NIFC",
        job_type=EJobType.DOWNLOAD,
        next_jobs=[
            Job(
                source_name="NIFC",
                job_type=EJobType.INGESTION,
                table_name="nifc_fire",
                next_jobs=[Job(source_name="NIFC", job_type=EJobType.DATA_QUALITY, table_name="nifc_fire")],
            )
        ],
    )
    log, _ = logger
    runner.results = [0, 0, 0]

    # Act
    job.run(logger=log, log_file="logs/NIFC/test.log")

    # Assert
    assert runner.commands == [
        "cd ..;cd GEO_Download;conda run --no-capture-output -n geo_download_311 python run_download.py NIFC",
        "cd ..;cd GEO_ETL;conda run --no-capture-output -n geo_etl_311 python run_etl.py INGESTION nifc_fire",
        "cd ..;cd GEO_ETL;conda run --no-capture-output -n geo_etl_311 python run_etl.py DATA_QUALITY nifc_fire",
    ]
    assert {log_file for _, log_file in runner.calls} == {"logs/NIFC/test.log"}


def test_run_skips_data_quality_when_ingestion_fails(runner: CommandRunner, logger: LogCapture) -> None:
    """A failing INGESTION with is_stop_if_error=true stops the chain — DATA_QUALITY never runs."""
    # Arrange
    job = Job(
        source_name="NIFC",
        job_type=EJobType.DOWNLOAD,
        next_jobs=[
            Job(
                source_name="NIFC",
                job_type=EJobType.INGESTION,
                table_name="nifc_fire",
                is_stop_if_error=True,
                next_jobs=[Job(source_name="NIFC", job_type=EJobType.DATA_QUALITY, table_name="nifc_fire")],
            )
        ],
    )
    log, stream = logger
    runner.results = [0, 1]

    # Act
    job.run(logger=log, log_file="logs/NIFC/test.log")

    # Assert
    assert runner.commands == [
        "cd ..;cd GEO_Download;conda run --no-capture-output -n geo_download_311 python run_download.py NIFC",
        "cd ..;cd GEO_ETL;conda run --no-capture-output -n geo_etl_311 python run_etl.py INGESTION nifc_fire",
    ]
    assert "next jobs will not be executed" in stream.getvalue()


def test_run_stops_chain_when_is_stop_if_error_is_true(runner: CommandRunner, logger: LogCapture) -> None:
    """With is_stop_if_error=true a non-zero return code skips next jobs and only warns."""
    # Arrange
    job = Job(
        source_name="NIFC",
        job_type=EJobType.DOWNLOAD,
        is_stop_if_error=True,
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


def test_run_continues_chain_when_failure_is_not_stopping(runner: CommandRunner, logger: LogCapture) -> None:
    """With both flags false a failing job warns and the next jobs run anyway."""
    # Arrange
    job = Job(
        source_name="NIFC",
        job_type=EJobType.DOWNLOAD,
        next_jobs=[Job(source_name="NIFC", job_type=EJobType.INGESTION, table_name="nifc_fire")],
    )
    log, stream = logger
    runner.results = [1, 0]

    # Act
    job.run(logger=log, log_file="logs/NIFC/test.log")

    # Assert
    assert runner.commands == [
        "cd ..;cd GEO_Download;conda run --no-capture-output -n geo_download_311 python run_download.py NIFC",
        "cd ..;cd GEO_ETL;conda run --no-capture-output -n geo_etl_311 python run_etl.py INGESTION nifc_fire",
    ]
    assert "continuing with next jobs" in stream.getvalue()


def test_run_does_not_raise_when_is_failed_if_error_is_false(runner: CommandRunner, logger: LogCapture) -> None:
    """With is_failed_if_error=false a failing job only logs a warning."""
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


def test_run_raises_when_is_failed_if_error_is_true(runner: CommandRunner, logger: LogCapture) -> None:
    """With is_failed_if_error=true a failing job raises CalledProcessError."""
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
