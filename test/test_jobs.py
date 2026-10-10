"""Tests for Job chains: loading from jobs.json and command building."""

import json
from pathlib import Path

import pytest

from model import EJobType, Job


@pytest.fixture()
def jobs_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point Job.from_config() at a temporary jobs.json"""
    path = tmp_path / "jobs.json"
    monkeypatch.setattr("model._JOBS_PATH", str(path))
    return path


def _chain() -> dict:
    """A minimal DOWNLOAD -> INGESTION -> (DATA_QUALITY, SINGLE_ETL -> DATA_QUALITY) chain for NIFC."""
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
                        },
                        {
                            "source_name": "NIFC",
                            "job_type": "SINGLE_ETL",
                            "table_name": "nifc_fire_etl",
                            "is_failed_if_error": False,
                            "next_jobs": [
                                {
                                    "source_name": "NIFC",
                                    "job_type": "DATA_QUALITY",
                                    "table_name": "nifc_fire_etl",
                                    "is_failed_if_error": False,
                                    "next_jobs": [],
                                }
                            ],
                        },
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

    ingestion = job.next_jobs[0]
    assert ingestion.job_type == EJobType.INGESTION
    assert ingestion.table_name == "nifc_fire"
    assert len(ingestion.next_jobs) == 2

    data_quality = ingestion.next_jobs[0]
    assert data_quality.job_type == EJobType.DATA_QUALITY
    assert data_quality.table_name == "nifc_fire"
    assert data_quality.next_jobs == []

    single_etl = ingestion.next_jobs[1]
    assert single_etl.job_type == EJobType.SINGLE_ETL
    assert single_etl.table_name == "nifc_fire_etl"
    assert len(single_etl.next_jobs) == 1

    etl_data_quality = single_etl.next_jobs[0]
    assert etl_data_quality.job_type == EJobType.DATA_QUALITY
    assert etl_data_quality.table_name == "nifc_fire_etl"
    assert etl_data_quality.next_jobs == []


def test_job_from_config_unknown_source(jobs_path: Path) -> None:
    """Job.from_config() raises KeyError for a source missing from jobs.json."""
    # Arrange
    jobs_path.write_text(json.dumps(_chain()))

    # Act & Assert
    with pytest.raises(KeyError):
        Job.from_config("UNKNOWN")


def test_jobs_json_chains_data_quality_after_ingestion() -> None:
    """Every source chain in jobs.json runs DATA_QUALITY on its table right after a successful INGESTION
    (the `Union` entry is skipped — it is a bare SINGLE_ETL job, not a source chain)."""
    # Arrange
    with open(Path(__file__).resolve().parents[1] / "jobs.json") as f:
        sources = json.load(f)

    # Act & Assert
    for source_name, raw in sources.items():
        if raw["job_type"] != "DOWNLOAD":
            continue
        ingestion = raw["next_jobs"][0]

        assert ingestion["job_type"] == "INGESTION", source_name
        assert len(ingestion["next_jobs"]) == 2, source_name

        data_quality = ingestion["next_jobs"][0]

        assert data_quality["job_type"] == "DATA_QUALITY", source_name
        assert data_quality["table_name"] == ingestion["table_name"], source_name
        assert data_quality["next_jobs"] == [], source_name


def test_jobs_json_chains_single_etl_and_etl_data_quality() -> None:
    """Every source chain in jobs.json chains SINGLE_ETL on the <table>_etl script under its INGESTION job,
    followed by a DATA_QUALITY job on the same etl table (the bare `Union` entry is skipped)."""
    # Arrange
    with open(Path(__file__).resolve().parents[1] / "jobs.json") as f:
        sources = json.load(f)

    # Act & Assert
    for source_name, raw in sources.items():
        if raw["job_type"] != "DOWNLOAD":
            continue
        ingestion = raw["next_jobs"][0]
        etl_table = f"{ingestion['table_name']}_etl"

        assert ingestion["job_type"] == "INGESTION", source_name

        single_etl = ingestion["next_jobs"][1]

        assert single_etl["job_type"] == "SINGLE_ETL", source_name
        assert single_etl["table_name"] == etl_table, source_name
        assert len(single_etl["next_jobs"]) == 1, source_name

        etl_data_quality = single_etl["next_jobs"][0]

        assert etl_data_quality["job_type"] == "DATA_QUALITY", source_name
        assert etl_data_quality["table_name"] == etl_table, source_name
        assert etl_data_quality["next_jobs"] == [], source_name


def test_jobs_json_every_job_stops_chain_without_failing_task() -> None:
    """Every job in jobs.json keeps is_failed_if_error=false and is_stop_if_error=true.
    The flags are documentation only — the DAG maps them to all_success trigger rules
    (failed jobs skip their downstream jobs)."""
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


def test_jobs_json_union_job_stacks_every_source_table() -> None:
    """The `Union` entry is a bare SINGLE_ETL job on union_etl with the shipped failure flags —
    the DAG turns it into the fan-in task every source chain waits for."""
    # Arrange
    with open(Path(__file__).resolve().parents[1] / "jobs.json") as f:
        union = json.load(f)["Union"]

    # Act & Assert
    assert union == {
        "source_name": "Union",
        "job_type": "SINGLE_ETL",
        "table_name": "union_etl",
        "is_failed_if_error": False,
        "is_stop_if_error": True,
        "next_jobs": [],
    }


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


def test_job_get_command_single_etl() -> None:
    """A SINGLE_ETL job builds the run_etl.py command with the etl script name."""
    # Arrange
    job = Job(source_name="NIFC", job_type=EJobType.SINGLE_ETL, table_name="nifc_fire_etl")

    # Act
    result = job.get_command()

    # Assert
    assert result == (
        "cd ..;cd GEO_ETL;conda run --no-capture-output -n geo_etl_311 python run_etl.py SINGLE_ETL nifc_fire_etl"
    )


def test_job_get_command_single_etl_without_table_name_raises() -> None:
    """A SINGLE_ETL job without table_name raises ValueError."""
    # Arrange
    job = Job(source_name="NIFC", job_type=EJobType.SINGLE_ETL)

    # Act & Assert
    with pytest.raises(ValueError, match="no table_name"):
        job.get_command()
