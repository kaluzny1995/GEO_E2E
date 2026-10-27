"""Tests for the flat jobs.json job registry: loading, reference validation and command building."""

import json
import re
from pathlib import Path

import pytest

from model import EJobType, Job, build_task_specs, load_jobs, resolve_schedule


@pytest.fixture()
def jobs_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point the jobs.json loader at a temporary file"""
    path = tmp_path / "jobs.json"
    monkeypatch.setattr("model._JOBS_PATH", str(path))
    return path


def _chain() -> list[dict]:
    """A minimal DOWNLOAD -> INGESTION -> (DATA_QUALITY, SINGLE_ETL -> DATA_QUALITY) chain for NIFC."""
    return [
        {
            "name": "DOWNLOAD-NIFC",
            "task_id": "NIFC.download",
            "source_name": "NIFC",
            "job_type": "DOWNLOAD",
            "schedule": "0 8 * * *",
            "is_next_skipped_if_error": True,
            "next_jobs": ["INGESTION-nifc_fire"],
        },
        {
            "name": "INGESTION-nifc_fire",
            "task_id": "NIFC.ingestion",
            "source_name": "NIFC",
            "job_type": "INGESTION",
            "table_name": "nifc_fire",
            "is_next_skipped_if_error": True,
            "next_jobs": ["DATA_QUALITY-nifc_fire", "SINGLE_ETL-nifc_fire_etl"],
        },
        {
            "name": "DATA_QUALITY-nifc_fire",
            "task_id": "NIFC.data_quality",
            "source_name": "NIFC",
            "job_type": "DATA_QUALITY",
            "table_name": "nifc_fire",
            "is_next_skipped_if_error": True,
            "next_jobs": [],
        },
        {
            "name": "SINGLE_ETL-nifc_fire_etl",
            "task_id": "NIFC.single_etl",
            "source_name": "NIFC",
            "job_type": "SINGLE_ETL",
            "table_name": "nifc_fire_etl",
            "is_next_skipped_if_error": True,
            "next_jobs": ["DATA_QUALITY-nifc_fire_etl"],
        },
        {
            "name": "DATA_QUALITY-nifc_fire_etl",
            "task_id": "NIFC_etl.data_quality",
            "source_name": "NIFC",
            "job_type": "DATA_QUALITY",
            "table_name": "nifc_fire_etl",
            "is_next_skipped_if_error": True,
            "next_jobs": [],
        },
    ]


def _load_shipped() -> list[dict]:
    with open(Path(__file__).resolve().parents[1] / "jobs.json") as f:
        return json.load(f)


def test_load_jobs_preserves_order_and_fields(jobs_path: Path) -> None:
    """load_jobs() parses the flat list in file order, keeping names and next_jobs references."""
    # Arrange
    jobs_path.write_text(json.dumps(_chain()))

    # Act
    jobs = load_jobs()

    # Assert
    assert [job.name for job in jobs] == [
        "DOWNLOAD-NIFC",
        "INGESTION-nifc_fire",
        "DATA_QUALITY-nifc_fire",
        "SINGLE_ETL-nifc_fire_etl",
        "DATA_QUALITY-nifc_fire_etl",
    ]
    ingestion = jobs[1]
    assert ingestion.task_id == "NIFC.ingestion"
    assert ingestion.source_name == "NIFC"
    assert ingestion.job_type == EJobType.INGESTION
    assert ingestion.table_name == "nifc_fire"
    assert ingestion.schedule is None  # downstreamed jobs carry no schedule
    assert ingestion.is_next_skipped_if_error is True
    assert ingestion.next_jobs == ["DATA_QUALITY-nifc_fire", "SINGLE_ETL-nifc_fire_etl"]
    assert jobs[0].table_name is None  # DOWNLOAD jobs carry no table_name
    assert jobs[0].schedule == "0 8 * * *"  # scheduled jobs declare their cron


def test_load_jobs_duplicate_name_raises(jobs_path: Path) -> None:
    """Two jobs sharing a name fail the registry validation."""
    # Arrange
    jobs = _chain()
    jobs.append({**jobs[2], "next_jobs": []})
    jobs_path.write_text(json.dumps(jobs))

    # Act & Assert
    with pytest.raises(ValueError, match='duplicate job name "DATA_QUALITY-nifc_fire"'):
        build_task_specs()


def test_load_jobs_duplicate_task_id_raises(jobs_path: Path) -> None:
    """Two jobs claiming the same task_id fail — Airflow task ids must be unique."""
    # Arrange
    jobs = _chain()
    jobs.append({**jobs[2], "name": "DATA_QUALITY-clone", "next_jobs": []})
    jobs_path.write_text(json.dumps(jobs))

    # Act & Assert
    with pytest.raises(ValueError, match=re.escape('duplicate task_id "NIFC.data_quality"')):
        build_task_specs()


def test_load_jobs_invalid_task_id_raises(jobs_path: Path) -> None:
    """A task_id Airflow cannot use (spaces, leading dots, empty segments) fails at load."""
    # Arrange
    jobs = _chain()
    jobs[0]["task_id"] = "NIFC download!"
    jobs_path.write_text(json.dumps(jobs))

    # Act & Assert
    with pytest.raises(ValueError, match="invalid task_id"):
        build_task_specs()


def test_load_jobs_unknown_reference_raises(jobs_path: Path) -> None:
    """A next_jobs entry naming a job that does not exist fails the registry validation."""
    # Arrange
    jobs = _chain()
    jobs[0]["next_jobs"] = ["INGESTION-missing"]
    jobs_path.write_text(json.dumps(jobs))

    # Act & Assert
    with pytest.raises(ValueError, match='"DOWNLOAD-NIFC" references unknown job "INGESTION-missing"'):
        build_task_specs()


def test_load_jobs_two_parents_raises(jobs_path: Path) -> None:
    """A job referenced by two parent jobs fails the registry validation."""
    # Arrange
    jobs = _chain()
    jobs[4]["next_jobs"] = ["DATA_QUALITY-nifc_fire"]  # the etl-table DQ also points at the plain-table DQ
    jobs_path.write_text(json.dumps(jobs))

    # Act & Assert
    with pytest.raises(ValueError, match='"DATA_QUALITY-nifc_fire" is referenced by both'):
        build_task_specs()


def test_load_jobs_dependency_cycle_raises(jobs_path: Path) -> None:
    """Jobs forming a reference cycle fail the registry validation."""
    # Arrange
    jobs = _chain()
    jobs.append(
        {
            "name": "DATA_QUALITY-cycle_a",
            "task_id": "NIFC.cycle_a",
            "source_name": "NIFC",
            "job_type": "DATA_QUALITY",
            "table_name": "nifc_fire",
            "is_next_skipped_if_error": True,
            "next_jobs": ["DATA_QUALITY-cycle_b"],
        }
    )
    jobs.append(
        {
            "name": "DATA_QUALITY-cycle_b",
            "task_id": "NIFC.cycle_b",
            "source_name": "NIFC",
            "job_type": "DATA_QUALITY",
            "table_name": "nifc_fire",
            "is_next_skipped_if_error": True,
            "next_jobs": ["DATA_QUALITY-cycle_a"],
        }
    )
    jobs_path.write_text(json.dumps(jobs))

    # Act & Assert
    with pytest.raises(ValueError, match="dependency cycle among: DATA_QUALITY-cycle_a, DATA_QUALITY-cycle_b"):
        build_task_specs()


def _union_job() -> dict:
    """The bare Union fan-in job of a valid registry."""
    return {
        "name": "SINGLE_ETL-union_etl",
        "task_id": "Union.single_etl",
        "source_name": "Union",
        "job_type": "SINGLE_ETL",
        "table_name": "union_etl",
        "is_next_skipped_if_error": True,
        "next_jobs": [],
    }


def test_scheduled_job_with_parent_raises(jobs_path: Path) -> None:
    """A scheduled job that is also referenced by a parent fails the registry validation."""
    # Arrange
    jobs = _chain()
    jobs[1]["next_jobs"] = ["DATA_QUALITY-nifc_fire", "SINGLE_ETL-nifc_fire_etl", "DOWNLOAD-NIFC"]
    jobs_path.write_text(json.dumps(jobs))

    # Act & Assert
    with pytest.raises(ValueError, match='scheduled job "DOWNLOAD-NIFC" is referenced by'):
        build_task_specs()


def test_root_without_schedule_raises(jobs_path: Path) -> None:
    """A root job that is neither scheduled nor the union job fails the registry validation."""
    # Arrange
    jobs = _chain()
    del jobs[0]["schedule"]
    jobs.append(_union_job())
    jobs_path.write_text(json.dumps(jobs))

    # Act & Assert
    with pytest.raises(ValueError, match='root job "DOWNLOAD-NIFC" is neither scheduled nor the union job'):
        build_task_specs()


def test_union_job_must_not_be_scheduled(jobs_path: Path) -> None:
    """A scheduled union job fails the registry validation — it waits for the fan-in."""
    # Arrange
    jobs = _chain()
    jobs.append({**_union_job(), "schedule": "0 8 * * *"})
    jobs_path.write_text(json.dumps(jobs))

    # Act & Assert
    with pytest.raises(ValueError, match='union job "SINGLE_ETL-union_etl" must not be scheduled'):
        build_task_specs()


def test_resolve_schedule_returns_the_shared_cron(jobs_path: Path) -> None:
    """resolve_schedule() returns the cron every scheduled job declares."""
    # Arrange
    jobs_path.write_text(json.dumps(_chain()))

    # Act
    schedule = resolve_schedule()

    # Assert
    assert schedule == "0 8 * * *"


def test_resolve_schedule_without_scheduled_job_raises(jobs_path: Path) -> None:
    """A registry without any scheduled job fails — the DAG needs a cron."""
    # Arrange
    jobs = _chain()
    del jobs[0]["schedule"]
    jobs_path.write_text(json.dumps(jobs))

    # Act & Assert
    with pytest.raises(ValueError, match="no scheduled job"):
        resolve_schedule()


def test_resolve_schedule_conflicting_crons_raises(jobs_path: Path) -> None:
    """Two scheduled jobs with different crons fail — Airflow supports one schedule per DAG."""
    # Arrange
    jobs = _chain()
    jobs.append(
        {
            "name": "DOWNLOAD-OTHER",
            "task_id": "OTHER.download",
            "source_name": "OTHER",
            "job_type": "DOWNLOAD",
            "schedule": "0 3 * * *",
            "is_next_skipped_if_error": True,
            "next_jobs": [],
        }
    )
    jobs_path.write_text(json.dumps(jobs))

    # Act & Assert
    with pytest.raises(ValueError, match="different schedules"):
        resolve_schedule()


def test_jobs_json_shipped_registry_loads() -> None:
    """The shipped jobs.json is a flat list of 64 uniquely named jobs."""
    # Arrange & Act
    jobs = load_jobs()

    # Assert
    assert len(jobs) == 64
    names = [job.name for job in jobs]
    assert len(names) == len(set(names))


def test_jobs_json_every_source_chains_ingestion_data_quality_and_single_etl() -> None:
    """Every source's DOWNLOAD reaches its INGESTION job, which fans out to a DATA_QUALITY job
    on the same table and a SINGLE_ETL job on <table>_etl (the Union job is not a source chain)."""
    # Arrange
    jobs = {raw["name"]: raw for raw in _load_shipped()}

    # Act & Assert
    downloads = [raw for raw in jobs.values() if raw["job_type"] == "DOWNLOAD"]
    assert downloads
    for download in downloads:
        source_name = download["source_name"]

        (ingestion_name,) = download["next_jobs"]
        ingestion = jobs[ingestion_name]

        assert ingestion["job_type"] == "INGESTION", source_name
        assert len(ingestion["next_jobs"]) == 2, source_name

        data_quality_name, single_etl_name = ingestion["next_jobs"]
        data_quality = jobs[data_quality_name]

        assert data_quality["job_type"] == "DATA_QUALITY", source_name
        assert data_quality["table_name"] == ingestion["table_name"], source_name
        assert data_quality["next_jobs"] == [], source_name

        etl_table = f"{ingestion['table_name']}_etl"
        single_etl = jobs[single_etl_name]

        assert single_etl["job_type"] == "SINGLE_ETL", source_name
        assert single_etl["table_name"] == etl_table, source_name
        assert len(single_etl["next_jobs"]) == 1, source_name

        (etl_data_quality_name,) = single_etl["next_jobs"]
        etl_data_quality = jobs[etl_data_quality_name]

        assert etl_data_quality["job_type"] == "DATA_QUALITY", source_name
        assert etl_data_quality["table_name"] == etl_table, source_name
        assert etl_data_quality["next_jobs"] == [], source_name


def test_jobs_json_every_job_skips_next_jobs_on_error() -> None:
    """Every job in jobs.json keeps is_next_skipped_if_error=true.
    The flag is documentation only — the DAG maps it to the all_success trigger rule
    (a failed job skips the jobs named in its next_jobs)."""
    # Arrange & Act
    jobs = _load_shipped()

    # Assert
    for job in jobs:
        assert job["is_next_skipped_if_error"] is True, job["name"]


def test_jobs_json_scheduled_jobs_are_the_downloads() -> None:
    """In the shipped registry every DOWNLOAD job is scheduled and every other job is
    downstreamed (no schedule)."""
    # Arrange & Act
    jobs = _load_shipped()

    # Assert
    for job in jobs:
        if job["job_type"] == "DOWNLOAD":
            assert job["schedule"] == "0 8 * * *", job["name"]
        else:
            assert "schedule" not in job, job["name"]


def _shipped_union_jobs() -> list[dict]:
    """Every shipped job of the Union chain: the fan-in root, its DATA_QUALITY job, the VIEW jobs."""
    return [raw for raw in _load_shipped() if raw["source_name"] == "Union"]


def test_jobs_json_union_job_stacks_every_source_table() -> None:
    """The Union chain is a SINGLE_ETL job on union_etl chaining a DATA_QUALITY job and the
    VIEW jobs — the DAG turns its root into the fan-in task that waits for every source's
    single_etl task, and each view reads the freshly stacked geo.union_etl."""
    # Arrange & Act
    union_jobs = _shipped_union_jobs()
    single_etl, data_quality, *views = union_jobs

    # Assert
    assert single_etl == {
        "name": "SINGLE_ETL-union_etl",
        "task_id": "Union.single_etl",
        "source_name": "Union",
        "job_type": "SINGLE_ETL",
        "table_name": "union_etl",
        "is_next_skipped_if_error": True,
        "next_jobs": ["DATA_QUALITY-union_etl", *(f"VIEW-{view['table_name']}" for view in views)],
    }
    assert data_quality == {
        "name": "DATA_QUALITY-union_etl",
        "task_id": "Union_etl.data_quality",
        "source_name": "Union",
        "job_type": "DATA_QUALITY",
        "table_name": "union_etl",
        "is_next_skipped_if_error": True,
        "next_jobs": [],
    }
    assert len(views) == 17
    for view in views:
        assert view == {
            "name": f"VIEW-{view['table_name']}",
            "task_id": f"View.{view['table_name']}",
            "source_name": "Union",
            "job_type": "VIEW",
            "table_name": view["table_name"],
            "is_next_skipped_if_error": True,
            "next_jobs": [],
        }
        assert "schedule" not in view, view["name"]


def test_shipped_view_jobs_wait_for_the_union_single_etl() -> None:
    """Every VIEW job of the shipped registry waits for the SINGLE_ETL of union_etl — all
    seventeen views read geo.union_etl, so they start right after it was stacked."""
    # Arrange
    specs = {spec.task_id: spec for spec in build_task_specs()}
    views = [job for job in load_jobs() if job.job_type == EJobType.VIEW]

    # Assert
    assert len(views) == 17
    for view in views:
        spec = specs[view.task_id]
        assert spec.upstream == ("Union.single_etl",)
        assert spec.trigger_rule == "all_success"
        assert spec.command == (
            f"cd ..;cd GEO_ETL;conda run --no-capture-output -n geo_etl_311 python run_etl.py VIEW {view.table_name}"
        )


def test_job_get_command_download() -> None:
    """A DOWNLOAD job builds the run_download.py command with the source name."""
    # Arrange
    job = Job(name="DOWNLOAD-NIFC", task_id="NIFC.download", source_name="NIFC", job_type=EJobType.DOWNLOAD)

    # Act
    result = job.get_command()

    # Assert
    assert result == (
        "cd ..;cd GEO_Download;conda run --no-capture-output -n geo_download_311 python run_download.py NIFC"
    )


def test_job_get_command_ingestion() -> None:
    """An INGESTION job builds the run_etl.py command with the table name."""
    # Arrange
    job = Job(
        name="INGESTION-nifc_fire",
        task_id="NIFC.ingestion",
        source_name="NIFC",
        job_type=EJobType.INGESTION,
        table_name="nifc_fire",
    )

    # Act
    result = job.get_command()

    # Assert
    assert result == (
        "cd ..;cd GEO_ETL;conda run --no-capture-output -n geo_etl_311 python run_etl.py INGESTION nifc_fire"
    )


def test_job_get_command_ingestion_without_table_name_raises() -> None:
    """An INGESTION job without table_name raises ValueError."""
    # Arrange
    job = Job(name="INGESTION-nifc_fire", task_id="NIFC.ingestion", source_name="NIFC", job_type=EJobType.INGESTION)

    # Act & Assert
    with pytest.raises(ValueError, match="no table_name"):
        job.get_command()


def test_job_get_command_data_quality() -> None:
    """A DATA_QUALITY job builds the run_etl.py command with the plain table name."""
    # Arrange
    job = Job(
        name="DATA_QUALITY-nifc_fire",
        task_id="NIFC.data_quality",
        source_name="NIFC",
        job_type=EJobType.DATA_QUALITY,
        table_name="nifc_fire",
    )

    # Act
    result = job.get_command()

    # Assert
    assert result == (
        "cd ..;cd GEO_ETL;conda run --no-capture-output -n geo_etl_311 python run_etl.py DATA_QUALITY nifc_fire"
    )


def test_job_get_command_data_quality_without_table_name_raises() -> None:
    """A DATA_QUALITY job without table_name raises ValueError."""
    # Arrange
    job = Job(
        name="DATA_QUALITY-nifc_fire", task_id="NIFC.data_quality", source_name="NIFC", job_type=EJobType.DATA_QUALITY
    )

    # Act & Assert
    with pytest.raises(ValueError, match="no table_name"):
        job.get_command()


def test_job_get_command_single_etl() -> None:
    """A SINGLE_ETL job builds the run_etl.py command with the etl script name."""
    # Arrange
    job = Job(
        name="SINGLE_ETL-nifc_fire_etl",
        task_id="NIFC.single_etl",
        source_name="NIFC",
        job_type=EJobType.SINGLE_ETL,
        table_name="nifc_fire_etl",
    )

    # Act
    result = job.get_command()

    # Assert
    assert result == (
        "cd ..;cd GEO_ETL;conda run --no-capture-output -n geo_etl_311 python run_etl.py SINGLE_ETL nifc_fire_etl"
    )


def test_job_get_command_single_etl_without_table_name_raises() -> None:
    """A SINGLE_ETL job without table_name raises ValueError."""
    # Arrange
    job = Job(
        name="SINGLE_ETL-nifc_fire_etl",
        task_id="NIFC.single_etl",
        source_name="NIFC",
        job_type=EJobType.SINGLE_ETL,
    )

    # Act & Assert
    with pytest.raises(ValueError, match="no table_name"):
        job.get_command()


def test_job_get_command_view() -> None:
    """A VIEW job builds the run_etl.py command with the view script name."""
    # Arrange
    job = Job(
        name="VIEW-metrics_flood_vw",
        task_id="View.metrics_flood_vw",
        source_name="Union",
        job_type=EJobType.VIEW,
        table_name="metrics_flood_vw",
    )

    # Act
    result = job.get_command()

    # Assert
    assert result == (
        "cd ..;cd GEO_ETL;conda run --no-capture-output -n geo_etl_311 python run_etl.py VIEW metrics_flood_vw"
    )


def test_job_get_command_view_without_table_name_raises() -> None:
    """A VIEW job without table_name raises ValueError."""
    # Arrange
    job = Job(
        name="VIEW-metrics_flood_vw", task_id="View.metrics_flood_vw", source_name="Union", job_type=EJobType.VIEW
    )

    # Act & Assert
    with pytest.raises(ValueError, match="no table_name"):
        job.get_command()


def test_job_get_command_verdict_raises() -> None:
    """A VERDICT job has no shell command — build_task_specs() synthesizes the verdict task."""
    # Arrange
    job = Job(name="VERDICT-union", task_id="union_verdict", source_name="Union", job_type=EJobType.VERDICT)

    # Act & Assert
    with pytest.raises(ValueError, match="no shell command"):
        job.get_command()


def test_job_task_id_pattern_is_validated() -> None:
    """A task_id with spaces or empty dot segments fails at config load, not at DAG parse."""
    # Act & Assert
    with pytest.raises(ValueError, match="invalid task_id"):
        Job(name="SINGLE_ETL-x", task_id="NIFC..single etl", source_name="NIFC", job_type=EJobType.SINGLE_ETL)
