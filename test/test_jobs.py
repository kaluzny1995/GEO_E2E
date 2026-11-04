"""Tests for the flat jobs.json job registry: loading, reference validation and command building."""

import json
import re
from collections.abc import Callable
from pathlib import Path

import pytest

from model import EJobType, Job, build_task_specs, load_jobs, resolve_schedule
from test._helpers import CRON, ETL_CMD_PREFIX, UNION_ROOT_ID, VIEW_COUNT, load_shipped_jobs


@pytest.fixture()
def jobs_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point the jobs.json loader at a temporary file"""
    path = tmp_path / "jobs.json"
    monkeypatch.setattr("model._JOBS_PATH", str(path))
    return path


def _chain() -> list[dict]:
    """A minimal DOWNLOAD -> (INGESTION -> DATA_QUALITY/SINGLE_ETL, DATA_SOURCE_FILE) chain for NIFC."""
    return [
        {
            "name": "DOWNLOAD-NIFC",
            "task_id": "NIFC.download",
            "source_name": "NIFC",
            "job_type": "DOWNLOAD",
            "schedule": CRON,
            "is_next_skipped_if_error": True,
            "next_jobs": ["INGESTION-nifc_fire", "DATA_SOURCE_FILE-NIFC"],
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
        {
            "name": "DATA_SOURCE_FILE-NIFC",
            "task_id": "NIFC.data_source_file",
            "source_name": "NIFC",
            "job_type": "DATA_SOURCE_FILE",
            "table_name": "NIFC",
            "is_next_skipped_if_error": True,
            "next_jobs": [],
        },
    ]


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
        "DATA_SOURCE_FILE-NIFC",
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
    assert jobs[0].schedule == CRON  # scheduled jobs declare their cron
    assert jobs[0].next_jobs == ["INGESTION-nifc_fire", "DATA_SOURCE_FILE-NIFC"]
    assert jobs[5].job_type == EJobType.DATA_SOURCE_FILE
    assert jobs[5].table_name == "NIFC"
    assert jobs[5].next_jobs == []


def _add_cycle_pair(jobs: list[dict]) -> None:
    """Append the two jobs forming a next_jobs cycle — a cycle the validation must reject."""
    jobs.extend(
        {
            "name": f"DATA_QUALITY-cycle_{suffix}",
            "task_id": f"NIFC.cycle_{suffix}",
            "source_name": "NIFC",
            "job_type": "DATA_QUALITY",
            "table_name": "nifc_fire",
            "is_next_skipped_if_error": True,
            "next_jobs": [f"DATA_QUALITY-cycle_{other}"],
        }
        for suffix, other in (("a", "b"), ("b", "a"))
    )


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        pytest.param(
            lambda jobs: jobs.append({**jobs[2], "next_jobs": []}),
            'duplicate job name "DATA_QUALITY-nifc_fire"',
            id="duplicate-name",
        ),
        pytest.param(
            lambda jobs: jobs.append({**jobs[2], "name": "DATA_QUALITY-clone", "next_jobs": []}),
            re.escape('duplicate task_id "NIFC.data_quality"'),
            id="duplicate-task-id",
        ),
        pytest.param(
            lambda jobs: jobs.__setitem__(0, {**jobs[0], "task_id": "NIFC download!"}),
            "invalid task_id",
            id="invalid-task-id",
        ),
        pytest.param(
            lambda jobs: jobs.__setitem__(0, {**jobs[0], "next_jobs": ["INGESTION-missing"]}),
            '"DOWNLOAD-NIFC" references unknown job "INGESTION-missing"',
            id="unknown-reference",
        ),
        pytest.param(
            # the etl-table DQ also points at the plain-table DQ — two parents for one job
            lambda jobs: jobs.__setitem__(4, {**jobs[4], "next_jobs": ["DATA_QUALITY-nifc_fire"]}),
            '"DATA_QUALITY-nifc_fire" is referenced by both',
            id="two-parents",
        ),
        pytest.param(
            _add_cycle_pair,
            "dependency cycle among: DATA_QUALITY-cycle_a, DATA_QUALITY-cycle_b",
            id="cycle",
        ),
        pytest.param(
            lambda jobs: jobs.__setitem__(
                1,
                {**jobs[1], "next_jobs": ["DATA_QUALITY-nifc_fire", "SINGLE_ETL-nifc_fire_etl", "DOWNLOAD-NIFC"]},
            ),
            'scheduled job "DOWNLOAD-NIFC" is referenced by',
            id="scheduled-with-parent",
        ),
        pytest.param(
            lambda jobs: (jobs[0].pop("schedule"), jobs.append(_union_job())),
            'root job "DOWNLOAD-NIFC" is neither scheduled nor the union job',
            id="root-without-schedule",
        ),
        pytest.param(
            lambda jobs: jobs.append({**_union_job(), "schedule": CRON}),
            'union job "SINGLE_ETL-union_etl" must not be scheduled',
            id="union-must-not-be-scheduled",
        ),
    ],
)
def test_registry_validation_rejects_broken_registries(
    jobs_path: Path, mutate: Callable[[list[dict]], object], message: str
) -> None:
    """Every broken registry fails with a specific error: duplicate names and task ids,
    unusable task ids, unknown references, jobs with two parents, reference cycles and the
    schedule/union rules of the root jobs."""
    # Arrange
    jobs = _chain()
    mutate(jobs)
    jobs_path.write_text(json.dumps(jobs))

    # Act & Assert
    with pytest.raises(ValueError, match=message):
        build_task_specs()


def _union_job() -> dict:
    """The bare Union fan-in job of a valid registry."""
    return {
        "name": "SINGLE_ETL-union_etl",
        "task_id": UNION_ROOT_ID,
        "source_name": "Union",
        "job_type": "SINGLE_ETL",
        "table_name": "union_etl",
        "is_next_skipped_if_error": True,
        "next_jobs": [],
    }


def test_resolve_schedule_returns_the_shared_cron(jobs_path: Path) -> None:
    """resolve_schedule() returns the cron every scheduled job declares."""
    # Arrange
    jobs_path.write_text(json.dumps(_chain()))

    # Act
    schedule = resolve_schedule()

    # Assert
    assert schedule == CRON


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
    """The shipped jobs.json is a flat list of 73 uniquely named jobs."""
    # Arrange & Act
    jobs = load_jobs()

    # Assert
    assert len(jobs) == 73
    names = [job.name for job in jobs]
    assert len(names) == len(set(names))


def test_jobs_json_every_source_chains_ingestion_data_quality_and_single_etl() -> None:
    """Every source's DOWNLOAD fans out to INGESTION and DATA_SOURCE_FILE; INGESTION fans
    out to a DATA_QUALITY job on the same table and a SINGLE_ETL job on <table>_etl."""
    # Arrange
    jobs = {raw["name"]: raw for raw in load_shipped_jobs()}

    # Act & Assert
    downloads = [raw for raw in jobs.values() if raw["job_type"] == "DOWNLOAD"]
    assert downloads
    for download in downloads:
        source_name = download["source_name"]

        assert len(download["next_jobs"]) == 2, source_name
        ingestion_name, data_source_file_name = download["next_jobs"]
        ingestion = jobs[ingestion_name]
        data_source_file = jobs[data_source_file_name]

        assert ingestion["job_type"] == "INGESTION", source_name
        assert len(ingestion["next_jobs"]) == 2, source_name

        data_quality_name, single_etl_name = ingestion["next_jobs"]
        data_quality = jobs[data_quality_name]

        assert data_quality["job_type"] == "DATA_QUALITY", source_name
        assert data_quality["table_name"] == ingestion["table_name"], source_name
        assert data_quality["next_jobs"] == [], source_name

        assert data_source_file["job_type"] == "DATA_SOURCE_FILE", source_name
        assert data_source_file["table_name"] == source_name, source_name
        assert data_source_file["next_jobs"] == [], source_name

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
    jobs = load_shipped_jobs()

    # Assert
    for job in jobs:
        assert job["is_next_skipped_if_error"] is True, job["name"]


def test_jobs_json_scheduled_jobs_are_the_downloads() -> None:
    """In the shipped registry every DOWNLOAD job is scheduled and every other job is
    downstreamed (no schedule)."""
    # Arrange & Act
    jobs = load_shipped_jobs()

    # Assert
    for job in jobs:
        if job["job_type"] == "DOWNLOAD":
            assert job["schedule"] == CRON, job["name"]
        else:
            assert "schedule" not in job, job["name"]


def _shipped_union_jobs() -> list[dict]:
    """Every shipped job of the Union chain: the fan-in root, its DATA_QUALITY job, the VIEW jobs."""
    return [raw for raw in load_shipped_jobs() if raw["source_name"] == "Union"]


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
        "task_id": UNION_ROOT_ID,
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
    assert len(views) == VIEW_COUNT
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
    assert len(views) == VIEW_COUNT
    for view in views:
        spec = specs[view.task_id]
        assert spec.upstream == (UNION_ROOT_ID,)
        assert spec.trigger_rule == "all_success"
        assert spec.command == f"{ETL_CMD_PREFIX} VIEW {view.table_name}"


@pytest.mark.parametrize(
    ("job", "expected_command"),
    [
        pytest.param(
            Job(name="DOWNLOAD-NIFC", task_id="NIFC.download", source_name="NIFC", job_type=EJobType.DOWNLOAD),
            "cd ..;cd GEO_Download;conda run --no-capture-output -n geo_download_311 python run_download.py NIFC",
            id="download",
        ),
        pytest.param(
            Job(
                name="INGESTION-nifc_fire",
                task_id="NIFC.ingestion",
                source_name="NIFC",
                job_type=EJobType.INGESTION,
                table_name="nifc_fire",
            ),
            f"{ETL_CMD_PREFIX} INGESTION nifc_fire",
            id="ingestion",
        ),
        pytest.param(
            Job(
                name="DATA_QUALITY-nifc_fire",
                task_id="NIFC.data_quality",
                source_name="NIFC",
                job_type=EJobType.DATA_QUALITY,
                table_name="nifc_fire",
            ),
            f"{ETL_CMD_PREFIX} DATA_QUALITY nifc_fire",
            id="data-quality",
        ),
        pytest.param(
            Job(
                name="SINGLE_ETL-nifc_fire_etl",
                task_id="NIFC.single_etl",
                source_name="NIFC",
                job_type=EJobType.SINGLE_ETL,
                table_name="nifc_fire_etl",
            ),
            f"{ETL_CMD_PREFIX} SINGLE_ETL nifc_fire_etl",
            id="single-etl",
        ),
        pytest.param(
            Job(
                name="VIEW-metrics_flood_vw",
                task_id="View.metrics_flood_vw",
                source_name="Union",
                job_type=EJobType.VIEW,
                table_name="metrics_flood_vw",
            ),
            f"{ETL_CMD_PREFIX} VIEW metrics_flood_vw",
            id="view",
        ),
    ],
)
def test_job_get_command_builds_the_run_call(job: Job, expected_command: str) -> None:
    """Each job type builds its shell command in its repo/conda env with its own argument:
    the source name for DOWNLOAD, the table name (or view name) for every other job type."""
    # Act
    result = job.get_command()

    # Assert
    assert result == expected_command


@pytest.mark.parametrize("job_type", [EJobType.INGESTION, EJobType.DATA_QUALITY, EJobType.SINGLE_ETL, EJobType.VIEW])
def test_job_get_command_without_table_name_raises(job_type: EJobType) -> None:
    """A job type addressing a table without a table_name raises ValueError."""
    # Arrange
    job = Job(name="JOB-no_table", task_id="NIFC.no_table", source_name="NIFC", job_type=job_type)

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
