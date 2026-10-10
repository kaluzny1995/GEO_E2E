"""Tests for build_task_specs(): jobs.json flattened into the task specs of the Airflow mega-DAG."""

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from model import EJobType, Job, build_task_specs


def _count_jobs(job: Job) -> int:
    """Count one job plus all of its next_jobs recursively."""
    return 1 + sum(_count_jobs(next_job) for next_job in job.next_jobs)


def _load_jobs() -> dict:
    with open(Path(__file__).resolve().parents[1] / "jobs.json") as f:
        return json.load(f)


def _write_jobs(tmp_path: Path, jobs: dict) -> Path:
    """Write the given chains to a temporary jobs.json and return its path."""
    path = tmp_path / "jobs.json"
    path.write_text(json.dumps(jobs))
    return path


def _expected_fan_in() -> set[str]:
    """The SINGLE_ETL task id every source chain contributes to the union fan-in."""
    return {
        f"{source_name}.ingestion.single_etl"
        for source_name, raw in _load_jobs().items()
        if raw["job_type"] == "DOWNLOAD"
    }


def test_specs_cover_every_job_in_jobs_json() -> None:
    """One spec per job in jobs.json, with unique task ids."""
    # Arrange
    jobs = _load_jobs()

    # Act
    specs = build_task_specs()

    # Assert
    assert len(specs) == sum(_count_jobs(Job(**raw)) for raw in jobs.values())
    task_ids = [spec.task_id for spec in specs]
    assert len(task_ids) == len(set(task_ids))


def test_source_chain_wiring() -> None:
    """Task ids follow the job-type path and every non-root task waits for its parent."""
    # Arrange & Act
    specs = {spec.task_id: spec for spec in build_task_specs()}

    # Assert
    assert specs["NIFC.download"].upstream == ()
    assert specs["NIFC.ingestion"].upstream == ("NIFC.download",)
    assert specs["NIFC.ingestion.data_quality"].upstream == ("NIFC.ingestion",)
    assert specs["NIFC.ingestion.single_etl"].upstream == ("NIFC.ingestion",)
    assert specs["NIFC.ingestion.single_etl.data_quality"].upstream == ("NIFC.ingestion.single_etl",)


def test_source_chain_tasks_use_all_success() -> None:
    """Every source-chain task waits with all_success (failed parents skip downstream jobs)."""
    # Arrange & Act
    specs = build_task_specs()

    # Assert
    assert all(spec.trigger_rule == "all_success" for spec in specs if spec.source_name != "Union")


def test_union_waits_for_every_single_etl_with_all_done() -> None:
    """The union root waits for the SINGLE_ETL task of all nine sources with all_done, so it
    stacks the fresh *_etl tables whatever the chains' results — the DATA_QUALITY jobs stay
    terminal with no successors (the fan-in does not include them)."""
    # Arrange & Act
    union_specs = [spec for spec in build_task_specs() if spec.source_name == "Union"]
    union_root = union_specs[0]

    # Assert
    assert union_root.task_id == "Union.single_etl"
    assert union_root.trigger_rule == "all_done"
    assert set(union_root.upstream) == _expected_fan_in()
    assert len(union_root.upstream) == 9
    assert len(union_specs) == 1  # Union is a bare SINGLE_ETL job — no children


def test_union_children_keep_all_success(tmp_path: Path) -> None:
    """Jobs chained under the union root (if ever added) wait for it with all_success."""
    # Arrange
    jobs = _load_jobs()
    jobs["Union"]["next_jobs"] = [
        {
            "source_name": "Union",
            "job_type": "DATA_QUALITY",
            "table_name": "union_etl",
            "is_failed_if_error": False,
            "is_stop_if_error": True,
            "next_jobs": [],
        }
    ]

    # Act
    with patch("model._JOBS_PATH", str(_write_jobs(tmp_path, jobs))):
        specs = {spec.task_id: spec for spec in build_task_specs()}

    # Assert
    child = specs["Union.single_etl.data_quality"]
    assert child.upstream == ("Union.single_etl",)
    assert child.trigger_rule == "all_success"


def test_commands_are_built_from_config() -> None:
    """Specs carry the shell commands of config.json (conda run of the source/etl envs)."""
    # Arrange & Act
    specs = {spec.task_id: spec for spec in build_task_specs()}

    # Assert
    assert specs["NIFC.download"].command == (
        "cd ..;cd GEO_Download;conda run --no-capture-output -n geo_download_311 python run_download.py NIFC"
    )
    assert specs["Union.single_etl"].command == (
        "cd ..;cd GEO_ETL;conda run --no-capture-output -n geo_etl_311 python run_etl.py SINGLE_ETL union_etl"
    )


def test_job_types_are_preserved_for_pooling() -> None:
    """job_type is kept on every spec so the DAG can route downloads to the download pool."""
    # Arrange & Act
    specs = {spec.task_id: spec for spec in build_task_specs()}

    # Assert
    assert specs["NIFC.download"].job_type == EJobType.DOWNLOAD
    assert specs["NIFC.ingestion"].job_type == EJobType.INGESTION
    assert specs["NIFC.ingestion.data_quality"].job_type == EJobType.DATA_QUALITY
    assert specs["NIFC.ingestion.single_etl"].job_type == EJobType.SINGLE_ETL


def test_missing_union_source_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A jobs.json without the union chain raises ValueError."""
    # Arrange
    jobs = _load_jobs()
    del jobs["Union"]
    monkeypatch.setattr("model._JOBS_PATH", str(_write_jobs(tmp_path, jobs)))

    # Act & Assert
    with pytest.raises(ValueError, match='no "Union" chain'):
        build_task_specs()
