"""Tests for build_task_specs(): the flat jobs.json registry flattened into the task specs of the Airflow mega-DAG."""

import json
from pathlib import Path

import pytest

from model import EJobType, build_task_specs


def _load_jobs() -> list[dict]:
    with open(Path(__file__).resolve().parents[1] / "jobs.json") as f:
        return json.load(f)


def _write_jobs(tmp_path: Path, jobs: list[dict]) -> Path:
    """Write the given job list to a temporary jobs.json and return its path."""
    path = tmp_path / "jobs.json"
    path.write_text(json.dumps(jobs))
    return path


def _expected_fan_in() -> set[str]:
    """The SINGLE_ETL task id every source chain contributes to the union fan-in."""
    return {f"{raw['source_name']}.ingestion.single_etl" for raw in _load_jobs() if raw["job_type"] == "DOWNLOAD"}


def _expected_view_task_ids() -> set[str]:
    """The VIEW task ids of the shipped registry — the script name keeps the siblings unique."""
    return {f"Union.single_etl.view.{raw['table_name']}" for raw in _load_jobs() if raw["job_type"] == "VIEW"}


def test_specs_cover_every_job_in_jobs_json() -> None:
    """One spec per job in the jobs.json registry, with unique task ids."""
    # Arrange
    jobs = _load_jobs()

    # Act
    specs = build_task_specs()

    # Assert
    assert len(specs) == len(jobs)
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


def test_union_waits_for_every_single_etl_and_needs_a_success() -> None:
    """The union root waits for the SINGLE_ETL task of all nine sources with
    all_done_min_one_success: it starts whatever the chains' results, but only when at least
    one single-ETL succeeded (nothing fresh to stack otherwise) — the DATA_QUALITY jobs stay
    outside the fan-in (the fan-in does not include them)."""
    # Arrange & Act
    union_specs = {spec.task_id: spec for spec in build_task_specs() if spec.source_name == "Union"}
    union_root = union_specs["Union.single_etl"]

    # Assert
    assert union_root.trigger_rule == "all_done_min_one_success"
    assert set(union_root.upstream) == _expected_fan_in()
    assert len(union_root.upstream) == 9
    assert set(union_specs) == {
        "Union.single_etl",
        "Union.single_etl.data_quality",
        *_expected_view_task_ids(),
    }


def test_only_the_union_root_uses_all_done_min_one_success() -> None:
    """all_done_min_one_success is reserved for the union root — every other task, the union
    data-quality and view steps included, runs when its parent succeeded."""
    # Arrange & Act
    specs = {spec.task_id: spec for spec in build_task_specs()}

    # Assert
    assert specs["Union.single_etl"].trigger_rule == "all_done_min_one_success"
    assert all(spec.trigger_rule == "all_success" for task_id, spec in specs.items() if task_id != "Union.single_etl")


def test_union_children_keep_all_success() -> None:
    """Every job chained under the union root — the DATA_QUALITY step and the seventeen VIEW
    jobs — waits for it with the default all_success rule."""
    # Arrange & Act
    specs = {spec.task_id: spec for spec in build_task_specs()}

    # Assert
    child = specs["Union.single_etl.data_quality"]
    assert child.upstream == ("Union.single_etl",)
    assert child.trigger_rule == "all_success"
    for task_id in _expected_view_task_ids():
        assert specs[task_id].upstream == ("Union.single_etl",)
        assert specs[task_id].trigger_rule == "all_success"


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
    assert specs["Union.single_etl.view.metrics_flood_vw"].job_type == EJobType.VIEW


def test_missing_union_source_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A jobs.json without the Union job raises ValueError."""
    # Arrange
    jobs = [raw for raw in _load_jobs() if raw["source_name"] != "Union"]
    monkeypatch.setattr("model._JOBS_PATH", str(_write_jobs(tmp_path, jobs)))

    # Act & Assert
    with pytest.raises(ValueError, match='no "Union" chain'):
        build_task_specs()
