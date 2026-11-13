"""Tests for build_task_specs(): the flat jobs.json registry flattened into the task specs of the Airflow mega-DAG."""

import json
from pathlib import Path

import pytest

from model import EJobType, TaskSpec, build_task_specs
from test._helpers import (
    ETL_CMD_PREFIX,
    OBSERVED_VIEW_TASK_ID,
    PREDICTION_TASK_ID,
    SOURCE_COUNT,
    UNION_ROOT_ID,
    load_shipped_jobs,
)


def _write_jobs(tmp_path: Path, jobs: list[dict]) -> Path:
    """Write the given job list to a temporary jobs.json and return its path."""
    path = tmp_path / "jobs.json"
    path.write_text(json.dumps(jobs))
    return path


def _expected_fan_in() -> set[str]:
    """The SINGLE_ETL task id every source chain contributes to the union fan-in (from jobs.json)."""
    return {
        raw["task_id"]
        for raw in load_shipped_jobs()
        if raw["job_type"] == "SINGLE_ETL" and raw["source_name"] != "Union"
    }


def _expected_view_task_ids() -> set[str]:
    """The VIEW task ids of the shipped registry — declared per view in jobs.json."""
    return {raw["task_id"] for raw in load_shipped_jobs() if raw["job_type"] == "VIEW"}


@pytest.fixture(scope="module")
def specs_map() -> dict[str, TaskSpec]:
    """The task specs of the shipped registry keyed by task id — every test only reads them."""
    return {spec.task_id: spec for spec in build_task_specs()}


def test_specs_cover_every_job_plus_the_verdict() -> None:
    """One spec per job in the jobs.json registry plus the synthesized verdict task, with
    unique task ids (the verdict carries no jobs.json entry — build_task_specs() appends it)."""
    # Arrange
    jobs = load_shipped_jobs()

    # Act
    specs = build_task_specs()

    # Assert
    assert len(specs) == len(jobs) + 1
    assert specs[-1].task_id == "Union.verdict"
    task_ids = [spec.task_id for spec in specs]
    assert len(task_ids) == len(set(task_ids))


def test_source_chain_wiring(specs_map: dict[str, TaskSpec]) -> None:
    """Task ids are the config's task_id fields and every non-root task waits for its parent."""
    # Arrange & Act
    specs = specs_map

    # Assert
    assert specs["NIFC.download"].upstream == ()
    assert specs["NIFC.ingestion"].upstream == ("NIFC.download",)
    assert specs["NIFC.data_source_file"].upstream == ("NIFC.download",)
    assert specs["NIFC.data_quality"].upstream == ("NIFC.ingestion",)
    assert specs["NIFC.single_etl"].upstream == ("NIFC.ingestion",)
    assert specs["NIFC_etl.data_quality"].upstream == ("NIFC.single_etl",)


def test_source_chain_tasks_use_all_success(specs_map: dict[str, TaskSpec]) -> None:
    """Every source-chain task waits with all_success (failed parents skip downstream jobs)."""
    # Arrange & Act
    specs = list(specs_map.values())

    # Assert
    assert all(spec.trigger_rule == "all_success" for spec in specs if spec.source_name != "Union")


def test_union_waits_for_every_single_etl_and_needs_a_success(specs_map: dict[str, TaskSpec]) -> None:
    """The union root waits for the SINGLE_ETL task of all nine sources with
    all_done_min_one_success: it starts whatever the chains' results, but only when at least
    one single-ETL succeeded (nothing fresh to stack otherwise) — the DATA_QUALITY jobs stay
    outside the fan-in (the fan-in does not include them)."""
    # Arrange & Act
    union_specs = {task_id: spec for task_id, spec in specs_map.items() if spec.source_name == "Union"}
    union_root = union_specs[UNION_ROOT_ID]

    # Assert
    assert union_root.trigger_rule == "all_done_min_one_success"
    assert set(union_root.upstream) == _expected_fan_in()
    assert len(union_root.upstream) == SOURCE_COUNT
    assert set(union_specs) == {
        UNION_ROOT_ID,
        PREDICTION_TASK_ID,
        "Union_etl.data_quality",
        "Union.verdict",
        *_expected_view_task_ids(),
    }


def test_only_the_union_root_and_the_verdict_use_the_special_trigger_rules(specs_map: dict[str, TaskSpec]) -> None:
    """all_done_min_one_success is reserved for the union root and all_done for the verdict —
    every other task, the union data-quality and view steps included, runs when its parent
    succeeded."""
    # Arrange & Act
    specs = specs_map

    # Assert
    assert specs[UNION_ROOT_ID].trigger_rule == "all_done_min_one_success"
    assert specs["Union.verdict"].trigger_rule == "all_done"
    assert all(
        spec.trigger_rule == "all_success"
        for task_id, spec in specs.items()
        if task_id not in {UNION_ROOT_ID, "Union.verdict"}
    )


def test_union_children_keep_all_success(specs_map: dict[str, TaskSpec]) -> None:
    """The union root feeds DATA_QUALITY, the observed view, PREDICTION and phenomenon views;
    PREDICTION feeds the eight combined metrics views."""
    # Arrange & Act
    specs = specs_map

    # Assert
    child = specs["Union_etl.data_quality"]
    assert child.upstream == (UNION_ROOT_ID,)
    assert child.trigger_rule == "all_success"
    assert specs[OBSERVED_VIEW_TASK_ID].upstream == (UNION_ROOT_ID,)
    assert specs[PREDICTION_TASK_ID].upstream == (OBSERVED_VIEW_TASK_ID,)
    for view in load_shipped_jobs():
        if view["job_type"] != "VIEW":
            continue
        task_id = view["task_id"]
        is_combined_metrics = view["table_name"].startswith("metrics_") and view["table_name"] != "metrics_observed_vw"
        expected = (PREDICTION_TASK_ID,) if is_combined_metrics else (UNION_ROOT_ID,)
        assert specs[task_id].upstream == expected
        assert specs[task_id].trigger_rule == "all_success"


def test_commands_are_built_from_config(specs_map: dict[str, TaskSpec]) -> None:
    """Specs carry the shell commands of config.json (conda run of the source/etl envs)."""
    # Arrange & Act
    specs = specs_map

    # Assert
    assert specs["NIFC.download"].command == (
        "cd ..;cd GEO_Download;conda run --no-capture-output -n geo_download_311 python run_download.py NIFC"
    )
    assert specs["NIFC.data_source_file"].command == (
        "cd ..;cd GEO_ETL;conda run --no-capture-output -n geo_etl_311 python run_etl.py DATA_SOURCE_FILE NIFC"
    )
    assert specs[UNION_ROOT_ID].command == f"{ETL_CMD_PREFIX} SINGLE_ETL union_etl"
    assert specs[PREDICTION_TASK_ID].command == f"{ETL_CMD_PREFIX} PREDICTION prediction_etl"


def test_job_types_are_preserved_for_pooling(specs_map: dict[str, TaskSpec]) -> None:
    """job_type is kept on every spec so the DAG can route downloads to the download pool."""
    # Arrange & Act
    specs = specs_map

    # Assert
    assert specs["NIFC.download"].job_type == EJobType.DOWNLOAD
    assert specs["NIFC.ingestion"].job_type == EJobType.INGESTION
    assert specs["NIFC.data_quality"].job_type == EJobType.DATA_QUALITY
    assert specs["NIFC.data_source_file"].job_type == EJobType.DATA_SOURCE_FILE
    assert specs["NIFC.single_etl"].job_type == EJobType.SINGLE_ETL
    assert specs["View.metrics_flood_vw"].job_type == EJobType.VIEW
    assert specs[PREDICTION_TASK_ID].job_type == EJobType.PREDICTION
    assert specs["Union.verdict"].job_type == EJobType.VERDICT


def test_missing_union_source_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A jobs.json without the Union job raises ValueError."""
    # Arrange
    jobs = [raw for raw in load_shipped_jobs() if raw["source_name"] != "Union"]
    monkeypatch.setattr("model._JOBS_PATH", str(_write_jobs(tmp_path, jobs)))

    # Act & Assert
    with pytest.raises(ValueError, match='no "Union" chain'):
        build_task_specs()


def test_verdict_waits_for_every_registry_task(specs_map: dict[str, TaskSpec]) -> None:
    """The synthesized verdict task (job type VERDICT, union source) waits with all_done on
    every registry task — it must start only after every branch reached a terminal state."""
    # Arrange & Act
    specs = specs_map
    verdict = specs["Union.verdict"]
    registry_ids = {task_id for task_id in specs if task_id != "Union.verdict"}

    # Assert
    assert verdict.source_name == "Union"
    assert verdict.trigger_rule == "all_done"
    assert set(verdict.upstream) == registry_ids
    assert len(verdict.upstream) == len(registry_ids)  # every registry task once, no duplicates


def test_the_verdict_is_the_dags_only_leaf(specs_map: dict[str, TaskSpec]) -> None:
    """Every registry task has a downstream successor (the verdict), so the verdict is the
    DAG's only leaf — Airflow derives a run's state from its leaf tasks, which is what makes
    the run reflect the union result alone."""
    # Arrange & Act
    specs = list(specs_map.values())
    waited_on = {upstream_id for spec in specs for upstream_id in spec.upstream}
    leaves = [spec.task_id for spec in specs if spec.task_id not in waited_on]

    # Assert
    assert leaves == ["Union.verdict"]


def test_verdict_command_reads_the_union_root_state(specs_map: dict[str, TaskSpec]) -> None:
    """The verdict's command is a Jinja template reading the union root's state of the current
    run through ti.get_task_states() — it exits 1 unless that state is `success`."""
    # Arrange & Act
    verdict = specs_map["Union.verdict"]

    # Assert
    assert verdict.command.startswith(f"{{% set task_0 = '{UNION_ROOT_ID}' %}}")
    assert f"{{% set task_1 = '{PREDICTION_TASK_ID}' %}}" in verdict.command
    assert "ti.get_task_states(ti.dag_id, task_ids=[task_0], run_ids=[run_id])" in verdict.command
    assert "ti.get_task_states(ti.dag_id, task_ids=[task_1], run_ids=[run_id])" in verdict.command
    assert "{% if state_0 == 'success' and state_1 == 'success' %}" in verdict.command
    assert verdict.command.count("exit 1") == 1


def test_verdict_job_declared_in_jobs_json_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A registry declaring a VERDICT job fails — build_task_specs() synthesizes the verdict task."""
    # Arrange
    jobs = [
        *load_shipped_jobs(),
        {
            "name": "VERDICT-union",
            "task_id": "union_verdict",
            "source_name": "Union",
            "job_type": "VERDICT",
            "next_jobs": [],
        },
    ]
    monkeypatch.setattr("model._JOBS_PATH", str(_write_jobs(tmp_path, jobs)))

    # Act & Assert
    with pytest.raises(ValueError, match="must not declare a VERDICT job"):
        build_task_specs()


def test_task_ids_come_from_the_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Renaming a job's task_id in jobs.json renames its task and re-points its child and the
    union fan-in — the DAG derives no names of its own."""
    # Arrange
    jobs = [
        dict(raw, task_id="NIFC.custom_name") if raw["name"] == "SINGLE_ETL-nifc_fire_etl" else raw
        for raw in load_shipped_jobs()
    ]
    monkeypatch.setattr("model._JOBS_PATH", str(_write_jobs(tmp_path, jobs)))

    # Act
    specs = {spec.task_id: spec for spec in build_task_specs()}

    # Assert
    assert "NIFC.single_etl" not in specs
    assert specs["NIFC.custom_name"].job_type == EJobType.SINGLE_ETL
    assert specs["NIFC_etl.data_quality"].upstream == ("NIFC.custom_name",)
    assert "NIFC.custom_name" in specs[UNION_ROOT_ID].upstream


def test_reserved_verdict_task_id_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A registry job claiming Union.verdict — the synthesized verdict's id — fails."""
    # Arrange
    jobs = [
        dict(raw, task_id="Union.verdict") if raw["name"] == "DOWNLOAD-NIFC" else raw for raw in load_shipped_jobs()
    ]
    monkeypatch.setattr("model._JOBS_PATH", str(_write_jobs(tmp_path, jobs)))

    # Act & Assert
    with pytest.raises(ValueError, match="reserved for the synthesized verdict task"):
        build_task_specs()
