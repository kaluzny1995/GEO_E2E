"""Tests for the Celery app: beat schedule entries, the geo_e2e task chain behaviour
and the union_etl fan-in (completion markers, waiting and the deadline)."""

from datetime import date
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

from util import mark_source_done


@pytest.fixture()
def app_module(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    """Import app.py with cwd switched to a temp dir so the module-level
    celerybeat_cleanup() does not delete real celerybeat-schedule files."""
    monkeypatch.chdir(tmp_path)
    import app

    return app


@pytest.fixture()
def commands(monkeypatch: pytest.MonkeyPatch) -> tuple[list[str], list[int]]:
    """Fake model.run_command — records commands, returns queued return codes."""
    calls = []
    results = []

    def fake_run_command(command: str, log_file: str) -> int:
        """Record the command and pop the next queued return code (0 when none left)."""
        calls.append(command)
        return results.pop(0) if results else 0

    monkeypatch.setattr("model.run_command", fake_run_command)
    return calls, results


def _marker_path(source_name: str) -> Path:
    """Path of today's completion marker for a source (relative to the temp cwd)."""
    return Path("logs") / "progress" / date.today().isoformat() / source_name


def _union_log_path() -> Path:
    """Path of today's union task log (one file per day, shared by all poll attempts)."""
    return Path("logs") / "Union" / f"celery_Union_{date.today().isoformat()}.log"


@pytest.fixture()
def redispatches(app_module: ModuleType, monkeypatch: pytest.MonkeyPatch) -> list[tuple[list[int] | None, int | None]]:
    """Capture union_etl.apply_async calls instead of publishing them to the broker."""
    calls: list[tuple[list[int] | None, int | None]] = []

    def fake_apply_async(args: list[int] | None = None, countdown: int | None = None, **kwargs: Any) -> None:
        calls.append((args, countdown))

    monkeypatch.setattr(app_module.union_etl, "apply_async", fake_apply_async)
    return calls


DOWNLOAD_COMMAND = "cd ..;cd GEO_Download;conda run --no-capture-output -n geo_download_311 python run_download.py NIFC"
INGESTION_COMMAND = (
    "cd ..;cd GEO_ETL;conda run --no-capture-output -n geo_etl_311 python run_etl.py INGESTION nifc_fire"
)
DATA_QUALITY_COMMAND = (
    "cd ..;cd GEO_ETL;conda run --no-capture-output -n geo_etl_311 python run_etl.py DATA_QUALITY nifc_fire"
)
SINGLE_ETL_COMMAND = (
    "cd ..;cd GEO_ETL;conda run --no-capture-output -n geo_etl_311 python run_etl.py SINGLE_ETL nifc_fire_etl"
)
DATA_QUALITY_ETL_COMMAND = (
    "cd ..;cd GEO_ETL;conda run --no-capture-output -n geo_etl_311 python run_etl.py DATA_QUALITY nifc_fire_etl"
)
UNION_COMMAND = "cd ..;cd GEO_ETL;conda run --no-capture-output -n geo_etl_311 python run_etl.py SINGLE_ETL union_etl"


def test_beat_schedule_task_names(app_module: ModuleType) -> None:
    """Beat schedule has one entry per data source pointing at app.geo_e2e,
    plus the union fan-in entry pointing at app.union_etl."""
    # Act
    schedule = app_module.geo_app.conf.beat_schedule

    # Assert
    assert set(schedule) == {
        "geo_e2e_USGS_Earthquake",
        "geo_e2e_GDACS",
        "geo_e2e_EONET",
        "geo_e2e_OpenMeteo_Flood",
        "geo_e2e_USGS_NWIS",
        "geo_e2e_Avalanche_ORG",
        "geo_e2e_IBTrACS",
        "geo_e2e_USGS_Volcano",
        "geo_e2e_NIFC",
        "geo_e2e_union",
    }
    source_entries = {name: entry for name, entry in schedule.items() if entry["task"] == "app.geo_e2e"}
    assert len(source_entries) == 9
    for entry in source_entries.values():
        assert len(entry["args"]) == 1

    union_entry = schedule["geo_e2e_union"]
    assert union_entry["task"] == "app.union_etl"
    assert union_entry["args"] == [0]


def test_geo_e2e_runs_whole_chain(app_module: ModuleType, commands: tuple[list[str], list[int]]) -> None:
    """A successful DOWNLOAD runs INGESTION, plain-table DATA_QUALITY, SINGLE_ETL and etl-table DATA_QUALITY."""
    # Arrange
    calls, results = commands
    results.extend([0, 0, 0, 0, 0])

    # Act
    app_module.geo_e2e("NIFC")

    # Assert
    assert calls == [
        DOWNLOAD_COMMAND,
        INGESTION_COMMAND,
        DATA_QUALITY_COMMAND,
        SINGLE_ETL_COMMAND,
        DATA_QUALITY_ETL_COMMAND,
    ]


def test_geo_e2e_stops_chain_when_download_fails(app_module: ModuleType, commands: tuple[list[str], list[int]]) -> None:
    """A failing DOWNLOAD skips INGESTION, SINGLE_ETL and both DATA_QUALITY jobs while the task still succeeds."""
    # Arrange
    calls, results = commands
    results.append(1)

    # Act
    app_module.geo_e2e("NIFC")

    # Assert
    assert calls == [DOWNLOAD_COMMAND]


def test_geo_e2e_completes_when_ingestion_fails(app_module: ModuleType, commands: tuple[list[str], list[int]]) -> None:
    """With is_failed_if_error=false an INGESTION failure still finishes the task and skips the rest of the chain."""
    # Arrange
    calls, results = commands
    results.extend([0, 1])

    # Act
    app_module.geo_e2e("NIFC")

    # Assert
    assert calls == [DOWNLOAD_COMMAND, INGESTION_COMMAND]


def test_geo_e2e_completes_when_data_quality_fails(
    app_module: ModuleType, commands: tuple[list[str], list[int]]
) -> None:
    """A failing DATA_QUALITY on the plain table only ends its own branch — its sibling SINGLE_ETL still runs."""
    # Arrange
    calls, results = commands
    results.extend([0, 0, 1, 0, 0])

    # Act
    app_module.geo_e2e("NIFC")

    # Assert
    assert calls == [
        DOWNLOAD_COMMAND,
        INGESTION_COMMAND,
        DATA_QUALITY_COMMAND,
        SINGLE_ETL_COMMAND,
        DATA_QUALITY_ETL_COMMAND,
    ]


def test_geo_e2e_completes_when_single_etl_fails(app_module: ModuleType, commands: tuple[list[str], list[int]]) -> None:
    """A failing SINGLE_ETL skips the final DATA_QUALITY job — the task still finishes normally."""
    # Arrange
    calls, results = commands
    results.extend([0, 0, 0, 1])

    # Act
    app_module.geo_e2e("NIFC")

    # Assert
    assert calls == [DOWNLOAD_COMMAND, INGESTION_COMMAND, DATA_QUALITY_COMMAND, SINGLE_ETL_COMMAND]


def test_geo_e2e_marks_source_done_when_chain_finishes(
    app_module: ModuleType, commands: tuple[list[str], list[int]]
) -> None:
    """A finished chain leaves today's completion marker for its source."""
    # Act
    app_module.geo_e2e("NIFC")

    # Assert
    assert _marker_path("NIFC").exists()


def test_geo_e2e_marks_source_done_when_chain_raises(app_module: ModuleType) -> None:
    """A chain failing before any subprocess still marks the source — the result does not matter."""
    # Act
    with pytest.raises(KeyError):
        app_module.geo_e2e("UNKNOWN")

    # Assert
    assert _marker_path("UNKNOWN").exists()


def test_union_etl_waits_while_sources_are_pending(
    app_module: ModuleType, commands: tuple[list[str], list[int]], redispatches: list[tuple]
) -> None:
    """With markers missing the union job does not run — the task re-queues itself with a countdown."""
    # Arrange
    calls, _ = commands

    # Act
    app_module.union_etl()

    # Assert
    assert calls == []
    assert redispatches == [([1], app_module.config.union.poll_seconds)]
    assert f"Waiting for {len(app_module.config.tasks)} source chain(s) to finish" in _union_log_path().read_text()


def test_union_etl_runs_after_every_source_finished(
    app_module: ModuleType, commands: tuple[list[str], list[int]], redispatches: list[tuple]
) -> None:
    """Once every source chain left its marker the union job runs the SINGLE_ETL union_etl command."""
    # Arrange
    for task in app_module.config.tasks:
        mark_source_done(task.source_name)
    calls, _ = commands

    # Act
    app_module.union_etl()

    # Assert
    assert calls == [UNION_COMMAND]
    assert redispatches == []


def test_union_etl_runs_anyway_after_deadline(
    app_module: ModuleType, commands: tuple[list[str], list[int]], redispatches: list[tuple]
) -> None:
    """After max_attempts the union job runs anyway, warning about the sources that never finished."""
    # Arrange
    calls, _ = commands

    # Act
    app_module.union_etl(attempt=app_module.config.union.max_attempts)

    # Assert
    assert calls == [UNION_COMMAND]
    assert redispatches == []
    assert "Deadline reached" in _union_log_path().read_text()


def test_union_etl_cleans_markers_of_previous_days(
    app_module: ModuleType, commands: tuple[list[str], list[int]], redispatches: list[tuple]
) -> None:
    """The union run removes stale marker directories and keeps today's."""
    # Arrange
    stale_dir = Path("logs") / "progress" / "2000-01-01"
    stale_dir.mkdir(parents=True)
    (stale_dir / "NIFC").write_text("2000-01-01T02:00:00")
    for task in app_module.config.tasks:
        mark_source_done(task.source_name)

    # Act
    app_module.union_etl()

    # Assert
    assert not stale_dir.exists()
    assert _marker_path(app_module.config.tasks[0].source_name).exists()
