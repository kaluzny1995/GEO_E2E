"""Tests for the Celery app: beat schedule entries and the geo_e2e task chain behaviour."""

from pathlib import Path
from types import ModuleType

import pytest


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


DOWNLOAD_COMMAND = "cd ..;cd GEO_Download;conda run --no-capture-output -n geo_download_311 python run_download.py NIFC"
INGESTION_COMMAND = (
    "cd ..;cd GEO_ETL;conda run --no-capture-output -n geo_etl_311 python run_etl.py INGESTION nifc_fire"
)
DATA_QUALITY_COMMAND = (
    "cd ..;cd GEO_ETL;conda run --no-capture-output -n geo_etl_311 python run_etl.py DATA_QUALITY nifc_fire"
)


def test_beat_schedule_task_names(app_module: ModuleType) -> None:
    """Beat schedule has one entry per data source, all pointing at app.geo_e2e."""
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
    }
    for entry in schedule.values():
        assert entry["task"] == "app.geo_e2e"
        assert len(entry["args"]) == 1


def test_geo_e2e_runs_whole_chain(app_module: ModuleType, commands: tuple[list[str], list[int]]) -> None:
    """A successful DOWNLOAD runs INGESTION and then the DATA_QUALITY job on the same table."""
    # Arrange
    calls, results = commands
    results.extend([0, 0, 0])

    # Act
    app_module.geo_e2e("NIFC")

    # Assert
    assert calls == [DOWNLOAD_COMMAND, INGESTION_COMMAND, DATA_QUALITY_COMMAND]


def test_geo_e2e_stops_chain_when_download_fails(app_module: ModuleType, commands: tuple[list[str], list[int]]) -> None:
    """A failing DOWNLOAD stops the chain — INGESTION and DATA_QUALITY are never executed."""
    # Arrange
    calls, results = commands
    results.append(1)

    # Act
    app_module.geo_e2e("NIFC")

    # Assert
    assert calls == [DOWNLOAD_COMMAND]


def test_geo_e2e_completes_when_ingestion_fails(app_module: ModuleType, commands: tuple[list[str], list[int]]) -> None:
    """With is_failed_if_error=false an INGESTION failure still finishes the task and skips DATA_QUALITY."""
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
    """A failing DATA_QUALITY job is the last step — the chain still finishes normally."""
    # Arrange
    calls, results = commands
    results.extend([0, 0, 1])

    # Act
    app_module.geo_e2e("NIFC")

    # Assert
    assert calls == [DOWNLOAD_COMMAND, INGESTION_COMMAND, DATA_QUALITY_COMMAND]
