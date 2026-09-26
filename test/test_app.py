import pytest


@pytest.fixture()
def app_module(tmp_path, monkeypatch):
    """ Import app.py with cwd switched to a temp dir so the module-level
        celerybeat_cleanup() does not delete real celerybeat-schedule files. """
    monkeypatch.chdir(tmp_path)
    import app
    return app


def _recorder(results):
    """ Build a fake _run_command that records commands and returns given return codes. """
    calls = []

    def fake_run_command(command, log_file):
        calls.append(command)
        return results[len(calls) - 1]

    return calls, fake_run_command


def test_geo_download_runs_ingestion_after_successful_download(app_module, monkeypatch):
    # Arrange
    calls, fake_run_command = _recorder([0, 0])
    monkeypatch.setattr(app_module, "_run_command", fake_run_command)

    # Act
    app_module.geo_download("NIFC", "nifc_fire")

    # Assert
    assert calls == [
        "cd ..;cd GEO_Download;conda run --no-capture-output -n geo_download_311 python run_download.py NIFC",
        "cd ..;cd GEO_ETL;conda run --no-capture-output -n geo_etl_311 python run_etl.py INGESTION nifc_fire",
    ]


def test_geo_download_skips_ingestion_when_download_fails(app_module, monkeypatch):
    # Arrange
    calls, fake_run_command = _recorder([1])
    monkeypatch.setattr(app_module, "_run_command", fake_run_command)

    # Act
    app_module.geo_download("NIFC", "nifc_fire")

    # Assert
    assert calls == [
        "cd ..;cd GEO_Download;conda run --no-capture-output -n geo_download_311 python run_download.py NIFC"
    ]


def test_geo_download_skips_ingestion_without_table_name(app_module, monkeypatch):
    # Arrange
    calls, fake_run_command = _recorder([0])
    monkeypatch.setattr(app_module, "_run_command", fake_run_command)

    # Act
    app_module.geo_download("NIFC")

    # Assert
    assert calls == [
        "cd ..;cd GEO_Download;conda run --no-capture-output -n geo_download_311 python run_download.py NIFC"
    ]


def test_geo_download_logs_ingestion_failure(app_module, monkeypatch, tmp_path):
    # Arrange
    calls, fake_run_command = _recorder([0, 1])
    monkeypatch.setattr(app_module, "_run_command", fake_run_command)

    # Act
    app_module.geo_download("NIFC", "nifc_fire")

    # Assert
    assert len(calls) == 2
    log_files = list((tmp_path / "logs" / "NIFC").glob("celery_NIFC_*.log"))
    assert len(log_files) == 1
    content = log_files[0].read_text()
    assert "likely no new files to upload" in content
