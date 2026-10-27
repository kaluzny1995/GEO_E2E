"""Tests for create_schema(): the schema step airflow-init runs before `airflow db migrate`."""

import importlib.util
from pathlib import Path
from unittest.mock import patch

import pytest

_CREATE_SCHEMA_PATH = Path(__file__).resolve().parents[1] / "docker" / "airflow" / "create_schema.py"


def _load_create_schema():
    """Import docker/airflow/create_schema.py by path (it is not part of a package)."""
    # tox's isolated envs carry no SQLAlchemy — same skip pattern as test_geo_pipeline.py
    pytest.importorskip("sqlalchemy")
    spec = importlib.util.spec_from_file_location("create_schema", _CREATE_SCHEMA_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_get_schema_defaults_to_geo_airflow(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without AIRFLOW_DB_SCHEMA the script targets the schema compose puts into search_path."""
    module = _load_create_schema()
    monkeypatch.delenv(module.ENV_SCHEMA, raising=False)
    assert module.get_schema() == "geo_airflow"


def test_get_schema_reads_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """AIRFLOW_DB_SCHEMA overrides the default (compose interpolates the same value into the URL)."""
    module = _load_create_schema()
    monkeypatch.setenv(module.ENV_SCHEMA, "other_schema")
    assert module.get_schema() == "other_schema"


def test_get_schema_rejects_non_identifier_values(monkeypatch: pytest.MonkeyPatch) -> None:
    """The value is interpolated into DDL — anything but a plain identifier is refused."""
    module = _load_create_schema()
    monkeypatch.setenv(module.ENV_SCHEMA, 'geo_airflow"; DROP TABLE dag --')
    with pytest.raises(ValueError, match="AIRFLOW_DB_SCHEMA"):
        module.get_schema()


def test_create_schema_runs_idempotent_ddl() -> None:
    """create_schema() executes `CREATE SCHEMA IF NOT EXISTS "…"` and closes the engine."""
    module = _load_create_schema()
    with patch.object(module, "create_engine") as create_engine:
        module.create_schema("postgresql+psycopg2://example", "geo_airflow")

    connection = create_engine.return_value.begin.return_value.__enter__.return_value
    assert str(connection.execute.call_args[0][0]) == 'CREATE SCHEMA IF NOT EXISTS "geo_airflow"'
    create_engine.return_value.dispose.assert_called_once()
