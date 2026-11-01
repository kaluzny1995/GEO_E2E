"""Tests for create_schema(): the schema step airflow-init runs before `airflow db migrate`."""

import importlib.util
from pathlib import Path
from unittest.mock import patch

import pytest

_CREATE_SCHEMA_PATH = Path(__file__).resolve().parents[1] / "docker" / "geo_airflow_spark" / "create_schema.py"


def _load_create_schema():
    """Import docker/geo_airflow_spark/create_schema.py by path (it is not part of a package)."""
    # tox's isolated envs carry no SQLAlchemy — same skip pattern as test_geo_pipeline.py
    pytest.importorskip("sqlalchemy")
    spec = importlib.util.spec_from_file_location("create_schema", _CREATE_SCHEMA_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def create_schema_module():
    """docker/geo_airflow_spark/create_schema.py imported once for the whole module (the exec is not free)."""
    return _load_create_schema()


@pytest.mark.parametrize(
    ("env_value", "expected"),
    [
        pytest.param(None, "geo_airflow", id="default"),
        pytest.param("other_schema", "other_schema", id="environment"),
    ],
)
def test_get_schema_defaults_to_geo_airflow_and_reads_the_environment(
    create_schema_module, monkeypatch: pytest.MonkeyPatch, env_value: str | None, expected: str
) -> None:
    """Without AIRFLOW_DB_SCHEMA the script targets the schema compose puts into search_path;
    AIRFLOW_DB_SCHEMA overrides that default (compose interpolates the same value into the URL)."""
    if env_value is None:
        monkeypatch.delenv(create_schema_module.ENV_SCHEMA, raising=False)
    else:
        monkeypatch.setenv(create_schema_module.ENV_SCHEMA, env_value)

    assert create_schema_module.get_schema() == expected


def test_get_schema_rejects_non_identifier_values(create_schema_module, monkeypatch: pytest.MonkeyPatch) -> None:
    """The value is interpolated into DDL — anything but a plain identifier is refused."""
    monkeypatch.setenv(create_schema_module.ENV_SCHEMA, 'geo_airflow"; DROP TABLE dag --')
    with pytest.raises(ValueError, match="AIRFLOW_DB_SCHEMA"):
        create_schema_module.get_schema()


def test_create_schema_runs_idempotent_ddl(create_schema_module) -> None:
    """create_schema() executes `CREATE SCHEMA IF NOT EXISTS "…"` and closes the engine."""
    with patch.object(create_schema_module, "create_engine") as create_engine:
        create_schema_module.create_schema("postgresql+psycopg2://example", "geo_airflow")

    connection = create_engine.return_value.begin.return_value.__enter__.return_value
    assert str(connection.execute.call_args[0][0]) == 'CREATE SCHEMA IF NOT EXISTS "geo_airflow"'
    create_engine.return_value.dispose.assert_called_once()
