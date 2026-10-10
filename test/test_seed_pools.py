"""Tests for seed_pools(): seeding the Airflow pools from config.json's dag.pool_slots."""

import importlib.util
import json
from pathlib import Path
from unittest.mock import patch

_SEED_POOLS_PATH = Path(__file__).resolve().parents[1] / "docker" / "airflow" / "seed_pools.py"


def _load_seed_pools():
    """Import docker/airflow/seed_pools.py by path (it is not part of a package)."""
    spec = importlib.util.spec_from_file_location("seed_pools", _SEED_POOLS_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_seed_pools_sets_every_configured_pool(tmp_path: Path) -> None:
    """Each pool of dag.pool_slots is created via the airflow CLI through /entrypoint."""
    # Arrange
    config = tmp_path / "config.json"
    config.write_text(json.dumps({"dag": {"pool_slots": {"download": 3, "etl": 2}}}))
    module = _load_seed_pools()

    # Act
    with patch("subprocess.run") as run:
        module.seed_pools(config)

    # Assert
    assert run.call_count == 2
    run.assert_any_call(["/entrypoint", "airflow", "pools", "set", "download", "3", "0"], check=True)
    run.assert_any_call(["/entrypoint", "airflow", "pools", "set", "etl", "2", "0"], check=True)


def test_seed_pools_shipped_config_matches_dag_settings() -> None:
    """The pools seeded at startup are exactly the pool_slots of the shipped config.json."""
    # Arrange
    module = _load_seed_pools()
    shipped = Path(__file__).resolve().parents[1] / "config.json"
    pool_slots = json.loads(shipped.read_text())["dag"]["pool_slots"]

    # Act
    with patch("subprocess.run") as run:
        module.seed_pools(shipped)

    # Assert
    assert pool_slots == {"download": 3, "etl": 2}
    assert run.call_count == len(pool_slots)
