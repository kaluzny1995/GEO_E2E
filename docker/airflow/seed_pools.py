"""Seed the Airflow concurrency pools from the `dag.pool_slots` block of config.json.

airflow-init runs this on every `docker compose up` (idempotent — re-running resets the
pools to the configured slot counts). The CLI is invoked through /entrypoint, which sets
up the airflow user environment (a bare `airflow` command fails as root).
"""

import json
import subprocess
from pathlib import Path


def seed_pools(config_path: Path) -> None:
    """Create or update every pool named in config.json's `dag.pool_slots`."""
    # Read config.json on every start — the slot counts live there, not in this script
    pool_slots = json.loads(config_path.read_text())["dag"]["pool_slots"]
    for name, slots in pool_slots.items():
        # airflow pools set <name> <slots> <import_tries>: 0 import retries, check=True fails the init on error
        subprocess.run(["/entrypoint", "airflow", "pools", "set", name, str(slots), "0"], check=True)


if __name__ == "__main__":
    seed_pools(Path(__file__).resolve().parents[2] / "config.json")
