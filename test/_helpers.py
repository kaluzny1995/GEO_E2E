"""Shared constants and registry loaders for the test suite.

One place for the values many test modules assert on (the shipped registry's cron, task ids,
counts and command prefixes) — tests reference them instead of repeating the literals.
"""

import json
from pathlib import Path

#: The repo root — the directory jobs.json ships in.
ROOT = Path(__file__).resolve().parents[1]

#: The cron every scheduled job of the shipped registry declares.
CRON = "0 8 * * *"
#: The task id of the union chain's single-ETL job — the mega-DAG's synthesized fan-in root.
UNION_ROOT_ID = "Union.single_etl"
#: The shell prefix of every GEO_ETL task command (hop to the sibling repo, run in its conda env).
ETL_CMD_PREFIX = "cd ..;cd GEO_ETL;conda run --no-capture-output -n geo_etl_311 python run_etl.py"
#: The number of data-source chains in the shipped registry (every source but Union).
SOURCE_COUNT = 9
#: The number of VIEW jobs in the shipped registry (they all read geo.union_etl).
VIEW_COUNT = 17


def load_shipped_jobs() -> list[dict]:
    """The raw job dicts of the shipped jobs.json registry, in file order."""
    with open(ROOT / "jobs.json") as f:
        return json.load(f)
