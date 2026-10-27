"""Create the schema that hosts Airflow's metadata tables on the Supabase database.

airflow-init runs this before `airflow db migrate`: the SQLAlchemy URL pins `search_path`
to the schema named by `AIRFLOW_DB_SCHEMA` (`geo_airflow` by default — the same value
docker-compose.yaml interpolates into that URL), but Postgres never creates a schema on
its own. Without this step the migration would silently fall back to the default schema
instead of `geo_airflow`. The script is idempotent — re-running it only re-checks.
"""

import os
import re

from sqlalchemy import create_engine, text

ENV_SCHEMA = "AIRFLOW_DB_SCHEMA"
DEFAULT_SCHEMA = "geo_airflow"
# Schema names are interpolated into the DDL below, so only unquoted-identifier characters
# are accepted (compose builds the search_path from the same characters)
_SCHEMA_PATTERN = re.compile(r"[A-Za-z0-9_]+")


def get_schema() -> str:
    """Read the schema name from `AIRFLOW_DB_SCHEMA` (default `geo_airflow`).

    Raises:
        ValueError: When the value is not a plain identifier (letters, digits, underscores),
            since it is interpolated into `CREATE SCHEMA`.
    """
    schema = os.environ.get(ENV_SCHEMA, DEFAULT_SCHEMA)
    if not _SCHEMA_PATTERN.fullmatch(schema):
        raise ValueError(f"invalid {ENV_SCHEMA} {schema!r} — expected letters, digits and underscores")
    return schema


def create_schema(connection_string: str, schema: str) -> None:
    """Create `schema` on the database behind `connection_string`, unless it already exists."""
    engine = create_engine(connection_string)
    try:
        with engine.begin() as connection:
            connection.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
    finally:
        engine.dispose()


if __name__ == "__main__":
    create_schema(os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"], get_schema())
