← [README](../README.md)

# Testing & Code Quality

## Tests

```bash
pytest                                    # config comes from pyproject.toml (testpaths = test, -q)
conda run -n geo_e2e_311 pytest test/     # same, through the project env
tox -e py311                              # the same suite, in an isolated tox environment
tox -e py311 -- -k dags                   # filtered run (arguments after -- are passed to pytest)
```

Tests cover:
- **Models** — `JobTypeConfig`, `JobConfig`, `Job`, `EJobType`, `DagSettings`/`PoolConfig` creation and method behavior (timezone validation, `pool_for()` job-type mapping)
- **Config loading** — `get_job_config()` and `get_dag_settings()` with mock JSON files, plus the shipped `config.json` values
- **Command pattern** — subprocess command construction (each step runs via `conda run --no-capture-output` in its configured env)
- **Job registry** — `load_jobs()` parses the flat `jobs.json`; the shipped registry is asserted structurally (DATA_QUALITY after INGESTION, SINGLE_ETL + etl-table DATA_QUALITY, `is_next_skipped_if_error` kept as documentation, the Union job chaining its DATA_QUALITY step and the seventeen VIEW jobs, the DOWNLOAD jobs scheduled and everything else downstreamed)
- **DAG generation** — `build_task_specs()` validates the flat jobs.json registry (unique `name`s and `task_id`s, resolvable `next_jobs` references, one parent per job, no cycles, scheduled jobs are unsourced roots sharing one cron via `resolve_schedule()`, the union job never scheduled, no `VERDICT` job declared) and turns it into Airflow task specs: one spec per job with the unique, declared `task_id` of its jobs.json entry (`NIFC.single_etl`, `NIFC_etl.data_quality`, `View.<view_name>`), parent/child wiring via `upstream`, `all_success` for every source-chain task, the union root waiting on the nine `<source>.single_etl` tasks with `all_done_min_one_success` (runs only if ≥1 of them succeeded; the DATA_QUALITY and VIEW jobs stay outside the fan-in), commands carried over from `config.json`, the synthesized verdict task (`Union.verdict`: `all_done` on every registry task, its Jinja command reading the union root's state via `ti.get_task_states()` so it alone decides the run's outcome), and `ValueError` when the `Union` job is missing or the registry is invalid
- **Airflow rendering** — `geo_dag.JobsSubDAG` / `geo_dag.GEO_DAG` (`test/test_geo_pipeline.py`, skipped where Airflow is not installed): the registry grouped into nine source subDAGs + the union subDAG (its twenty tasks: fan-in, data quality, views, verdict), the rendered DAG matching `build_task_specs()` task-for-task (ids, upstreams, trigger rules, commands, pools, cwd), the union fan-in and its data-quality/view children, the verdict rendered as the DAG's only leaf, the verdict command's Jinja rendering (exits 1 unless `Union.single_etl` succeeded), and the `@dag` keywords
- **Pool seeding** — `test/test_seed_pools.py`: `seed_pools()` (`docker/airflow/seed_pools.py`) creates every `dag.pool_slots` pool via the airflow CLI through `/entrypoint`, and the pools seeded at startup are asserted to match the shipped `config.json`
- **Stack wiring** — `test/test_compose.py` asserts the Supabase metadata DB setup: no local Postgres service/volume/port left in `docker-compose.yaml`, the connection URL built from the `SUPABASE_DB_*` env vars with `sslmode=require` and `search_path=geo_airflow`, the password travelling in `PGPASSWORD` (never in the URL), `create_schema.py` running before the migration, the `.env.example` template documenting every variable, the credentials staying git-ignored and both setup scripts writing them; `test/test_create_schema.py` covers the schema step itself (`get_schema()` default/environment override/identifier validation and the idempotent `CREATE SCHEMA IF NOT EXISTS`, skipped where SQLAlchemy is absent — same pattern as the Airflow tests)
- **Memory logging** — `get_memory_info()` and `log_memory_usage()` output format and correctness

The Airflow conversion (`geo_dag.py`) is unit-tested in the project env and skipped elsewhere: `test/test_geo_pipeline.py` starts with `pytest.importorskip("airflow")`, so the isolated `tox -e py311` run (no Airflow in its deps) skips it while `pytest` in `geo_e2e_311` runs it. The DAG wrapper (`geo_pipeline.py`) is type-checked by mypy and importable locally but not unit-tested — `apache-airflow`, `apache-airflow-providers-standard` and `pendulum` are installed in the dev env with the same pins as the Airflow container (see `environment.yml`), so the IDE resolves the imports and import regressions fail fast:

```bash
conda run -n geo_e2e_311 python -c "import geo_pipeline"
```

Problems that only show up against the container (e.g. broken job config) still surface in the Airflow UI (**Import Errors**) and via `docker compose --env-file .env.local run airflow-cli dags list-import-errors`.

## Ruff, mypy and tox

Linting, formatting, type checking and tests are driven by **[Ruff](https://docs.astral.sh/ruff/)**, **[mypy](https://mypy.readthedocs.io/)** and **[tox](https://tox.wiki/)** — the same tooling as GEO_Download. All three are installed from the `pip:` section of `environment.yml`:

```bash
conda env update -f environment.yml   # only needed once if the env already existed
```

| Tool | Role | Where it is configured |
|------|------|------------------------|
| **Ruff** | Linter + formatter + import sorter (replaces flake8 / isort / black) | `pyproject.toml` → `[tool.ruff]`, `[tool.ruff.lint]`, `[tool.ruff.format]` |
| **mypy** | Static type checking of `model.py`, `geo_dag.py`, `geo_pipeline.py`, `util.py` and the `docker/airflow/` scripts | `pyproject.toml` → `[tool.mypy]`, `tox.ini` → `[testenv:typecheck]` |
| **tox** | Runs every check in a throwaway virtualenv with its own dependencies | `tox.ini` |
| **pytest** | Test suite | `pyproject.toml` → `[tool.pytest.ini_options]` |

Direct commands while editing:

```bash
ruff check .                            # lint everything, writes nothing
ruff check --fix .                      # apply safe autofixes (imports, modernization, unused code)
ruff check --output-format=concise .    # one line per finding — good for CI logs
ruff format --check .                   # list files that are not canonically formatted
ruff format .                           # rewrite files in style (line length 120)
mypy                                    # type check (target files come from pyproject.toml)
```

Ruff rules: `line-length = 120`, `target-version = "py311"`; `logs/`, `__pycache__` and `.idea` are excluded. Enabled families: `E,W` (pycodestyle), `F` (pyflakes), `I` (import sorting), `B` (bugbear), `UP` (pyupgrade), `C4` (comprehensions), `SIM` (simplify), `RUF` (ruff-specific), with `SIM108` ignored; `geo_pipeline.py` ignores `E402` (the `sys.path` setup must precede the `geo_dag`/`model`/`util` imports). `apache-airflow` and `pendulum` are installed in `geo_e2e_311`, but `airflow.*` and `pendulum` are also marked `ignore_missing_imports` in `[tool.mypy.overrides]` so mypy still passes in tox's minimal environments where those packages are absent — when present, `geo_dag.py` and the DAG file are checked against the real packages.

> `ruff format` also walks `README.md` and `docs/*.md`, but only rewrites code inside fenced blocks tagged `python` — prose, tables and `bash`/`json`/`text` blocks are left untouched. `ruff check` (lint) only looks at `.py` files.

## tox environments

`tox` with no arguments runs the default `envlist` from `tox.ini` (`lint, py311`):

| Command | What runs | Status |
|---------|-----------|--------|
| `tox` | default gate = `lint` + `py311` | ✅ |
| `tox -e lint` | `ruff check --output-format=concise .` | ✅ 0 findings |
| `tox -e py311` | full pytest suite (the Airflow rendering tests are skipped there) | ✅ |
| `tox -e typecheck` | `mypy` (`model.py`, `geo_dag.py`, `geo_pipeline.py`, `util.py`, `docker/`) | ✅ 0 errors |
| `tox -e format` | `ruff format --check .` | ✅ |
| `tox -e warnings` | `pytest -W error::DeprecationWarning` | ✅ |
| `tox -e format-fix` | `ruff format .` — writes the changes | – |
| `tox -e lint,py311,typecheck` | any list of environments in one invocation | – |

Notes:

- tox creates its environments under `.tox/` (git-ignored) and installs each env's `deps` from `tox.ini`; your shell only needs `tox` itself — no conda activation is required.
- `skip_install = true` everywhere — the project is not installed as a package, tests import it from the working directory.
- To add a new check, add a `[testenv:xxx]` block; append `xxx` to `envlist` to make it part of the default `tox` run.
