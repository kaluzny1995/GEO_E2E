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
- **Models** — `JobTypeConfig`, `JobConfig`, `Job`, `EJobType` creation and method behavior
- **Config loading** — `get_job_config()` and `Job.from_config()` with mock JSON files
- **Command pattern** — subprocess command construction (each step runs via `conda run --no-capture-output` in its configured env)
- **Job chains** — `Job.from_config()` loads `jobs.json`; the shipped chains are asserted structurally (DATA_QUALITY after INGESTION, SINGLE_ETL + etl-table DATA_QUALITY, failure flags kept as documentation)
- **DAG generation** — `build_task_specs()` turns jobs.json into Airflow task specs: one spec per job with unique path-based task ids (`NIFC.ingestion.single_etl.data_quality`), parent/child wiring via `upstream`, `all_success` for every source-chain task, the union root waiting on the nine `ingestion.single_etl` tasks with `all_done` (the DATA_QUALITY jobs stay terminal), commands carried over from `config.json`, and `ValueError` when the `Union` chain is missing
- **Memory logging** — `get_memory_info()` and `log_memory_usage()` output format and correctness

The DAG file itself (`dags/geo_pipeline.py`) is type-checked by mypy and importable locally but not unit-tested — `apache-airflow`, `apache-airflow-providers-standard` and `pendulum` are installed in the dev env with the same pins as the Airflow container (see `environment.yml`), so the IDE resolves the imports and import regressions fail fast:

```bash
conda run -n geo_e2e_311 python -c "import sys; sys.path.insert(0, 'dags'); import geo_pipeline"
```

Problems that only show up against the container (e.g. broken job config) still surface in the Airflow UI (**Import Errors**) and via `docker compose run airflow-cli dags list-import-errors`.

## Ruff, mypy and tox

Linting, formatting, type checking and tests are driven by **[Ruff](https://docs.astral.sh/ruff/)**, **[mypy](https://mypy.readthedocs.io/)** and **[tox](https://tox.wiki/)** — the same tooling as GEO_Download. All three are installed from the `pip:` section of `environment.yml`:

```bash
conda env update -f environment.yml   # only needed once if the env already existed
```

| Tool | Role | Where it is configured |
|------|------|------------------------|
| **Ruff** | Linter + formatter + import sorter (replaces flake8 / isort / black) | `pyproject.toml` → `[tool.ruff]`, `[tool.ruff.lint]`, `[tool.ruff.format]` |
| **mypy** | Static type checking of `model.py`, `util.py`, `dags/` | `pyproject.toml` → `[tool.mypy]`, `tox.ini` → `[testenv:typecheck]` |
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

Ruff rules: `line-length = 120`, `target-version = "py311"`; `logs/`, `__pycache__` and `.idea` are excluded. Enabled families: `E,W` (pycodestyle), `F` (pyflakes), `I` (import sorting), `B` (bugbear), `UP` (pyupgrade), `C4` (comprehensions), `SIM` (simplify), `RUF` (ruff-specific), with `SIM108` ignored; `dags/**` ignores `E402` (the `sys.path` setup must precede the `model`/`util` imports). `apache-airflow` and `pendulum` are installed in `geo_e2e_311`, but `airflow.*` and `pendulum` are also marked `ignore_missing_imports` in `[tool.mypy.overrides]` so mypy still passes in tox's minimal environments where those packages are absent — when present, the DAG file is checked against the real packages.

> `ruff format` also walks `README.md` and `docs/*.md`, but only rewrites code inside fenced blocks tagged `python` — prose, tables and `bash`/`json`/`text` blocks are left untouched. `ruff check` (lint) only looks at `.py` files.

## tox environments

`tox` with no arguments runs the default `envlist` from `tox.ini` (`lint, py311`):

| Command | What runs | Status |
|---------|-----------|--------|
| `tox` | default gate = `lint` + `py311` | ✅ |
| `tox -e lint` | `ruff check --output-format=concise .` | ✅ 0 findings |
| `tox -e py311` | full pytest suite (32 tests) | ✅ |
| `tox -e typecheck` | `mypy` (`model.py`, `util.py`, `dags/`) | ✅ 0 errors |
| `tox -e format` | `ruff format --check .` | ✅ 13 files already formatted |
| `tox -e warnings` | `pytest -W error::DeprecationWarning` | ✅ |
| `tox -e format-fix` | `ruff format .` — writes the changes | – |
| `tox -e lint,py311,typecheck` | any list of environments in one invocation | – |

Notes:

- tox creates its environments under `.tox/` (git-ignored) and installs each env's `deps` from `tox.ini`; your shell only needs `tox` itself — no conda activation is required.
- `skip_install = true` everywhere — the project is not installed as a package, tests import it from the working directory.
- To add a new check, add a `[testenv:xxx]` block; append `xxx` to `envlist` to make it part of the default `tox` run.
