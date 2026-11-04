"""Tests for geo_dag.JobsSubDAG and geo_dag.GEO_DAG: the flat jobs.json registry grouped into
per-source subDAGs and rendered into the Airflow mega-DAG. The subDAGs organize the code only —
the rendered tasks must be exactly the ones of model.build_task_specs()."""

from datetime import timedelta
from pathlib import Path

import pytest

# tox's isolated envs carry neither Airflow nor pendulum — the project env runs these tests
pytest.importorskip("airflow")

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG
from airflow.task.trigger_rule import TriggerRule
from jinja2 import Environment

from geo_dag import GEO_DAG, JobsSubDAG
from model import EJobType, TaskSpec, build_task_specs, get_dag_settings
from test._helpers import CRON, SOURCE_COUNT, UNION_ROOT_ID, VIEW_COUNT

_PROJECT_DIR = Path(__file__).resolve().parents[1]


def _render_geo_dag() -> tuple[GEO_DAG, DAG, dict[str, BashOperator]]:
    """Build the mega-DAG the way geo_pipeline.py does: the @dag keywords of GEO_DAG plus
    its to_airflow() called inside the DAG context."""
    geo_dag = GEO_DAG()
    with DAG(**geo_dag.dag_kwargs()) as airflow_dag:
        tasks = geo_dag.to_airflow()
    return geo_dag, airflow_dag, tasks


@pytest.fixture(scope="module")
def rendered_dag() -> tuple[GEO_DAG, DAG, dict[str, BashOperator]]:
    """The mega-DAG rendered once per module — every test only reads it."""
    return _render_geo_dag()


def test_groups_the_registry_into_nine_source_subdags_and_the_union(
    rendered_dag: tuple[GEO_DAG, DAG, dict[str, BashOperator]],
) -> None:
    """GEO_DAG splits the flat spec list by source_name: one subDAG per data source (nine,
    jobs.json order) plus the union subDAG."""
    # Arrange & Act
    geo_dag, _, _ = rendered_dag

    # Assert
    assert len(geo_dag.source_subdags) == SOURCE_COUNT
    assert {subdag.source_name for subdag in geo_dag.source_subdags} == {
        "Avalanche_ORG",
        "EONET",
        "GDACS",
        "IBTrACS",
        "NIFC",
        "OpenMeteo_Flood",
        "USGS_Earthquake",
        "USGS_NWIS",
        "USGS_Volcano",
    }
    assert [subdag.source_name for subdag in geo_dag.subdags][-1] == "Union"
    assert geo_dag.union_subdag is geo_dag.subdags[-1]
    assert len(geo_dag.subdags) == 10


def test_each_subdag_holds_one_source_chain(rendered_dag: tuple[GEO_DAG, DAG, dict[str, BashOperator]]) -> None:
    """Every subDAG carries the task ids of exactly one source: six tasks per source chain,
    twenty for the union chain (fan-in + data quality + the seventeen views + the verdict),
    and no task id is shared between subDAGs."""
    # Arrange & Act
    geo_dag, _, _ = rendered_dag

    # Assert
    rendered: set[str] = set()
    for subdag in geo_dag.subdags:
        assert {spec.source_name for spec in subdag.specs} == {subdag.source_name}
        assert not subdag.task_ids & rendered, f"{subdag.source_name} shares a task id with another subDAG"
        rendered |= subdag.task_ids

        if subdag is geo_dag.union_subdag:
            assert subdag.task_ids == {
                UNION_ROOT_ID,
                "Union_etl.data_quality",
                "Union.verdict",
                *(spec.task_id for spec in build_task_specs() if spec.job_type == EJobType.VIEW),
            }
        else:
            assert len(subdag.task_ids) == 6, subdag.source_name
    assert len(rendered) == len(build_task_specs())


def test_rendered_dag_matches_the_flat_task_specs(rendered_dag: tuple[GEO_DAG, DAG, dict[str, BashOperator]]) -> None:
    """The mega-DAG is the flat spec list rendered: same task ids, same upstreams, same trigger
    rules, commands, pools and cwd — grouping the code into subDAGs changes nothing else."""
    # Arrange
    specs = {spec.task_id: spec for spec in build_task_specs()}
    settings = get_dag_settings()

    # Act
    _, airflow_dag, tasks = rendered_dag

    # Assert
    assert set(tasks) == set(specs) == set(airflow_dag.task_dict)
    for task_id, spec in specs.items():
        task = tasks[task_id]
        assert task is airflow_dag.task_dict[task_id]
        assert set(task.upstream_task_ids) == set(spec.upstream), task_id
        assert task.trigger_rule == spec.trigger_rule, task_id
        assert task.bash_command == spec.command, task_id
        assert task.pool == settings.pool_for(spec.job_type), task_id
        assert task.cwd == str(_PROJECT_DIR), task_id


def test_union_subdag_waits_for_the_single_etl_of_every_source_subdag(
    rendered_dag: tuple[GEO_DAG, DAG, dict[str, BashOperator]],
) -> None:
    """The one dependency between subDAGs: the union root waits for the SINGLE_ETL task — the
    last job — of all nine source subDAGs with all_done_min_one_success, so it runs only when
    at least one of them succeeded."""
    # Arrange & Act
    geo_dag, _, tasks = rendered_dag
    union_root = tasks[UNION_ROOT_ID]

    # Assert
    fan_in = {task_id for subdag in geo_dag.source_subdags for task_id in subdag.single_etl_task_ids}
    assert fan_in == {f"{subdag.source_name}.single_etl" for subdag in geo_dag.source_subdags}
    assert len(fan_in) == SOURCE_COUNT
    assert set(union_root.upstream_task_ids) == fan_in
    assert union_root.trigger_rule == TriggerRule.ALL_DONE_MIN_ONE_SUCCESS
    # the DATA_QUALITY jobs stay out of the fan-in
    assert set(tasks["Union_etl.data_quality"].upstream_task_ids) == {UNION_ROOT_ID}


def test_union_data_quality_wired_under_the_union_single_etl(
    rendered_dag: tuple[GEO_DAG, DAG, dict[str, BashOperator]],
) -> None:
    """The union chain ends in its DATA_QUALITY job, waiting for the union single-ETL with the
    default all_success rule (a failed union stack skips it)."""
    # Arrange & Act
    _, _, tasks = rendered_dag
    child = tasks["Union_etl.data_quality"]

    # Assert
    assert child.upstream_task_ids == {UNION_ROOT_ID}
    assert child.trigger_rule == TriggerRule.ALL_SUCCESS
    assert child.pool == get_dag_settings().pool_for(EJobType.DATA_QUALITY)


def test_view_tasks_render_under_the_union_single_etl_in_the_etl_pool(
    rendered_dag: tuple[GEO_DAG, DAG, dict[str, BashOperator]],
) -> None:
    """Every VIEW job renders into a BashOperator waiting for the union single-ETL with
    all_success, running in the etl pool — the seventeen views read geo.union_etl together."""
    # Arrange & Act
    _, _, tasks = rendered_dag
    view_tasks = [task for task_id, task in tasks.items() if task_id.startswith("View.")]

    # Assert
    assert len(view_tasks) == VIEW_COUNT
    for task in view_tasks:
        assert task.upstream_task_ids == {UNION_ROOT_ID}
        assert task.trigger_rule == TriggerRule.ALL_SUCCESS
        assert task.pool == get_dag_settings().pool_for(EJobType.VIEW)
        view_name = task.task_id.rsplit(".", 1)[-1]
        assert str(task.bash_command).endswith(f"run_etl.py VIEW {view_name}")


def test_verdict_renders_as_the_dags_only_leaf(rendered_dag: tuple[GEO_DAG, DAG, dict[str, BashOperator]]) -> None:
    """The verdict BashOperator waits (all_done) on every other rendered task, has no
    successors itself and runs in the configured verdict pool — Airflow reads a run's state
    from its leaf tasks, so this one task alone decides the run's outcome."""
    # Arrange & Act
    _, _, tasks = rendered_dag
    verdict = tasks["Union.verdict"]

    # Assert
    assert set(verdict.upstream_task_ids) == set(tasks) - {"Union.verdict"}
    assert verdict.downstream_task_ids == set()
    assert verdict.trigger_rule == TriggerRule.ALL_DONE
    assert verdict.pool == get_dag_settings().pool_for(EJobType.VERDICT)
    for task_id, task in tasks.items():
        if task_id != "Union.verdict":
            assert "Union.verdict" in task.downstream_task_ids, task_id


def _verdict_command() -> str:
    """The verdict spec's Jinja command of the shipped registry."""
    return {spec.task_id: spec for spec in build_task_specs()}["Union.verdict"].command


class _FakeTI:
    """The slice of the runtime task instance the template touches: dag_id + state lookup."""

    dag_id = "geo_pipeline"

    def __init__(self, state: str | None) -> None:
        self._state = state

    def get_task_states(self, dag_id: str, *, task_ids: list[str], run_ids: list[str]) -> dict:
        return {run_ids[0]: {task_ids[0]: self._state}}


def test_verdict_command_succeeds_when_the_union_single_etl_succeeded() -> None:
    """Rendered by the BashOperator just before it runs, the verdict command exits 0 only when
    the union single-ETL finished `success` in this run."""
    # Arrange
    environment = Environment()

    # Act
    succeeded = environment.from_string(_verdict_command()).render(ti=_FakeTI("success"), run_id="manual__1")

    # Assert
    assert "exit 1" not in succeeded
    assert f"{UNION_ROOT_ID} = success -> DAG run succeeds" in succeeded


@pytest.mark.parametrize(
    "state",
    [
        pytest.param("failed", id="failed"),
        pytest.param("upstream_failed", id="upstream_failed"),
        pytest.param(None, id="missing"),
    ],
)
def test_verdict_command_fails_unless_the_union_single_etl_succeeded(state: str | None) -> None:
    """Every other state — failed, upstream_failed, a TI missing from the run — renders to
    `exit 1`, so the verdict fails the DAG run."""
    # Arrange
    environment = Environment()

    # Act
    rendered = environment.from_string(_verdict_command()).render(ti=_FakeTI(state), run_id="manual__1")

    # Assert
    assert "exit 1" in rendered
    assert f"{UNION_ROOT_ID} = " in rendered and "-> DAG run fails" in rendered


def test_jobs_subdag_rejects_specs_of_several_sources_or_no_specs() -> None:
    """A JobsSubDAG holds the partial registry of exactly one source."""
    # Arrange
    per_source = {spec.source_name: spec for spec in build_task_specs()}
    settings = get_dag_settings()

    # Act & Assert
    with pytest.raises(ValueError, match="at least one task spec"):
        JobsSubDAG([], dag_settings=settings, project_dir=_PROJECT_DIR)
    with pytest.raises(ValueError, match="one source only"):
        JobsSubDAG([per_source["NIFC"], per_source["GDACS"]], dag_settings=settings, project_dir=_PROJECT_DIR)


def test_union_fan_in_rejects_upstreams_no_subdag_renders() -> None:
    """A foreign upstream id that no subDAG renders fails with a clear error, not a KeyError."""
    # Arrange
    geo_dag = GEO_DAG()

    # Act & Assert
    with DAG(**geo_dag.dag_kwargs()):
        tasks = geo_dag.union_subdag.to_airflow()
        with pytest.raises(ValueError, match="waits for unknown task"):
            geo_dag.union_subdag.wire_upstreams_from(tasks)


def test_union_subdag_without_specs_raises() -> None:
    """A spec list without the union chain fails — the mega-DAG needs its fan-in."""
    # Arrange
    specs: list[TaskSpec] = [spec for spec in build_task_specs() if spec.source_name != "Union"]

    # Act
    geo_dag = GEO_DAG(specs=specs)

    # Assert
    assert len(geo_dag.source_subdags) == SOURCE_COUNT
    with pytest.raises(ValueError, match='no "Union" chain'):
        _ = geo_dag.union_subdag


def test_dag_kwargs_carry_the_mega_dag_settings() -> None:
    """The @dag keywords come from config.json (dag block) and jobs.json (the shared cron)."""
    # Arrange & Act
    kwargs = GEO_DAG().dag_kwargs()
    settings = get_dag_settings()

    # Assert
    assert kwargs["dag_id"] == settings.dag_id
    assert kwargs["schedule"] == CRON
    assert kwargs["catchup"] is settings.catchup
    assert kwargs["max_active_runs"] == settings.max_active_runs
    assert kwargs["tags"] == settings.tags
    assert kwargs["default_args"]["retries"] == settings.retries
    assert kwargs["default_args"]["execution_timeout"] == timedelta(minutes=settings.execution_timeout_minutes)
    assert kwargs["start_date"] == pendulum.datetime(
        settings.start_date.year, settings.start_date.month, settings.start_date.day, tz=settings.timezone
    )
