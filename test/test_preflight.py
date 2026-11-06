"""Tests for the pre-`docker compose up` preflight checks."""

from __future__ import annotations

import shutil
from pathlib import Path

import preflight

VALID_ENV = """\
# Supabase credentials used by the tests
SUPABASE_DB_USER=postgres.projectref
SUPABASE_DB_PASSWORD=super-secret
SUPABASE_DB_HOST=aws-1-eu-west-1.pooler.supabase.com
SUPABASE_DB_PORT=6543
SUPABASE_DB_NAME=postgres
"""


def _layout(tmp_path: Path) -> tuple[Path, Path]:
    """Create a complete six-repository checkout with a valid .env."""
    root = tmp_path / "GeographicData"
    project = root / "GEO_E2E"
    for name, marker in preflight.REQUIRED_REPOS.items():
        repository = root / name
        repository.mkdir(parents=True)
        (repository / marker).write_text("checkout marker\n", encoding="utf-8")
    env_file = project / ".env"
    env_file.write_text(VALID_ENV, encoding="utf-8")
    return project, env_file


def test_all_six_repositories_and_credentials_pass(tmp_path: Path) -> None:
    """The real checkout shape passes and reports all six repositories."""
    project, env_file = _layout(tmp_path)

    report = preflight.check(project_dir=project, environ={})

    assert report.ok
    assert report.geo_root == project.parent
    assert report.env_file == env_file.resolve()
    assert report.repository_problems == ()
    assert report.supabase_problems == ()
    assert "all six repositories" in preflight.format_success(report)


def test_missing_repository_is_reported_with_its_path(tmp_path: Path) -> None:
    """A missing sibling repository fails the check and names the expected directory."""
    project, _ = _layout(tmp_path)
    missing = project.parent / "GEO_API"
    shutil.rmtree(missing)

    report = preflight.check(project_dir=project, environ={})

    assert not report.ok
    assert len(report.repository_problems) == 1
    problem = report.repository_problems[0]
    assert "GEO_API" in problem
    assert str(missing) in problem
    failure = preflight.format_failure(report)
    assert "Required repositories" in failure
    assert "GEO_API" in failure


def test_incomplete_repository_is_reported(tmp_path: Path) -> None:
    """An empty/partial directory is not mistaken for a usable checkout."""
    project, _ = _layout(tmp_path)
    (project.parent / "GEO_ETL" / "run_etl.py").unlink()

    report = preflight.check(project_dir=project, environ={})

    assert not report.ok
    assert any("incomplete checkout" in problem and "GEO_ETL" in problem for problem in report.repository_problems)


def test_missing_env_file_is_reported_with_the_copy_command(tmp_path: Path) -> None:
    """A missing .env fails with the exact template command in the error."""
    project, env_file = _layout(tmp_path)
    env_file.unlink()

    report = preflight.check(project_dir=project, environ={})

    assert not report.ok
    assert len(report.supabase_problems) == 1
    assert "file not found" in report.supabase_problems[0]
    failure = preflight.format_failure(report)
    assert f"cp {project / '.env.example'} {env_file}" in failure


def test_all_missing_supabase_variables_are_named(tmp_path: Path) -> None:
    """The error lists every required key instead of only the first missing one."""
    _, env_file = _layout(tmp_path)
    env_file.write_text("SUPABASE_DB_USER=postgres.projectref\n", encoding="utf-8")

    problems = preflight.check_supabase(env_file, environ={})

    for key in preflight.SUPABASE_KEYS:
        if key == "SUPABASE_DB_USER":
            continue
        assert any(key in problem for problem in problems), key


def test_env_example_placeholders_are_rejected(tmp_path: Path) -> None:
    """Copying .env.example without editing the placeholders does not pass."""
    _, env_file = _layout(tmp_path)
    env_file.write_text(
        "\n".join(
            [
                "SUPABASE_DB_USER=postgres.<YOUR-PROJECT-REF>",
                "SUPABASE_DB_PASSWORD=<YOUR-PASSWORD>",
                "SUPABASE_DB_HOST=<pooler host>",
                "SUPABASE_DB_PORT=6543",
                "SUPABASE_DB_NAME=postgres",
            ]
        ),
        encoding="utf-8",
    )

    problems = preflight.check_supabase(env_file, environ={})

    assert sum("placeholder" in problem for problem in problems) == 3


def test_invalid_port_is_rejected(tmp_path: Path) -> None:
    """The pooler port must be a number in the valid TCP range."""
    _, env_file = _layout(tmp_path)
    env_file.write_text(VALID_ENV.replace("SUPABASE_DB_PORT=6543", "SUPABASE_DB_PORT=not-a-port"), encoding="utf-8")

    problems = preflight.check_supabase(env_file, environ={})
    assert any("not a number" in problem for problem in problems)

    env_file.write_text(VALID_ENV.replace("SUPABASE_DB_PORT=6543", "SUPABASE_DB_PORT=70000"), encoding="utf-8")
    problems = preflight.check_supabase(env_file, environ={})
    assert any("between 1 and 65535" in problem for problem in problems)


def test_env_parser_handles_comments_quotes_and_export(tmp_path: Path) -> None:
    """Compose-style syntax does not break the credential lookup."""
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "# comment",
                'export SUPABASE_DB_USER="postgres.projectref"',
                "SUPABASE_DB_PASSWORD='pass # not a comment'",
                "SUPABASE_DB_HOST=aws-1-eu-west-1.pooler.supabase.com # pooler",
                "SUPABASE_DB_PORT=6543",
                "SUPABASE_DB_NAME=postgres",
            ]
        ),
        encoding="utf-8",
    )

    values = preflight.read_env_file(env_file)

    assert values["SUPABASE_DB_USER"] == "postgres.projectref"
    assert values["SUPABASE_DB_PASSWORD"] == "pass # not a comment"
    assert values["SUPABASE_DB_HOST"] == "aws-1-eu-west-1.pooler.supabase.com"
    assert preflight.check_supabase(env_file, environ={}) == ()


def test_process_environment_overrides_the_env_file(tmp_path: Path) -> None:
    """Compose lets exported variables win over .env; the checker follows the same rule."""
    _, env_file = _layout(tmp_path)
    env_file.write_text(VALID_ENV.replace("SUPABASE_DB_PASSWORD=super-secret\n", ""), encoding="utf-8")

    assert any("SUPABASE_DB_PASSWORD" in problem for problem in preflight.check_supabase(env_file, environ={}))
    assert preflight.check_supabase(env_file, environ={"SUPABASE_DB_PASSWORD": "from-shell"}) == ()


def test_failure_output_never_prints_secret_values(tmp_path: Path) -> None:
    """Repository errors must not leak the password from a valid .env."""
    project, _ = _layout(tmp_path)
    shutil.rmtree(project.parent / "GEO_Visual")

    failure = preflight.format_failure(preflight.check(project_dir=project, environ={}))

    assert "super-secret" not in failure


def test_main_returns_zero_on_success_and_one_on_failure(tmp_path: Path, capsys, monkeypatch) -> None:
    """The CLI is the actual gate used by check.sh/up.sh."""
    for key in preflight.SUPABASE_KEYS:
        monkeypatch.delenv(key, raising=False)
    project, _ = _layout(tmp_path)

    assert preflight.main(["--project-dir", str(project)]) == 0
    assert "Preflight OK" in capsys.readouterr().out

    shutil.rmtree(project.parent / "GEO_APP")
    assert preflight.main(["--project-dir", str(project)]) == 1
    err = capsys.readouterr().err
    assert "Preflight failed" in err
    assert "GEO_APP" in err
