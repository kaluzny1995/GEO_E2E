"""Preflight checks for the GEO E2E Docker Compose stack.

`up.sh` runs this before `docker compose up`, and `build-images.sh` runs it before the
builds. It verifies the two hard requirements of this checkout:

1. All six GEO repositories are checked out side by side:
   GEO_API, GEO_APP, GEO_Download, GEO_E2E, GEO_ETL and GEO_Visual.
2. `GEO_E2E/.env` exists and carries the five Supabase credentials.

Only the standard library is used, so the check runs before any conda environment exists.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

#: The GEO_E2E directory (the Compose project directory).
PROJECT_DIR = Path(__file__).resolve().parent
#: The directory that must contain all six repositories.
GEO_ROOT = PROJECT_DIR.parent

#: Every required sibling repository and a file that proves its checkout is complete.
REQUIRED_REPOS: dict[str, str] = {
    "GEO_API": "main.py",
    "GEO_APP": "package.json",
    "GEO_Download": "run_download.py",
    "GEO_E2E": "docker-compose.yaml",
    "GEO_ETL": "run_etl.py",
    "GEO_Visual": "app.py",
}

#: The Supabase values Docker Compose interpolates into every service.
SUPABASE_KEYS = (
    "SUPABASE_DB_USER",
    "SUPABASE_DB_PASSWORD",
    "SUPABASE_DB_HOST",
    "SUPABASE_DB_PORT",
    "SUPABASE_DB_NAME",
)

_ENV_LINE_RE = re.compile(r"^(?:export\s+)?(?P<key>[A-Za-z_][A-Za-z0-9_]*)\s*=\s*(?P<value>.*)$")
_PLACEHOLDER_RE = re.compile(r"<[^>]+>")
_INLINE_COMMENT_RE = re.compile(r"\s+#")


@dataclass(frozen=True)
class PreflightReport:
    """The outcome of the preflight checks."""

    geo_root: Path
    env_file: Path
    repository_problems: tuple[str, ...] = ()
    supabase_problems: tuple[str, ...] = ()

    @property
    def ok(self) -> bool:
        """True when every check passed."""
        return not self.repository_problems and not self.supabase_problems


def _clean_env_value(raw: str) -> str:
    """Strip whitespace, quotes and unquoted inline comments from a .env value."""
    value = raw.strip()
    if value[:1] in {"'", '"'}:
        quote = value[0]
        end = value.find(quote, 1)
        return value[1:end] if end != -1 else value[1:]
    return _INLINE_COMMENT_RE.split(value, maxsplit=1)[0].strip()


def read_env_file(path: Path) -> dict[str, str]:
    """Parse a Compose-style .env file into a key/value mapping."""
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        match = _ENV_LINE_RE.match(line)
        if match:
            values[match.group("key")] = _clean_env_value(match.group("value"))
    return values


def _is_placeholder(value: str) -> bool:
    """True for the `<YOUR-PASSWORD>`-style values shipped in .env.example."""
    lowered = value.lower()
    return bool(_PLACEHOLDER_RE.search(value)) or lowered in {
        "changeme",
        "change-me",
        "your-password",
        "your_password",
        "replace-me",
        "replace_me",
    }


def _effective_value(key: str, file_values: Mapping[str, str], environ: Mapping[str, str]) -> str:
    """The value Compose sees: a process environment value wins over .env, even when empty."""
    if key in environ:
        return environ[key].strip()
    return file_values.get(key, "").strip()


def check_repositories(geo_root: Path) -> tuple[str, ...]:
    """Return one problem per missing or incomplete required repository."""
    problems: list[str] = []
    for name, marker in REQUIRED_REPOS.items():
        path = geo_root / name
        if not path.is_dir():
            problems.append(f"{name}: missing directory (expected {path})")
        elif not (path / marker).is_file():
            problems.append(f"{name}: incomplete checkout (missing {path / marker})")
    return tuple(problems)


def check_supabase(env_file: Path, environ: Mapping[str, str] | None = None) -> tuple[str, ...]:
    """Return one problem per missing, placeholder or malformed Supabase value."""
    if not env_file.is_file():
        return (f"{env_file}: file not found",)

    try:
        file_values = read_env_file(env_file)
    except OSError as exc:  # unreadable file, permission denied, ...
        return (f"{env_file}: cannot read the file ({exc})",)

    environment = os.environ if environ is None else environ
    problems: list[str] = []

    for key in SUPABASE_KEYS:
        value = _effective_value(key, file_values, environment)
        if not value:
            problems.append(f"{key}: missing or empty")
        elif _is_placeholder(value):
            problems.append(f"{key}: still contains the .env.example placeholder")

    port = _effective_value("SUPABASE_DB_PORT", file_values, environment)
    if port and not _is_placeholder(port):
        try:
            port_number = int(port)
        except ValueError:
            problems.append(f"SUPABASE_DB_PORT: not a number ({port!r})")
        else:
            if not 1 <= port_number <= 65535:
                problems.append(f"SUPABASE_DB_PORT: must be between 1 and 65535 (got {port_number})")

    return tuple(problems)


def check(
    project_dir: Path | None = None,
    geo_root: Path | None = None,
    env_file: Path | None = None,
    environ: Mapping[str, str] | None = None,
) -> PreflightReport:
    """Run every preflight check and return the combined report."""
    project = (project_dir or PROJECT_DIR).resolve()
    root = (geo_root or project.parent).resolve()
    env = (env_file or project / ".env").resolve()
    return PreflightReport(
        geo_root=root,
        env_file=env,
        repository_problems=check_repositories(root),
        supabase_problems=check_supabase(env, environ),
    )


def format_success(report: PreflightReport) -> str:
    """The one-line message printed when the stack is ready to start."""
    return (
        f"Preflight OK: all six repositories are present under {report.geo_root}; "
        f"the Supabase credentials are set in {report.env_file}."
    )


def format_failure(report: PreflightReport) -> str:
    """The actionable error printed when one or more checks fail."""
    lines = ["Preflight failed - fix the following before running docker compose up:", ""]

    if report.repository_problems:
        lines.extend(
            [
                "1. Required repositories (all six must be in the same folder)",
                f"   Expected under: {report.geo_root}",
                *(f"   - {problem}" for problem in report.repository_problems),
                "",
                "   Clone or restore the missing/incomplete repositories next to GEO_E2E.",
                "   Required: " + ", ".join(REQUIRED_REPOS) + ".",
                "",
            ]
        )

    if report.supabase_problems:
        lines.extend(
            [
                "2. Supabase credentials in .env",
                f"   File: {report.env_file}",
                *(f"   - {problem}" for problem in report.supabase_problems),
                "",
                f"   Create the file with: cp {report.env_file.parent / '.env.example'} {report.env_file}",
                "   Then fill in all five SUPABASE_DB_* values from the Supabase dashboard:",
                "   Project -> Connect -> Connection pooling -> Transaction pooler.",
                "   Alternatively run ./setup.sh, which imports existing credentials from the",
                "   sibling GEO_ETL, GEO_API or GEO_Visual .env files.",
                "",
            ]
        )

    lines.extend(
        [
            "Fix the items above, then re-run ./check.sh (or ./up.sh -d).",
            "See GEO_E2E/README.md > Dockerizing the solution for the full setup.",
        ]
    )
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point: exit 0 when ready, 1 with an actionable error otherwise."""
    parser = argparse.ArgumentParser(description="Check the GEO E2E Docker Compose prerequisites.")
    parser.add_argument(
        "--project-dir",
        type=Path,
        default=PROJECT_DIR,
        help="GEO_E2E directory (default: the directory containing this script)",
    )
    parser.add_argument(
        "--geo-root",
        type=Path,
        default=None,
        help="directory containing all six repositories (default: parent of --project-dir)",
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=None,
        help="Compose env file (default: <project-dir>/.env)",
    )
    args = parser.parse_args(argv)

    report = check(project_dir=args.project_dir, geo_root=args.geo_root, env_file=args.env_file)
    if report.ok:
        print(format_success(report))
        return 0

    print(format_failure(report), file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
