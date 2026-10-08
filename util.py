"""Shared helpers: Celery beat cleanup, subprocess execution, logging, memory reporting
and the completion markers the union task waits for."""

import datetime as dt
import glob
import logging
import os
import shutil
import subprocess

import psutil
import pytz

# Completion markers of finished source chains: logs/progress/<date>/<source_name>
_PROGRESS_ROOT = os.path.join("logs", "progress")


def celerybeat_cleanup() -> None:
    """Celery beat cleanup before app start"""
    for f in glob.glob("celerybeat-schedule*"):
        os.remove(f)


def run_command(command: str, log_file: str) -> int:
    """Run a shell command streaming its output directly to the log file
    instead of buffering it all in worker memory via PIPE.
    Returns the command's return code."""
    with open(log_file, "a") as log_fh:
        process = subprocess.Popen(
            command,
            stdout=log_fh,
            stderr=subprocess.STDOUT,
            shell=True,
            close_fds=True,
            start_new_session=True,  # detach so the child is killed cleanly on timeout
        )
        process.wait()

    return process.returncode


def setup_logger(name: str, log_file: str, level: int = logging.INFO) -> logging.Logger:
    """Setup logger — avoids duplicate handlers when called multiple times
    with the same name (e.g. periodic Celery task invocations)."""
    log_file_folder = os.path.dirname(log_file)
    if log_file_folder and not os.path.exists(log_file_folder):
        os.makedirs(log_file_folder)

    logger = logging.getLogger(name)
    logger.setLevel(level)

    existing_paths = {os.path.abspath(h.baseFilename) for h in logger.handlers if isinstance(h, logging.FileHandler)}
    if os.path.abspath(log_file) not in existing_paths:
        handler = logging.FileHandler(log_file)
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        logger.addHandler(handler)

    return logger


def mark_source_done(source_name: str) -> None:
    """Write today's completion marker for a finished source chain.

    Called when a chain ends — whatever its result (success, failure or config error),
    so the union task can tell that the source will not produce anything new today.
    """
    marker_dir = os.path.join(_PROGRESS_ROOT, dt.date.today().isoformat())
    os.makedirs(marker_dir, exist_ok=True)
    with open(os.path.join(marker_dir, source_name), "w") as marker:
        marker.write(dt.datetime.now().isoformat(timespec="seconds"))


def get_pending_sources(sources: list[str]) -> list[str]:
    """Return the sources from *sources* with no completion marker for today, sorted by name."""
    marker_dir = os.path.join(_PROGRESS_ROOT, dt.date.today().isoformat())
    done = set(os.listdir(marker_dir)) if os.path.isdir(marker_dir) else set()
    return sorted(set(sources) - done)


def cleanup_stale_progress() -> None:
    """Remove the completion markers of previous days (today's markers are kept)."""
    if not os.path.isdir(_PROGRESS_ROOT):
        return
    today = dt.date.today().isoformat()
    for name in os.listdir(_PROGRESS_ROOT):
        path = os.path.join(_PROGRESS_ROOT, name)
        if name != today and os.path.isdir(path):
            shutil.rmtree(path)


def to_utc(hour: int, timezone: str = "Europe/Warsaw") -> int:
    """Convert local hour to UTC"""
    now = dt.datetime.now()
    local_datetime = dt.datetime(year=now.year, month=now.month, day=now.day, hour=hour)
    utc_datetime = pytz.timezone(timezone).localize(local_datetime).astimezone(pytz.UTC)
    return utc_datetime.hour


def get_memory_info() -> tuple[float, float, float]:
    """Return (used_gb, total_gb, percent) of system memory."""
    mem = psutil.virtual_memory()
    return mem.used / (1024**3), mem.total / (1024**3), mem.percent


def log_memory_usage(logger: logging.Logger, label: str) -> None:
    """Log memory usage in the format: label Memory: X out of Y GiB (Z %) used"""
    used_gb, total_gb, percent = get_memory_info()
    logger.info(f"{label} Memory: {used_gb:.2f} out of {total_gb:.2f} GiB ({percent:.2f} %) used")
