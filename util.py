"""Shared helpers: system, process and cgroup memory reporting for Airflow task callbacks."""

import logging
from pathlib import Path

import psutil

_MIB = 1024**2

# cgroup v2 files used by Docker's per-container memory accounting.
_CGROUP_ROOT = Path("/sys/fs/cgroup")


def get_memory_info() -> tuple[float, float, float]:
    """Return (used_gb, total_gb, percent) of system memory."""
    mem = psutil.virtual_memory()
    # 1024**3 = bytes -> GiB, the unit used in the log message
    return mem.used / (1024**3), mem.total / (1024**3), mem.percent


def get_host_memory_detail() -> tuple[float, float]:
    """Return (available, buff/cache) host memory in GiB.

    ``free``'s "used" percentage moves with the page cache even when anonymous
    process memory is flat. Logging available and buff/cache next to the process
    and cgroup figures makes that difference visible in the task log.
    """
    mem = psutil.virtual_memory()
    return mem.available / (1024**3), (mem.cached + mem.buffers) / (1024**3)


def get_process_memory() -> tuple[float, float]:
    """Return (own RSS, children RSS) of the current Python process, in MiB.

    The task callback runs in the Airflow scheduler process, whose children
    are the LocalExecutor worker pool and the running task subprocess. Logging
    both numbers separates a leak in the orchestrator from one in a task
    (e.g. a PySpark driver) or from an unrelated host process.
    """
    process = psutil.Process()
    own = process.memory_info().rss
    children = sum(child.memory_info().rss for child in process.children(recursive=True))
    return own / _MIB, children / _MIB


def get_cgroup_memory(cgroup_root: Path = _CGROUP_ROOT) -> tuple[float, float, float | None] | None:
    """Return (current, peak, limit) of the current cgroup in MiB, or None.

    Docker mounts the container's own cgroup at ``/sys/fs/cgroup``, so this
    reads the memory of the whole container rather than of one process. The
    limit is ``None`` when the cgroup is unlimited (``memory.max`` = ``max``).
    Returns ``None`` outside cgroup v2 or when the files are unreadable.
    """
    try:
        current = int((cgroup_root / "memory.current").read_text().strip())
    except (OSError, ValueError):
        return None
    try:
        peak = int((cgroup_root / "memory.peak").read_text().strip())
    except (OSError, ValueError):
        # memory.peak was added in Linux 5.19; older kernels still expose the
        # current usage, so report the current value as the known peak.
        peak = current
    try:
        raw_limit = (cgroup_root / "memory.max").read_text().strip()
        limit = None if raw_limit == "max" else int(raw_limit)
    except (OSError, ValueError):
        limit = None
    return current / _MIB, peak / _MIB, None if limit is None else limit / _MIB


def log_memory_usage(logger: logging.Logger, label: str) -> None:
    """Log memory usage in the format: label Memory: X out of Y GiB (Z %) used"""
    used_gb, total_gb, percent = get_memory_info()
    logger.info(f"{label} Memory: {used_gb:.2f} out of {total_gb:.2f} GiB ({percent:.2f} %) used")


def log_memory_breakdown(logger: logging.Logger, label: str) -> None:
    """Log where the host memory is held: process, container and host cache.

    Paired with :func:`log_memory_usage`, this line makes a leak attributable:
    host memory can rise because of a task process, the Airflow container, a
    completely unrelated desktop process or ordinary page cache. The Airflow
    task log cannot see the last two, which is why the host/cgroup split and
    the available/cache figures matter.
    """
    parts: list[str] = []
    try:
        own_mib, children_mib = get_process_memory()
        parts.append(f"{own_mib:.1f} MiB RSS, {children_mib:.1f} MiB children")
    except (psutil.Error, OSError):
        pass
    try:
        cgroup = get_cgroup_memory()
    except (psutil.Error, OSError):
        cgroup = None
    if cgroup is not None:
        current_mib, peak_mib, limit_mib = cgroup
        limit_text = "unlimited" if limit_mib is None else f"{limit_mib:.1f} MiB"
        parts.append(f"cgroup {current_mib:.1f}/{limit_text} (peak {peak_mib:.1f} MiB)")
    try:
        available_gib, cached_gib = get_host_memory_detail()
        parts.append(f"host available {available_gib:.1f} GiB, cache {cached_gib:.1f} GiB")
    except (psutil.Error, OSError):
        pass
    if not parts:
        return
    logger.info(f"{label} Memory breakdown: {'; '.join(parts)}")
