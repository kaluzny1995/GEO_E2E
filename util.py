"""Shared helpers: system memory reporting used as an Airflow task callback."""

import logging

import psutil


def get_memory_info() -> tuple[float, float, float]:
    """Return (used_gb, total_gb, percent) of system memory."""
    mem = psutil.virtual_memory()
    return mem.used / (1024**3), mem.total / (1024**3), mem.percent


def log_memory_usage(logger: logging.Logger, label: str) -> None:
    """Log memory usage in the format: label Memory: X out of Y GiB (Z %) used"""
    used_gb, total_gb, percent = get_memory_info()
    logger.info(f"{label} Memory: {used_gb:.2f} out of {total_gb:.2f} GiB ({percent:.2f} %) used")
