"""Unit tests for the helper functions defined in util.py."""

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import util


def test_to_utc_warsaw() -> None:
    """A local hour converts to UTC using the Warsaw offset (CEST = UTC+2)."""
    # Arrange
    # Act
    result = util.to_utc(10, "Europe/Warsaw")

    # Assert
    assert result == 8  # CEST (summer) = UTC+2


def test_to_utc_warsaw_midnight() -> None:
    """Midnight local time maps to 22:00 UTC."""
    # Arrange
    # Act
    result = util.to_utc(0, "Europe/Warsaw")

    # Assert
    assert result == 22  # CEST (summer) = UTC+2


@pytest.mark.parametrize(
    "hour, timezone, expected",
    [
        (10, "Europe/Warsaw", 8),  # CEST (summer) = UTC+2
        (0, "Europe/Warsaw", 22),  # CEST (summer) = UTC+2
        (12, "US/Eastern", 16),  # EDT (summer) = UTC-4
        (0, "Asia/Tokyo", 15),  # JST = UTC+9 (no DST)
    ],
)
def test_to_utc_parametrized(hour: int, timezone: str, expected: int) -> None:
    """to_utc() is correct across different time zones."""
    # Arrange
    # Act
    result = util.to_utc(hour, timezone)

    # Assert
    assert result == expected


def test_celerybeat_cleanup_removes_files(tmp_path: Path) -> None:
    """Every celerybeat-schedule* file is removed from the current directory."""
    # Arrange
    (tmp_path / "celerybeat-schedule.bak").touch()
    (tmp_path / "celerybeat-schedule.dat").touch()
    (tmp_path / "celerybeat-schedule.dir").touch()

    original_cwd = os.getcwd()
    os.chdir(tmp_path)
    try:
        # Act
        util.celerybeat_cleanup()

        # Assert
        assert not (tmp_path / "celerybeat-schedule.bak").exists()
        assert not (tmp_path / "celerybeat-schedule.dat").exists()
        assert not (tmp_path / "celerybeat-schedule.dir").exists()
    finally:
        os.chdir(original_cwd)


def test_celerybeat_cleanup_no_files(tmp_path: Path) -> None:
    """Cleanup is a no-op when no schedule files exist."""
    # Arrange
    original_cwd = os.getcwd()
    os.chdir(tmp_path)
    try:
        # Act & Assert (should not raise)
        util.celerybeat_cleanup()
    finally:
        os.chdir(original_cwd)


def test_setup_logger_creates_log_file(tmp_path: Path) -> None:
    """setup_logger() creates the log file and returns an INFO logger."""
    # Arrange
    log_file = str(tmp_path / "logs" / "test.log")

    # Act
    logger = util.setup_logger(name="test_logger", log_file=log_file)

    # Assert
    assert os.path.exists(log_file)
    assert logger.name == "test_logger"
    assert logger.level == 20  # logging.INFO


def test_setup_logger_creates_nested_directory(tmp_path: Path) -> None:
    """Missing parent directories of the log file are created."""
    # Arrange
    log_file = str(tmp_path / "a" / "b" / "c" / "test.log")

    # Act
    util.setup_logger(name="test_nested", log_file=log_file)

    # Assert
    assert os.path.exists(log_file)


def test_get_memory_info() -> None:
    """get_memory_info() returns sane (used, total, percent) values."""
    # Arrange
    # Act
    used_gb, total_gb, percent = util.get_memory_info()

    # Assert
    assert total_gb > 0
    assert used_gb > 0
    assert used_gb <= total_gb
    assert 0 <= percent <= 100


def test_get_memory_info_returns_floats() -> None:
    """Every value returned by get_memory_info() is a float."""
    # Arrange
    # Act
    used_gb, total_gb, percent = util.get_memory_info()

    # Assert
    assert isinstance(used_gb, float)
    assert isinstance(total_gb, float)
    assert isinstance(percent, float)


@patch("util.psutil.virtual_memory")
def test_get_memory_info_mocked(mock_vmem: MagicMock) -> None:
    """Values are read from psutil.virtual_memory()."""
    # Arrange
    mock_vmem.return_value = MagicMock(used=8 * 1024**3, total=16 * 1024**3, percent=50.0)

    # Act
    used_gb, total_gb, percent = util.get_memory_info()

    # Assert
    assert used_gb == 8.0
    assert total_gb == 16.0
    assert percent == 50.0


@patch("util.get_memory_info")
def test_log_memory_usage(mock_get_memory: MagicMock, tmp_path: Path) -> None:
    """Memory usage is written to the log file in the expected format."""
    # Arrange
    mock_get_memory.return_value = (3.21, 15.54, 20.66)
    log_file = str(tmp_path / "test.log")
    logger = util.setup_logger(name="test_log_memory", log_file=log_file)

    # Act
    util.log_memory_usage(logger, "Worker")

    # Assert
    with open(log_file) as f:
        content = f.read()
    assert "Worker Memory: 3.21 out of 15.54 GiB (20.66 %) used" in content


@patch("util.get_memory_info")
def test_log_memory_usage_subprocess_label(mock_get_memory: MagicMock, tmp_path: Path) -> None:
    """The label appears in the logged line (e.g. 'Subprocess Memory: ...')."""
    # Arrange
    mock_get_memory.return_value = (1.50, 8.00, 18.75)
    log_file = str(tmp_path / "test2.log")
    logger = util.setup_logger(name="test_log_memory_sub", log_file=log_file)

    # Act
    util.log_memory_usage(logger, "Subprocess")

    # Assert
    with open(log_file) as f:
        content = f.read()
    assert "Subprocess Memory: 1.50 out of 8.00 GiB (18.75 %) used" in content
