"""Unit tests for the helper functions defined in util.py."""

from unittest.mock import MagicMock, patch

import pytest

import util


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


@pytest.mark.parametrize(
    ("usage", "label", "expected"),
    [
        pytest.param(
            (3.21, 15.54, 20.66),
            "Worker",
            "Worker Memory: 3.21 out of 15.54 GiB (20.66 %) used",
            id="worker",
        ),
        pytest.param(
            (1.50, 8.00, 18.75),
            "Subprocess",
            "Subprocess Memory: 1.50 out of 8.00 GiB (18.75 %) used",
            id="subprocess",
        ),
    ],
)
@patch("util.get_memory_info")
def test_log_memory_usage(
    mock_get_memory: MagicMock, usage: tuple[float, float, float], label: str, expected: str
) -> None:
    """Memory usage is written to the given logger as `<label> Memory: <used> out of
    <total> GiB (<percent> %) used` — the label is part of the logged line."""
    # Arrange
    mock_get_memory.return_value = usage
    logger = MagicMock()

    # Act
    util.log_memory_usage(logger, label)

    # Assert
    logger.info.assert_called_once_with(expected)
