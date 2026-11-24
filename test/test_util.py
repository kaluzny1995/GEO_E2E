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


@patch("util.get_host_memory_detail", return_value=(40.5, 30.0))
@patch("util.get_cgroup_memory")
@patch("util.get_process_memory")
def test_log_memory_breakdown_attributes_process_children_and_cgroup(
    mock_process_memory: MagicMock,
    mock_cgroup_memory: MagicMock,
    _mock_host_memory: MagicMock,
) -> None:
    """The breakdown line splits host memory into process, container, cache and available."""
    # Arrange
    mock_process_memory.return_value = (123.4, 567.8)
    mock_cgroup_memory.return_value = (690.1, 2048.0, 4096.0)
    logger = MagicMock()

    # Act
    util.log_memory_breakdown(logger, "NIFC.download")

    # Assert
    logger.info.assert_called_once_with(
        "NIFC.download Memory breakdown: 123.4 MiB RSS, 567.8 MiB children; "
        "cgroup 690.1/4096.0 MiB (peak 2048.0 MiB); "
        "host available 40.5 GiB, cache 30.0 GiB"
    )


@patch("util.get_host_memory_detail", return_value=(40.5, 30.0))
@patch("util.get_cgroup_memory", return_value=None)
@patch("util.get_process_memory")
def test_log_memory_breakdown_omits_unavailable_cgroup(
    mock_process_memory: MagicMock,
    _mock_cgroup_memory: MagicMock,
    _mock_host_memory: MagicMock,
) -> None:
    """Outside a cgroup (or without cgroup v2 files) process and host figures remain."""
    # Arrange
    mock_process_memory.return_value = (10.0, 20.0)
    logger = MagicMock()

    # Act
    util.log_memory_breakdown(logger, "Union.verdict")

    # Assert
    logger.info.assert_called_once_with(
        "Union.verdict Memory breakdown: 10.0 MiB RSS, 20.0 MiB children; host available 40.5 GiB, cache 30.0 GiB"
    )


@patch("util.get_host_memory_detail", side_effect=OSError("proc unavailable"))
@patch("util.get_process_memory", side_effect=OSError("proc unavailable"))
@patch("util.get_cgroup_memory", return_value=None)
def test_log_memory_breakdown_is_silent_without_any_measurement(
    _mock_cgroup_memory: MagicMock,
    _mock_process_memory: MagicMock,
    _mock_host_memory: MagicMock,
) -> None:
    """Unreadable memory sources skip the line instead of failing the task callback."""
    # Arrange
    logger = MagicMock()

    # Act
    util.log_memory_breakdown(logger, "NIFC.download")

    # Assert
    logger.info.assert_not_called()


@patch("util.psutil.virtual_memory")
def test_get_host_memory_detail_returns_available_and_cache(mock_vmem: MagicMock) -> None:
    """Available and cached bytes are converted to GiB."""
    # Arrange
    mock_vmem.return_value = MagicMock(available=8 * 1024**3, cached=3 * 1024**3, buffers=1 * 1024**3)

    # Act
    available_gib, cached_gib = util.get_host_memory_detail()

    # Assert
    assert (available_gib, cached_gib) == (8.0, 4.0)


def test_get_cgroup_memory_reads_current_peak_and_limit(tmp_path) -> None:
    """cgroup v2 memory files are converted from bytes to MiB."""
    # Arrange
    (tmp_path / "memory.current").write_text(str(512 * 1024**2))
    (tmp_path / "memory.peak").write_text(str(1024 * 1024**2))
    (tmp_path / "memory.max").write_text(str(4096 * 1024**2))

    # Act
    memory = util.get_cgroup_memory(tmp_path)

    # Assert
    assert memory is not None
    assert memory == (512.0, 1024.0, 4096.0)


def test_get_cgroup_memory_handles_unlimited_memory(tmp_path) -> None:
    """A cgroup without a hard limit reports None as the limit."""
    # Arrange
    (tmp_path / "memory.current").write_text(str(64 * 1024**2))
    (tmp_path / "memory.peak").write_text(str(128 * 1024**2))
    (tmp_path / "memory.max").write_text("max")

    # Act
    memory = util.get_cgroup_memory(tmp_path)

    # Assert
    assert memory is not None
    assert memory == (64.0, 128.0, None)


def test_get_cgroup_memory_returns_none_without_cgroup_v2(tmp_path) -> None:
    """Missing memory files (cgroup v1, non-Linux) are reported as unavailable."""
    assert util.get_cgroup_memory(tmp_path) is None


def test_get_cgroup_memory_falls_back_when_peak_is_missing(tmp_path) -> None:
    """Kernels before 5.19 expose memory.current but not memory.peak."""
    # Arrange
    (tmp_path / "memory.current").write_text(str(256 * 1024**2))
    (tmp_path / "memory.max").write_text(str(1024 * 1024**2))

    # Act
    memory = util.get_cgroup_memory(tmp_path)

    # Assert
    assert memory == (256.0, 256.0, 1024.0)
