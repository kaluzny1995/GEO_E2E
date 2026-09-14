import os
import pytest
import datetime as dt

import util


def test_to_utc_warsaw():
    # Arrange
    # Act
    result = util.to_utc(10, "Europe/Warsaw")

    # Assert
    assert result == 8  # CEST (summer) = UTC+2


def test_to_utc_warsaw_midnight():
    # Arrange
    # Act
    result = util.to_utc(0, "Europe/Warsaw")

    # Assert
    assert result == 22  # CEST (summer) = UTC+2


@pytest.mark.parametrize("hour, timezone, expected", [
    (10, "Europe/Warsaw", 8),   # CEST (summer) = UTC+2
    (0, "Europe/Warsaw", 22),   # CEST (summer) = UTC+2
    (12, "US/Eastern", 16),     # EDT (summer) = UTC-4
    (0, "Asia/Tokyo", 15),      # JST = UTC+9 (no DST)
])
def test_to_utc_parametrized(hour, timezone, expected):
    # Arrange
    # Act
    result = util.to_utc(hour, timezone)

    # Assert
    assert result == expected


def test_celerybeat_cleanup_removes_files(tmp_path):
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


def test_celerybeat_cleanup_no_files(tmp_path):
    # Arrange
    original_cwd = os.getcwd()
    os.chdir(tmp_path)
    try:
        # Act & Assert (should not raise)
        util.celerybeat_cleanup()
    finally:
        os.chdir(original_cwd)


def test_setup_logger_creates_log_file(tmp_path):
    # Arrange
    log_file = str(tmp_path / "logs" / "test.log")

    # Act
    logger = util.setup_logger(name="test_logger", log_file=log_file)

    # Assert
    assert os.path.exists(log_file)
    assert logger.name == "test_logger"
    assert logger.level == 20  # logging.INFO


def test_setup_logger_creates_nested_directory(tmp_path):
    # Arrange
    log_file = str(tmp_path / "a" / "b" / "c" / "test.log")

    # Act
    util.setup_logger(name="test_nested", log_file=log_file)

    # Assert
    assert os.path.exists(log_file)
