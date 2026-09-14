import os
import contextlib
import logging
import pytz
import datetime as dt


def celerybeat_cleanup() -> None:
    """ Celery beat cleanup before app start """
    with contextlib.suppress(FileNotFoundError):
        os.remove("celerybeat-schedule.bak")
        os.remove("celerybeat-schedule.dat")
        os.remove("celerybeat-schedule.dir")


def setup_logger(name: str, log_file: str, level: int = logging.INFO) -> logging.Logger:
    """ Setup logger """
    log_file_folder = os.path.dirname(log_file)
    if not os.path.exists(log_file_folder):
        os.makedirs(log_file_folder)

    handler = logging.FileHandler(log_file)
    handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger


def to_utc(hour: int, timezone: str = "Europe/Warsaw") -> int:
    """ Convert local hour to UTC """
    now = dt.datetime.now()
    local_datetime = dt.datetime(year=now.year, month=now.month, day=now.day, hour=hour)
    utc_datetime = pytz.timezone(timezone).localize(local_datetime).astimezone(pytz.UTC)
    return utc_datetime.hour
