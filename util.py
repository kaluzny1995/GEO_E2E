import os
import glob
import logging
import pytz
import datetime as dt


def celerybeat_cleanup() -> None:
    """ Celery beat cleanup before app start """
    for f in glob.glob("celerybeat-schedule*"):
        os.remove(f)


def setup_logger(name: str, log_file: str, level: int = logging.INFO) -> logging.Logger:
    """ Setup logger — avoids duplicate handlers when called multiple times
        with the same name (e.g. periodic Celery task invocations). """
    log_file_folder = os.path.dirname(log_file)
    if log_file_folder and not os.path.exists(log_file_folder):
        os.makedirs(log_file_folder)

    logger = logging.getLogger(name)
    logger.setLevel(level)

    existing_paths = {
        os.path.abspath(h.baseFilename)
        for h in logger.handlers
        if isinstance(h, logging.FileHandler)
    }
    if os.path.abspath(log_file) not in existing_paths:
        handler = logging.FileHandler(log_file)
        handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
        logger.addHandler(handler)

    return logger


def to_utc(hour: int, timezone: str = "Europe/Warsaw") -> int:
    """ Convert local hour to UTC """
    now = dt.datetime.now()
    local_datetime = dt.datetime(year=now.year, month=now.month, day=now.day, hour=hour)
    utc_datetime = pytz.timezone(timezone).localize(local_datetime).astimezone(pytz.UTC)
    return utc_datetime.hour
