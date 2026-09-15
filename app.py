import subprocess
import datetime as dt

import celery
from celery.schedules import crontab

from util import celerybeat_cleanup, setup_logger, to_utc
from model import CeleryAppConfig, get_job_config

celerybeat_cleanup()

config = CeleryAppConfig.from_config()
job_config = get_job_config()
geo_app = celery.Celery(config.name, broker=config.get_rabbitmq_broker_string())

geo_app.conf.beat_schedule = {
    task.name: {
        "task": "app.geo_download",
        "schedule": crontab(hour=to_utc(task.hour, task.timezone), minute=task.minute,
                            day_of_month=task.day, month_of_year=task.month),
        "args": [task.source_name],
    }
    for task in config.tasks
}


@geo_app.task
def geo_download(source_name: str) -> None:
    """ Download geo data for given source """
    logger = setup_logger(
        name=f"celery_download_{source_name}",
        log_file=f"logs/{source_name}/celery_{source_name}_{dt.datetime.now().isoformat()}.log"
    )

    logger.info(f"---- Celery triggered download for {source_name} ----")

    command = f"{job_config.get_command_pattern_string()} {source_name}"
    logger.info(f"Running command: {command}")

    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    stdout, stderr = process.communicate()

    if stdout:
        logger.info(stdout.decode("utf-8"))
    if stderr:
        logger.error(stderr.decode("utf-8"))

    if process.returncode != 0:
        logger.error(f"Download failed for {source_name} with return code {process.returncode}")
        raise subprocess.CalledProcessError(returncode=process.returncode, cmd=command)

    logger.info(f"Download completed successfully for {source_name}")
