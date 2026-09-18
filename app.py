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

# --- Resource limits to prevent memory exhaustion ---
s = config.celery_settings
geo_app.conf.update(
    # Task execution limits (seconds)
    task_time_limit=s.task_time_limit,                 # hard kill after N minutes
    task_soft_time_limit=s.task_soft_time_limit,       # soft timeout (SoftTimeLimitExceeded) after N minutes

    # Worker process recycling — force-fork a fresh worker after N tasks
    worker_max_tasks_per_child=s.worker_max_tasks_per_child,      # recycle worker (clears leaked memory)
    worker_max_memory_per_child=s.worker_max_memory_per_child,    # per worker child (kilobytes)

    # Prefetch — only grab 1 task at a time so downloads don't pile up
    worker_prefetch_multiplier=s.worker_prefetch_multiplier,

    # Ack late so tasks are requeued if the worker crashes mid-download
    task_acks_late=s.task_acks_late,
)

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
    log_dir = f"logs/{source_name}"
    log_file = f"{log_dir}/celery_{source_name}_{dt.datetime.now().isoformat()}.log"
    logger = setup_logger(
        name=f"celery_download_{source_name}",
        log_file=log_file,
    )

    logger.info(f"---- Celery triggered download for {source_name} ----")

    command = f"{job_config.get_command_pattern_string()} {source_name}"
    logger.info(f"Running command: {command}")

    # Stream subprocess output directly to the log file instead of buffering
    # it all in worker memory via PIPE.
    with open(log_file, "a") as log_fh:
        process = subprocess.Popen(
            command,
            stdout=log_fh,
            stderr=subprocess.STDOUT,
            shell=True,
            close_fds=True,
            start_new_session=True,   # detach so the child is killed cleanly on timeout
        )
        process.wait()

    if process.returncode != 0:
        logger.warning(f"Download for {source_name} exited with return code {process.returncode} (likely nothing to download)")
    else:
        logger.info(f"Download completed successfully for {source_name}")
