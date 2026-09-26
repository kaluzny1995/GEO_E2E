""" Celery application: beat schedule built from config.json and the `geo_e2e` E2E chain task. """
import logging
import datetime as dt

import celery
from celery.schedules import crontab

from util import celerybeat_cleanup, setup_logger, to_utc, log_memory_usage
from model import CeleryAppConfig, Job

celerybeat_cleanup()

config = CeleryAppConfig.from_config()
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
        "task": "app.geo_e2e",
        "schedule": crontab(hour=to_utc(task.hour, task.timezone), minute=task.minute,
                            day_of_month=task.day, month_of_year=task.month),
        "args": [task.source_name],
    }
    for task in config.tasks
}

_startup_logger = logging.getLogger("geo_e2e.startup")
log_memory_usage(_startup_logger, "System")


@geo_app.task
def geo_e2e(source_name: str) -> None:
    """ Run the E2E job chain (download and everything chained after it) for given source """
    log_file = f"logs/{source_name}/celery_{source_name}_{dt.datetime.now().isoformat()}.log"
    logger = setup_logger(
        name=f"celery_e2e_{source_name}",
        log_file=log_file,
    )

    logger.info(f"---- Celery triggered E2E job chain for {source_name} ----")
    log_memory_usage(logger, "Worker")

    job = Job.from_config(source_name)
    job.run(logger=logger, log_file=log_file)

    log_memory_usage(logger, "Worker")
