"""Celery application: beat schedule built from config.json, the `geo_e2e` E2E chain task
and the `union_etl` fan-in task that runs after every source chain finished."""

import datetime as dt
import logging

import celery
from celery.schedules import crontab

from model import CeleryAppConfig, Job
from util import (
    celerybeat_cleanup,
    cleanup_stale_progress,
    get_pending_sources,
    log_memory_usage,
    mark_source_done,
    setup_logger,
    to_utc,
)

celerybeat_cleanup()

config = CeleryAppConfig.from_config()
geo_app = celery.Celery(config.name, broker=config.get_rabbitmq_broker_string())

# --- Resource limits to prevent memory exhaustion ---
s = config.celery_settings
geo_app.conf.update(
    # Task execution limits (seconds)
    task_time_limit=s.task_time_limit,  # hard kill after N minutes
    task_soft_time_limit=s.task_soft_time_limit,  # soft timeout (SoftTimeLimitExceeded) after N minutes
    # Worker process recycling — force-fork a fresh worker after N tasks
    worker_max_tasks_per_child=s.worker_max_tasks_per_child,  # recycle worker (clears leaked memory)
    worker_max_memory_per_child=s.worker_max_memory_per_child,  # per worker child (kilobytes)
    # Prefetch — only grab 1 task at a time so downloads don't pile up
    worker_prefetch_multiplier=s.worker_prefetch_multiplier,
    # Ack late so tasks are requeued if the worker crashes mid-download
    task_acks_late=s.task_acks_late,
)

geo_app.conf.beat_schedule = {
    task.name: {
        "task": "app.geo_e2e",
        "schedule": crontab(
            hour=to_utc(task.hour, task.timezone), minute=task.minute, day_of_month=task.day, month_of_year=task.month
        ),
        "args": [task.source_name],
    }
    for task in config.tasks
}

# The union fan-in — triggered after the source window and waits for every source chain to finish
geo_app.conf.beat_schedule[config.union.name] = {
    "task": "app.union_etl",
    "schedule": crontab(
        hour=to_utc(config.union.hour, config.union.timezone),
        minute=config.union.minute,
        day_of_month=config.union.day,
        month_of_year=config.union.month,
    ),
    "args": [0],
}

# jobs.json key of the chain the union task runs (a bare SINGLE_ETL job on union_etl)
UNION_JOB_NAME = "Union"

_startup_logger = logging.getLogger("geo_e2e.startup")
log_memory_usage(_startup_logger, "System")


@geo_app.task
def geo_e2e(source_name: str) -> None:
    """Run the E2E job chain (download and everything chained after it) for given source.

    Whatever the chain's result (success, failure or config error), the source is marked as
    done so the `union_etl` task knows when all sources finished and it may run the union job.
    """
    log_file = f"logs/{source_name}/celery_{source_name}_{dt.datetime.now().isoformat()}.log"
    logger = setup_logger(
        name=f"celery_e2e_{source_name}",
        log_file=log_file,
    )

    logger.info(f"---- Celery triggered E2E job chain for {source_name} ----")
    log_memory_usage(logger, "Worker")

    try:
        job = Job.from_config(source_name)
        job.run(logger=logger, log_file=log_file)
    finally:
        mark_source_done(source_name)

    log_memory_usage(logger, "Worker")


@geo_app.task
def union_etl(attempt: int = 0) -> None:
    """Run the union job once every source chain finished — whatever its result.

    Beat triggers this task after the source window; while sources are still pending it
    re-queues itself every `config.union.poll_seconds` (up to `config.union.max_attempts` times).
    After the deadline it logs a warning and runs the union job anyway, so one stuck or
    missing source cannot block the daily union forever.
    """
    today = dt.date.today().isoformat()
    log_file = f"logs/Union/celery_Union_{today}.log"
    logger = setup_logger(name=f"celery_union_{today}", log_file=log_file)

    logger.info("---- Celery triggered union E2E job chain ----")
    log_memory_usage(logger, "Worker")

    cleanup_stale_progress()
    pending = get_pending_sources([task.source_name for task in config.tasks])
    if pending:
        if attempt < config.union.max_attempts:
            logger.info(
                f"Waiting for {len(pending)} source chain(s) to finish: {', '.join(pending)} — "
                f"checking again in {config.union.poll_seconds}s "
                f"(attempt {attempt + 1} of {config.union.max_attempts})"
            )
            union_etl.apply_async(args=[attempt + 1], countdown=config.union.poll_seconds)
            return
        logger.warning(
            f"Deadline reached — {len(pending)} source chain(s) never finished: {', '.join(pending)} — "
            "running the union job anyway"
        )

    job = Job.from_config(UNION_JOB_NAME)
    job.run(logger=logger, log_file=log_file)

    log_memory_usage(logger, "Worker")
