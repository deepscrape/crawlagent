import logging
import os
import signal
import sys

from celery import Celery
from dotenv import load_dotenv

from celery_config import (
    BROKER_URL,
    RESULT_BACKEND,
    accept_content,
    beat_schedule,
    broker_transport_options,
    default_queue,
    max_retries,
    queues,
    result_expires,
    result_extended,
    result_serializer,
    task_acks_late,
    task_default_exchange,
    task_default_queue,
    task_default_retry_delay,
    task_default_routing_key,
    task_reject_on_worker_lost,
    task_send_sent_event,
    task_serializer,
    task_soft_time_limit,
    task_time_limit,
    worker_concurrency,
    worker_enable_remote_control,
    worker_log_format,
    worker_prefetch_multiplier,
    worker_redirect_stdouts_level,
    worker_send_task_events,
    worker_task_log_format,
)

production = os.getenv("PYTHON_ENV", "development").lower() == "production"
env_file = ".env" if production else "dev.env"
load_dotenv(env_file, verbose=True)

logger = logging.getLogger("crawlagent")

celery_app = Celery(
    "crawlagent",
    broker=BROKER_URL,
    backend=RESULT_BACKEND,
    include=["tasks", "backup_tasks"]  # Include your task modules here,
)

celery_app.conf.update(
    broker_transport_options=broker_transport_options if 'broker_transport_options' in locals() else {},
    worker_concurrency=worker_concurrency,
    worker_prefetch_multiplier=worker_prefetch_multiplier,
    task_time_limit=task_time_limit,
    task_soft_time_limit=task_soft_time_limit,
    accept_content=accept_content,
    task_serializer=task_serializer,
    result_serializer=result_serializer,
    result_expires=result_expires,
    result_extended=result_extended,
    worker_send_task_events=worker_send_task_events,
    task_send_sent_event=task_send_sent_event,
    worker_enable_remote_control=worker_enable_remote_control,
    worker_log_format=worker_log_format,
    worker_task_log_format=worker_task_log_format,
    worker_redirect_stdouts_level=worker_redirect_stdouts_level,
    task_acks_late=task_acks_late,
    task_reject_on_worker_lost=task_reject_on_worker_lost,
    task_default_retry_delay=task_default_retry_delay,
    max_retries=max_retries,
    task_default_queue=task_default_queue,
    task_default_exchange=task_default_exchange,
    task_default_routing_key=task_default_routing_key,
    task_queues=queues,
    beat_schedule=beat_schedule,
    timezone="UTC",
    enable_utc=True,
)

# Windows-specific signal handling
if os.name == 'nt':
    def windows_shutdown_handler(*args):
        logger.info("Windows shutdown signal received")
        celery_app.control.broadcast('shutdown')
        sys.exit(0)
    signal.signal(signal.SIGTERM, windows_shutdown_handler)
    signal.signal(signal.SIGINT, windows_shutdown_handler)

def graceful_shutdown(signum, frame):
    logging.info(f"Received signal {signum}, shutting down Celery worker gracefully...")
    from celery.worker import state
    state.should_stop = True

signal.signal(signal.SIGTERM, graceful_shutdown)
signal.signal(signal.SIGINT, graceful_shutdown)

if __name__ == "__main__":
    celery_app.start()
