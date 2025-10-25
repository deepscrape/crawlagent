# celery_config.py - Celery configuration file
import os

from celery.schedules import crontab
from kombu import Queue

# --------------------
# Broker/backend config
# --------------------
# Select broker via environment: 'redis' or 'lavinmq'
BROKER_TYPE = os.getenv("CELERY_BROKER_TYPE", "redis")

if BROKER_TYPE == "redis":
    BROKER_URL = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
    RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", BROKER_URL)
    broker_transport_options = {
        "max_retries": int(os.getenv("REDIS_MAX_RETRIES", "100")),
        "retry_delay": int(os.getenv("REDIS_RETRY_DELAY_SEC", "3")),
        "visibility_timeout": 7200,
    }
else:
    # LavinMQ example: 'amqp://guest:guest@localhost:5672//'
    BROKER_URL = os.getenv("CELERY_BROKER_URL", "amqp://guest:guest@localhost:5672//")
    RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", "rpc://")
    broker_pool_limit = int(os.getenv("LAVINMQ_POOL_LIMIT", "1"))
    broker_heartbeat = os.getenv("LAVINMQ_HEARTBEAT", None)
    broker_connection_timeout = int(os.getenv("LAVINMQ_CONN_TIMEOUT", "30"))
    event_queue_expires = int(os.getenv("LAVINMQ_EVENT_QUEUE_EXPIRES", "60"))

# --------------------
# Worker settings
# --------------------
worker_concurrency = int(os.getenv("CELERY_WORKER_CONCURRENCY", "4"))
worker_prefetch_multiplier = int(os.getenv("CELERY_PREFETCH_MULTIPLIER", "4"))
task_time_limit = int(os.getenv("CELERY_TASK_TIME_LIMIT", "600"))
task_soft_time_limit = int(os.getenv("CELERY_TASK_SOFT_TIME_LIMIT", "540"))

# --------------------
# Serialization
# --------------------
accept_content = ["json"]
task_serializer = "json"
result_serializer = "json"

# --------------------
# Results
# --------------------
result_expires = int(os.getenv("CELERY_RESULT_EXPIRES", "3600"))
result_extended = True

# --------------------
# Monitoring/events
# --------------------
worker_send_task_events = True
task_send_sent_event = True
worker_enable_remote_control = True

# --------------------
# Logging
# --------------------
worker_log_format = "%(asctime)s %(levelname)s %(processName)s %(message)s"
worker_task_log_format = "%(asctime)s %(levelname)s %(taskName)s %(message)s"
worker_redirect_stdouts_level = "INFO"

# --------------------
# Queues and routing
# --------------------
default_queue = "default"
queues = (
    Queue("default"),
    Queue("priority", routing_key="priority.#"),
    Queue("low", routing_key="low.#"),
)
task_default_queue = default_queue
task_default_exchange = "tasks"
task_default_routing_key = "default"

# --------------------
# Retry and reliability
# --------------------
task_acks_late = True
task_reject_on_worker_lost = True
task_default_retry_delay = int(os.getenv("CELERY_TASK_DEFAULT_RETRY_DELAY", "10"))
max_retries = int(os.getenv("CELERY_MAX_RETRIES", "5"))

# --------------------
# Prometheus exporter (optional)
# --------------------
prometheus_enabled = os.getenv("PROMETHEUS_ENABLED", "true").lower() == "true"
prometheus_port = int(os.getenv("PROMETHEUS_PORT", "9808"))
prometheus_host = os.getenv("PROMETHEUS_HOST", "0.0.0.0")


# --------------------
# Periodic tasks (example)
# --------------------
beat_schedule = {
    "cleanup-stale-sessions": {
        "task": "tasks.cleanup_stale_sessions",
        "schedule": crontab(minute="*/15"),
    },
}

# --------------------
# End of celery_config.py
# --------------------