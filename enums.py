from enum import Enum


class TaskStatus(str, Enum):
    READY = "Ready"
    STARTED = "Started"
    SCHEDULED = "Scheduled"
    IN_PROGRESS = "In Progress"
    PENDING = "Pending"
    CANCELED = "Canceled"
    REVOKED = "Revoked"
    RETRY = "Retry"
    COMPLETED = "Completed"
    FAILED = "Failed"

class CeleryTaskStatus(str, Enum):
    PENDING = "PENDING"
    STARTED = "STARTED"
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    RETRY = "RETRY"
    REVOKED = "REVOKED"

class FilterType(str, Enum):
    RAW = "raw"
    FIT = "fit"
    BM25 = "bm25"
    LLM = "llm"