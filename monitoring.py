# monitoring.py
import os

from fastapi import FastAPI
from prometheus_client import CollectorRegistry, make_asgi_app, multiprocess
from prometheus_fastapi_instrumentator import Instrumentator, metrics


def create_instrumentator():
    instr = Instrumentator(
        should_group_status_codes=True,
        should_ignore_untemplated=True,
        # should_respect_env_var=True,
        # env_var_name="ENABLE_METRICS",
        should_instrument_requests_inprogress=True,
        inprogress_name="inprogress",
        inprogress_labels=False,
        excluded_handlers=[r".*admin.*", r"/metrics", r"/health", r"/status"],
    )

    # add recommended metrics
    instr.add(metrics.latency(buckets=(0.005, 0.01, 0.025, 0.05, 0.1,
                                       0.25, 0.5, 1.0, 2.5, 5.0)))
    instr.add(metrics.request_size())
    instr.add(metrics.response_size())
    return instr


def _init_metrics_app(app: FastAPI, config: dict):
    prometheus_cfg = config.get("observability", {}).get("prometheus", {})
    metrics_enabled = prometheus_cfg.get("enabled", False)
    metrics_endpoint = prometheus_cfg.get("endpoint", "/metrics")

    if metrics_enabled:
        instrumentator = create_instrumentator().instrument(app)
        mp_dir = os.getenv("PROMETHEUS_MULTIPROC_DIR")

        if mp_dir:
            # multiprocess mode → must mount with trailing slash
            if not metrics_endpoint.endswith("/"):
                metrics_endpoint += "/"

            registry = CollectorRegistry()
            multiprocess.MultiProcessCollector(registry)
            metrics_app = make_asgi_app(registry=registry)
            app.mount(metrics_endpoint, metrics_app)
        else:
            # single-process → safe to expose directly (no redirect)
            instrumentator.expose(
                app,
                endpoint=metrics_endpoint,
                include_in_schema=False,
                should_gzip=True,
            )