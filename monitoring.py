# monitoring.py
import logging
import os
import uuid
from datetime import datetime
from typing import Any, Dict, Optional

from fastapi import FastAPI, WebSocket
from prometheus_client import CollectorRegistry, make_asgi_app, multiprocess
from prometheus_fastapi_instrumentator import Instrumentator, metrics

from grafana import WEBSOCKET_CONNECTIONS, WEBSOCKET_MESSAGES_RECEIVED, WEBSOCKET_MESSAGES_SENT

logger = logging.getLogger("crawlagent")

# Advanced WebSocket Monitoring
# Per-Connection Metrics
class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, Dict] = {}
        self.connection_stats: Dict = {
            "total_connections": 0,
            "active_connections": 0,
            "messages_received": 0,
            "messages_sent": 0,
        }
        
    async def connect(self, websocket: WebSocket, client_id: str, session_id: Optional[str] = None, metadata: Optional[dict] = None):
        await websocket.accept()
        connection_id = str(uuid.uuid4())
        
        # Initialize connection data
        connection_data = {
            "websocket": websocket,
            "client_id": client_id,
            "connected_at": datetime.now(),
            "messages_received": 0,
            "messages_sent": 0,
            "last_activity": datetime.now(),
        }
        
        # Add session_id if provided
        if session_id:
            connection_data["session_id"] = session_id
            
        # Add any additional metadata
        if metadata:
            connection_data.update(metadata)
            
        self.active_connections[connection_id] = connection_data
        self.connection_stats["total_connections"] += 1
        self.connection_stats["active_connections"] += 1
        WEBSOCKET_CONNECTIONS.inc()
        return connection_id
        
    def disconnect(self, connection_id: str):
        if connection_id in self.active_connections:
            del self.active_connections[connection_id]
            self.connection_stats["active_connections"] -= 1
            WEBSOCKET_CONNECTIONS.dec()
        
    async def send_personal_message(self, message: Any, connection_id: str):
        if connection_id in self.active_connections:
            await self.active_connections[connection_id]["websocket"].send_json(message)
            self.active_connections[connection_id]["messages_sent"] += 1
            self.active_connections[connection_id]["last_activity"] = datetime.now()
            self.connection_stats["messages_sent"] += 1
            WEBSOCKET_MESSAGES_SENT.inc()
        
    async def broadcast(self, message: Any):
        for _, connection in self.active_connections.items():
            await connection["websocket"].send_json(message)
            connection["messages_sent"] += 1
            connection["last_activity"] = datetime.now()
            self.connection_stats["messages_sent"] += 1
    def record_message_received(self, connection_id: str):
        if connection_id in self.active_connections:
            self.active_connections[connection_id]["messages_received"] += 1
            self.active_connections[connection_id]["last_activity"] = datetime.now()
            self.connection_stats["messages_received"] += 1
            WEBSOCKET_MESSAGES_RECEIVED.inc()
            
    def get_connection_by_session_id(self, session_id: str):
        """Get a connection by its associated session ID"""
        for conn_id, conn in self.active_connections.items():
            if conn.get("session_id") == session_id:
                return conn_id, conn
        return None, None
        
    def get_websocket_by_session_id(self, session_id: str) -> Optional[WebSocket]:
        """Get the WebSocket object for a given session ID"""
        _, conn = self.get_connection_by_session_id(session_id)
        if conn:
            return conn.get("websocket", None)
        return None
        
    async def send_message_by_session_id(self, message: Any, session_id: str) -> bool:
        """Send a message to a WebSocket by session ID instead of connection ID"""
        conn_id, conn = self.get_connection_by_session_id(session_id)
        if conn_id and conn:
            await self.send_personal_message(message, conn_id)
            return True
        return False
    
    def get_stats(self):
        connection_details = []
        for conn_id, conn in self.active_connections.items():
            connection_details.append({
                "connection_id": conn_id,
                "client_id": conn["client_id"],
                "connected_at": conn["connected_at"].isoformat(),
                "messages_received": conn["messages_received"],
                "messages_sent": conn["messages_sent"],
                "last_activity": conn["last_activity"].isoformat(),
                "duration": (datetime.now() - conn["connected_at"]).total_seconds()
            })
            
        return {
            "global_stats": self.connection_stats,
            "connections": connection_details
        }
manager = ConnectionManager()



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


def init_metrics_app(app: FastAPI, config: dict):
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