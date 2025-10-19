from prometheus_client import Counter, Gauge, Histogram

# Define metrics
QUEUE_SIZE = Gauge("scraper_queue_size", "Current operation queue size")
CPU_USAGE = Gauge("scraper_cpu_usage", "CPU usage percentage")
MEMORY_USAGE = Gauge("scraper_memory_usage", "Memory usage percentage")
OPERATION_DURATION = Histogram(
    "scraper_operation_duration_seconds", "Time spent on scraping operations"
)
ERROR_COUNTER = Counter("scraper_errors_total", "Total number of scraping errors")
SUCCESS_COUNTER = Counter("scraper_success_total", "Total number of successful scrapes")



# Define metrics
WEBSOCKET_CONNECTIONS = Gauge(
    'websocket_connections_total', 
    'Number of active WebSocket connections'
)
WEBSOCKET_MESSAGES_RECEIVED = Counter(
    'websocket_messages_received_total', 
    'Number of WebSocket messages received'
)
WEBSOCKET_MESSAGES_SENT = Counter(
    'websocket_messages_sent_total', 
    'Number of WebSocket messages sent'
)