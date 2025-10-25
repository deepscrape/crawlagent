"""
Utils package for CrawlerAI.

This package contains utility modules for server, WebRTC and WSL functionality.
Provides centralized access to all utility functions from various modules.

Categories:
- Core utilities: Basic configuration and setup
- Network utilities: Network-related functions including WSL detection
- WebRTC utilities: Configuration and connectivity for WebRTC
- WSL utilities: Windows Subsystem for Linux specific utilities
- Task utilities: Task management and configuration functions
- Redis utilities: Functions for Redis interaction and streaming
- Async utilities: Asynchronous programming helpers
- Rate limiting: API rate limiting functionality
- Proxy utilities: Proxy configuration and testing
- Memory utilities: Memory management and monitoring
"""

# Server utilities
from utils.tools import (
    # Memory utilities
    _get_memory_mb,
    convert_celery_status,
    create_proxies_config,
    create_task_status_response,
    datetime_handler,
    # Redis utilities
    decode_redis_hash,
    get_wsl2_host_ip,
    is_config_valid,
    is_scheduled_time_in_past,
    # Task utilities
    is_task_id,
    # Network utilities
    is_wsl,
    # Core utilities
    measure_stats,
    periodic_client_cleanup,
    # Rate limiting
    rate_limited,
    # Proxy utilities
    read_proxies_from_file,
    remove_stale_clients,
    # Async utilities
    retry_async,
    safe_eval_config,
    setup_logging,
    should_cleanup_task,
    stream_add_crawl_results,
    stream_results,
    stream_seeder_results,
    task_status_color,
    test_proxy_socket,
    url_to_unique_name,
)

# WebRTC utilities
from utils.webrtc import (
    # WebRTC connectivity
    check_udp_port_open,
    # WebRTC configuration
    configure_ice_servers,
    configure_webrtc_for_wsl,
    open_firewall_ports_on_windows_host,
    test_webrtc_connectivity,
)

# WSL utilities
from utils.wsl import (
    # WSL diagnostics and configuration
    check_wsl_port_forwarding,
)

__all__ = [
    # Core utilities
    'load_config', 'setup_logging', 'datetime_handler',
    
    # Network utilities
    'is_wsl', 'get_wsl2_host_ip',
    
    # WebRTC utilities
    'configure_ice_servers', 'configure_webrtc_for_wsl',
    'check_udp_port_open', 'test_webrtc_connectivity',
    'open_firewall_ports_on_windows_host',
    
    # WSL utilities
    'check_wsl_port_forwarding',
    
    # Task utilities
    'is_task_id', 'safe_eval_config', 'is_config_valid',
    'should_cleanup_task', 'is_scheduled_time_in_past',
    'url_to_unique_name', 'task_status_color',
    'convert_celery_status', 'create_task_status_response',
    
    # Redis utilities
    'decode_redis_hash', 'stream_results', 
    'stream_add_crawl_results', 'stream_seeder_results',
    
    # Async utilities
    'retry_async', 'periodic_client_cleanup', 'remove_stale_clients',
    
    # Rate limiting
    'rate_limited',
    
    # Proxy utilities
    'read_proxies_from_file', 'test_proxy_socket', 'create_proxies_config',
    
    # Memory utilities
    '_get_memory_mb', 'measure_stats'
]
