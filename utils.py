import ast
import asyncio
import inspect
import json
import logging
import os
import random
import re
import subprocess
import typing
import urllib.parse
from contextlib import asynccontextmanager
from datetime import datetime
from functools import wraps
from pathlib import Path
from typing import Any, AsyncGenerator, Callable, Dict, List, Optional, Union, cast

import crawl4ai as _c4
import psutil
import yaml
from celery.result import AsyncResult  # Moved to function scope to avoid circular dependency
from fastapi import HTTPException, Request

# from upstash_redis.asyncio import Redis
from redis.asyncio import Redis  # Use redis.asyncio for async Redis operations 
from upstash_ratelimit.asyncio import Ratelimit

from enums import CeleryTaskStatus, TaskStatus
from monitoring import manager
from redisCache import default_limiter
from schemas import CrawlConfigValidResponse, SystemTaskStats
from validators import BrowserConfigValidator, CrawlerRunConfigValidator, SeedingConfigValidator

logger = logging.getLogger("crawlagent")
production = os.getenv("PYTHON_ENV", "development").lower() == "production"

def load_config() -> Dict:
    """
    Load and return application configuration with environment variable expansion.

    This function reads the 'config.yml' file located in the same directory as this script,
    expands any environment variable placeholders in the form of ${VAR} by tagging them with
    '!env_var', and parses the YAML content into a Python dictionary.

    Returns:
        Dict: The parsed configuration as a dictionary. Returns an empty dictionary if
        the file cannot be read or parsed.

    Raises:
        None: All exceptions are caught and result in an empty dictionary being returned.
    """
    config_path = Path(__file__).parent / "config.yml"
    try:
        with open(config_path, "r", encoding='utf-8') as config_file:
            content = config_file.read()
        # Tag all lines containing ${VAR} with !env_var
        content = re.sub(r'^(.*\$\{[^}^{]+\}.*)$', r'!env_var \1', content, flags=re.MULTILINE)
        
        def env_var_constructor(loader, node):
            value = loader.construct_scalar(node)
            return re.sub(r'\$\{([^}^{]+)\}', lambda m: os.environ.get(m.group(1), ""), value)

        class EnvVarLoader(yaml.SafeLoader):
            pass
        EnvVarLoader.add_implicit_resolver('!env_var', re.compile(r'.*\$\{[^}^{]+\}.*'), None)
        EnvVarLoader.add_constructor('!env_var', env_var_constructor)
        return yaml.load(content, Loader=EnvVarLoader)

    except Exception as e:
        logger.error(f"Error loading config: {e}")
        return {}

def setup_logging(config: dict) -> None:
    if not config.get("logging", {}).get("enabled", True):
        logging.disable(logging.CRITICAL)
    else:
        logging.basicConfig(
            level=config.get("logging", {}).get("level", "INFO"),
            format=config.get("logging", {}).get("format", "%(asctime)s - %(name)s - %(levelname)s - %(message)s"),
            datefmt=config.get("logging", {}).get("datefmt", "%Y-%m-%d %H:%M:%S"),
        )

def datetime_handler(obj: Any) -> Optional[str]:
    """Handle datetime serialization for JSON."""
    if hasattr(obj, 'isoformat'):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
    
async def remove_stale_clients(now) -> None:
    """Remove stale WebSocket clients."""
     # Find inactive connections (idle for more than 5 minutes)
    inactive_connections = []
    for conn_id, conn in manager.active_connections.items():
        if (now - conn["last_activity"]).total_seconds() > 300:  # 5 minutes
            inactive_connections.append(conn_id)

    # Log inactive connections
    if inactive_connections:
        logger.info(f"Found {len(inactive_connections)} inactive connections")

     # Optionally disconnect inactive connections
        for conn_id in inactive_connections:
            manager.disconnect(conn_id)
            # socket_client.remove(client)

    # disconnected_clients:set[WebSocket] = set()
    # for client in socket_client:
    #     try:
    #         await client.send_text("ping")  # Ping the client
    #     except Exception:
    #         disconnected_clients.add(client)
    # for client in disconnected_clients:
    #     socket_client.remove(client)

async def periodic_client_cleanup() -> None:
    """Periodically check and remove stale clients."""
    while True:
        logging.info("Checking for stale clients...")
        await asyncio.sleep(30)  # Check every minute
        now = datetime.now()
        await remove_stale_clients(now)

def is_task_id(value: str) -> bool:
    """Check if the value matches task ID pattern."""
    return value.startswith("llm_") and "_" in value

def safe_eval_config(expr: str) -> dict:
    """
    Accept exactly one top‑level call to CrawlerRunConfig(...) or BrowserConfig(...).
    Whatever is inside the parentheses is fine *except* further function calls
    (so no  __import__('os') stuff).  All public names from crawl4ai are available
    when we eval.
    """
    tree = ast.parse(expr, mode="eval")

    # must be a single call
    if not isinstance(tree.body, ast.Call):
        raise ValueError("Expression must be a single constructor call")

    call = tree.body
    if not (isinstance(call.func, ast.Name) and call.func.id in {"CrawlerRunConfig", "BrowserConfig"}):
        raise ValueError(
            "Only CrawlerRunConfig(...) or BrowserConfig(...) are allowed")

    # forbid nested calls to keep the surface tiny
    for node in ast.walk(call):
        if isinstance(node, ast.Call) and node is not call:
            raise ValueError("Nested function calls are not permitted")

    # expose everything that crawl4ai exports, nothing else
    safe_env = {name: getattr(_c4, name)
                for name in dir(_c4) if not name.startswith("_")}
    obj = eval(compile(tree, "<config>", "eval"),
               {"__builtins__": {}}, safe_env)
    return obj.dump()

def is_config_valid(browser_config: Dict | None, 
                    crawler_config: Dict | None,
                    seeder_config: Dict | None,
                    ):
    """
    Validate browser and crawler configurations.
    Returns a FastAPI JSONResponse with details about which config is valid/invalid.
    """
    errors: Dict = {}

    # Validate browser config
    try:
        if browser_config:
            BrowserConfigValidator.validate(browser_config)
    except Exception as e:
        errors["browser_config"] = str(e)

    # Validate crawler config
    try:
        if crawler_config:    
            CrawlerRunConfigValidator.validate(crawler_config)
    except Exception as e:
        errors["crawler_config"] = str(e)

    try:
        if seeder_config:
            SeedingConfigValidator.validate(seeder_config)
    except Exception as e:
        errors["seeder_config"] = str(e)

    is_valid = not errors
    result = CrawlConfigValidResponse(
        is_valid=is_valid,
        errors=errors if errors else None
    )
    status_code = 200 if is_valid else 422
    return JSONResponse(content=result.model_dump(), status_code=status_code)

def should_cleanup_task(created_at: str, ttl_seconds: int = 3600) -> bool:
    """Check if task should be cleaned up based on creation time."""
    created = datetime.fromisoformat(created_at)
    return (datetime.now() - created).total_seconds() > ttl_seconds

def is_scheduled_time_in_past(scheduled_at: int) -> tuple[bool, datetime]:
    """
    Checks if the given scheduled time (in milliseconds since the epoch) is in the past relative to the current system time.
    Args:
        scheduled_at (int): The scheduled time in milliseconds since the Unix epoch.
    Returns:
        bool: True if the scheduled time is in the past or equal to the current time, False otherwise.
    """
    
    scheduled_at_dt = datetime.fromtimestamp(scheduled_at / 1000)
    now = datetime.now()
    
    return now >= scheduled_at_dt, scheduled_at_dt


def url_to_unique_name(url):
    parsed_url = urllib.parse.urlparse(url)
    slug = re.sub(r"[^a-zA-Z0-9_\-]", "-", parsed_url.path)
    slug = slug.strip("-")
    return f"{parsed_url.netloc}_{slug}"


def task_status_color(status: TaskStatus) -> str:
    """
    Returns the color associated with the given TaskStatus enum member.

    Args:
        status (TaskStatus): The task status enum member.

    Returns:
        str: The color associated with the status.
    """
    color_map = {
        TaskStatus.IN_PROGRESS: 'cyan',
        TaskStatus.FAILED: 'red',
        TaskStatus.CANCELED: 'deep-orange',
        TaskStatus.SCHEDULED: 'emerald',
        TaskStatus.COMPLETED: 'green',
        TaskStatus.STARTED: 'blue',
        TaskStatus.READY: 'gray',
        TaskStatus.PENDING: 'deep-purple',
        TaskStatus.RETRY: 'yellow',
    }
    return color_map.get(status, 'gray')

def decode_redis_hash(hash_data: Dict[bytes, bytes] | Dict[str, str]) -> Dict[str, str]:
    """Decode Redis hash data from bytes to strings."""
    result = {}
    for k, v in hash_data.items():
        if isinstance(k, bytes):
            k = k.decode('utf-8')
        if isinstance(v, bytes):
            v = v.decode('utf-8')
        result[k] = v
    return result

# --- Helper to get memory ---
def _get_memory_mb():
    try:
        return psutil.Process().memory_info().rss / (1024 * 1024)
    except Exception as e:
        logger.warning(f"Could not get memory info: {e}")
        return None

def convert_celery_status(celery_status: CeleryTaskStatus) -> TaskStatus:
    status_mapping = {
        CeleryTaskStatus.PENDING: TaskStatus.PENDING,
        CeleryTaskStatus.STARTED: TaskStatus.IN_PROGRESS,
        CeleryTaskStatus.SUCCESS: TaskStatus.COMPLETED,
        CeleryTaskStatus.FAILURE: TaskStatus.FAILED,
        CeleryTaskStatus.RETRY: TaskStatus.RETRY,
        CeleryTaskStatus.REVOKED: TaskStatus.CANCELED
    }
    
    return status_mapping.get(celery_status, TaskStatus.READY)  # Default to READY if status is unknown

def create_task_status_response(celery_task: AsyncResult, task: Dict[str, str], task_id: str, base_url: str) -> dict:
    """Create response for task status check."""
    response = {
        "task_id": task_id,
        "status": convert_celery_status(celery_task.state) or task["status"],
        "created_at": task["created_at"],
        "urls": task.get("urls", ""),
        "_links": {
            "self": {"href": f"{base_url}llm/{task_id}"},
            "refresh": {"href": f"{base_url}llm/{task_id}"}
        }
    }

    if task["status"] == TaskStatus.COMPLETED or celery_task.ready():
        # Handle successful tasks
        if celery_task.successful():
            # Always prioritize Celery result, even if it's None or empty
            response["result"] = celery_task.result
        # Only fall back to Redis result if Celery result is not available
        elif not hasattr(celery_task, 'result') and task.get("result"):
            try:
                # Try to parse Redis task result as JSON
                response["result"] = json.loads(task["result"])
            except json.JSONDecodeError:
                # If parsing fails, use it as a string
                response["result"] = task["result"]
        else:
            # Set explicit None if no result is available
            response["result"] = None
    elif task["status"] == TaskStatus.FAILED or celery_task.failed():
        response["error"] = task.get("error", "Unknown error")
        response["result"] = celery_task.result

    return response

async def stream_results(crawler: _c4.AsyncWebCrawler, results_gen: AsyncGenerator) -> AsyncGenerator[bytes, None]:
    """Stream results with heartbeats and completion markers."""

    try:
        async for result in results_gen:
            try:
                server_memory_mb = _get_memory_mb()
                result_dict = result.model_dump()
                result_dict['server_memory_mb'] = server_memory_mb
                logger.info(f"Streaming result for {result_dict.get('url', 'unknown')}")
                data = json.dumps(result_dict, default=datetime_handler) + "\n"
                yield data.encode('utf-8')
            except Exception as e:
                logger.error(f"Serialization error: {e}")
                error_response = {"error": str(e), "url": getattr(result, 'url', 'unknown')}
                yield (json.dumps(error_response) + "\n").encode('utf-8')

        yield json.dumps({"status": "completed"}).encode('utf-8')
        
    except asyncio.CancelledError:
        logger.warning("Client disconnected during streaming")
    finally:
        try:
            await crawler.close()
        except Exception as e:
            logger.error(f"Crawler cleanup error: {e}")
        pass




async def stream_add_crawl_results(redis: Redis, channel: str, results_gen: AsyncGenerator,
                                chunk_size: int = 4096) -> bool:
    """
    Publish results to a Redis stream using pipeline batching.
    Each result is published as a stream entry in batches.
    Error entries are published if serialization fails.
    A final 'completed' marker is always published.
    Returns a list of successfully published result dicts.
    """
    
    result: _c4.CrawlResult
    complete = {"status": "ok", "message": "completed"}

    pipe2 = redis.pipeline()
    try:
        async for result in results_gen:
            try:
                server_memory_mb = _get_memory_mb()
                if hasattr(result, "html"):
                    result.html = ""  # Clear HTML content to reduce size
                    result_dict = result.model_dump()
                    result_dict["html"] = ""  # type: ignore
     
                # Convert result to dict if it's a CrawlResult, otherwise use as is
                result_dict = result.model_dump() if hasattr(result, "model_dump") else dict(result)
                
                result_dict['server_memory_mb'] = server_memory_mb

                # result_dict['status'] = "model_dump"
                url = result_dict.get('url', 'unknown')

                model_dump = result_dict if hasattr(result, 'model_dump') \
                    else {"status": "error", "message": "No model_dump available skipping", 
                          "url": getattr(result, 'url', 'unknown')}

                if isinstance(model_dump, dict):
                    # data.append(model_dump)
                    logger.info(f"Publishing result for {url}")
                    # buffer.append(model_dump)
                else:
                    raise ValueError(model_dump)

                batch_json = json.dumps(model_dump, default=datetime_handler, ensure_ascii=False)
                pipe = redis.pipeline()
                # chunk_size = 4096  # Define chunk_size as a constant (adjust as needed)
                total_chunks = (len(batch_json) + chunk_size - 1) // chunk_size  # Calculate total chunks
                # Split batch_json into chunks of chunk_size
                for i in range(0, len(batch_json), chunk_size):
                    chunk = batch_json[i:i+chunk_size]
                    pipe.xadd(channel, {
                        "status": "ok",
                        "message": "processing",
                        "type": "batch_chunk",
                        "url": url,
                        "chunk_index": str(i // chunk_size),
                        "total_chunks": str(total_chunks),  # Add total_chunks attribute
                        "dump": chunk #.encode("utf-8") if isinstance(chunk, str) else chunk
                    })
                await pipe.execute()

            except Exception as e:
                logger.error(f"Serialization error: {e}")
                error_response = {"status": "error", "message": str(e), "url": getattr(result, 'url', 'unknown')}
                
                pipe2.xadd(channel, {key: str(value) if isinstance(value, bool) else value 
                                     for key, value in error_response.items()
                                     } )
                complete = {"status": "error", "message": "completed"}

        pipe2.xadd(channel, {key: str(value) if isinstance(value, bool) else value  for key, value in complete.items()})

    except asyncio.CancelledError:
        logger.warning("Client disconnected during streaming")
        pipe2.xadd(channel, {"status": "canceled", "message": "streaming canceled"})
    except Exception as e:
        logger.error(f"Unexpected error in stream_add_crawl_results: {e}")
        pipe2.xadd(channel, {"status": "error", "message": str(e)})
        await pipe2.execute()
        return False

    await pipe2.execute()
    return True


async def stream_seeder_results(redis: Redis, channel: str, results,
                                chunk_size: int = 4096) -> bool:
    """
    Publish results to a Redis stream using pipeline batching.
    Each result is published as a stream entry in batches.
    Error entries are published if serialization fails.
    A final 'completed' marker is always published.
    Returns a list of successfully published result dicts.
    """
    
    complete = {"status": "ok", "message": "completed"}

    pipe2 = redis.pipeline()
    try:
        print(f"all results are {len(results)}!!!")
        for result in results:
            try:                
                # Convert result to dict if it's a CrawlResult, otherwise use as is

                # result_dict['status'] = "model_dump"
                url = result.get('url', 'unknown')

                logger.info(f"Publishing result for {url}")
                if isinstance(result, dict):
                    logger.info(f"Publishing result for {result.get('domain', 'unknown')}")
                else:
                    raise ValueError(result)

                batch_json = json.dumps(result, default=datetime_handler, ensure_ascii=False)
                pipe = redis.pipeline()
                # chunk_size = 4096  # Define chunk_size as a constant (adjust as needed)
                total_chunks = (len(batch_json) + chunk_size - 1) // chunk_size  # Calculate total chunks
                # Split batch_json into chunks of chunk_size
                for i in range(0, len(batch_json), chunk_size):
                    chunk = batch_json[i:i+chunk_size]
                    pipe.xadd(channel, {
                        "status": "ok",
                        "message": "processing",
                        "type": "batch_chunk",
                        "url": url,
                        "chunk_index": str(i // chunk_size),
                        "total_chunks": str(total_chunks),  # Add total_chunks attribute
                        "dump": chunk #.encode("utf-8") if isinstance(chunk, str) else chunk
                    })
                await pipe.execute()

            except Exception as e:
                logger.error(f"Serialization error: {e}")
                error_response = {"status": "error", "message": str(e), "url": getattr(result, 'url', 'unknown')}
                
                pipe2.xadd(channel, {key: str(value) if isinstance(value, bool) else value 
                                     for key, value in error_response.items()
                                     } )
                complete = {"status": "error", "message": "completed"}

        pipe2.xadd(channel, {key: str(value) if isinstance(value, bool) else value  for key, value in complete.items()})

    except asyncio.CancelledError:
        logger.warning("Client disconnected during streaming")
        pipe2.xadd(channel, {"status": "canceled", "message": "streaming canceled"})
    except Exception as e:
        logger.error(f"Unexpected error in stream_seeder_results: {e}")
        pipe2.xadd(channel, {"status": "error", "message": str(e)})
        await pipe2.execute()
        return False

    await pipe2.execute()
    return True

async def retry_async(func, *args, retries=3, base_delay=0.5, max_delay=5, **kwargs):
    """Retry async function with exponential backoff and jitter."""
    attempt = 0
    last_exception = None
    while attempt < retries:
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            last_exception = e
            attempt += 1
            delay = min(max_delay, base_delay * (2 ** attempt) + random.uniform(0, 0.5))
            logger.warning(f"Retry {attempt}/{retries} for {func.__name__} due to error: {e}. Waiting {delay:.2f}s.")
            await asyncio.sleep(delay)
    if last_exception:
        raise last_exception
    # This case should ideally not be reached if retries > 0 and func always raises on failure
    raise RuntimeError("Function failed after multiple retries without capturing an exception.")

# Define the rate limiting decorator
def rate_limited(rate: int = 1, limiter: Ratelimit = default_limiter) -> Callable:
    """Rate limiting decorator for FastAPI endpoints.

    Args:
        limit: Rate limit string (e.g. "100/minute", "1000/hour")
        limiter: Rate limiter instance to use (defaults to default_limiter)

    Returns:
        Decorator function that applies rate limiting
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            # Extract Request object
            request = next(
                (arg for arg in args if isinstance(arg, Request)), kwargs.get("request")
            )

            if not request:
                raise ValueError("Request parameter not found in function arguments")

            # Create unique identifier for this request
            client_ip = request.client.host if request.client else "unknown"
            identifier = f"{client_ip}:{request.url.path}"

            print(f"Rate limit identifier: {identifier}")
            # Apply rate limiting
            response = await limiter.limit(identifier, rate)

            # Add rate limit headers to response
            request.state.ratelimit = {
                "limit": response.limit,
                "remaining": response.remaining,
                "reset": response.reset,
            }

            if not response.allowed:
                raise HTTPException(
                    status_code=429,
                    detail="Rate limit exceeded",
                    headers={
                        "Retry-After": str(response.reset),
                        "X-RateLimit-Limit": str(response.limit),
                        "X-RateLimit-Remaining": "0",
                        "X-RateLimit-Reset": str(response.reset),
                    },
                )

            return await func(*args, **kwargs)

        return wrapper

    return decorator

def read_proxies_from_file(file_path: str) -> List[str]:
    proxies = []
    with open(file_path, "r") as f:
        for line in f:
            line = line.strip()
            if "://" in line:
                protocol, address = line.split("://", 1)
            else:
                address = line
                protocol = "http"
            if "@" in address:
                auth, server = address.split("@", 1)
                username, password = auth.split(":", 1)
                ip, port = server.split(":", 1)
                proxies.append(f"{ip}:{port}:{username}:{password}")
            else:
                ip, port = address.split(":", 1)
                proxies.append(f"{ip}:{port}")
    return proxies
    
import concurrent.futures
import time
import socket
from fastapi.responses import JSONResponse

def test_proxy_socket(proxy: str, timeout: float = 3.0) -> bool:
    """Test if a proxy is reachable by opening a socket connection."""
    try:
        # Handle proxies with or without authentication
        if "@" in proxy:
            # Format: ip:port:username:password or ip:port@username:password
            parts = proxy.split(":")
            ip, port = parts[0], int(parts[1])
        else:
            ip, port = proxy.split(":")
            port = int(port)
        with socket.create_connection((ip, port), timeout=timeout):
            return True
    except Exception as e:
        logger.warning(f"Proxy {proxy} not reachable: {e}")
        return False


def create_proxies_config(file_path: str) -> Optional[List[_c4.ProxyConfig]]:
    proxy_list = read_proxies_from_file(file_path)
    if not proxy_list:
        print("No proxies found in proxies_list.txt")
        return None

    # Test proxies using socket connection
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(test_proxy_socket, proxy_list))
        working_proxies = [proxy for proxy, ok in zip(proxy_list, results, strict=True) if ok]

    if not working_proxies:
        print("No working proxies found.")
        return None

    proxies_config: List[_c4.ProxyConfig] = [_c4.ProxyConfig.from_string(proxy) for proxy in cast(List, working_proxies)]
    print(f"Working proxies: {working_proxies}")
    return proxies_config


@asynccontextmanager
async def measure_stats(stats: Optional[SystemTaskStats] = None, logger=None, label="operation"):
    start_mem_mb = _get_memory_mb()
    start_time = time.time()
    peak_mem_mb = start_mem_mb
    mem_delta_mb = None
    try:
        yield
    finally:
        end_mem_mb = _get_memory_mb()
        end_time = time.time()
        total_time = end_time - start_time
        if start_mem_mb is not None and end_mem_mb is not None:
            mem_delta_mb = end_mem_mb - start_mem_mb
            peak_mem_mb = max(peak_mem_mb if peak_mem_mb else 0, end_mem_mb)
        if stats is not None:
            stats.end_time = end_time
            stats.start_time = start_time
            stats.duration = total_time
            stats.start_mem_mb = start_mem_mb
            stats.end_mem_mb = end_mem_mb
            stats.mem_delta_mb = mem_delta_mb
            stats.peak_mem_mb = peak_mem_mb
        if logger:
            logger.info(
                f"[{label}] Memory usage: Start: {start_mem_mb} MB, End: {end_mem_mb} MB, "
                f"Delta: {mem_delta_mb} MB, Peak: {peak_mem_mb} MB, Total Time: {total_time:.2f}s"
            )

def get_wsl2_host_ip():
    try:
        route = subprocess.check_output("ip route | grep default", shell=True).decode()
        return route.split()[2]
    except Exception:
        return "127.0.0.1"