# config.py
import logging
import os
import re
from pathlib import Path
from typing import Callable, Dict

import yaml
from fastapi import HTTPException, Request, WebSocket, WebSocketException, status

from redisCache import custom_limiter, custom_websocket_limiter

logger = logging.getLogger("crawlagent")
# Determine which .env file to load
production = os.getenv("PYTHON_ENV", "development").lower() == "production"
env_file = ".env" if production else "dev.env"

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


config = load_config()
# Load proxies and create rotation strategy
# proxies = create_proxies_config("Free_Proxy_List.txt")  # Ensure this file exists with valid proxies

# def config_limiter(request: Request) -> RateLimiter | None:
#     request.state._skip_upstash_limiter = True
#     print("Skipping rate limiting for this request")
#     config_limiter_var = config.get("rate_limiting", {}).get("default_limit", "1000/minute")
#     split = config_limiter_var.split("/")
#     times = 1000
#     seconds = 60
#     if len(split) == 2:
#         try:
#             if config.get("rate_limiting", {}).get("enabled", True) is False:
#                 return None 
#             times = int(split[0])
#         except ValueError:
#             times = 1000
#         time_unit = split[1].strip().lower()
#         seconds_map = {"second": 1, "minute": 60, "hour": 3600, "day": 86400}
#         seconds = seconds_map.get(time_unit, 60)
#     return RateLimiter(times=times, seconds=seconds)

def get_custom_limiter(config: Dict) -> Callable:
    async def dependency(request: Request):

        request.state._skip_upstash_limiter = True
        client_ip = request.client.host if request.client and request.client.host else "unknown"
        identifier = f"{client_ip}:{request.url.path}"
        response = await custom_limiter.limit(identifier)
        
        # Add rate limit headers to response
        request.state.ratelimit = {
            "limit": response.limit,
            "remaining": response.remaining,
            "reset": response.reset,
        }
        if not response.allowed:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Rate limit exceeded. Please try again later.",
                headers={
                    "Retry-After": str(response.reset),
                    "X-RateLimit-Limit": str(response.limit),
                    "X-RateLimit-Remaining": "0",
                    "X-RateLimit-Reset": str(response.reset),
                },
            )
        
        return True
    return dependency if config.get("rate_limiting", {}).get("enabled", True) else lambda: True

# Define the custom WebSocket limiter dependency
async def get_websocket_custom_limiter(websocket: WebSocket):

    if config.get("rate_limiting", {}).get("enabled", True) is False:
        return True  # No limiting if disabled

    client_ip = websocket.client.host if websocket.client and websocket.client.host else "unknown"
    identifier = f"{client_ip}:{websocket.url.path}"
    
    response = await custom_websocket_limiter.limit(identifier)
    print(f"Rate limit response: {response.remaining}")
    if not response.allowed:
        headers = {
            "Retry-After": str(response.reset),
            "X-RateLimit-Limit": str(response.limit),
            "X-RateLimit-Remaining": "0",
            "X-RateLimit-Reset": str(response.reset),
        }
        
        raise WebSocketException(
            code=status.WS_1008_POLICY_VIOLATION,
            reason="Rate limit exceeded. Please try again later."
        )
    return True # Indicate that the request is allowed

def skip_upstash_limiter(route_func):
    route_func._skip_upstash_limiter = True
    return route_func
