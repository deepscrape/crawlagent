# config.py
import os
from typing import Callable, Dict

from fastapi import HTTPException, Request, WebSocket, WebSocketException, status

from redisCache import custom_limiter, custom_websocket_limiter
from utils import load_config

# Determine which .env file to load
production = os.getenv("PYTHON_ENV", "development").lower() == "production"
env_file = ".env" if production else "dev.env"

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
