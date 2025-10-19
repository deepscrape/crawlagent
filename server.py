import asyncio
import logging
import os
import sys
import time
from contextlib import asynccontextmanager
from pathlib import Path

from crawl4ai import AsyncWebCrawler, BrowserConfig
from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.middleware.httpsredirect import HTTPSRedirectMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend
from fastapi_cache.decorator import cache

# prometheus fast api
from starlette.datastructures import Address  # Import Address

# from crawl import on_browser_created
from auth import get_token_dependency
from config import config, get_custom_limiter, skip_upstash_limiter
from crawler_pool import close_all, get_crawler, janitor
from monitoring import init_metrics_app
from redisCache import default_limiter, pure_redis, test_connection
from routers.crawl_router import crawl_router
from routers.job_v1 import job_router
from routers.seeder_router import seeder_router
from routers.websocket_router import socket_router

# Use uvloop for enhanced performance
# from tests.ml_model import ml_model
# from tests.pydantic_models import PredictionRequest
from utils import periodic_client_cleanup, setup_logging
from virtual_display_manager import VirtualDisplayManager  # Import VirtualDisplayManager

# ── internal imports (after sys.path append) ─────────────────
# sys.path.append(os.path.dirname(os.path.realpath(__file__)))

# Add parent directory to Python path
sys.path.append(str(Path(__file__).parent.parent))

####################################################################
# ────────────────── configuration / logging ──────────────────
####################################################################


setup_logging(config)
logger = logging.getLogger("crawlagent")


__version__ = config["app"]["version"] or "0.0.1"

# Environment
PRODUCTION = os.getenv("PYTHON_ENV", "development").lower() == "production"

# Set the number of workers
NUM_WORKERS = int(os.getenv("NUM_WORKERS", os.cpu_count() or 1))

# ── global page semaphore (hard cap) ─────────────────────────
MAX_PAGES = int(os.getenv("MAX_PAGES", config["crawler"]["pool"].get("max_pages", 40)))
GLOBAL_SEM = asyncio.Semaphore(MAX_PAGES)


orig_arun = AsyncWebCrawler.arun


async def capped_arun(self, *a, **kw):
    async with GLOBAL_SEM:
        return await orig_arun(self, *a, **kw)


AsyncWebCrawler.arun = capped_arun


orig_arun_many = AsyncWebCrawler.arun_many


async def capped_arun_many(self, urls, config=None, dispatcher=None, **kwargs):
    async with GLOBAL_SEM:
        return await orig_arun_many(self, urls, config, dispatcher, **kwargs)


AsyncWebCrawler.arun_many = capped_arun_many


# Store connected WebSocket clients
# socket_client: set[WebSocket] = set()

if sys.platform != "win32":
    import uvloop  # type: ignore

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
else:
    from asyncio import WindowsProactorEventLoopPolicy as EventLoopPolicy

    asyncio.set_event_loop_policy(EventLoopPolicy())
    # logger.warning("uvloop is not supported on Windows, using default(auto) event loop")


if PRODUCTION:
    print(
        "\033[94mINFO-SERVER:\033[0m  \033[92mRunning in Production mode. PYTHON_ENV\033[0m",
        PRODUCTION,
    )
else:
    print(
        "\033[94mWARNIN-SERVER:\033[92m Running in Development mode. PYTHON_ENV",
        PRODUCTION,
    )

###############################################################
# ───────────────────── FastAPI lifespan ──────────────────────
###############################################################


# Graceful shutdown for FastAPI
@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        # Initialize VirtualDisplayManager
        app.state.virtual_display_manager = VirtualDisplayManager(
            max_displays=config["virtual_display_manager"].get("max_displays", 5),
            start_display_num=config["virtual_display_manager"].get("start_display_num", 99),
            start_debug_port=config["virtual_display_manager"].get("start_debug_port", 9222)
        )
        
        await test_connection(pure_redis)  # Moved from on_event("startup")
        # _ = ml_model.get_feature_info()  # Pre-load the model
        
        FastAPICache.init(RedisBackend(pure_redis), prefix="fastapi-cache")
        await asyncio.gather(
             get_crawler(
                BrowserConfig(
                    extra_args=config["crawler"]["browser"].get("extra_args", []),
                    **config["crawler"]["browser"].get("kwargs", {}),
                )
            ),  # warm‑up
            # Pre-load the model
            
            return_exceptions=True
        )
        # await test_connection(pure_redis) # Moved from on_event("startup")
        app.state.janitor = asyncio.create_task(janitor())  # idle GC
        app.state.websocket = asyncio.create_task(periodic_client_cleanup())
        # No browser_cleanup task here, as displays are managed by VirtualDisplayManager and closed on release.

        yield
    except Exception as e:
        logger.error(f"Startup failed: {e}", exc_info=True)
        raise
    finally:
        if hasattr(app.state, "janitor"):
            app.state.janitor.cancel()
        if hasattr(app.state, "websocket"):
            app.state.websocket.cancel()
        # if hasattr(app.state, "browser_cleanup"): # Removed as per new architecture
        #     app.state.browser_cleanup.cancel()
        
        await asyncio.gather( close_all(), app.state.virtual_display_manager.close_all_displays(), return_exceptions=True)

        # Wait for background tasks to finish
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


####################################################################
# ───────────────────── FastAPI instance ──────────────────────
###############################################################

# Initialize FastAPI app on_startup=[startup_event], on_shutdown=[shutdown_event]

app = FastAPI(
    title=config["app"]["title"],
    version=config["app"]["version"],
    lifespan=lifespan,
)

app.mount("/static", StaticFiles(directory="static"), name="static")


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    client: Address | None = request.client
    client_info = "unknown"
    if client is not None and client.host is not None:
        client_info = client.host
    if exc.status_code == status.HTTP_429_TOO_MANY_REQUESTS:
        logger.warning(f"Rate limit exceeded for {client_info}:{request.url.path}")
        return JSONResponse(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            content={"detail": "Rate limit exceeded. Please try again later."},
            headers=exc.headers,
        )
    return JSONResponse(
        status_code=exc.status_code, content={"detail": exc.detail}, headers=exc.headers
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        content={"detail": exc.errors(), "body": exc.body},
    )


####################################################################
# ───────────────────── FastAPI Rate Limiting ──────────────────────
####################################################################


# Middleware to apply default rate limiting
# Hybrid: Only add middleware if enabled]
if config.get("rate_limiting", {}).get("enabled", False):
    @app.middleware("http")
    async def rate_limit_middleware(request: Request, call_next):
        # Define routes to exclude from rate limiting
        excluded_paths = ["/health", "/status", "/metrics"]

        # Skip rate limiting for routes with the skip_upstash_limiter attribute
        # Check if the endpoint has _skip_upstash_limiter
        logger.info(f"Skipping rate limiting for {request.url.path} due to route attribute")
        route = request.scope.get("endpoint")
        if route and getattr(route, "_skip_upstash_limiter", True):
            logger.info(f"Skipping rate limiting for {request.url.path} due to route attribute")
            return await call_next(request)

         # Skip rate limiting for excluded paths
        if request.url.path in excluded_paths:
            return await call_next(request)

        # Use client IP as the identifier
        client: Address | None = request.client  # Explicitly type client
        client_ip = "unknown"
        if client is not None and client.host is not None:
            client_ip = client.host
        identifier = f"{client_ip}:{request.url.path}"

        # Apply default rate limiting
        
        response = await default_limiter.limit(identifier)

        logger.info(f"Rate limiting for {identifier}, Remaining: {response.remaining}")
        # Add rate limit headers to response
        request.state.ratelimit = {
            "limit": response.limit,
            "remaining": response.remaining,
            "reset": response.reset,
        }

        if not response.allowed:
            logger.warning(f"Rate limit exceeded for {identifier}")
            return JSONResponse(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                content={"detail": "Rate limit exceeded. Please try again later."},
                headers={
                    "Retry-After": str(response.reset),
                    "X-RateLimit-Limit": str(response.limit),
                    "X-RateLimit-Remaining": "0",
                    "X-RateLimit-Reset": str(response.reset),
                },
            )

        # If allowed, proceed with the request
        response = await call_next(request)

        # Add rate limit headers to the response
        response.headers["X-RateLimit-Limit"] = str(request.state.ratelimit["limit"])
        response.headers["X-RateLimit-Remaining"] = str(
            request.state.ratelimit["remaining"]
        )
        response.headers["X-RateLimit-Reset"] = str(request.state.ratelimit["reset"])
        return response

################################################################
# ───────────────────── FastAPI Security ───────────────────────
################################################################


def _setup_security(app_: FastAPI):
    sec = config["security"]
    if not sec["enabled"]:
        return
    if sec.get("https_redirect"):
        app_.add_middleware(HTTPSRedirectMiddleware)
    if sec.get("trusted_hosts", []) != ["*"]:
        app_.add_middleware(TrustedHostMiddleware, allowed_hosts=sec["trusted_hosts"])


_setup_security(app)

# setup Prometheus metrics and health check endpoints
init_metrics_app(app, config)


# Set the token dependency for token verification, jwt if enabled from config file
verify_token = get_token_dependency(config)


# ─────────────────── Redis connection Global  ─────────────────────
# redis = aioredis.from_url(config["redis"].get("uri", "redis://localhost"))


################################################################
# ───────────────────── FastAPI routes ───────────────────────
################################################################


# ── job router ────────────────────────────────────────────── init_job_router(redis, config, verify_token, socket_client)
# Register routers with appropriate prefixes
app.include_router(socket_router, prefix="/api/v1/ws")
app.include_router(job_router, prefix="/api/v1/job")
app.include_router(crawl_router, prefix="/api/v1/crawl")
app.include_router(seeder_router, prefix="/api/v1/seeder")


# ── root endpoint ──────────────────────────────────────────────
@skip_upstash_limiter
@cache(expire=60)  # cache for 60 seconds
@app.get("/", dependencies=[Depends(get_custom_limiter(config))])
async def root(
    decoded_token: bool = Depends(verify_token),
):
    return {
        "message": "server is running."
    }

# perfomance stress test api endpoints
# @app.get("/model-info")    
# @cache(expire=30)  # cache for 30 seconds
# async def model_info(
# ):
#     """Get information about the ML model"""
#     try:
#         feature_info = await asyncio.to_thread(ml_model.get_feature_info)
#         return {
#             "model_type": "Random Forest Regressor",
#             "dataset": "California Housing Dataset",
#             "features": feature_info # ,
#         }
#     except Exception as e:
#         raise HTTPException(
#             status_code=500, detail="Error retrieving model information"
#         ) from e


# @app.post("/predict")
# @cache(expire=30)  # cache for 30 seconds
# async def predict(
#         request: PredictionRequest,
#     ):
#     """Make house price prediction"""
#     if len(request.features) != 8:
#         raise HTTPException(
#             status_code=status.HTTP_400_BAD_REQUEST,
#             detail=f"Expected 8 features, got {len(request.features)}",
#         )
#     try:
#         prediction = ml_model.predict(request.features)
#         # browser, sign = await get_crawler(BrowserConfig(verbose=True, headless=True, 
#         #                                                 browser_type="chromium",
#         #                                                 extra_args=config["crawler"]["browser"].get("extra_args", [])))
        
#         # crawled = await test_crawl(browser, sign, proxies=None)
#         return {
#             "prediction": float(prediction),
#             # "prediction": 123.45,  # Placeholder value
#             # "crawled": crawled,
#             "status": "success",
#             "features_used": request.features,
#         }
#     except RuntimeError as e:
#         raise HTTPException(status_code=status.HTTP_204_NO_CONTENT, detail=str(e)) from e
#     except ValueError as e:
#         raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e)) from e
#     except Exception as e:
#         raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Prediction error") from e
    
# health check endpoint
@app.get(config["observability"]["health_check"]["endpoint"])
async def health(_: Request):
    """Health check endpoint."""
    try:
        return JSONResponse(
            {"status": "ok", "timestamp": time.time(), "version": __version__}
        )

    except Exception:
        logger.exception("Health check failed")
        # Do not expose internal error details to clients
        return JSONResponse(
            {"status": "error"}, status_code=status.HTTP_503_SERVICE_UNAVAILABLE
        )


# prometheus metrics endpoint
@app.get(config["observability"]["prometheus"]["endpoint"])
async def metrics():
    return RedirectResponse(config["observability"]["prometheus"]["endpoint"])

################################################################
# ───────────────────── FastAPI middlewares ──────────────────────
################################################################


@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = f"{process_time * 1000:.2f}"

    logger.info(f"Request: {request.url.path} - Response time: {process_time * 1000:.2f} ms")
    return response


# security headers middleware
@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    resp = await call_next(request)
    if config["security"]["enabled"]:
        resp.headers.update(config["security"]["headers"])
    return resp



# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=config["app"].get(
        "cors_origins" if PRODUCTION else "cors_origins_dev", ["*"]
    ),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# # Add Gzip compression
app.add_middleware(GZipMiddleware, minimum_size=config["app"].get("minimum_size", 1000))

################################################################
# ────────────────────────── cli ──────────────────────────────
################################################################

if __name__ == "__main__":
    import uvicorn
    # if sys.platform == "win32":
    #     import winloop
    #     winloop.install()
    #     loop = asyncio.get_event_loop()
    # logger.info("Using winloop event loop", loop.run_forever())

    # Winloop's eventlooppolicy will be passed to uvicorn after this point...
    uvicorn.run(
        "server:app",
        host=config["app"]["host"],
        port=config["app"]["port"],
        reload=config["app"]["reload"],
        loop=config["app"]["uvloop"]
        if sys.platform == "win32"
        else "uvloop",  # force uvloop on unix
        timeout_keep_alive=config["app"]["timeout_keep_alive"],
        workers=int(config["app"]["workers"] or NUM_WORKERS),
    )
# ─────────────────────────────────────────────────────────────
