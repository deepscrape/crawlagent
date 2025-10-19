import asyncio
import json
import logging
import os
import platform
import sys
import time
from datetime import datetime, timezone
from functools import partial  # Import partial
from typing import AsyncGenerator, Dict, List, Optional, cast
from urllib.parse import unquote

from celery.exceptions import SoftTimeLimitExceeded
from crawl4ai import (
    AsyncWebCrawler,
    BrowserConfig,
    CacheMode,
    CrawlerRunConfig,
    CrawlResult,
    DefaultMarkdownGenerator,
    LLMConfig,
    LLMContentFilter,
    LLMExtractionStrategy,
    LXMLWebScrapingStrategy,
    MemoryAdaptiveDispatcher,
    PruningContentFilter,
    RateLimiter,
)

# from crawl4ai.utils import perform_completion_with_backoff
from fastapi import HTTPException, status
from redis import RedisError

from apps.seeder import multi_domain_research
from celery_app import celery_app
from config import config
from crawler_pool import cancel_crawler, get_crawler

# import psutil
from crawlstore import updateCrawlOperation
from firestore import FirebaseClient
from monitor import WorkerMonitor
from redisCache import REDIS_CHANNEL
from redisCache import pure_redis as pure_redis_cache
from redisCache import redis as redis_cache
from schemas import OperationResult, SystemTaskStats
from utils import (
    TaskStatus,
    _get_memory_mb,
    datetime_handler,
    decode_redis_hash,
    is_task_id,
    load_config,
    measure_stats,
    setup_logging,
    should_cleanup_task,
    stream_add_crawl_results,
    stream_seeder_results,
    task_status_color,
)

if sys.platform != "win32":
    import uvloop  # type: ignore

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
else:
    from asyncio import WindowsProactorEventLoopPolicy as EventLoopPolicy

    asyncio.set_event_loop_policy(EventLoopPolicy())

# Track if we're on Windows
IS_WINDOWS = platform.system() == "Windows"

# Only use a global event loop on Windows with pool=solo
# On Linux with concurrency, each worker process will manage its own loop
_event_loop = None


def get_event_loop():
    """
    Get or create an event loop in a platform-specific way.

    On Windows with pool=solo: Returns a persistent global event loop
    On Linux with multiprocessing: Returns a process-specific event loop
    """
    global _event_loop

    if IS_WINDOWS:
        # Windows approach: reuse the same event loop for all tasks
        if _event_loop is None or _event_loop.is_closed():
            _event_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(_event_loop)
        return _event_loop
    else:
        # Linux approach: get the current process's event loop
        try:
            loop = asyncio.get_event_loop()
            if loop.is_closed():
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            return loop
        except RuntimeError:
            # No event loop in current thread, create one
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            return loop


# # Global Redis clients for all Celery tasks
# redis_url = os.environ.get("UPSTASH_REDIS_REST_URL")
# redis_token = os.environ.get("UPSTASH_REDIS_REST_TOKEN")
# REDIS_PORT = os.environ.get("UPSTASH_REDIS_PORT")
# REDIS_USERNAME = os.environ.get("UPSTASH_REDIS_USER")
# REDIS_PASSWORD = os.environ.get("UPSTASH_REDIS_PASS")

# if not redis_url or not redis_token or not REDIS_PORT or not REDIS_USERNAME or not REDIS_PASSWORD:
#     raise RuntimeError("Missing one or more Upstash Redis environment variables.")

# REDIS_URL = redis_url.replace("https://", "")

# # Global Upstash Redis client
# redis = Redis(
#     url=str(redis_url),
#     token=str(redis_token),
#     allow_telemetry=False,
# )

# # # Global PureRedis client
# pure_redis = PureRedis(
#     host=str(REDIS_URL),
#     port=int(REDIS_PORT),
#     db=0,
#     username=str(REDIS_USERNAME),
#     password=str(REDIS_PASSWORD),
#     ssl=True,
#     decode_responses=True
# )

logger = logging.getLogger("crawlagent")
# At module level
_firebase_client = None
_db_instance = None

_redis = None
_pure_redis = None


def get_redis():
    global _redis, _pure_redis
    global redis, pure_redis
    if _redis is None:
        _redis = redis_cache
        _pure_redis = pure_redis_cache
    return _redis, _pure_redis


redis, pure_redis = get_redis()


async def get_firebase_client():
    global _firebase_client, _db_instance
    if _firebase_client is None:
        _firebase_client = FirebaseClient()

        # create a client one for all
        if _db_instance is None:
            _db_instance, _ = _firebase_client.init_firebase()

    return _db_instance


@celery_app.task(
    bind=True,
    max_retries=5,
    autoretry_for=(Exception,),
    retry_backoff=True,  # exponential backoff
    retry_backoff_max=60,  # max 60s backoff
    retry_jitter=True,  # random jitter to avoid thundering herd
)
def seeder_multi_research_task(self, uid: str, operation_id: str, domains: List[str], config: Dict):
    """Celery task to process seeder."""
    try:
        # Get an appropriate event loop for the platform
        loop = get_event_loop()

        # Run the implementation
        loop.run_until_complete(_seeder_multi_research_task_impl(self, uid, operation_id, domains, config))

        # On Linux, we need to clean up pending tasks before returning
        # On Windows with solo pool, we keep tasks running
        if not IS_WINDOWS:
            pending = [task for task in asyncio.all_tasks(loop) if not task.done() and task is not asyncio.current_task(loop)]
            if pending:
                logger.info(f"Cleaning up {len(pending)} pending tasks")
                for task in pending:
                    task.cancel()
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))

        return {"status": TaskStatus.COMPLETED, "message": "seeder task completed successfully."}

    except Exception as e:
        logger.error(f"Error in seeder task: {e}")
        # return {"status": TaskStatus.FAILED, "message": str(e)}
        raise self.retry(exc=e) from e


# --------------------------------- Crawl Tasks ---------------------------------
@celery_app.task(
    bind=True,
)
def crawl_task(self, urls: List[str], browser_config: Dict, crawler_config: Dict):
    """Celery task to handle crawl requests."""
    logger.info("Starting crawl task with URLs")
    # logger.info(f"Browser config: {browser_config}")
    # logger.info(f"Crawler config: {crawler_config}")

    try:
        # # Periodically check for revocation
        # if self.is_aborted():
        #         # await redis.hset(f"task:{task_id}", values={"status": TaskStatus.CANCELED})
        #         return {"status": TaskStatus.CANCELED, "message": "Task aborted by user."}

        asyncio.run(_crawl_task_impl(self, urls, browser_config, crawler_config))

        return {"status": TaskStatus.COMPLETED, "message": "Crawl task completed successfully."}

    except SoftTimeLimitExceeded:
        return {"status": "CANCELED", "message": "Task exceeded time limit and was aborted."}
    except Exception as e:
        logger.error(f"Error in crawl task: {e}")
        return {"status": TaskStatus.FAILED, "message": str(e)}


@celery_app.task(bind=True)
def llm_extraction_task(self, url: str, instruction: str, schema: Optional[str] = None, cache: str = "0") -> Dict:
    """Celery task to process LLM extraction."""

    try:
        # Get an appropriate event loop for the platform
        loop = get_event_loop()

        # Run the implementation
        result = loop.run_until_complete(_llm_extraction_task_impl(self, url, instruction, schema, cache))

        # On Linux, we need to clean up pending tasks before returning
        # On Windows with solo pool, we keep tasks running
        if not IS_WINDOWS:
            pending = [task for task in asyncio.all_tasks(loop) if not task.done() and task is not asyncio.current_task(loop)]
            if pending:
                logger.info(f"Cleaning up {len(pending)} pending tasks")
                for task in pending:
                    task.cancel()
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))

        return {"status": TaskStatus.COMPLETED, "message": "LLM extraction task completed successfully.", "result": result}
    except SoftTimeLimitExceeded:
        return {"status": "CANCELED", "message": "Task exceeded time limit and was aborted."}
    except Exception as e:
        logger.error(f"Error in crawl task: {e}")
        return {"status": TaskStatus.FAILED, "message": str(e)}


@celery_app.task(
    bind=True,
    max_retries=5,
    autoretry_for=(Exception,),
    retry_backoff=True,  # exponential backoff
    retry_backoff_max=60,  # max 60s backoff
    retry_jitter=True,  # random jitter to avoid thundering herd
)
def crawl_stream_task(self, uid: str, operation_id: str, urls: List[str], browser_config: Dict, crawler_config: Dict) -> Dict:
    """Celery task to process crawl stream."""

    try:
        # Get an appropriate event loop for the platform
        loop = get_event_loop()

        # Run the implementation
        loop.run_until_complete(_crawl_stream_task_impl(self, uid, operation_id, urls, browser_config, crawler_config))

        # On Linux, we need to clean up pending tasks before returning
        # On Windows with solo pool, we keep tasks running
        if not IS_WINDOWS:
            pending = [task for task in asyncio.all_tasks(loop) if not task.done() and task is not asyncio.current_task(loop)]
            if pending:
                logger.info(f"Cleaning up {len(pending)} pending tasks")
                for task in pending:
                    task.cancel()
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))

        return {"status": TaskStatus.COMPLETED, "message": "crawl stream task completed successfully."}

    except Exception as e:
        logger.error(f"Error in crawl stream task: {e}")
        # return {"status": TaskStatus.FAILED, "message": str(e)}
        raise self.retry(exc=e) from e


async def _seeder_multi_research_task_impl(self, uid: str, operation_id: str, domains: List[str], config: Dict):
    """Internal implementation of the multi-domain research task."""
    task_id = self.request.id
    logger.info(f"Celery task {task_id} started for multi-domain research.")
    start_time = time.time()

    try:
        # Initialize Firebase client
        db = await get_firebase_client()

        # Set up monitor with process ID and Firebase client
        monitor = WorkerMonitor(os.getpid(), db)

        # Update system metrics and record operation start in parallel
        await asyncio.gather(monitor.update_metrics(), monitor.record_operation_start(operation_id, start_time))
        stats = SystemTaskStats()
        all_urls: list = []

        async with measure_stats(stats, logger, label="_seeder_multi_research_task_impl"):
            all_urls = await multi_domain_research(domains=domains, cfg=config)


        # Stream results to Redis Pub/Sub
        channel = f"{REDIS_CHANNEL}:{task_id}"

        if pure_redis is None:
            raise ValueError("pure_redis is not initialized.")

        _ = await stream_seeder_results(pure_redis, channel, all_urls)

        operResult = OperationResult(
            machine_id=monitor.machine_id,
            operation_id=operation_id,
            start_time=start_time,
            end_time=stats.end_time,
            duration=stats.duration,
            peak_memory=stats.peak_mem_mb,
            memory_used=stats.mem_delta_mb,
            status=TaskStatus.COMPLETED,
            urls_processed=len(all_urls),
        )

        await asyncio.gather(
            redis.hset(
                f"task:{task_id}",
                values={
                    "status": TaskStatus.COMPLETED.value,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                    "result": json.dumps(all_urls, ensure_ascii=False),
                },
            ),
            monitor.record_metrics(operResult),
        )
        await asyncio.sleep(5)  # Give Redis time to process the update

        # FIXME: Update operation status CHECK THE RESULTS
        if all_urls and task_id and uid and uid != "jwt_disabled" and operation_id:
            # TODO: FIREBASE UPDATE - change to redis update
            await updateCrawlOperation(
                uid,
                operation_id,
                {
                    "status": TaskStatus.COMPLETED,
                    "color": task_status_color(TaskStatus.COMPLETED),
                    "task_id": task_id,
                    # "storage": results,
                },
                db,
            )
        # return {"success": True, "message": "Multi-domain research completed successfully."}

    except Exception as e:
        logger.error(f"Error in multi-domain research task: {e}", exc_info=True)
        # if 'crawler' in locals() and crawler.ready: # Check if crawler was initialized and started
        # Record the end time for error handling
        end_time = time.time()

        # Handle exceptions and record error metrics
        operResult = OperationResult(
            machine_id=monitor.machine_id,
            operation_id=operation_id,
            error=str(e),
            status=TaskStatus.FAILED,
            duration=end_time - start_time,
        )

        if task_id and uid and uid != "jwt_disabled" and operation_id:
            print(f"Error updating operation: {e}")

            # TODO: FIREBASE UPDATE - change to redis update
            # Update the operation status in the fire database
            await updateCrawlOperation(
                uid,
                operation_id,
                {
                    "status": TaskStatus.FAILED,
                    "color": task_status_color(TaskStatus.FAILED),
                    "task_id": task_id,
                    "error": str(e),
                },
                db,
            )

        # Execute error handling operations in parallel
        await asyncio.gather(
            # Record error metrics
            monitor.record_metrics(operResult),
            redis.hset(
                f"task:{task_id}",
                values={
                    "status": TaskStatus.FAILED,
                    "error": str(e),
                },
            ),
        )
        raise


async def _crawl_task_impl(self, urls: List[str], browser_config: Dict, crawler_config: Dict):
    """Internal implementation of the crawl task."""
    task_id = self.request.id
    logger.info(f"Celery task {task_id} started.")
    sign = "no_signature"

    try:
        browser = BrowserConfig.load(browser_config)
        crawler, sign = await get_crawler(browser)  # get_crawler returns crawler and signature

        # init client firebase
        # db = await get_firebase_client()

        # # set monitor pid
        # monitor = WorkerMonitor(os.getpid(), db)
        # monitor.record_operation_start()

        # Check for cancellation
        # if self.request.aborted:
        #     await redis.hset(f"task:{task_id}", values={"status": TaskStatus.CANCELED})
        #     return {"status": TaskStatus.CANCELED, "message": "Task aborted by user."}
        pipe = redis.pipeline()
        pipe.hset(
            f"task:{task_id}",
            values={
                "signature": sign if sign else "no_signature"  # Store signature if available
            },
        )

        results, total_time, mem_delta_mb, peak_mem_mb = await handle_crawl_request(
            urls=urls,
            crawler=crawler,
            crawler_config=crawler_config,
            config=config,
        )

        crawled = {
            "success": True,
            "results": results,
            "server_processing_time_s": total_time,
            "server_memory_delta_mb": mem_delta_mb,
            "server_peak_memory_mb": peak_mem_mb,
        }
        # Check if result is a dict
        if isinstance(crawled, Dict):
            logger.info(f"Result is a dict with keys: {list(crawled.keys())}")
        else:
            logger.info(f"Result type: {type(crawled)}")

        pipe.hset(
            f"task:{task_id}",
            values={
                "signature": sign if sign else "no_signature",  # Store signature if available
                "status": TaskStatus.COMPLETED,
                "result": json.dumps(crawled),  # Ensure result is a string
                "updated_at": datetime.now(timezone.utc).isoformat(),  # Store update time
            },
        )

        await pipe.exec()

        await cancel_crawler(sign)  # Remove the crawler to free resources

        await asyncio.sleep(5)  # Give Redis time to process the update

        # return {"success": True, "results": crawled}

    except RedisError as e:
        logger.error(f"Redis error in crawl task: {str(e)}", exc_info=True)
        await redis.hset(
            f"task:{task_id}",
            values={
                "status": TaskStatus.FAILED,
                "error": str(e),
            },
        )
        await cancel_crawler(sign)  # Remove the crawler to free resources
        raise

    except Exception as e:
        logger.error(f"Celery crawl task error: {str(e)}", exc_info=True)
        await redis.hset(
            f"task:{task_id}",
            values={
                "status": TaskStatus.FAILED,
                "error": str(e),
            },
        )
        await cancel_crawler(sign)  # Remove the crawler to free resources
        raise


async def _llm_extraction_task_impl(self, url, instruction, schema, cache):
    task_id = self.request.id
    try:
        # Check for cancellation
        if self.request.aborted:
            await redis.hset(f"task:{task_id}", values={"status": TaskStatus.CANCELED})
            return {"status": TaskStatus.CANCELED, "message": "Task aborted by user."}

        api_key = ""
        if "api_key" in config["llm"]:
            api_key = config["llm"]["api_key"]
        else:
            api_key = os.environ.get(config["llm"].get("api_key_env", None), "")

        # Ensure schema is always a dict (empty if not provided)
        schema_dict = {}
        if schema:
            try:
                schema_dict = json.loads(schema)
                if not isinstance(schema_dict, dict):
                    raise ValueError("Schema must be a JSON object")
            except Exception as e:
                logger.error(f"Invalid schema JSON: {e}")
                schema_dict = {}

        llm_strategy = LLMExtractionStrategy(
            llm_config=LLMConfig(provider=config["llm"]["provider"], api_token=api_key),
            instruction=instruction,
            schema=schema_dict,  # Always a dict
        )

        cache_mode = CacheMode.ENABLED if cache == "1" else CacheMode.WRITE_ONLY

        async with AsyncWebCrawler() as crawler:
            result = cast(
                CrawlResult,
                await crawler.arun(
                    url=url,
                    config=CrawlerRunConfig(
                        extraction_strategy=llm_strategy, scraping_strategy=LXMLWebScrapingStrategy(), cache_mode=cache_mode
                    ),
                ),
            )

        if not result.success:
            await redis.hset(
                f"task:{task_id}",
                values={
                    "status": TaskStatus.FAILED.value,  # Convert Enum to string
                    "error": result.error_message or "",  # Ensure string
                },
            )
            return {"success": False, "error": result.error_message}

        try:
            content = json.loads(result.extracted_content) if result.extracted_content else None  # Handle None
        except json.JSONDecodeError:
            content = result.extracted_content

        await redis.hset(
            f"task:{task_id}",
            values={
                "status": TaskStatus.COMPLETED.value,  # Convert Enum to string
                "result": json.dumps(content) if content is not None else "",  # Ensure string
            },
        )
        return {"success": True, "result": content}

    except Exception as e:
        logger.error(f"Celery LLM extraction task error: {str(e)}", exc_info=True)
        await redis.hset(f"task:{task_id}", values={"status": TaskStatus.FAILED, "error": str(e)})
        raise


async def handle_crawl_request(urls: List[str], crawler: AsyncWebCrawler, crawler_config: dict, config: dict) -> tuple:
    """Handle non-streaming crawl requests."""

    start_mem_mb = _get_memory_mb()  # <--- Get memory before
    start_time = time.time()
    mem_delta_mb = None
    peak_mem_mb = start_mem_mb
    try:
        urls = [("https://" + url) if not url.startswith(("http://", "https://")) else url for url in urls]

        crawler_conf = CrawlerRunConfig.load(crawler_config)

        dispatcher = MemoryAdaptiveDispatcher(
            memory_threshold_percent=config["crawler"]["memory_threshold_percent"],
            rate_limiter=RateLimiter(base_delay=tuple(config["crawler"]["rate_limiter"]["base_delay"]))
            if config["crawler"]["rate_limiter"]["enabled"]
            else None,
        )

        base_config = config["crawler"]["base_config"]
        # Iterate on key-value pairs in global_config then use haseattr to set them
        for key, value in base_config.items():
            if hasattr(crawler_conf, key):
                setattr(crawler_conf, key, value)

        results = []
        func = getattr(crawler, "arun" if len(urls) == 1 else "arun_many")
        partial_func = partial(func, urls[0] if len(urls) == 1 else urls, config=crawler_conf, dispatcher=dispatcher)

        results = await partial_func()

        # await crawler.close()

        end_mem_mb = _get_memory_mb()  # <--- Get memory after
        end_time = time.time()
        total_time = end_time - start_time

        if start_mem_mb is not None and end_mem_mb is not None:
            mem_delta_mb = end_mem_mb - start_mem_mb  # <--- Calculate delta
            peak_mem_mb = max(peak_mem_mb if peak_mem_mb else 0, end_mem_mb)  # <--- Get peak memory
            logger.info(
                f"Memory usage: Start: {start_mem_mb} MB, End: {end_mem_mb} MB, Delta: {mem_delta_mb} MB, Peak: {peak_mem_mb} MB, Total Time: {total_time}"
            )

        return (
            [
                {
                    "url": url,
                    "dump": result.model_dump() if hasattr(result, "model_dump") else "No model_dump available",
                }
                for url, result in zip(urls, results, strict=True)
            ],
            total_time,
            mem_delta_mb,
            peak_mem_mb,
        )

    except Exception as e:
        logger.error(f"Crawl error: {str(e)}", exc_info=True)
        if "crawler" in locals() and crawler.ready:  # Check if crawler was initialized and started
            try:
                await crawler.close()
            except Exception as e:
                logger.error(f"Error closing crawler during exception handling: {str(e)}")
                raise e

        # Measure memory even on error if possible
        end_mem_mb_error = _get_memory_mb()
        if start_mem_mb is not None and end_mem_mb_error is not None:
            mem_delta_mb = end_mem_mb_error - start_mem_mb

        error_detail = json.dumps(
            {  # Send structured error
                "error": str(e),
                "server_memory_delta_mb": mem_delta_mb,
                "server_peak_memory_mb": max(peak_mem_mb if peak_mem_mb else 0, end_mem_mb_error or 0),
            }
        )

        raise RuntimeError(f"Crawl failed: {error_detail}") from e


async def _crawl_stream_task_impl(self, uid: str, operation_id: str, urls: List[str], browser_config: dict, crawler_config: dict):
    """Handle streaming crawl requests."""
    # set task id
    task_id = self.request.id

    start_time = time.time()
    logger.info(f"Celery task {task_id} started.")
    stats = SystemTaskStats()
    # set browser signature
    sign = "no_signature"

    try:
        # Initialize Firebase client
        db = await get_firebase_client()

        # Set up monitor with process ID and Firebase client
        monitor = WorkerMonitor(os.getpid(), db)

        # Update system metrics and record operation start in parallel
        await asyncio.gather(monitor.update_metrics(), monitor.record_operation_start(operation_id, start_time))

        # load browser configuration
        browser = BrowserConfig.load(browser_config)
        # get browser
        crawler, sign = await get_crawler(browser)  # get_crawler returns crawler and signature

        # update the task in redis to report the signature of the browser will be used
        await redis.hset(
            f"task:{task_id}",
            values={
                "signature": sign if sign else "no_signature"  # Store signature if available
            },
        )

        async with measure_stats(stats, logger, label="_crawl_stream_task_impl"):
            # handle stream crawl request
            results_gen = await handle_stream_crawl_request(
                urls=urls,
                crawler=crawler,
                _browser=browser,
                _crawler_config=crawler_config,
                config=config,  # this is the server config yml file data
            )
            """Handle streaming crawl requests."""

            # Stream results to Redis Pub/Sub
            channel = f"{REDIS_CHANNEL}:{task_id}"

            if pure_redis is None:
                raise ValueError("pure_redis is not initialized.")

            _ = await stream_add_crawl_results(pure_redis, channel, results_gen)

        operResult = OperationResult(
            machine_id=monitor.machine_id,
            operation_id=operation_id,
            start_time=start_time,
            end_time=stats.end_time,
            duration=stats.duration,
            peak_memory=stats.peak_mem_mb,
            memory_used=stats.mem_delta_mb,
            status=TaskStatus.COMPLETED,
            urls_processed=len(urls),
        )

        # Record completed operation metrics and release crawler resources in parallel
        await asyncio.gather(
            redis.hset(
                f"task:{task_id}",
                values={
                    "signature": sign if sign else "no_signature",  # Store signature if available
                    "status": TaskStatus.COMPLETED.value,
                    "result": "",  # json.dumps(model_dump_list, default=datetime_handler),  # Ensure result is a string
                    "updated_at": datetime.now(timezone.utc).isoformat(),  # Store update time
                    # "server_processing_time_s": total_time,
                    # "server_memory_delta_mb": mem_delta_mb,
                    # "server_peak_memory_mb": peak_mem_mb
                },
            ),
            monitor.record_metrics(operResult),
            # cancel_crawler(sign)  # Remove the crawler to free resources
        )
        await asyncio.sleep(5)  # Give Redis time to process the update

        # FIXME: Update operation status CHECK THE RESULTS
        if results_gen and task_id and uid and uid != "jwt_disabled" and operation_id:
            # TODO: FIREBASE UPDATE - change to redis update
            await updateCrawlOperation(
                uid,
                operation_id,
                {
                    "status": TaskStatus.COMPLETED,
                    "color": task_status_color(TaskStatus.COMPLETED),
                    "task_id": task_id,
                    # "storage": results,
                },
                db,
            )

    # except RedisError as e:
    #     logger.error(f"Redis error in crawl task: {str(e)}", exc_info=True)
    #     await redis.hset(f"task:{task_id}", values={
    #         "status": TaskStatus.FAILED,
    #         "error": str(e),
    #     })
    #     await cancel_crawler(sign)  # Remove the crawler to free resources
    #     raise

    except Exception as e:
        logger.error(f"Celery crawl task error: {str(e)}", exc_info=True)
        # if 'crawler' in locals() and crawler.ready: # Check if crawler was initialized and started
        # Record the end time for error handling
        end_time = time.time()

        # Handle exceptions and record error metrics
        operResult = OperationResult(
            machine_id=monitor.machine_id,
            operation_id=operation_id,
            error=str(e),
            status=TaskStatus.FAILED,
            duration=end_time - start_time,
        )

        if task_id and uid and uid != "jwt_disabled" and operation_id:
            print(f"Error updating operation: {e}")

            # TODO: FIREBASE UPDATE - change to redis update
            # Update the operation status in the fire database
            await updateCrawlOperation(
                uid,
                operation_id,
                {
                    "status": TaskStatus.FAILED,
                    "color": task_status_color(TaskStatus.FAILED),
                    "task_id": task_id,
                    "error": str(e),
                },
                db,
            )

        # Execute error handling operations in parallel
        await asyncio.gather(
            # Record error metrics
            monitor.record_metrics(operResult),
            redis.hset(
                f"task:{task_id}",
                values={
                    "status": TaskStatus.FAILED,
                    "error": str(e),
                },
            ),
            cancel_crawler(sign),  # Remove the crawler to free resources
        )
        raise


async def handle_stream_crawl_request(
    urls: List[str], crawler: AsyncWebCrawler, _browser: BrowserConfig, _crawler_config: dict, config: dict
) -> AsyncGenerator:
    start_mem_mb = _get_memory_mb()  # <--- Get memory before
    try:
        browser_config: BrowserConfig = _browser
        # Set to False or remove for production stress testing
        browser_config.verbose = True
        crawler_config: CrawlerRunConfig = CrawlerRunConfig.load(_crawler_config)
        crawler_config.scraping_strategy = LXMLWebScrapingStrategy()
        crawler_config.stream = True

        dispatcher = MemoryAdaptiveDispatcher(
            memory_threshold_percent=config["crawler"]["memory_threshold_percent"],
            rate_limiter=RateLimiter(base_delay=tuple(config["crawler"]["rate_limiter"]["base_delay"])),
        )
        base_config = config["crawler"]["base_config"]
        # Iterate on key-value pairs in global_config then use haseattr to set them
        for key, value in base_config.items():
            if hasattr(crawler_config, key):
                setattr(crawler_config, key, value)

        results_gen: AsyncGenerator = cast(
            AsyncGenerator, await crawler.arun_many(urls=urls, config=crawler_config, dispatcher=dispatcher)
        )

        return results_gen
    except Exception as e:
        # Make sure to close crawler if started during an error here
        if "crawler" in locals() and crawler.ready:
            logger.error(f"Error closing crawler during stream setup exception: {str(e)}")
        logger.error(f"Stream crawl error: {str(e)}", exc_info=True)

        # Measure memory even on error if possible
        end_mem_mb_error = _get_memory_mb()
        if start_mem_mb is not None and end_mem_mb_error is not None:
            mem_delta_mb = end_mem_mb_error - start_mem_mb

        # Using a standard exception instead of HTTPException
        error_detail = json.dumps(
            {
                "error": str(e),
                "server_memory_delta_mb": mem_delta_mb,
                # "server_peak_memory_mb": max(peak_mem_mb if peak_mem_mb else 0, end_mem_mb_error or 0)
            }
        )
        raise RuntimeError(f"Stream crawl failed: {error_detail}") from e
