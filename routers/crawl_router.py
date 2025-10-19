import logging
from typing import Dict

from celery import uuid
from celery.result import AsyncResult
from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, HttpUrl

from api import (
    handle_crawl_job,
    handle_crawl_stream_job,
    handle_markdown_request,
    handle_task_status,
)
from auth import get_token_dependency
from config import config
from crawl import reader
from redisCache import REDIS_CHANNEL, pure_redis, redis
from schemas import CrawlConfigValidRequest, CrawlRequest, MarkdownRequest, RawCode
from triggers import event_stream
from utils import is_config_valid, safe_eval_config

logger = logging.getLogger("crawlagent")

crawl_router = APIRouter()

verify_token = get_token_dependency(config)


class CrawlJobPayload(BaseModel):
    urls:           list[HttpUrl]
    browser_config: Dict = {}
    crawler_config: Dict = {}



# ---------- Temporary job ---------------------------------------------------------
@crawl_router.post("/", tags=["crawl"])
async def crawl(
    request: Request, 
    response: Response, 
    decoded_token: bool = Depends(verify_token)
):
    try:
        return await reader(request, response)
    except HTTPException as e:
        return {"error": str(e.detail)}
    except Exception as e:
        logger.warning(f"An unexpected error occurred: {e}")
        raise HTTPException(status_code=500, detail="Internal server error") from e
    

@crawl_router.post("/config/dump")
async def config_dump(
     request: Request,
     raw: RawCode,
     decoded_token: bool = Depends(verify_token)
     ):
    try:
        return JSONResponse(safe_eval_config(raw.code.strip()))
    except Exception as e:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, str(e)) from e

@crawl_router.post("/config/validation")
async def config_validation(
    request: Request,
    raw: CrawlConfigValidRequest,
    decoded_token: bool = Depends(verify_token)
):
    try:
        browser_config = raw.browser_config
        crawler_config = raw.crawler_config
        seeder_config = raw.seeder_config
        if not browser_config and not crawler_config and not seeder_config:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, 
                                "At least one of browser_config or crawler_config or seeder_config must be " \
                                "provided in the request body")

        return is_config_valid(browser_config, crawler_config, seeder_config)
    except Exception as e:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, str(e)) from e


# ---------- Crawl jobs ---------------------------------------------------------
@crawl_router.post("/md", tags=["crawl"])
async def get_markdown(
    request: Request,
    body: MarkdownRequest,
    decoded_token: bool = Depends(verify_token),
):
    logger.info(f"Received request: {request.method} {request.url}")
    if not body.urls:
        raise HTTPException(400, "At least one URL required")
    for url in body.urls:
        if not url.startswith(("http://", "https://")):
            raise HTTPException(400, "URL must be absolute and start with http/https")
    markdowns, server_processing_time_s, server_memory_delta_mb, server_peak_memory_mb = await handle_markdown_request(
        body.urls, body.f, body.q, body.c if body.c is not None else "0", config, body.browser_config or None
    )
    return JSONResponse({
        "results": markdowns,
        "filter": body.f,
        "query": body.q,
        "cache": body.c,
        "server_processing_time_s": server_processing_time_s,
        "server_memory_delta_mb": server_memory_delta_mb,
        "server_peak_memory_mb": server_peak_memory_mb,
        "success": True
    })

@crawl_router.post("/job", status_code=202, tags=["crawl"])
async def crawl_job_enqueue(
        request: Request,
        payload: CrawlJobPayload,
        decoded_token: bool = Depends(verify_token)
):
    return await handle_crawl_job(
        redis,
        [str(u) for u in payload.urls],
        payload.browser_config,
        payload.crawler_config,
        config=config or {},
    )

@crawl_router.get("/job/status/{task_id}", tags=["crawl"])
async def crawl_job_status(
    request: Request,
    task_id: str,
    decoded_token: Dict = Depends(verify_token)
):
    return await handle_task_status(redis, task_id, base_url=str(request.base_url))

@crawl_router.post("/stream/job", status_code=202, tags=["crawl"])
async def crawl_stream_job_enqueue(
    request: Request,
    payload: CrawlRequest,
    decoded_token: Dict = Depends(verify_token),
):
    if not payload.urls:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "At least one URL required")
    # if not payload.temp_task_id:
    #     raise HTTPException(status.HTTP_400_BAD_REQUEST, "temp task id missing, is required")
    if not payload.operation_data:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "operation data missing, is required")
    if not payload.browser_config:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "browser configuration is missing, is required")
    if not payload.crawler_config:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "crawler configuration data missing, is required")
    try:
        uid = decoded_token.get("uid") or "jwt_disabled"
        urls = [str(u) for u in payload.urls]
        # temp_task_id = payload.temp_task_id
        operation_data = payload.operation_data
        return await handle_crawl_stream_job(
                # temp_task_id,
                redis,
                uid=uid,
                base_url=str(request.base_url),
                urls=urls,
                operation_data = operation_data,
                browser_config = payload.browser_config,
                crawler_config = payload.crawler_config,
                config=config or {}
            )
    except Exception as e:
        raise HTTPException(500, str(e)) from e

@crawl_router.post("/stream", tags=["crawl"])
async def stream(request: Request,
                 decoded_token: bool = Depends(verify_token)
):
    try:
        data = await request.json()
        url = data.get("url")
        return StreamingResponse(event_stream(url), media_type="text/event-stream")
    except HTTPException as e:
        return {"error": str(e.detail)}
    except Exception as e:
        logger.warning(f"An unexpected error occurred: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
