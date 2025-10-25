# ------------- dependency placeholders -------------
import asyncio
import json
import logging
from typing import Annotated, Any, Callable, Dict, Optional, Union

from celery.result import AsyncResult
from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Request,
    Response,
    status,
)
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.security import HTTPAuthorizationCredentials
from pydantic import BaseModel, HttpUrl

from api import (
    cancel_a_job,
    handle_llm_request,
    handle_stream_task_status,
    handle_task_status,
)
from auth import get_token_dependency
from celery_app import celery_app
from configure import config
from redisCache import REDIS_CHANNEL, pure_redis, redis
from utils import decode_redis_hash

logger = logging.getLogger("crawlagent")

# Type definition for the verify_token callable
# VerifyTokenCallable = Union[
#     Callable[[HTTPAuthorizationCredentials], bool],
#     Callable[[], None]
# ]

# public router
job_router = APIRouter()

# ---------- payload models --------------------------------------------------
class LlmJobPayload(BaseModel):
    url:    HttpUrl
    q:      str
    schema_str: Optional[str] = None  # Renamed from 'schema' to 'schema_str'
    cache:  bool = False



# Combine verification code for token/cookie
verify_token = get_token_dependency(config)

# ---------- LL​M job ---------------------------------------------------------
@job_router.post("/llm/job", status_code=202, dependencies=[Depends(verify_token)])
async def llm_job_enqueue(
        payload: LlmJobPayload,
        request: Request
):
    return await handle_llm_request(
        redis,
        request,
        str(payload.url),
        query=payload.q,
        schema=payload.schema_str,  # Updated usage
        cache="1" if payload.cache else "0",
        config=config,
    )

@job_router.get("/llm/job/{task_id}", dependencies=[Depends(verify_token)] )
async def llm_job_status(
    request: Request,
    task_id: str
):
    return await handle_task_status(redis, task_id)


@job_router.put("/{task_id}/cancel", tags=["job", "cancel"], dependencies=[Depends(verify_token)])
async def crawl_job_cancel(
    request: Request,
    task_id: str
):
    if not task_id:
        return JSONResponse(
            status_code=404,
            content={
                "status": "error",
                "error":  "task id required"
            }
        )
    try:
        uid = request.state.uid or "jwt_disabled"
        return await cancel_a_job(redis, uid, task_id)
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "error": "Cannot cancel the job",
                "internal_message": str(e)
            }
        )

@job_router.get("/stream/status/{task_id}", tags=["job", "status", "stream", "crawl", "seeder", "docs"])
async def stream_job_status(
    request: Request,
    task_id: str,
    decoded_token: Dict = Depends(verify_token)
):
    retries = 0
    while retries < 3:
        try:
            task = await redis.hgetall(f"task:{task_id}")
            task = decode_redis_hash(task)
            if task_id == 'empty' or not task:
                retries += 1
                logger.info(f"Task {task_id} not found or empty. Retrying {retries}/3")
                await asyncio.sleep(1)
                continue
            return await handle_stream_task_status(task, task_id, base_url=str(request.base_url))
        except Exception as e:
            logger.error(f"Error fetching task status for task_id: {task_id}. Error: {str(e)}")
            return JSONResponse(
                status_code=500,
                content={
                    "status": "error",
                    "error": "Internal server error while streaming task status",
                    "internal_message": str(e)
                }
            )
    return JSONResponse(
        status_code=404,
        content={
            "status": "error",
            "error": "Task id not found after multiple attempts"
        }
    )


@job_router.get("/stream/{task_id}", tags=["job", "results", "stream", "crawl", "seeder", "docs"])
async def stream_read_db_results(
    task_id: str,
    decoded_token: bool = Depends(verify_token),
    ):
    channel = f"{REDIS_CHANNEL}:{task_id}"
    async def event_stream(channel: str):
        completed_yielded = False
        seen_messages = set()
        retries = 0
        last_id = "0"
        poll_interval = 0.5
        count = 20
        block_ms = 5000
        max_retries = 10
        celery_task = AsyncResult(task_id, app=celery_app)
        try:
            while True:
                try:
                    if (retries > max_retries and not completed_yielded) or (celery_task.ready() and retries > 3):
                        logger.info(f"Task {task_id}: Ending stream after {retries} retries with no activity")
                        break
                    elif retries > max_retries and not completed_yielded and (celery_task.state in {"PENDING", "STARTED"}):
                        retries = 0
                    messages = await pure_redis.xread({channel: last_id}, count, block=block_ms)
                    if messages and isinstance(messages, list):
                        retries = 0
                        logger.info(f"Received messages from Redis: {len(messages)} messages")
                        for _, message_list in messages:
                            for msg_id, msg_data in message_list:
                                last_id = msg_id
                                try:
                                    if isinstance(msg_data, bytes):
                                        msg_data_dict = json.loads(msg_data.decode("utf-8"))
                                    elif isinstance(msg_data, dict):
                                        msg_data_dict = msg_data
                                    else:
                                        logger.warning(f"Unexpected msg_data format: {msg_data}")
                                        continue
                                except json.JSONDecodeError as e:
                                    logger.error(f"JSON decode error: {e} for message: {msg_data}")
                                    continue
                                if msg_data_dict.get("message") == "completed":
                                    if not completed_yielded:
                                        logger.info(f"Task {task_id}: Yielding completion message")
                                        yield f"data: {json.dumps(msg_data_dict, ensure_ascii=False)}\n\n".encode('utf-8')
                                        completed_yielded = True
                                        yield b"data: [DONE]\n\n"
                                        return
                                    continue
                                unique_id = (
                                    f"{msg_data_dict.get('chunk_index', '')}_{msg_data_dict.get('url', msg_id)}"
                                    if "chunk_index" in msg_data_dict 
                                    else msg_data_dict.get("id", msg_data_dict.get("url", msg_id))
                                )
                                if unique_id in seen_messages:
                                    continue
                                seen_messages.add(unique_id)
                                yield f"data: {json.dumps(msg_data_dict, ensure_ascii=False)}\n\n".encode('utf-8')
                    else:
                        retries += 1
                        logger.warning(f"No messages returned or malformed response. Retry count: {retries}")
                except Exception as e:
                    logger.exception("Error reading from Redis stream")
                    yield f"event: error\ndata: {json.dumps({'error': 'stream read error'})}\n\n".encode('utf-8')
                    retries += 1
                await asyncio.sleep(poll_interval)
        except asyncio.CancelledError:
            logger.info("Task %s: Stream cancelled by client", task_id)
            yield b"event: canceled\ndata: {\"message\":\"stream_cancelled\"}\n\n"
        except Exception as e:
            logger.exception("Fatal error in event stream")
            yield f"event: error\ndata: {json.dumps({'error': 'fatal stream error', 'fatal': True})}\n\n".encode('utf-8')
        finally:
            seen_messages.clear()
            if not completed_yielded:
                yield b"data: [DONE]\n\n"
            logger.info(f"Task {task_id}: Stream closed")
    return StreamingResponse(
        event_stream(channel),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
            "X-Stream-Status": "active",
        })