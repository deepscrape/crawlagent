import logging
import uuid
from typing import Any, Dict, List, Optional

from crawl4ai import SeedingConfig
from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field, ValidationError

from api import multi_domain_research_job
from apps.seeder import SeederRequest
from auth import get_token_dependency
from config import config
from redisCache import REDIS_CHANNEL, pure_redis, redis
from schemas import CrawlOperation

logger = logging.getLogger("crawlagent")
seeder_router = APIRouter()


verify_token = get_token_dependency(config)


# --- Endpoint: Multi-Domain Discovery ---
@seeder_router.post("/stream/job/multi-research", tags=["seeder"])
async def multi_research(
    request: Request,
    seeder: SeederRequest,
    token: Any = Depends(verify_token)  # noqa: B008
):
    """
    Multi-domain research: discover and rank URLs across multiple domains.
    """
    try:
        domains = seeder.domains
        if not domains or not isinstance(domains, list) or len(domains) == 0:
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content={
                    "status": "error",
                    "error": "no domains provided",
                    "internal_message": "The 'domains' field must be a non-empty list."
                }
            )
        
        config_dict = seeder.config.model_dump()

        uid = token.get("uid") or "jwt_disabled"

        if uid == "jwt_disabled" or not config["security"].get("jwt_enabled", True):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Authentication required."
            )

        logger.info(f"Received multi-domain research request for domains: {domains} with config: {config_dict}")

        return await multi_domain_research_job(
            redis=redis,
            uid=uid,
            domains=domains,
            config=config_dict,
            base_url=str(request.base_url),
            operation_data = seeder.operation_data.model_dump(),
            task_options=None  # Provide appropriate task options here
        )
    except HTTPException as e:
        return JSONResponse(
            status_code=e.status_code,
            content={
                "status": "error",
                "error": e.detail,
                "internal_message": str(e.detail)
            }
        )
    
    except ValidationError as e:
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content={
                "status": "error",
                "error":  "invalid configuration or invalid domains",
                "internal_message": str(e)
            }
        )
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "status": "error",
                "error": "internal server error",
                "internal_message": str(e)
            }
        )





# --- Endpoint: Comprehensive Validation ---
# @seeder_router.post("/comprehensive-validation", tags=["seeder"],response_model=TaskStatusResponse)
# async def comprehensive_validation_endpoint(
#     domain: str,
#     config: Dict[str, Any],
#     token: Any = Depends(verify_token)
# ):
#     """
#     Combined validation and metadata extraction for a domain.
#     """
#     task_id = submit_celery_task(comprehensive_validation_task, domain, config)
#     return {"task_id": task_id, "status": "pending"}

# # --- Endpoint: Pattern Filtering ---
# @seeder_router.post("/pattern-filtering", tags=["seeder"], response_model=TaskStatusResponse)
# async def pattern_filtering_endpoint(
#     domain: str,
#     config: Dict[str, Any],
#     token: Any = Depends(verify_token)
# ):
#     """
#     Pattern-based URL filtering for a domain.
#     """
#     task_id = submit_celery_task(pattern_filtering_task, domain, config)
#     return {"task_id": task_id, "status": "pending"}

# # --- Endpoint: Performance Tuning ---
# @seeder_router.post("/performance-tuning", tags=["seeder"], response_model=TaskStatusResponse)
# async def performance_tuning_endpoint(
#     domain: str,
#     config: Dict[str, Any],
#     token: Any = Depends(verify_token)
# ):
#     """
#     Performance-optimized seeding for large domains.
#     """
#     task_id = submit_celery_task(performance_tuning_task, domain, config)
#     return {"task_id": task_id, "status": "pending"}

# # --- Endpoint: Large Domain Processing ---
# @seeder_router.post("/large-domain-processing", tags=["seeder"], response_model=TaskStatusResponse)
# async def large_domain_processing_endpoint(
#     domain: str,
#     config: Dict[str, Any],
#     token: Any = Depends(verify_token)
# ):
#     """
#     Memory-safe processing for very large domains.
#     """
#     task_id = submit_celery_task(large_domain_processing_task, domain, config)
#     return {"task_id": task_id, "status": "pending"}

# # --- Endpoint: Metadata Extraction ---
# @seeder_router.post("/metadata-extraction", tags=["seeder"], response_model=TaskStatusResponse)
# async def metadata_extraction_endpoint(
#     domain: str,
#     config: Dict[str, Any],
#     token: Any = Depends(verify_token)
# ):
#     """
#     Extract metadata for URLs in a domain.
#     """
#     task_id = submit_celery_task(metadata_extraction_task, domain, config)
#     return {"task_id": task_id, "status": "pending"}

# # --- Endpoint: Metadata Filtering ---
# @seeder_router.post("/metadata-filtering", tags=["seeder"], response_model=TaskStatusResponse)
# async def metadata_filtering_endpoint(
#     domain: str,
#     config: Dict[str, Any],
#     token: Any = Depends(verify_token)
# ):
#     """
#     Filter URLs by metadata (e.g., recent articles).
#     """
#     task_id = submit_celery_task(metadata_filtering_task, domain, config)
#     return {"task_id": task_id, "status": "pending"}

# # --- Endpoint: Relevance Scoring ---
# @seeder_router.post("/relevance-scoring", tags=["seeder"], response_model=TaskStatusResponse)
# async def relevance_scoring_endpoint(
#     domain: str,
#     config: Dict[str, Any],
#     token: Any = Depends(verify_token)
# ):
#     """
#     BM25 relevance scoring for a domain.
#     """
#     task_id = submit_celery_task(relevance_scoring_task, domain, config)
#     return {"task_id": task_id, "status": "pending"}

# # --- Endpoint: URL-Based Scoring ---
# @seeder_router.post("/url-based-scoring", tags=["seeder"], response_model=TaskStatusResponse)
# async def url_based_scoring_endpoint(
#     domain: str,
#     config: Dict[str, Any],
#     token: Any = Depends(verify_token)
# ):
#     """
#     Fast URL-based scoring for a domain.
#     """
#     task_id = submit_celery_task(url_based_scoring_task, domain, config)
#     return {"task_id": task_id, "status": "pending"}

# # --- Endpoint: Complex Queries ---
# @seeder_router.post("/complex-queries", tags=["seeder"], response_model=TaskStatusResponse)
# async def complex_queries_endpoint(
#     domain: str,
#     queries: List[str],
#     config: Dict[str, Any],
#     token: Any = Depends(verify_token)
# ):
#     """
#     Multi-concept queries for a domain.
#     """
#     task_id = submit_celery_task(complex_queries_task, domain, queries, config)
#     return {"task_id": task_id, "status": "pending"}

# # --- Endpoint: Live URL Validation ---
# @seeder_router.post("/url-validation", tags=["seeder"], response_model=TaskStatusResponse)
# async def url_validation_endpoint(
#     urls: List[str],
#     config: Dict[str, Any],
#     token: Any = Depends(verify_token)
# ):
#     """
#     Live validation of a list of URLs.
#     """
#     task_id = submit_celery_task(url_validation_task, urls, config)
#     return {"task_id": task_id, "status": "pending"}

# # --- Endpoint: Task Status ---
# @seeder_router.get("/status/{task_id}", tags=["seeder"], response_model=TaskStatusResponse)
# async def seeder_task_status(task_id: str, token: Any = Depends(verify_token)):
#     """
#     Get status and result of a seeder task.
#     """
#     return get_celery_task_status(task_id)