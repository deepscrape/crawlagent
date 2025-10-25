import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from functools import wraps
from typing import Any, Dict, List, Optional

import aioboto3
import pydantic
import zstandard as zstd
from botocore.config import Config
from botocore.exceptions import ClientError
from fastapi.responses import StreamingResponse

from configure import config
from redisCache import redis as _redis_client

# Set up logging
logger = logging.getLogger("crawlagent")

# Load storage configuration from config.yml
storage_config = config.get("storage", {})
primary_config = storage_config.get("primary", {})
backup_config = storage_config.get("backup", {})
pool_config = primary_config.get("pool", {})
class TigrisBucketResult(pydantic.BaseModel):
    key_name: str
    file_name: str
    file_size: float = pydantic.Field(
        ge=0
    )  # ensures file size is non-negative in kilobytes
    file_compressed_size: float = pydantic.Field(ge=0)
    created_at: datetime = pydantic.Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: Optional[datetime] = pydantic.Field(default_factory=lambda: datetime.now(timezone.utc))

    def to_str(self) -> str:
        """Return a formatted string representation"""
        return (
            f"File: {self.file_name}\n"
            f"Key: {self.key_name}\n"
            f"Size: {self.file_size} KB\n"
            f"Created: {self.created_at.strftime('%Y-%m-%d %H:%M:%S')}"
        )

    def to_json(self) -> str:
        """Return JSON string"""
        return self.model_dump_json()

    def to_dict(self) -> Dict[str, Any]:
        """Return dictionary representation"""
        return self.model_dump()

# Tigris Buckets configuration - Load from config.yml with fallbacks
TIGRIS_BUCKET_NAME = primary_config.get("bucket", os.getenv("TIGRIS_BUCKET", "crawlagent.bucket.a"))
TIGRIS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
TIGRIS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
TIGRIS_ENDPOINT_URL = primary_config.get("endpoint", os.getenv("AWS_ENDPOINT_URL_S3", "https://fly.storage.tigris.dev"))
TIGRIS_REGION = primary_config.get("region", "auto")

# Connection pool configuration - Load from config.yml
POOL_SIZE = pool_config.get("max_connections", 20) 
CONCURRENT_REQUEST_LIMIT = pool_config.get("concurrent_request_limit", 100)

# Cache configuration - Load from config.yml
cache_config = storage_config.get("cache", {})
CACHE_ENABLED = cache_config.get("enabled", True)
CACHE_TTL = cache_config.get("ttl", int(os.getenv("STORAGE_CACHE_TTL", "300")))
CACHE_PREFIX = cache_config.get("prefix", "storage")

# Retry configuration - Load from config.yml
retry_config = storage_config.get("retry", {})
MAX_RETRY_ATTEMPTS = retry_config.get("max_attempts", 3)
RETRY_BASE_DELAY = retry_config.get("base_delay", 1.0)
RETRY_MAX_DELAY = retry_config.get("max_delay", 10.0)
RETRY_EXPONENTIAL_BASE = retry_config.get("exponential_base", 2)

# Monitoring configuration - Load from config.yml
monitoring_config = storage_config.get("monitoring", {})
MONITORING_ENABLED = monitoring_config.get("enabled", True)
METRICS_ENABLED = monitoring_config.get("metrics_enabled", True)
HEALTH_CHECK_ENABLED = monitoring_config.get("health_check_enabled", True)

# Compression configuration - Load from config.yml
compression_config = storage_config.get("compression", {})
COMPRESSION_ENABLED = compression_config.get("enabled", True)
COMPRESSION_LEVEL = compression_config.get("level", 3)
COMPRESSION_MIN_SIZE = compression_config.get("min_size", 1024)

redis_client = None 

# Configure boto3 with connection pooling and retries
boto_config = Config(
    region_name=TIGRIS_REGION,
    max_pool_connections=POOL_SIZE,
    retries={
        'max_attempts': MAX_RETRY_ATTEMPTS,
        'mode': 'adaptive'
    }
)

# Create connection pool and semaphore for rate limiting
session = aioboto3.Session()
_client_pool = []
_pool_semaphore = asyncio.Semaphore(CONCURRENT_REQUEST_LIMIT)


async def init_redis_cache():
    """Initialize Redis cache for metadata caching."""
    global redis_client
    if redis_client is None:
        try:
            redis_client = _redis_client
            await redis_client.ping()
            logger.info("Redis cache initialized successfully")
        except Exception as e:
            logger.warning(f"Failed to initialize Redis cache: {e}")
            redis_client = None

async def get_cached_metadata(key: str) -> Optional[Dict[str, Any]]:
    """Get cached metadata from Redis."""
    if not redis_client or not CACHE_ENABLED:
        return None
    try:
        cached = await redis_client.get(f"{CACHE_PREFIX}:metadata:{key}")
        if cached:
            storage_metrics["cache_hits"] += 1
            return json.loads(cached)
        else:
            storage_metrics["cache_misses"] += 1
            return None
    except Exception as e:
        logger.warning(f"Cache get error for {key}: {e}")
        storage_metrics["cache_misses"] += 1
        return None

async def set_cached_metadata(key: str, data: Dict[str, Any]):
    """Set cached metadata in Redis."""
    if not redis_client or not CACHE_ENABLED:
        return
    try:
        await redis_client.setex(
            f"{CACHE_PREFIX}:metadata:{key}", 
            CACHE_TTL, 
            json.dumps(data, default=str)
        )
    except Exception as e:
        logger.warning(f"Cache set error for {key}: {e}")

# Metrics tracking
storage_metrics = {
    "requests": 0,
    "errors": 0,
    "cache_hits": 0,
    "cache_misses": 0,
    "avg_response_time": 0.0
}

# Get or create S3 client with proper context management
async def get_s3_client():
    """Get S3 client from connection pool."""
    global _client_pool
    
    # Try to get a client from the pool
    if _client_pool:
        return _client_pool.pop()
    
    # Create a new client if pool is empty
    client = session.client(
        "s3",
        endpoint_url=TIGRIS_ENDPOINT_URL,
        aws_access_key_id=TIGRIS_ACCESS_KEY,
        aws_secret_access_key=TIGRIS_SECRET_KEY,
        config=boto_config
    )
    return await client.__aenter__()

async def return_s3_client(client):
    """Return S3 client to the pool."""
    global _client_pool
    if len(_client_pool) < POOL_SIZE:
        _client_pool.append(client)
    else:
        # Pool is full, close the client
        try:
            await client.__aexit__(None, None, None)
        except Exception:
            pass

# Use this function to create a context manager
class S3ClientManager():
    def __init__(self):
        self.client = None
        
    async def __aenter__(self):
        # Rate limiting
        await _pool_semaphore.acquire()
        self.client = await get_s3_client()
        return self.client
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Return client to pool and release semaphore
        if self.client:
            await return_s3_client(self.client)
        _pool_semaphore.release()

# Retry decorator for S3 operations
def retry_on_failure(max_retries: Optional[int] = None, delay: Optional[float] = None):
    """Decorator to retry failed S3 operations with exponential backoff."""
    # Use config values if not provided
    actual_max_retries = max_retries if max_retries is not None else MAX_RETRY_ATTEMPTS
    actual_delay = delay if delay is not None else RETRY_BASE_DELAY
    
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(actual_max_retries):
                try:
                    start_time = time.time()
                    result = await func(*args, **kwargs)
                    
                    # Update metrics if enabled
                    if METRICS_ENABLED:
                        storage_metrics["requests"] += 1
                        response_time = time.time() - start_time
                        storage_metrics["avg_response_time"] = (
                            storage_metrics["avg_response_time"] * 0.9 + 
                            response_time * 0.1
                        )
                    
                    return result
                except Exception as e:
                    last_exception = e
                    if METRICS_ENABLED:
                        storage_metrics["errors"] += 1
                    
                    if attempt < actual_max_retries - 1:
                        # Exponential backoff with max delay
                        wait_time = min(
                            actual_delay * (RETRY_EXPONENTIAL_BASE ** attempt), 
                            RETRY_MAX_DELAY
                        )
                        logger.warning(f"Attempt {attempt + 1} failed for {func.__name__}: {e}. Retrying in {wait_time}s")
                        await asyncio.sleep(wait_time)
                    else:                        logger.error(f"All {actual_max_retries} attempts failed for {func.__name__}: {e}")
            
            if last_exception is not None:
                raise last_exception
            else:
                raise Exception("Unknown error occurred in retry_on_failure wrapper")
        return wrapper
    return decorator

@retry_on_failure(max_retries=3)
async def folder_exists(folder_name: str) -> bool:
    """Check if a folder exists in Tigris Buckets."""
    try:
        async with S3ClientManager() as svc:
            response = await svc.list_objects_v2(
                Bucket=TIGRIS_BUCKET_NAME, 
                Prefix=f"{folder_name}/", 
                MaxKeys=1
            )
            return "Contents" in response
    except ClientError as e:
        logger.error(f"S3 error checking folder {folder_name}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error checking folder {folder_name}: {e}")
        return False
    
@retry_on_failure(max_retries=3)
async def upload_compressed_file(
    markdown_str: str, folder_name: str, file_name: str
) -> Optional[TigrisBucketResult]:
    """Uploads a Markdown string compressed with zstd to Tigris Buckets."""
    
    file_size_kb = len(markdown_str.encode("utf-8")) / 1024
    key_name = f"{folder_name}/{file_name}"
      # Compress data based on configuration
    try:
        file_size_bytes = len(markdown_str.encode("utf-8"))
        
        if COMPRESSION_ENABLED and file_size_bytes >= COMPRESSION_MIN_SIZE:
            compressor = zstd.ZstdCompressor(level=COMPRESSION_LEVEL)
            compressed_data = compressor.compress(markdown_str.encode("utf-8"))
            file_compressed_size = len(compressed_data) / 1024
            upload_data = compressed_data
            logger.debug(f"File compressed: {file_size_kb:.2f} KB → {file_compressed_size:.2f} KB")
        else:
            # Upload uncompressed if disabled or file too small
            upload_data = markdown_str.encode("utf-8")
            file_compressed_size = file_size_kb
            logger.debug(f"File uploaded uncompressed: {file_size_kb:.2f} KB")
          # Determine if we should use multipart upload (>5MB)
        use_multipart = len(upload_data) > 5 * 1024 * 1024
        
        async with S3ClientManager() as svc:
            if use_multipart:
                # For large files, use multipart upload
                return await _upload_multipart(
                    svc, upload_data, key_name, file_name, file_size_kb, file_compressed_size
                )
            else:
                # For smaller files, use simple put_object
                await svc.put_object(
                    Bucket=TIGRIS_BUCKET_NAME,
                    Key=key_name,
                    Body=upload_data,
                    ContentType="text/markdown",
                )
                logger.info(f"Uploaded {file_name} to {key_name} ({file_size_kb:.2f} KB → {file_compressed_size:.2f} KB)")
                
                return TigrisBucketResult(
                    key_name=key_name,
                    file_name=file_name,
                    file_size=file_size_kb,
                    file_compressed_size=file_compressed_size,
                )
    except ClientError as e:
        logger.error(f"S3 upload error for {key_name}: {e}")
        return None
    except Exception as e:
        logger.error(f"Compression/upload error for {file_name}: {e}")
        return None

async def _upload_multipart(svc, data, key_name, file_name, file_size_kb, file_compressed_size):
    """Helper function for multipart uploads of large files."""
    try:
        # Create multipart upload
        mpu = await svc.create_multipart_upload(
            Bucket=TIGRIS_BUCKET_NAME,
            Key=key_name,
            ContentType="text/markdown"
        )
        
        # Split data into chunks (5MB per chunk)
        chunk_size = 5 * 1024 * 1024
        chunks = [data[i:i+chunk_size] for i in range(0, len(data), chunk_size)]
        
        # Upload parts
        parts = []
        for i, chunk in enumerate(chunks):
            part_number = i + 1
            response = await svc.upload_part(
                Bucket=TIGRIS_BUCKET_NAME,
                Key=key_name,
                PartNumber=part_number,
                UploadId=mpu["UploadId"],
                Body=chunk
            )
            parts.append({
                "PartNumber": part_number,
                "ETag": response["ETag"]
            })
        
        # Complete multipart upload
        await svc.complete_multipart_upload(
            Bucket=TIGRIS_BUCKET_NAME,
            Key=key_name,
            UploadId=mpu["UploadId"],
            MultipartUpload={"Parts": parts}
        )
        
        logger.info(f"Multipart uploaded {file_name} to {key_name} ({file_size_kb:.2f} KB → {file_compressed_size:.2f} KB)")
        
        return TigrisBucketResult(
            key_name=key_name,
            file_name=file_name,
            file_size=file_size_kb,
            file_compressed_size=file_compressed_size,
        )
    except Exception as e:
        logger.error(f"Multipart upload error: {e}")
        # Try to abort the multipart upload to avoid orphaned uploads
        try:
            await svc.abort_multipart_upload(
                Bucket=TIGRIS_BUCKET_NAME,
                Key=key_name,
                UploadId=mpu["UploadId"]
            )
        except Exception as abort_error:
            logger.error(f"Failed to abort multipart upload: {abort_error}")
        return None

@retry_on_failure(max_retries=3)
async def download_file_decompressed(folder_name: str, file_name: str) -> Optional[str]:
    """Downloads and decompresses a file from Tigris Buckets."""
    key_name = f"{folder_name}/{file_name}"
    
    # Implement exponential backoff retry for downloads
    max_retries = 3
    retry_delay = 1  # Start with 1 second delay
    
    for attempt in range(max_retries):
        try:
            async with S3ClientManager() as svc:
                response = await svc.get_object(
                    Bucket=TIGRIS_BUCKET_NAME, 
                    Key=key_name
                )
                compressed_data = await response["Body"].read()
                
                # Decompress the data
                decompressor = zstd.ZstdDecompressor()
                decompressed_data = decompressor.decompress(compressed_data)
                str_file = decompressed_data.decode("utf-8")
                
                logger.info(f"Downloaded and decompressed {file_name} from {key_name}")
                return str_file
                
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                logger.error(f"File not found: {key_name}")
                return None
            logger.warning(f"S3 error on attempt {attempt+1}/{max_retries}: {e}")
        except Exception as e:
            logger.warning(f"Download error on attempt {attempt+1}/{max_retries}: {e}")
        
        # Only sleep if we're going to retry
        if attempt < max_retries - 1:
            await asyncio.sleep(retry_delay)
            retry_delay *= 2  # Exponential backoff
    
    logger.error(f"Download failed after {max_retries} attempts: {key_name}")
    return None

@retry_on_failure(max_retries=3)
async def list_files(folder_name: str) -> List[Dict[str, Any]]:
    """List all files in a folder."""
    try:
        async with S3ClientManager() as svc:
            response = await svc.list_objects_v2(
                Bucket=TIGRIS_BUCKET_NAME,
                Prefix=f"{folder_name}/"
            )
            
            if "Contents" not in response:
                return []
                
            return [
                {
                    "key": obj["Key"],
                    "size": obj["Size"],
                    "last_modified": obj["LastModified"],
                    "file_name": obj["Key"].split("/")[-1]
                }
                for obj in response["Contents"]
            ]
    except Exception as e:
        logger.error(f"Error listing files in {folder_name}: {e}")
        return []


@retry_on_failure(max_retries=3)
async def get_presigned_url(folder_name: str, file_name: str, expiration: int = 3600) -> Optional[str]:
    """
    Generate a presigned URL for direct download of the (compressed) file.
    
    Args:
        folder_name: The folder/prefix containing the file
        file_name: The file name to download
        expiration: URL expiration time in seconds (default 1 hour)
        
    Returns:
        Presigned URL string or None if error
    """
    key_name = f"{folder_name}/{file_name}"
    
    try:
        async with S3ClientManager() as svc:
            # Create the presigned URL
            url = await svc.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': TIGRIS_BUCKET_NAME,
                    'Key': key_name
                },
                ExpiresIn=expiration
            )
            
            logger.info(f"Generated presigned URL for {key_name}, expires in {expiration} seconds")
            return url
            
    except Exception as e:
        logger.error(f"Error generating presigned URL for {key_name}: {e}")
        return None

@retry_on_failure(max_retries=3)
async def download_and_decompress_stream(folder_name: str, file_name: str):
    """
    Downloads and decompresses a file, returning it as a streaming response.
    This function should be used with FastAPI's StreamingResponse.
    
    Args:
        folder_name: The folder/prefix containing the file
        file_name: The file name to download
        
    Returns:
        An async generator yielding decompressed content
    """
    key_name = f"{folder_name}/{file_name}"
    
    async def content_stream():
        try:
            async with S3ClientManager() as svc:
                response = await svc.get_object(
                    Bucket=TIGRIS_BUCKET_NAME,
                    Key=key_name
                )
                
                # Create streaming decompressor
                decompressor = zstd.ZstdDecompressor()
                
                # Read and decompress in chunks
                chunk_size = 1024 * 1024  # 1MB chunks
                while True:
                    chunk = await response["Body"].read(chunk_size)
                    if not chunk:
                        break
                    
                    # Decompress and yield chunk
                    try:
                        decompressed_chunk = decompressor.decompress(chunk)
                        yield decompressed_chunk
                    except Exception as decomp_error:
                        logger.error(f"Decompression error: {decomp_error}")
                        yield "Error: Decompression failed".encode('utf-8')
                        break
                        
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                logger.error(f"File not found: {key_name}")
                yield "Error: File not found".encode('utf-8')
            else:
                logger.error(f"S3 error downloading {key_name}: {e}")
                yield "Error: S3 download failed".encode('utf-8')
        except Exception as e:
            logger.error(f"Error streaming {key_name}: {e}")
            yield "Error: Stream failed".encode('utf-8')
    
    return StreamingResponse(content_stream(), media_type="application/octet-stream")

# ===================== NEW STORAGE FUNCTIONALITIES =====================

@retry_on_failure(max_retries=3)
async def delete_file(folder_name: str, file_name: str) -> bool:
    """Delete a single file from the bucket."""
    key_name = f"{folder_name}/{file_name}"
    try:
        async with S3ClientManager() as svc:
            await svc.delete_object(Bucket=TIGRIS_BUCKET_NAME, Key=key_name)
        logger.info(f"Deleted file {key_name}")
        return True
    except Exception as e:
        logger.error(f"Error deleting file {key_name}: {e}")
        return False

@retry_on_failure(max_retries=3)
async def delete_folder(folder_name: str) -> bool:
    """Delete an entire folder (all objects with the folder prefix)."""
    try:
        async with S3ClientManager() as svc:
            # List all objects in the folder
            response = await svc.list_objects_v2(
                Bucket=TIGRIS_BUCKET_NAME,
                Prefix=f"{folder_name}/"
            )
            
            if "Contents" not in response:
                logger.info(f"Folder {folder_name} is empty or doesn't exist")
                return True
            
            # Delete all objects in batches (up to 1000 per batch)
            objects_to_delete = [{"Key": obj["Key"]} for obj in response["Contents"]]
            
            if objects_to_delete:
                await svc.delete_objects(
                    Bucket=TIGRIS_BUCKET_NAME,
                    Delete={"Objects": objects_to_delete}
                )
                logger.info(f"Deleted folder {folder_name} with {len(objects_to_delete)} objects")
            
        return True
    except Exception as e:
        logger.error(f"Error deleting folder {folder_name}: {e}")
        return False

@retry_on_failure(max_retries=3)
async def get_file_metadata(folder_name: str, file_name: str) -> Optional[Dict[str, Any]]:
    """Retrieve metadata for a file without downloading the content."""
    key_name = f"{folder_name}/{file_name}"
    try:
        async with S3ClientManager() as svc:
            response = await svc.head_object(Bucket=TIGRIS_BUCKET_NAME, Key=key_name)
            
            # Extract useful metadata
            metadata = {
                "key": key_name,
                "size": response.get("ContentLength", 0),
                "last_modified": response.get("LastModified"),
                "content_type": response.get("ContentType"),
                "etag": response.get("ETag"),
                "metadata": response.get("Metadata", {}),
                "file_name": file_name,
                "folder_name": folder_name
            }
            
        logger.info(f"Retrieved metadata for {key_name}")
        return metadata
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            logger.error(f"File not found: {key_name}")
        else:
            logger.error(f"S3 error getting metadata for {key_name}: {e}")
        return None
    except Exception as e:
        logger.error(f"Error getting metadata for {key_name}: {e}")
        return None

@retry_on_failure(max_retries=3)
async def move_file(src_folder: str, src_file: str, dest_folder: str, dest_file: str) -> bool:
    """Move or rename a file within the bucket (copy then delete original)."""
    src_key = f"{src_folder}/{src_file}"
    dest_key = f"{dest_folder}/{dest_file}"
    
    try:
        async with S3ClientManager() as svc:
            # Copy the object
            await svc.copy_object(
                Bucket=TIGRIS_BUCKET_NAME,
                CopySource={'Bucket': TIGRIS_BUCKET_NAME, 'Key': src_key},
                Key=dest_key
            )
            
            # Delete the original
            await svc.delete_object(Bucket=TIGRIS_BUCKET_NAME, Key=src_key)
            
        logger.info(f"Moved {src_key} to {dest_key}")
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            logger.error(f"Source file not found: {src_key}")
        else:
            logger.error(f"S3 error moving {src_key} to {dest_key}: {e}")
        return False
    except Exception as e:
        logger.error(f"Error moving {src_key} to {dest_key}: {e}")
        return False

async def copy_file(src_folder: str, src_file: str, dest_folder: str, dest_file: str) -> bool:
    """Copy a file within the bucket (without deleting original)."""
    src_key = f"{src_folder}/{src_file}"
    dest_key = f"{dest_folder}/{dest_file}"
    
    try:
        async with S3ClientManager() as svc:
            await svc.copy_object(
                Bucket=TIGRIS_BUCKET_NAME,
                CopySource={'Bucket': TIGRIS_BUCKET_NAME, 'Key': src_key},
                Key=dest_key
            )
            
        logger.info(f"Copied {src_key} to {dest_key}")
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            logger.error(f"Source file not found: {src_key}")
        else:
            logger.error(f"S3 error copying {src_key} to {dest_key}: {e}")
        return False
    except Exception as e:
        logger.error(f"Error copying {src_key} to {dest_key}: {e}")
        return False

@retry_on_failure(max_retries=3)
async def get_presigned_upload_url(folder_name: str, file_name: str, expiration: int = 3600) -> Optional[str]:
    """
    Generate a presigned URL for direct upload (PUT) to the bucket.
    Useful for client-side uploads without exposing credentials.
    
    Args:
        folder_name: The folder/prefix for the file
        file_name: The file name to upload
        expiration: URL expiration time in seconds (default 1 hour)
        
    Returns:
        Presigned upload URL string or None if error
    """
    key_name = f"{folder_name}/{file_name}"
    
    try:
        async with S3ClientManager() as svc:
            url = await svc.generate_presigned_url(
                'put_object',
                Params={
                    'Bucket': TIGRIS_BUCKET_NAME,
                    'Key': key_name,
                    'ContentType': 'text/markdown'  # Adjust as needed
                },
                ExpiresIn=expiration
            )
            
        logger.info(f"Generated presigned upload URL for {key_name}, expires in {expiration} seconds")
        return url
    except Exception as e:
        logger.error(f"Error generating presigned upload URL for {key_name}: {e}")
        return None

@retry_on_failure(max_retries=3)
async def get_storage_usage(folder_name: Optional[str] = None) -> Dict[str, Any]:
    """
    Calculate storage usage for a folder or entire bucket.
    
    Args:
        folder_name: Optional folder to calculate usage for (None for entire bucket)
        
    Returns:
        Dictionary with usage statistics
    """
    try:
        async with S3ClientManager() as svc:
            prefix = f"{folder_name}/" if folder_name else ""
            
            response = await svc.list_objects_v2(
                Bucket=TIGRIS_BUCKET_NAME,
                Prefix=prefix
            )
            
            if "Contents" not in response:
                return {
                    "folder_name": folder_name or "entire_bucket",
                    "total_files": 0,
                    "total_size_bytes": 0,
                    "total_size_mb": 0.0,
                    "total_size_gb": 0.0
                }
            
            total_size = sum(obj["Size"] for obj in response["Contents"])
            total_files = len(response["Contents"])
            
            usage_stats = {
                "folder_name": folder_name or "entire_bucket",
                "total_files": total_files,
                "total_size_bytes": total_size,
                "total_size_mb": round(total_size / (1024 * 1024), 2),
                "total_size_gb": round(total_size / (1024 * 1024 * 1024), 4)
            }
            
        logger.info(f"Calculated storage usage for {folder_name or 'entire bucket'}: {usage_stats}")
        return usage_stats
        
    except Exception as e:
        logger.error(f"Error calculating storage usage for {folder_name}: {e}")
        return {
            "folder_name": folder_name or "entire_bucket",
            "error": str(e),
            "total_files": 0,
            "total_size_bytes": 0
        }

async def health_check() -> Dict[str, Any]:
    """
    Check the health and connectivity of the Tigris bucket service.
    
    Returns:
        Dictionary with health status and basic bucket info
    """
    try:
        async with S3ClientManager() as svc:
            # Try a simple head_bucket operation
            await svc.head_bucket(Bucket=TIGRIS_BUCKET_NAME)
            
            # Get basic bucket info
            response = await svc.list_objects_v2(
                Bucket=TIGRIS_BUCKET_NAME,
                MaxKeys=1  # Just need to know if we can list
            )
            
            health_info = {
                "status": "healthy",
                "bucket_name": TIGRIS_BUCKET_NAME,
                "bucket_accessible": True,
                "endpoint": TIGRIS_ENDPOINT_URL,
                "timestamp": datetime.now().isoformat()
            }
            
        logger.info("Tigris bucket health check passed")
        return health_info
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        health_info = {
            "status": "unhealthy",
            "bucket_name": TIGRIS_BUCKET_NAME,
            "bucket_accessible": False,
            "error_code": error_code,
            "error_message": str(e),
            "endpoint": TIGRIS_ENDPOINT_URL,
            "timestamp": datetime.now().isoformat()
        }
        logger.error(f"Tigris bucket health check failed with S3 error: {e}")
        return health_info
        
    except Exception as e:
        health_info = {
            "status": "unhealthy",
            "bucket_name": TIGRIS_BUCKET_NAME,
            "bucket_accessible": False,
            "error": str(e),
            "endpoint": TIGRIS_ENDPOINT_URL,
            "timestamp": datetime.now().isoformat()
        }
        logger.error(f"Tigris bucket health check failed: {e}")
        return health_info

@retry_on_failure(max_retries=3)
async def file_exists(folder_name: str, file_name: str) -> bool:
    """Check if a specific file exists in the bucket."""
    key_name = f"{folder_name}/{file_name}"
    try:
        async with S3ClientManager() as svc:
            await svc.head_object(Bucket=TIGRIS_BUCKET_NAME, Key=key_name)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            return False
        logger.error(f"Error checking if file exists {key_name}: {e}")
        return False
    except Exception as e:
        logger.error(f"Error checking if file exists {key_name}: {e}")
        return False

# ===================== ENHANCED STORAGE FUNCTIONALITIES =====================

@retry_on_failure(max_retries=3)
async def list_files_paginated(folder_name: str, max_keys: int = 1000, continuation_token: Optional[str] = None) -> Dict[str, Any]:
    """List files in a folder with pagination support."""
    try:
        async with S3ClientManager() as svc:
            params = {
                "Bucket": TIGRIS_BUCKET_NAME,
                "Prefix": f"{folder_name}/",
                "MaxKeys": max_keys
            }
            
            if continuation_token:
                params["ContinuationToken"] = continuation_token
            
            response = await svc.list_objects_v2(**params)
            
            files = []
            if "Contents" in response:
                files = [
                    {
                        "key": obj["Key"],
                        "size": obj["Size"],
                        "last_modified": obj["LastModified"],
                        "file_name": obj["Key"].split("/")[-1],
                        "etag": obj["ETag"]
                    }
                    for obj in response["Contents"]
                ]
            
            return {
                "files": files,
                "is_truncated": response.get("IsTruncated", False),
                "next_continuation_token": response.get("NextContinuationToken"),
                "total_files": len(files)
            }
            
    except Exception as e:
        logger.error(f"Error listing files in {folder_name}: {e}")
        return {"files": [], "is_truncated": False, "next_continuation_token": None, "total_files": 0}

@retry_on_failure(max_retries=3)
async def delete_folder_paginated(folder_name: str) -> Dict[str, Any]:
    """Delete an entire folder with pagination support for large folders."""
    total_deleted = 0
    continuation_token = None
    
    try:
        while True:
            # List objects in batches
            async with S3ClientManager() as svc:
                params = {
                    "Bucket": TIGRIS_BUCKET_NAME,
                    "Prefix": f"{folder_name}/",
                    "MaxKeys": 1000
                }
                
                if continuation_token:
                    params["ContinuationToken"] = continuation_token
                
                response = await svc.list_objects_v2(**params)
                
                if "Contents" not in response:
                    break
                
                # Delete objects in batches (up to 1000 per batch)
                objects_to_delete = [{"Key": obj["Key"]} for obj in response["Contents"]]
                
                if objects_to_delete:
                    await svc.delete_objects(
                        Bucket=TIGRIS_BUCKET_NAME,
                        Delete={"Objects": objects_to_delete}
                    )
                    total_deleted += len(objects_to_delete)
                    logger.info(f"Deleted batch of {len(objects_to_delete)} objects from {folder_name}")
                
                # Check if there are more objects to delete
                if not response.get("IsTruncated", False):
                    break
                
                continuation_token = response.get("NextContinuationToken")
        
        logger.info(f"Successfully deleted folder {folder_name} with {total_deleted} objects")
        return {"success": True, "total_deleted": total_deleted}
        
    except Exception as e:
        logger.error(f"Error deleting folder {folder_name}: {e}")
        return {"success": False, "error": str(e), "total_deleted": total_deleted}

@retry_on_failure(max_retries=3)
async def get_file_metadata_cached(folder_name: str, file_name: str) -> Optional[Dict[str, Any]]:
    """Retrieve metadata for a file with Redis caching."""
    key_name = f"{folder_name}/{file_name}"
    cache_key = f"{key_name}:metadata"
    
    # Try to get from cache first
    cached_metadata = await get_cached_metadata(cache_key)
    if cached_metadata:
        storage_metrics["cache_hits"] += 1
        return cached_metadata
    
    storage_metrics["cache_misses"] += 1
    
    try:
        async with S3ClientManager() as svc:
            response = await svc.head_object(Bucket=TIGRIS_BUCKET_NAME, Key=key_name)
            
            # Extract useful metadata
            metadata = {
                "key": key_name,
                "size": response.get("ContentLength", 0),
                "last_modified": response.get("LastModified").isoformat() if response.get("LastModified") else None,
                "content_type": response.get("ContentType"),
                "etag": response.get("ETag"),
                "metadata": response.get("Metadata", {}),
                "file_name": file_name,
                "folder_name": folder_name,
                "cached_at": datetime.now(timezone.utc).isoformat()
            }
            
            # Cache the metadata
            await set_cached_metadata(cache_key, metadata)
            
        logger.info(f"Retrieved and cached metadata for {key_name}")
        return metadata
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            logger.error(f"File not found: {key_name}")
        else:
            logger.error(f"S3 error getting metadata for {key_name}: {e}")
        return None
    except Exception as e:
        logger.error(f"Error getting metadata for {key_name}: {e}")
        return None

@retry_on_failure(max_retries=3)
async def file_exists_cached(folder_name: str, file_name: str) -> bool:
    """Check if a specific file exists with Redis caching."""
    key_name = f"{folder_name}/{file_name}"
    cache_key = f"{key_name}:exists"
    
    # Try to get from cache first
    if redis_client:
        try:
            cached = await redis_client.get(f"storage:exists:{cache_key}")
            if cached is not None:
                storage_metrics["cache_hits"] += 1
                return cached.lower() == "true"
        except Exception:
            pass
    
    storage_metrics["cache_misses"] += 1
    
    try:
        async with S3ClientManager() as svc:
            await svc.head_object(Bucket=TIGRIS_BUCKET_NAME, Key=key_name)
        
        # Cache the existence
        if redis_client:
            try:
                await redis_client.setex(f"storage:exists:{cache_key}", CACHE_TTL, "true")
            except Exception:
                pass
        
        return True
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            # Cache the non-existence
            if redis_client:
                try:
                    await redis_client.setex(f"storage:exists:{cache_key}", CACHE_TTL, "false")
                except Exception:
                    pass
            return False
        logger.error(f"Error checking if file exists {key_name}: {e}")
        return False
    except Exception as e:
        logger.error(f"Error checking if file exists {key_name}: {e}")
        return False

# ===================== BACKUP AND DISASTER RECOVERY =====================

@retry_on_failure(max_retries=5)
async def backup_to_secondary_bucket(folder_name: str, backup_bucket: str, backup_region: Optional[str] = None) -> Dict[str, Any]:
    """
    Backup a folder to a secondary bucket for disaster recovery.
    
    Args:
        folder_name: The folder to backup
        backup_bucket: Secondary bucket name
        backup_region: Optional different region for backup
    """
    total_backed_up = 0
    total_size = 0
    continuation_token = None
    
    try:
        # Create backup session for different region if needed
        backup_session = aioboto3.Session()
        backup_config = boto_config
        
        backup_endpoint = TIGRIS_ENDPOINT_URL
        if backup_region:
            backup_endpoint = f"https://fly.storage.tigris.dev"  # Adjust based on your multi-region setup
        
        while True:
            # List source files
            async with S3ClientManager() as source_svc:
                params = {
                    "Bucket": TIGRIS_BUCKET_NAME,
                    "Prefix": f"{folder_name}/",
                    "MaxKeys": 100  # Smaller batches for backup
                }
                
                if continuation_token:
                    params["ContinuationToken"] = continuation_token
                
                response = await source_svc.list_objects_v2(**params)
                
                if "Contents" not in response:
                    break
                
                # Copy each file to backup bucket
                backup_client = await backup_session.client(
                    "s3",
                    endpoint_url=backup_endpoint,
                    aws_access_key_id=TIGRIS_ACCESS_KEY,
                    aws_secret_access_key=TIGRIS_SECRET_KEY,
                    config=backup_config
                ).__aenter__()
                
                try:
                    for obj in response["Contents"]:
                        source_key = obj["Key"]
                        # Add timestamp to backup key for versioning
                        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
                        backup_key = f"backup_{timestamp}/{source_key}"
                        
                        # Copy object
                        await backup_client.copy_object(
                            Bucket=backup_bucket,
                            CopySource={'Bucket': TIGRIS_BUCKET_NAME, 'Key': source_key},
                            Key=backup_key,
                            MetadataDirective='COPY'
                        )
                        
                        total_backed_up += 1
                        total_size += obj["Size"]
                        
                        logger.debug(f"Backed up {source_key} to {backup_key}")
                
                finally:
                    await backup_client.__aexit__(None, None, None)
                
                # Check if there are more objects
                if not response.get("IsTruncated", False):
                    break
                
                continuation_token = response.get("NextContinuationToken")
        
        backup_info = {
            "success": True,
            "total_files_backed_up": total_backed_up,
            "total_size_bytes": total_size,
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "backup_bucket": backup_bucket,
            "backup_region": backup_region,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        logger.info(f"Backup completed: {backup_info}")
        return backup_info
        
    except Exception as e:
        error_info = {
            "success": False,
            "error": str(e),
            "files_backed_up": total_backed_up,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        logger.error(f"Backup failed: {error_info}")
        return error_info

async def setup_bucket_versioning(bucket_name: Optional[str] = None) -> bool:
    """Enable versioning on the bucket for data protection."""
    target_bucket = bucket_name or TIGRIS_BUCKET_NAME
    
    try:
        async with S3ClientManager() as svc:
            await svc.put_bucket_versioning(
                Bucket=target_bucket,
                VersioningConfiguration={'Status': 'Enabled'}
            )
        logger.info(f"Versioning enabled for bucket {target_bucket}")
        return True
    except Exception as e:
        logger.error(f"Failed to enable versioning for {target_bucket}: {e}")
        return False

async def setup_lifecycle_policy(bucket_name: Optional[str] = None) -> bool:
    """
    Setup lifecycle policy for automatic cleanup of old versions and multipart uploads.
    """
    target_bucket = bucket_name or TIGRIS_BUCKET_NAME
    
    lifecycle_config = {
        'Rules': [
            {
                'ID': 'DeleteOldVersions',
                'Status': 'Enabled',
                'Filter': {'Prefix': ''},
                'NoncurrentVersionExpiration': {'NoncurrentDays': 30},
                'AbortIncompleteMultipartUpload': {'DaysAfterInitiation': 7}
            }
        ]
    }
    
    try:
        async with S3ClientManager() as svc:
            await svc.put_bucket_lifecycle_configuration(
                Bucket=target_bucket,
                LifecycleConfiguration=lifecycle_config
            )
        logger.info(f"Lifecycle policy configured for bucket {target_bucket}")
        return True
    except Exception as e:
        logger.error(f"Failed to setup lifecycle policy for {target_bucket}: {e}")
        return False

# ===================== MONITORING AND METRICS =====================

async def get_storage_metrics() -> Dict[str, Any]:
    """Get current storage performance metrics."""
    return {
        "requests_total": storage_metrics["requests"],
        "errors_total": storage_metrics["errors"],
        "cache_hits": storage_metrics["cache_hits"],
        "cache_misses": storage_metrics["cache_misses"],
        "cache_hit_ratio": (
            storage_metrics["cache_hits"] / 
            max(storage_metrics["cache_hits"] + storage_metrics["cache_misses"], 1)
        ),
        "avg_response_time_seconds": storage_metrics["avg_response_time"],
        "error_rate": (
            storage_metrics["errors"] / max(storage_metrics["requests"], 1)
        ),
        "pool_size": len(_client_pool),
        "max_pool_size": POOL_SIZE,
        "concurrent_limit": CONCURRENT_REQUEST_LIMIT,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

def log_high_latency_alert(operation: str, duration: float, threshold: float = 5.0):
    """Log alert for high latency operations."""
    if duration > threshold:
        logger.warning(
            f"HIGH LATENCY ALERT: {operation} took {duration:.2f}s "
            f"(threshold: {threshold}s)"
        )

async def comprehensive_health_check() -> Dict[str, Any]:
    """
    Comprehensive health check including bucket access, cache, and performance.
    """
    health_status: Dict = {"timestamp": datetime.now(timezone.utc).isoformat()}
    
    try:
        # Test bucket access
        start_time = time.time()
        async with S3ClientManager() as svc:
            await svc.head_bucket(Bucket=TIGRIS_BUCKET_NAME)
            await svc.list_objects_v2(Bucket=TIGRIS_BUCKET_NAME, MaxKeys=1)
        
        bucket_latency = time.time() - start_time
        health_status["bucket"] = {
            "status": "healthy",
            "latency_seconds": round(bucket_latency, 3)
        }
        
        # Log high latency
        log_high_latency_alert("bucket_health_check", bucket_latency, 2.0)
        
        # Test cache if available
        if redis_client:
            try:
                start_time = time.time()
                await redis_client.ping()
                cache_latency = time.time() - start_time
                health_status["cache"] = {
                    "status": "healthy",
                    "latency_seconds": round(cache_latency, 3)
                }
            except Exception as e:
                health_status["cache"] = {
                    "status": "unhealthy",
                    "error": str(e)
                }
        else:
            health_status["cache"] = {"status": "disabled"}
        
        # Add metrics
        health_status["metrics"] = await get_storage_metrics()
        
        # Overall status
        cache_ok = health_status["cache"]["status"] in ["healthy", "disabled"]
        health_status["overall_status"] = "healthy" if cache_ok else "degraded"
        
    except Exception as e:
        health_status["bucket"] = {"status": "unhealthy", "error": str(e)}
        health_status["overall_status"] = "unhealthy"
        logger.error(f"Comprehensive health check failed: {e}")
    
    return health_status

# ===================== INITIALIZATION =====================

async def initialize_storage_system():
    """Initialize the storage system with all enhancements."""
    logger.info("Initializing enhanced storage system...")
    
    # Initialize Redis cache
    await init_redis_cache()
    
    # Setup bucket policies if needed
    await setup_bucket_versioning()
    await setup_lifecycle_policy()
    
    # Run initial health check
    health = await comprehensive_health_check()
    logger.info(f"Storage system initialized. Status: {health['overall_status']}")
    
    return health


