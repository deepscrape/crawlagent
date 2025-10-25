# Backup and DR Tasks for Celery
# Add these tasks to your tasks.py file

import json
import logging
import time
from asyncio import get_event_loop
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from celery_app import celery_app
from redisCache import redis as redis_client

logger = logging.getLogger("crawlagent")

@celery_app.task(
    bind=True,
    max_retries=3,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=300,
)
def incremental_backup_task(self, folder_names: Optional[List[str]] = None):
    """Celery task for incremental backups."""
    try:
        loop = get_event_loop()
        result = loop.run_until_complete(_incremental_backup_impl(self, folder_names))
        return result
    except Exception as e:
        logger.error(f"Incremental backup task failed: {e}")
        raise

@celery_app.task(
    bind=True,
    max_retries=2,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=600,
)
def full_backup_task(self):
    """Celery task for full system backup."""
    try:
        loop = get_event_loop()
        result = loop.run_until_complete(_full_backup_impl(self))
        return result
    except Exception as e:
        logger.error(f"Full backup task failed: {e}")
        raise

@celery_app.task(bind=True)
def cleanup_old_backups_task(self, retention_days: int = 30):
    """Celery task to cleanup old backups."""
    try:
        loop = get_event_loop()
        result = loop.run_until_complete(_cleanup_old_backups_impl(self, retention_days))
        return result
    except Exception as e:
        logger.error(f"Cleanup backup task failed: {e}")
        raise

# @celery_app.task(bind=True)
# def disaster_recovery_test_task(self):
#     """Celery task for automated DR testing."""
#     try:
#         loop = get_event_loop()
#         result = loop.run_until_complete(_dr_test_impl(self))
#         return result
#     except Exception as e:
#         logger.error(f"DR test task failed: {e}")
#         raise

# Task implementations

async def _incremental_backup_impl(self, folder_names: Optional[List[str]] = None):
    """Implementation for incremental backup."""
    task_id = self.request.id
    start_time = time.time()
    
    try:
        from storage import backup_to_secondary_bucket, list_files_paginated
        
        logger.info(f"Starting incremental backup task {task_id}")
        
        # If no folders specified, backup recent changes only
        if not folder_names:
            # Get folders modified in last 6 hours (for incremental)
            folder_names = await get_recently_modified_folders(hours=6)
        
        backup_results = []
        total_files = 0
        total_size = 0
        
        for folder_name in folder_names:
            try:
                backup_result = await backup_to_secondary_bucket(
                    folder_name=folder_name,
                    backup_bucket="crawlagent-backup-incremental",
                    backup_region="us-east-1"
                )
                
                if backup_result.get("success"):
                    total_files += backup_result.get("total_files_backed_up", 0)
                    total_size += backup_result.get("total_size_bytes", 0)
                    backup_results.append({
                        "folder": folder_name,
                        "status": "success",
                        "files": backup_result.get("total_files_backed_up", 0),
                        "size_mb": backup_result.get("total_size_mb", 0)
                    })
                else:
                    backup_results.append({
                        "folder": folder_name,
                        "status": "failed",
                        "error": backup_result.get("error", "Unknown error")
                    })
                    
            except Exception as e:
                logger.error(f"Failed to backup folder {folder_name}: {e}")
                backup_results.append({
                    "folder": folder_name,
                    "status": "failed", 
                    "error": str(e)
                })
        
        end_time = time.time()
        duration = end_time - start_time
        
        result = {
            "task_id": task_id,
            "type": "incremental_backup",
            "status": "completed",
            "duration_seconds": duration,
            "folders_processed": len(folder_names),
            "total_files_backed_up": total_files,
            "total_size_bytes": total_size,
            "backup_results": backup_results,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        # Update Redis with task result
        if redis_client:
            await redis_client.setex(
                f"backup_task:{task_id}",
                86400,  # 24 hours
                json.dumps(result, default=str)
            )
        
        logger.info(f"Incremental backup completed: {result}")
        return result
        
    except Exception as e:
        logger.error(f"Incremental backup failed: {e}")
        raise

async def _full_backup_impl(self):
    """Implementation for full system backup."""
    task_id = self.request.id
    start_time = time.time()
    
    try:
        from storage import backup_to_secondary_bucket, list_files_paginated
        
        logger.info(f"Starting full backup task {task_id}")
        
        # Backup entire bucket
        backup_result = await backup_to_secondary_bucket(
            folder_name="",  # Empty means entire bucket
            backup_bucket="crawlagent-backup-full",
            backup_region="eu-west-1"
        )
        
        end_time = time.time()
        duration = end_time - start_time
        
        result = {
            "task_id": task_id,
            "type": "full_backup",
            "status": "completed" if backup_result.get("success") else "failed",
            "duration_seconds": duration,
            "total_files_backed_up": backup_result.get("total_files_backed_up", 0),
            "total_size_bytes": backup_result.get("total_size_bytes", 0),
            "total_size_gb": backup_result.get("total_size_bytes", 0) / (1024**3),
            "backup_bucket": backup_result.get("backup_bucket"),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        if not backup_result.get("success"):
            result["error"] = backup_result.get("error", "Unknown error")
        
        # Update Redis with task result
        if redis_client:
            await redis_client.setex(
                f"backup_task:{task_id}",
                86400 * 7,  # 7 days for full backups
                json.dumps(result, default=str)
            )
        
        logger.info(f"Full backup completed: {result}")
        return result
        
    except Exception as e:
        logger.error(f"Full backup failed: {e}")
        raise

async def _cleanup_old_backups_impl(self, retention_days: int = 30):
    """Implementation for cleaning up old backups."""
    task_id = self.request.id
    
    try:
        from storage import S3ClientManager, TIGRIS_BUCKET_NAME
        
        logger.info(f"Starting backup cleanup task {task_id}")
        
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=retention_days)
        deleted_count = 0
        freed_bytes = 0
        
        backup_buckets = [
            "crawlagent-backup-incremental",
            "crawlagent-backup-full",
            "crawlagent-snapshots"
        ]
        
        for backup_bucket in backup_buckets:
            try:
                async with S3ClientManager() as svc:
                    # List objects in backup bucket
                    response = await svc.list_objects_v2(
                        Bucket=backup_bucket,
                        Prefix="backup_"
                    )
                    
                    if "Contents" not in response:
                        continue
                    
                    # Find old backup objects
                    old_objects = []
                    for obj in response["Contents"]:
                        if obj["LastModified"].replace(tzinfo=timezone.utc) < cutoff_date:
                            old_objects.append({
                                "Key": obj["Key"],
                                "Size": obj["Size"]
                            })
                    
                    # Delete old objects in batches
                    if old_objects:
                        # Delete in batches of 1000
                        for i in range(0, len(old_objects), 1000):
                            batch = old_objects[i:i+1000]
                            objects_to_delete = [{"Key": obj["Key"]} for obj in batch]
                            
                            await svc.delete_objects(
                                Bucket=backup_bucket,
                                Delete={"Objects": objects_to_delete}
                            )
                            
                            deleted_count += len(batch)
                            freed_bytes += sum(obj["Size"] for obj in batch)
                        
                        logger.info(f"Deleted {len(old_objects)} old objects from {backup_bucket}")
                        
            except Exception as e:
                logger.error(f"Failed to cleanup bucket {backup_bucket}: {e}")
        
        result = {
            "task_id": task_id,
            "type": "cleanup_old_backups",
            "status": "completed",
            "retention_days": retention_days,
            "deleted_objects": deleted_count,
            "freed_bytes": freed_bytes,
            "freed_mb": round(freed_bytes / (1024*1024), 2),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        logger.info(f"Backup cleanup completed: {result}")
        return result
        
    except Exception as e:
        logger.error(f"Backup cleanup failed: {e}")
        raise

# async def _dr_test_impl(self):
#     """Implementation for disaster recovery testing."""
#     task_id = self.request.id
    
#     try:
#         from storage import (
#             backup_to_secondary_bucket,
#             create_test_data,
#             delete_folder_paginated,
#             recover_from_backup,
#             verify_data_integrity,
#         )
        
#         logger.info(f"Starting DR test task {task_id}")
        
#         test_folder = f"dr_test/test_{int(time.time())}"
        
#         # 1. Create test data
#         test_files = await create_test_data(test_folder, count=50)
        
#         # 2. Backup test data
#         backup_start = time.time()
#         backup_result = await backup_to_secondary_bucket(
#             test_folder,
#             "crawlagent-dr-test"
#         )
#         backup_duration = time.time() - backup_start
        
#         # 3. Delete original data
#         await delete_folder_paginated(test_folder)
        
#         # 4. Recover from backup
#         recovery_start = time.time()
#         recovery_result = await recover_from_backup(
#             test_folder,
#             "crawlagent-dr-test"
#         )
#         recovery_duration = time.time() - recovery_start
        
#         # 5. Verify data integrity
#         integrity_result = await verify_data_integrity(test_folder, test_files)
        
#         # 6. Cleanup test data
#         await delete_folder_paginated(test_folder)
        
#         result = {
#             "task_id": task_id,
#             "type": "dr_test",
#             "status": "completed",
#             "test_folder": test_folder,
#             "backup_duration_seconds": backup_duration,
#             "recovery_duration_seconds": recovery_duration,
#             "data_integrity_score": integrity_result.get("integrity_score", 0),
#             "files_tested": len(test_files),
#             "success": integrity_result.get("success", False),
#             "timestamp": datetime.now(timezone.utc).isoformat()
#         }
        
#         # Alert if DR test failed
#         if not result["success"]:
#             await send_dr_test_alert(result)
        
#         logger.info(f"DR test completed: {result}")
#         return result
        
#     except Exception as e:
#         logger.error(f"DR test failed: {e}")
#         await send_dr_test_alert({"error": str(e), "task_id": task_id})
#         raise

# Helper functions

async def get_recently_modified_folders(hours: int = 6) -> List[str]:
    """Get list of folders modified in the last N hours."""
    try:
        from storage import S3ClientManager, TIGRIS_BUCKET_NAME
        
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours)
        modified_folders = set()
        
        async with S3ClientManager() as svc:
            response = await svc.list_objects_v2(
                Bucket=TIGRIS_BUCKET_NAME
            )
            
            if "Contents" in response:
                for obj in response["Contents"]:
                    if obj["LastModified"].replace(tzinfo=timezone.utc) > cutoff_time:
                        # Extract folder name from key
                        folder = obj["Key"].split('/')[0] if '/' in obj["Key"] else ""
                        if folder:
                            modified_folders.add(folder)
        
        return list(modified_folders)
        
    except Exception as e:
        logger.error(f"Failed to get recently modified folders: {e}")
        return []

async def send_dr_test_alert(test_result: Dict[str, Any]):
    """Send alert when DR test fails."""
    alert = {
        "severity": "high",
        "service": "disaster_recovery",
        "message": f"DR test failed: {test_result.get('error', 'Unknown error')}",
        "test_result": test_result,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "action_required": True
    }
    
    # Log the alert (implement your alerting system here)
    logger.error(f"DR TEST ALERT: {alert}")
