# Backup and Disaster Recovery Setup Guide

## Overview
This guide covers implementing robust backup and disaster recovery for your Tigris bucket storage system supporting millions of users.

## 1. Multi-Region Architecture

### Primary Setup
```yaml
# config.yml - Add backup configuration
storage:
  primary:
    bucket: "crawlagent.bucket.a"
    region: "auto"
    endpoint: "https://fly.storage.tigris.dev"
  
  backup:
    enabled: true
    buckets:
      - name: "crawlagent-backup-us"
        region: "us-east-1"
        endpoint: "https://fly.storage.tigris.dev"
      - name: "crawlagent-backup-eu" 
        region: "eu-west-1"
        endpoint: "https://fly.storage.tigris.dev"
    
    retention:
      daily_backups: 7
      weekly_backups: 4
      monthly_backups: 12
    
    schedule:
      incremental: "0 */6 * * *"  # Every 6 hours
      full: "0 2 * * 0"           # Weekly on Sunday 2 AM
```

### Environment Variables
```bash
# Primary bucket
AWS_ACCESS_KEY_ID=your_primary_key
AWS_SECRET_ACCESS_KEY=your_primary_secret
AWS_ENDPOINT_URL_S3=https://fly.storage.tigris.dev

# Backup buckets
BACKUP_ACCESS_KEY_ID=your_backup_key
BACKUP_SECRET_ACCESS_KEY=your_backup_secret
BACKUP_ENDPOINT_URL=https://fly.storage.tigris.dev

# Backup configuration
BACKUP_ENABLED=true
BACKUP_RETENTION_DAYS=30
BACKUP_SCHEDULE_CRON="0 2 * * *"
```

## 2. Backup Strategies

### A. Real-Time Replication
```python
# Enable cross-region replication
async def setup_cross_region_replication():
    """Setup automatic cross-region replication for disaster recovery."""
    
    replication_config = {
        'Role': 'arn:aws:iam::account:role/replication-role',
        'Rules': [
            {
                'ID': 'ReplicateToBackup',
                'Status': 'Enabled',
                'Priority': 1,
                'Filter': {'Prefix': ''},
                'DeleteMarkerReplication': {'Status': 'Enabled'},
                'Destination': {
                    'Bucket': 'arn:aws:s3:::crawlagent-backup-us',
                    'StorageClass': 'STANDARD_IA',
                    'ReplicationTime': {
                        'Status': 'Enabled',
                        'Time': {'Minutes': 15}
                    }
                }
            }
        ]
    }
    
    try:
        async with S3ClientManager() as svc:
            await svc.put_bucket_replication(
                Bucket=TIGRIS_BUCKET_NAME,
                ReplicationConfiguration=replication_config
            )
        logger.info("Cross-region replication enabled")
        return True
    except Exception as e:
        logger.error(f"Failed to setup replication: {e}")
        return False
```

### B. Scheduled Backups
```python
# Add to celery_config.py
beat_schedule = {
    # ...existing schedules...
    "incremental-backup": {
        "task": "tasks.incremental_backup_task",
        "schedule": crontab(minute=0, hour='*/6'),  # Every 6 hours
    },
    "full-backup": {
        "task": "tasks.full_backup_task", 
        "schedule": crontab(minute=0, hour=2, day_of_week=0),  # Weekly
    },
    "cleanup-old-backups": {
        "task": "tasks.cleanup_old_backups_task",
        "schedule": crontab(minute=0, hour=3),  # Daily at 3 AM
    },
}
```

### C. Point-in-Time Recovery
```python
async def create_snapshot(folder_name: str = None) -> Dict[str, Any]:
    """
    Create a point-in-time snapshot of the bucket or specific folder.
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    snapshot_name = f"snapshot_{timestamp}"
    
    if folder_name:
        snapshot_name = f"snapshot_{folder_name}_{timestamp}"
    
    try:
        # Create backup with timestamp
        backup_result = await backup_to_secondary_bucket(
            folder_name=folder_name or "",
            backup_bucket=f"crawlagent-snapshots",
            backup_region="us-east-1"
        )
        
        # Record snapshot metadata
        snapshot_metadata = {
            "snapshot_id": snapshot_name,
            "created_at": timestamp,
            "source_folder": folder_name,
            "files_count": backup_result.get("total_files_backed_up", 0),
            "size_bytes": backup_result.get("total_size_bytes", 0),
            "status": "completed" if backup_result.get("success") else "failed"
        }
        
        # Store snapshot metadata
        if redis_client:
            await redis_client.hset(
                f"snapshots:{snapshot_name}",
                mapping=snapshot_metadata
            )
            await redis_client.expire(f"snapshots:{snapshot_name}", 86400 * 30)  # 30 days
        
        logger.info(f"Snapshot created: {snapshot_name}")
        return snapshot_metadata
        
    except Exception as e:
        logger.error(f"Failed to create snapshot: {e}")
        return {"status": "failed", "error": str(e)}
```

## 3. Monitoring and Alerting

### A. Health Monitoring
```python
async def monitor_backup_health():
    """Monitor backup system health and send alerts."""
    
    health_checks = {
        "replication_status": await check_replication_status(),
        "backup_buckets": await verify_backup_buckets(),
        "recent_backups": await check_recent_backups(),
        "storage_usage": await monitor_storage_usage()
    }
    
    # Check for issues
    issues = []
    for check, result in health_checks.items():
        if not result.get("healthy", False):
            issues.append(f"{check}: {result.get('error', 'Unknown issue')}")
    
    if issues:
        await send_backup_alert(issues)
    
    return health_checks

async def send_backup_alert(issues: List[str]):
    """Send backup system alerts."""
    alert_message = {
        "severity": "high" if len(issues) > 2 else "medium",
        "service": "storage_backup",
        "issues": issues,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "action_required": True
    }
    
    # Send to monitoring system (implement based on your setup)
    logger.error(f"BACKUP ALERT: {alert_message}")
```

### B. Prometheus Metrics
```python
# Add to storage.py
BACKUP_METRICS = {
    "backup_operations_total": 0,
    "backup_failures_total": 0,
    "backup_duration_seconds": 0.0,
    "backup_size_bytes": 0,
    "last_successful_backup": 0,
    "replication_lag_seconds": 0.0
}

def update_backup_metrics(operation: str, duration: float, size_bytes: int = 0, success: bool = True):
    """Update backup-related metrics."""
    BACKUP_METRICS["backup_operations_total"] += 1
    
    if success:
        BACKUP_METRICS["last_successful_backup"] = time.time()
        BACKUP_METRICS["backup_size_bytes"] += size_bytes
    else:
        BACKUP_METRICS["backup_failures_total"] += 1
    
    BACKUP_METRICS["backup_duration_seconds"] = duration
    
    # Log metrics for Prometheus scraping
    logger.info(
        f"backup_operation_complete "
        f"operation='{operation}' "
        f"duration={duration:.2f} "
        f"size_bytes={size_bytes} "
        f"success={success}"
    )
```

## 4. Recovery Procedures

### A. Automated Recovery
```python
async def recover_from_backup(
    target_folder: str,
    backup_source: str,
    recovery_point: str = None
) -> Dict[str, Any]:
    """
    Recover data from backup to primary bucket.
    
    Args:
        target_folder: Folder to recover to
        backup_source: Backup bucket/snapshot to recover from
        recovery_point: Specific point in time (timestamp)
    """
    
    recovery_id = f"recovery_{int(time.time())}"
    
    try:
        # Validate backup source exists
        backup_exists = await verify_backup_exists(backup_source, recovery_point)
        if not backup_exists:
            return {"success": False, "error": "Backup source not found"}
        
        # Create recovery log
        recovery_log = {
            "recovery_id": recovery_id,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "target_folder": target_folder,
            "backup_source": backup_source,
            "recovery_point": recovery_point,
            "status": "in_progress"
        }
        
        # Perform recovery
        recovered_files = 0
        recovered_size = 0
        
        # List backup files
        backup_files = await list_backup_files(backup_source, recovery_point)
        
        for backup_file in backup_files:
            try:
                # Copy from backup to primary
                await copy_from_backup_to_primary(
                    backup_file["key"],
                    f"{target_folder}/{backup_file['original_name']}"
                )
                recovered_files += 1
                recovered_size += backup_file["size"]
                
            except Exception as e:
                logger.error(f"Failed to recover file {backup_file['key']}: {e}")
        
        # Update recovery log
        recovery_log.update({
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "status": "completed",
            "recovered_files": recovered_files,
            "recovered_size_bytes": recovered_size
        })
        
        logger.info(f"Recovery completed: {recovery_log}")
        return recovery_log
        
    except Exception as e:
        recovery_log.update({
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "status": "failed",
            "error": str(e)
        })
        logger.error(f"Recovery failed: {recovery_log}")
        return recovery_log
```

### B. Manual Recovery Steps
```bash
# 1. Identify the issue
python -m storage health_check

# 2. List available backups
python -m storage list_backups --date-range "7d"

# 3. Restore specific folder
python -m storage recover \
  --backup-id "backup_20241024_120000" \
  --target-folder "users/user123" \
  --dry-run

# 4. Execute recovery
python -m storage recover \
  --backup-id "backup_20241024_120000" \
  --target-folder "users/user123" \
  --confirm

# 5. Verify recovery
python -m storage verify_integrity --folder "users/user123"
```

## 5. Testing and Validation

### A. Regular DR Tests
```python
async def run_disaster_recovery_test() -> Dict[str, Any]:
    """
    Run automated disaster recovery test.
    """
    test_id = f"dr_test_{int(time.time())}"
    
    try:
        # 1. Create test data
        test_folder = f"dr_test/{test_id}"
        test_files = await create_test_data(test_folder, count=100)
        
        # 2. Trigger backup
        backup_result = await backup_to_secondary_bucket(
            test_folder, 
            "crawlagent-dr-test"
        )
        
        # 3. Delete original data
        await delete_folder_paginated(test_folder)
        
        # 4. Recover from backup
        recovery_result = await recover_from_backup(
            test_folder,
            "crawlagent-dr-test"
        )
        
        # 5. Verify integrity
        verification_result = await verify_data_integrity(test_folder, test_files)
        
        # 6. Cleanup
        await cleanup_dr_test(test_folder, test_id)
        
        test_result = {
            "test_id": test_id,
            "success": verification_result["success"],
            "backup_time": backup_result.get("duration", 0),
            "recovery_time": recovery_result.get("duration", 0),
            "data_integrity": verification_result["integrity_score"],
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        logger.info(f"DR test completed: {test_result}")
        return test_result
        
    except Exception as e:
        logger.error(f"DR test failed: {e}")
        return {"test_id": test_id, "success": False, "error": str(e)}
```

### B. Automated Testing Schedule
```yaml
# Add to your CI/CD pipeline
disaster_recovery_tests:
  schedule: "0 3 * * 1"  # Weekly on Monday at 3 AM
  
  steps:
    - name: "Run DR Test"
      command: "python -m storage run_dr_test"
    
    - name: "Verify Backup Health"
      command: "python -m storage verify_backup_health"
    
    - name: "Test Recovery Speed"
      command: "python -m storage benchmark_recovery"
    
    - name: "Generate DR Report"
      command: "python -m storage generate_dr_report"
```

## 6. Best Practices Summary

### Data Protection:
1. **3-2-1 Rule**: 3 copies, 2 different media, 1 offsite
2. **Version Control**: Enable object versioning
3. **Immutable Backups**: Use object lock for critical backups
4. **Encryption**: Encrypt data at rest and in transit

### Operational:
1. **Automate Everything**: Backups, monitoring, alerts, testing
2. **Regular Testing**: Monthly DR drills, weekly backup verification
3. **Documentation**: Keep recovery procedures updated
4. **Training**: Ensure team knows recovery procedures

### Monitoring:
1. **Real-time Alerts**: Backup failures, replication lag
2. **Metrics**: Backup success rate, recovery time objectives
3. **Dashboards**: Backup status, storage usage, DR readiness
4. **Audit Logs**: All backup and recovery operations

### Security:
1. **Access Control**: Separate backup credentials
2. **Encryption Keys**: Secure key management
3. **Network Isolation**: Backup traffic over private networks
4. **Compliance**: Meet regulatory requirements (GDPR, etc.)

This comprehensive backup and DR setup ensures your million-user system can recover from any disaster scenario with minimal data loss and downtime.
