# Enhanced Storage System Setup and Test Script
# Run this script to set up and test your enhanced storage system

param(
    [Parameter(Mandatory=$false)]
    [string]$Action = "setup",
    
    [Parameter(Mandatory=$false)]
    [string]$Environment = "dev"
)

Write-Host "🚀 Crawl Agent Enhanced Storage System Setup" -ForegroundColor Green
Write-Host "Action: $Action | Environment: $Environment" -ForegroundColor Cyan

function Install-Dependencies {
    Write-Host "📦 Installing required dependencies..." -ForegroundColor Yellow
    
    # Install Redis (if not already installed)
    if (!(Get-Command redis-server -ErrorAction SilentlyContinue)) {
        Write-Host "Installing Redis via Chocolatey..."
        choco install redis-64 -y
    }
    
    # Install Python dependencies
    pip install -r requirements.txt
    pip install redis prometheus-client aioredis
    
    Write-Host "✅ Dependencies installed" -ForegroundColor Green
}

function Setup-Environment {
    Write-Host "🔧 Setting up environment variables..." -ForegroundColor Yellow
    
    $envFile = if ($Environment -eq "prod") { ".env" } else { "dev.env" }
    
    # Check if environment file exists
    if (!(Test-Path $envFile)) {
        Write-Host "⚠️  Environment file $envFile not found. Creating template..." -ForegroundColor Yellow
        
        @"
# Enhanced Storage Configuration
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_ENDPOINT_URL_S3=https://fly.storage.tigris.dev

# Connection Pool Settings
S3_POOL_SIZE=20
S3_CONCURRENT_LIMIT=100

# Redis Cache Settings
REDIS_URL=redis://localhost:6379
STORAGE_CACHE_TTL=300

# Backup Configuration
BACKUP_ENABLED=true
BACKUP_RETENTION_DAYS=30
BACKUP_ACCESS_KEY_ID=your_backup_key
BACKUP_SECRET_ACCESS_KEY=your_backup_secret

# Monitoring
PROMETHEUS_ENABLED=true
PROMETHEUS_PORT=9808
"@ | Out-File -FilePath $envFile -Encoding utf8
        
        Write-Host "📝 Template created at $envFile. Please update with your values." -ForegroundColor Yellow
    }
    
    Write-Host "✅ Environment setup complete" -ForegroundColor Green
}

function Start-Services {
    Write-Host "🔄 Starting required services..." -ForegroundColor Yellow
    
    # Start Redis
    Write-Host "Starting Redis server..."
    Start-Process -FilePath "redis-server" -WindowStyle Hidden
    Start-Sleep 2
    
    # Start Celery worker
    Write-Host "Starting Celery worker..."
    Start-Process -FilePath "python" -ArgumentList "-m", "celery", "-A", "celery_app.celery_app", "worker", "--loglevel=info", "-E" -WindowStyle Hidden
    
    # Start Celery beat (for scheduled tasks)
    Write-Host "Starting Celery beat scheduler..."
    Start-Process -FilePath "python" -ArgumentList "-m", "celery", "-A", "celery_app.celery_app", "beat", "--loglevel=info" -WindowStyle Hidden
    
    Write-Host "✅ Services started" -ForegroundColor Green
}

function Test-StorageSystem {
    Write-Host "🧪 Testing enhanced storage system..." -ForegroundColor Yellow
    
    # Test script content
    $testScript = @"
import asyncio
import sys
import os
sys.path.append('.')

from storage import (
    initialize_storage_system,
    comprehensive_health_check,
    list_files_paginated,
    get_file_metadata_cached,
    file_exists_cached,
    backup_to_secondary_bucket,
    get_storage_metrics
)

async def run_tests():
    print("🔄 Initializing storage system...")
    init_result = await initialize_storage_system()
    print(f"Init result: {init_result['overall_status']}")
    
    print("\n🏥 Running health check...")
    health = await comprehensive_health_check()
    print(f"Health status: {health['overall_status']}")
    
    print("\n📊 Getting storage metrics...")
    metrics = await get_storage_metrics()
    print(f"Cache hit ratio: {metrics['cache_hit_ratio']:.2%}")
    print(f"Average response time: {metrics['avg_response_time_seconds']:.3f}s")
    
    print("\n📁 Testing file operations...")
    # Test pagination
    files_result = await list_files_paginated("test", max_keys=10)
    print(f"Files listed: {files_result['total_files']}")
    
    # Test caching
    exists = await file_exists_cached("test", "nonexistent.md")
    print(f"File exists (cached): {exists}")
    
    print("\n✅ All tests completed!")

if __name__ == "__main__":
    asyncio.run(run_tests())
"@
    
    # Write and run test script
    $testScript | Out-File -FilePath "test_storage.py" -Encoding utf8
    python test_storage.py
    Remove-Item "test_storage.py" -Force
    
    Write-Host "✅ Storage system tests completed" -ForegroundColor Green
}

function Run-BackupTest {
    Write-Host "💾 Running backup system test..." -ForegroundColor Yellow
    
    # Create test backup task
    $backupTestScript = @"
import asyncio
import sys
sys.path.append('.')

from celery_app import celery_app
from tasks import incremental_backup_task

async def test_backup():
    print("🔄 Starting backup test...")
    
    # Queue a backup task
    task = incremental_backup_task.delay(["test_folder"])
    print(f"Backup task queued: {task.id}")
    
    # Wait for result (timeout after 5 minutes)
    try:
        result = task.get(timeout=300)
        print(f"Backup completed: {result['status']}")
        return True
    except Exception as e:
        print(f"Backup failed: {e}")
        return False

if __name__ == "__main__":
    import asyncio
    success = asyncio.run(test_backup())
    exit(0 if success else 1)
"@
    
    $backupTestScript | Out-File -FilePath "test_backup.py" -Encoding utf8
    python test_backup.py
    $backupSuccess = $LASTEXITCODE -eq 0
    Remove-Item "test_backup.py" -Force
    
    if ($backupSuccess) {
        Write-Host "✅ Backup system test passed" -ForegroundColor Green
    } else {
        Write-Host "❌ Backup system test failed" -ForegroundColor Red
    }
}

function Show-Metrics {
    Write-Host "📊 Displaying system metrics..." -ForegroundColor Yellow
    
    $metricsScript = @"
import asyncio
import sys
import json
sys.path.append('.')

from storage import get_storage_metrics, comprehensive_health_check

async def show_metrics():
    print("📊 Current Storage Metrics:")
    print("=" * 40)
    
    metrics = await get_storage_metrics()
    health = await comprehensive_health_check()
    
    print(f"Requests Total: {metrics['requests_total']}")
    print(f"Error Rate: {metrics['error_rate']:.2%}")
    print(f"Cache Hit Ratio: {metrics['cache_hit_ratio']:.2%}")
    print(f"Avg Response Time: {metrics['avg_response_time_seconds']:.3f}s")
    print(f"Pool Utilization: {metrics['pool_size']}/{metrics['max_pool_size']}")
    
    print(f"\n🏥 Health Status: {health['overall_status'].upper()}")
    if 'bucket' in health:
        print(f"Bucket Latency: {health['bucket'].get('latency_seconds', 'N/A')}s")
    if 'cache' in health:
        print(f"Cache Status: {health['cache']['status']}")
    
    print("\n" + "=" * 40)

if __name__ == "__main__":
    asyncio.run(show_metrics())
"@
    
    $metricsScript | Out-File -FilePath "show_metrics.py" -Encoding utf8
    python show_metrics.py
    Remove-Item "show_metrics.py" -Force
}

function Setup-Monitoring {
    Write-Host "📈 Setting up monitoring dashboard..." -ForegroundColor Yellow
    
    # Create a simple monitoring script
    $monitorScript = @"
import asyncio
import time
import sys
sys.path.append('.')

from storage import comprehensive_health_check, get_storage_metrics

async def monitor_loop():
    print("🔄 Starting storage system monitor...")
    print("Press Ctrl+C to stop")
    
    try:
        while True:
            health = await comprehensive_health_check()
            metrics = await get_storage_metrics()
            
            # Clear screen (Windows)
            import os
            os.system('cls' if os.name == 'nt' else 'clear')
            
            print("🏥 CRAWL AGENT STORAGE MONITOR")
            print("=" * 50)
            print(f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"Status: {health['overall_status'].upper()}")
            print(f"Requests: {metrics['requests_total']}")
            print(f"Errors: {metrics['errors_total']} ({metrics['error_rate']:.2%})")
            print(f"Cache Hit Ratio: {metrics['cache_hit_ratio']:.2%}")
            print(f"Avg Response: {metrics['avg_response_time_seconds']:.3f}s")
            print(f"Pool Usage: {metrics['pool_size']}/{metrics['max_pool_size']}")
            print("=" * 50)
            
            await asyncio.sleep(5)  # Update every 5 seconds
            
    except KeyboardInterrupt:
        print("\n👋 Monitor stopped")

if __name__ == "__main__":
    asyncio.run(monitor_loop())
"@
    
    $monitorScript | Out-File -FilePath "storage_monitor.py" -Encoding utf8
    
    Write-Host "✅ Monitor script created: storage_monitor.py" -ForegroundColor Green
    Write-Host "Run 'python storage_monitor.py' to start monitoring" -ForegroundColor Cyan
}

# Main execution logic
switch ($Action) {
    "setup" {
        Install-Dependencies
        Setup-Environment
        Write-Host "🎉 Setup completed! Next steps:" -ForegroundColor Green
        Write-Host "1. Update your environment file with actual credentials" -ForegroundColor Yellow
        Write-Host "2. Run: .\setup_storage.ps1 -Action start" -ForegroundColor Yellow
        Write-Host "3. Run: .\setup_storage.ps1 -Action test" -ForegroundColor Yellow
    }
    
    "start" {
        Start-Services
        Write-Host "🚀 Services started! System is ready." -ForegroundColor Green
    }
    
    "test" {
        Test-StorageSystem
        Run-BackupTest
    }
    
    "metrics" {
        Show-Metrics
    }
    
    "monitor" {
        Setup-Monitoring
    }
    
    "all" {
        Install-Dependencies
        Setup-Environment
        Start-Services
        Start-Sleep 5
        Test-StorageSystem
        Run-BackupTest
        Show-Metrics
        Setup-Monitoring
        Write-Host "🎉 Complete setup and testing finished!" -ForegroundColor Green
    }
    
    default {
        Write-Host "❌ Unknown action: $Action" -ForegroundColor Red
        Write-Host "Available actions: setup, start, test, metrics, monitor, all" -ForegroundColor Yellow
    }
}

Write-Host "`n📚 For more information, see:" -ForegroundColor Cyan
Write-Host "- iam-policies.md (IAM setup)" -ForegroundColor White  
Write-Host "- backup-dr-setup.md (Backup & DR guide)" -ForegroundColor White
Write-Host "- backup_tasks.py (Celery backup tasks)" -ForegroundColor White
