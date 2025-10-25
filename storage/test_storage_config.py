#!/usr/bin/env python3
"""
Test script to verify that storage.py properly reads from config.yml
"""

import asyncio
import os

from storage import (
    CACHE_ENABLED,
    CACHE_PREFIX,
    CACHE_TTL,
    COMPRESSION_ENABLED,
    COMPRESSION_LEVEL,
    COMPRESSION_MIN_SIZE,
    CONCURRENT_REQUEST_LIMIT,
    HEALTH_CHECK_ENABLED,
    MAX_RETRY_ATTEMPTS,
    METRICS_ENABLED,
    MONITORING_ENABLED,
    POOL_SIZE,
    RETRY_BASE_DELAY,
    RETRY_EXPONENTIAL_BASE,
    RETRY_MAX_DELAY,
    TIGRIS_BUCKET_NAME,
    TIGRIS_ENDPOINT_URL,
    TIGRIS_REGION,
    comprehensive_health_check,
    get_storage_metrics,
)


def test_configuration_loading():
    """Test that configuration values are loaded correctly from config.yml"""
    print("=== Storage Configuration Test ===")
    print(f"Bucket Name: {TIGRIS_BUCKET_NAME}")
    print(f"Endpoint URL: {TIGRIS_ENDPOINT_URL}")
    print(f"Region: {TIGRIS_REGION}")
    print()
    
    print("=== Connection Pool Configuration ===")
    print(f"Pool Size: {POOL_SIZE}")
    print(f"Concurrent Request Limit: {CONCURRENT_REQUEST_LIMIT}")
    print()
    
    print("=== Cache Configuration ===")
    print(f"Cache Enabled: {CACHE_ENABLED}")
    print(f"Cache TTL: {CACHE_TTL} seconds")
    print(f"Cache Prefix: {CACHE_PREFIX}")
    print()
    
    print("=== Retry Configuration ===")
    print(f"Max Retry Attempts: {MAX_RETRY_ATTEMPTS}")
    print(f"Base Delay: {RETRY_BASE_DELAY}s")
    print(f"Max Delay: {RETRY_MAX_DELAY}s")
    print(f"Exponential Base: {RETRY_EXPONENTIAL_BASE}")
    print()
    
    print("=== Monitoring Configuration ===")
    print(f"Monitoring Enabled: {MONITORING_ENABLED}")
    print(f"Metrics Enabled: {METRICS_ENABLED}")
    print(f"Health Check Enabled: {HEALTH_CHECK_ENABLED}")
    print()
    
    print("=== Compression Configuration ===")
    print(f"Compression Enabled: {COMPRESSION_ENABLED}")
    print(f"Compression Level: {COMPRESSION_LEVEL}")
    print(f"Compression Min Size: {COMPRESSION_MIN_SIZE} bytes")
    print()

async def test_storage_functionality():
    """Test basic storage functionality"""
    print("=== Storage Functionality Test ===")
    
    try:
        # Test health check
        print("Testing comprehensive health check...")
        health = await comprehensive_health_check()
        print(f"Health Status: {health.get('overall_status', 'unknown')}")
        
        # Test metrics
        print("\nTesting storage metrics...")
        metrics = await get_storage_metrics()
        print(f"Current Metrics: {metrics}")
        
        print("\n✅ Storage functionality tests completed successfully!")
        
    except Exception as e:
        print(f"❌ Storage functionality test failed: {e}")

def main():
    """Main test function"""
    print("Testing Storage Configuration Optimization\n")
    
    # Test configuration loading
    test_configuration_loading()
    
    # Test async functionality
    print("Running async functionality tests...")
    asyncio.run(test_storage_functionality())
    
    print("\n=== Configuration Validation ===")
    
    # Validate that we're using config.yml values instead of hardcoded ones
    expected_values = {
        "bucket": "crawlagent.bucket.a",
        "pool_size": 20,
        "concurrent_limit": 100,
        "cache_ttl": 300,
        "compression_level": 3,
        "max_retry_attempts": 3
    }
    
    actual_values = {
        "bucket": TIGRIS_BUCKET_NAME,
        "pool_size": POOL_SIZE,
        "concurrent_limit": CONCURRENT_REQUEST_LIMIT,
        "cache_ttl": CACHE_TTL,
        "compression_level": COMPRESSION_LEVEL,
        "max_retry_attempts": MAX_RETRY_ATTEMPTS
    }
    
    print("Validating configuration values match config.yml...")
    all_good = True
    for key, expected in expected_values.items():
        actual = actual_values[key]
        if actual == expected:
            print(f"✅ {key}: {actual} (matches config.yml)")
        else:
            print(f"❌ {key}: {actual} (expected: {expected})")
            all_good = False
    
    if all_good:
        print("\n🎉 All configuration values are correctly loaded from config.yml!")
    else:
        print("\n⚠️  Some configuration values may not be loading correctly.")
    
    print("\n=== Environment Variables Check ===")
    required_env_vars = [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY"
    ]
    
    for env_var in required_env_vars:
        value = os.getenv(env_var)
        if value:
            print(f"✅ {env_var}: {'*' * min(len(value), 8)}... (set)")
        else:
            print(f"❌ {env_var}: Not set")

if __name__ == "__main__":
    main()
