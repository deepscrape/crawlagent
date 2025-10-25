"""
Diagnostic endpoints for system health checks and troubleshooting.
"""

import logging
import os
import platform
import subprocess
from typing import Any, Dict, List

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from auth import get_token_dependency
from configure import config
from utils import check_udp_port_open, is_wsl, open_firewall_ports_on_windows_host, test_webrtc_connectivity
from storage import S3ClientManager, TIGRIS_BUCKET_NAME, health_check

logger = logging.getLogger("crawlagent")
diagnostics_router = APIRouter(tags=["diagnostics"])

# Authentication dependency
verify_token = get_token_dependency(config)


class FirewallPortRequest(BaseModel):
    port_range: str = "40000-65535"
    rule_name: str = "CrawlerAI_WebRTC"


class DiagnosticInfoResponse(BaseModel):
    system: Dict[str, Any]
    network: Dict[str, Any]
    wsl: Dict[str, Any]  # Accepts mixed types for values (bool, str, etc.)
    webrtc: Dict[str, Any]
    environment: Dict[str, str]


@diagnostics_router.get("/info", response_model=DiagnosticInfoResponse, dependencies=[Depends(verify_token)])
async def get_system_info():
    """
    Get system diagnostic information including:
    - System information (OS, version, etc.)
    - Network configuration
    - WSL configuration (if applicable)
    - WebRTC diagnostics
    - Environment variables
    """
    # System info
    system_info = {
        "platform": platform.system(),
        "platform_release": platform.release(),
        "platform_version": platform.version(),
        "architecture": platform.machine(),
        "processor": platform.processor(),
        "hostname": platform.node(),
        "python_version": platform.python_version(),
    }
    
    # Network info
    network_info = {}
    try:
        if platform.system() == "Linux":
            # Get IP addresses on Linux
            ip_result = subprocess.run(
                ["ip", "addr", "show"], capture_output=True, text=True, check=True
            )
            network_info["ip_addresses"] = ip_result.stdout
            
            # Get default gateway
            route_result = subprocess.run(
                ["ip", "route", "show", "default"], capture_output=True, text=True, check=True
            )
            network_info["default_gateway"] = route_result.stdout.strip()
        elif platform.system() == "Windows":
            # Get IP addresses on Windows
            ipconfig_result = subprocess.run(
                ["ipconfig", "/all"], capture_output=True, text=True, check=True
            )
            network_info["ipconfig"] = ipconfig_result.stdout
    except Exception as e:
        network_info["error"] = str(e)
      # WSL info
    wsl_info: Dict[str, Any] = {"is_wsl": is_wsl()}
    if is_wsl():
        try:
            # Get WSL version
            wsl_result = subprocess.run(
                ["wsl.exe", "--version"], capture_output=True, text=True
            )
            wsl_info["wsl_version"] = wsl_result.stdout.strip()
            
            # Get WSL distribution info
            lsb_result = subprocess.run(
                ["lsb_release", "-a"], capture_output=True, text=True
            )
            wsl_info["distro_info"] = lsb_result.stdout.strip()
        except Exception as e:
            wsl_info["error"] = str(e)
    
    # WebRTC diagnostics
    webrtc_info = await test_webrtc_connectivity()
    
    # Environment variables (filtered for security)
    safe_env_vars = [
        "PATH", "USER", "HOME", "SHELL", "LANG", "DISPLAY", 
        "AIORTC_ICE_IP", "AIORTC_ICE_PORT_RANGE", "PYTHON_ENV"
    ]
    environment = {k: v for k, v in os.environ.items() if k in safe_env_vars}
    
    return DiagnosticInfoResponse(
        system=system_info,
        network=network_info,
        wsl=wsl_info,
        webrtc=webrtc_info,
        environment=environment
    )


@diagnostics_router.post("/open-firewall-ports", dependencies=[Depends(verify_token)])
async def open_firewall_ports(
    request: FirewallPortRequest
) -> Dict[str, Any]:
    """
    Open UDP ports in Windows Firewall for WebRTC connectivity from WSL.
    This endpoint can help fix WebRTC connectivity issues when running in WSL.
    """
    if not is_wsl():
        return {
            "success": False,
            "message": "This endpoint is only useful when running in WSL",
            "is_wsl": False
        }
    
    success = open_firewall_ports_on_windows_host(
        port_range=request.port_range,
        rule_name=request.rule_name
    )
    
    return {
        "success": success,
        "is_wsl": True,
        "port_range": request.port_range,
        "rule_name": request.rule_name,
        "message": "Firewall ports opened successfully" if success else 
                  "Failed to open firewall ports. Try running Windows PowerShell as administrator."
    }


@diagnostics_router.get("/webrtc-connectivity", dependencies=[Depends(verify_token)])
async def test_webrtc_connectivity_endpoint(
    stun_server: str = "stun.l.google.com:19302"
) -> Dict[str, Any]:
    """
    Test WebRTC connectivity by creating a connection and gathering ICE candidates.
    This endpoint can help diagnose WebRTC connectivity issues.
    """
    return await test_webrtc_connectivity(stun_server)


@diagnostics_router.get("/check-udp-ports", dependencies=[Depends(verify_token)])
async def check_udp_ports(
    host: str,
    ports: List[int]
) -> Dict[str, Any]:
    """
    Check if specific UDP ports are open on a remote host.
    This can help diagnose WebRTC connectivity issues with STUN/TURN servers.
    """
    results = {}
    
    for port in ports:
        is_open = check_udp_port_open(host, port)
        results[f"{host}:{port}"] = "open" if is_open else "closed/filtered"
    
    return {
        "host": host,
        "ports": ports,
        "results": results
    }


@diagnostics_router.get("/storage-health", dependencies=[Depends(verify_token)])
async def storage_health_check() -> Dict[str, Any]:
    """
    Health check for Tigris storage backend (S3-compatible).
    Calls the storage.health_check() coroutine and returns its result.
    """
    return await health_check()

