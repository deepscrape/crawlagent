"""
Utilities for working with WSL (Windows Subsystem for Linux).
"""
import logging
import os
import platform
import socket
import subprocess
from typing import Any, Dict

from utils.tools import get_wsl2_host_ip, is_wsl

logger = logging.getLogger("crawlagent")

def check_wsl_port_forwarding(port_range: str = "40000-65535") -> Dict[str, Any]:
    """
    Check if port forwarding is properly set up for WebRTC in WSL.
    
    Args:
        port_range: Range of ports to check (format: "min-max")
        
    Returns:
        Dict with check results
    """
    if not is_wsl():
        return {"is_wsl": False, "message": "Not running in WSL"}
    
    results = {
        "is_wsl": True,
        "host_ip": get_wsl2_host_ip(),
        "port_checks": {}
    }
    
    try:
        # Check if we can resolve the host IP
        if not results["host_ip"]:
            results["host_reachable"] = False
            results["message"] = "Could not determine Windows host IP"
            return results
        
        # Check if the host is reachable
        ping_cmd = f"ping -c 1 -W 1 {results['host_ip']}"
        ping_result = subprocess.run(ping_cmd, shell=True, capture_output=True)
        results["host_reachable"] = ping_result.returncode == 0
        
        # Check a sample of ports from the range
        min_port, max_port = map(int, port_range.split("-"))
        port_sample = [min_port, min_port + (max_port - min_port) // 2, max_port]
        
        for port in port_sample:
            # Create a UDP socket and try to send a packet
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.settimeout(0.5)
                sock.sendto(b"test", (results["host_ip"], port))
                results["port_checks"][port] = "Packet sent (no response expected)"
            except Exception as e:
                results["port_checks"][port] = f"Error: {str(e)}"
            finally:
                sock.close()
        
        results["success"] = results["host_reachable"]
        if results["success"]:
            results["message"] = "Windows host is reachable from WSL"
        else:
            results["message"] = "Windows host is not reachable from WSL"
            
    except Exception as e:
        results["success"] = False
        results["error"] = str(e)
        results["message"] = "Error checking WSL port forwarding"
    
    return results