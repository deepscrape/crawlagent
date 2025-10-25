"""
WebRTC utilities for handling WSL and Windows connectivity differences.
"""
import asyncio
import logging
import os
import platform
import socket
import subprocess
from typing import Any, Dict, List, Optional

from aiortc import RTCConfiguration, RTCIceServer, RTCPeerConnection

from utils.tools import get_wsl2_host_ip, is_wsl

logger = logging.getLogger("crawlagent")

def check_udp_port_open(host: str, port: int, timeout: float = 1.0) -> bool:
    """Check if a UDP port is open on a remote host by sending a packet and checking for a response."""
    sock = None
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(timeout)
        sock.sendto(b'ping', (host, port))
        data, _ = sock.recvfrom(1024)
        return True
    except (socket.timeout, socket.error):
        return False
    finally:
        if sock:
            sock.close()

def configure_ice_servers(stun_servers: Optional[List[str]] = None, 
                          turn_servers: Optional[List[Dict[str, Any]]] = None) -> RTCConfiguration:
    """
    Configure ICE servers for WebRTC connections.
    
    Args:
        stun_servers: List of STUN server URLs
        turn_servers: List of TURN server configurations with urls, username, and credential
        
    Returns:
        RTCConfiguration object with configured ICE servers
    """
    ice_servers = []
    
    # Add STUN servers
    if stun_servers:
        for stun in stun_servers:
            ice_servers.append(RTCIceServer(urls=[stun]))
    else:
        # Default STUN servers
        ice_servers.append(RTCIceServer(urls=["stun:stun.relay.metered.ca:80"]))
    
    # Add TURN servers
    if turn_servers:
        for turn in turn_servers:
            ice_servers.append(RTCIceServer(
                urls=turn.get("urls", []),
                username=turn.get("username", ""),
                credential=turn.get("credential", "")
            ))
    else:
        # Default TURN servers
        ice_servers.extend([
            RTCIceServer(
                urls=["turn:global.relay.metered.ca:80"],
                username="84b2500cd237d2601e7056bd",
                credential="HUbJ8CeFXMFqVPcL"
            ),
            RTCIceServer(
                urls=["turn:global.relay.metered.ca:80?transport=tcp"],
                username="84b2500cd237d2601e7056bd",
                credential="HUbJ8CeFXMFqVPcL"
            ),
            RTCIceServer(
                urls=["turn:global.relay.metered.ca:443"],
                username="84b2500cd237d2601e7056bd",
                credential="HUbJ8CeFXMFqVPcL"
            ),
            RTCIceServer(
                urls=["turns:global.relay.metered.ca:443?transport=tcp"],
                username="84b2500cd237d2601e7056bd",
                credential="HUbJ8CeFXMFqVPcL"
            ),
        ])
    
    return RTCConfiguration(iceServers=ice_servers)

def configure_webrtc_for_wsl(port_range: str = "40000-65535") -> dict:
    """
    Configure WebRTC for WSL environment by:
    1. Setting up host IP for ICE candidates
    2. Configuring port range for ICE
    3. Setting environment variables needed for aiortc
    4. Attempting to open firewall ports
    
    Args:
        port_range: Range of ports to use for WebRTC (format: "min-max")
        
    Returns:
        dict: Configuration details including host_ip, port_range, and success status
    """
    result = {
        "is_wsl": is_wsl(),
        "host_ip": None,
        "port_range": port_range,
        "success": False,
        "env_vars_set": False,
        "firewall_configured": False
    }
    
    if not result["is_wsl"]:
        logger.info("Not running in WSL, skipping WSL-specific WebRTC configuration")
        return result
    
    # Get WSL2 host IP (Windows host)
    host_ip = get_wsl2_host_ip()
    if not host_ip:
        logger.warning("Could not determine WSL2 host IP, using default")
        host_ip = "172.17.0.1"  # Common default gateway for WSL2
    
    result["host_ip"] = host_ip
    
    # Check if the environment variables are already set and match
    current_ice_ip = os.environ.get("AIORTC_ICE_IP")
    current_port_range = os.environ.get("AIORTC_ICE_PORT_RANGE")
    
    if current_ice_ip and current_ice_ip != str(host_ip):
        logger.warning(f"Overriding existing AIORTC_ICE_IP: {current_ice_ip} → {host_ip}")
    
    if current_port_range and current_port_range != port_range:
        logger.warning(f"Overriding existing AIORTC_ICE_PORT_RANGE: {current_port_range} → {port_range}")
    
    # Set environment variables for aiortc
    try:
        os.environ["AIORTC_ICE_IP"] = str(host_ip)
        os.environ["AIORTC_ICE_PORT_RANGE"] = port_range
        result["env_vars_set"] = True
    except Exception as e:
        logger.error(f"Failed to set environment variables: {e}")
        result["env_vars_set"] = False
    
    # Log configuration
    logger.info(f"Configured WebRTC for WSL with ICE IP: {host_ip} and port range: {port_range}")
    
    # Parse port range
    try:
        min_port, max_port = map(int, port_range.split("-"))
        logger.info(f"WebRTC will use UDP ports {min_port}-{max_port}, ensure Windows firewall allows these ports")
    except Exception as e:
        logger.error(f"Invalid port range format '{port_range}': {e}")
        result["success"] = False
        return result
    
    # Attempt to open firewall ports
    try:
        result["firewall_configured"] = open_firewall_ports_on_windows_host(port_range)
    except Exception as e:
        logger.warning(f"Failed to configure Windows firewall: {e}")
        result["firewall_configured"] = False
    
    result["success"] = result["env_vars_set"]
    return result

def open_firewall_ports_on_windows_host(port_range: str = "40000-65535", 
                                        rule_name: str = "CrawlerAI_WebRTC") -> bool:
    """
    Open Windows firewall ports for WebRTC from WSL.
    
    This function attempts to open the specified UDP ports in the Windows firewall
    by executing PowerShell commands from WSL.
    
    Args:
        port_range: Range of ports to open (format: "min-max")
        rule_name: Name for the firewall rule
        
    Returns:
        True if successful, False otherwise
    """
    if not is_wsl():
        logger.warning("Not running in WSL, skipping Windows firewall configuration")
        return False

    try:
        min_port, max_port = map(int, port_range.split("-"))
        
        # PowerShell command to check if the rule exists
        check_cmd = f"""powershell.exe -Command "Get-NetFirewallRule -DisplayName '{rule_name}' 2>$null | Out-Null; $?"
        """
        
        # Execute the check command and capture output
        check_result = subprocess.run(check_cmd, shell=True, capture_output=True, text=True)
        rule_exists = check_result.stdout.strip().lower() == "true"
        
        if rule_exists:
            logger.info(f"Firewall rule '{rule_name}' already exists")
            return True
        
        # PowerShell command to add the firewall rule
        cmd = f"""powershell.exe -Command "
            New-NetFirewallRule -DisplayName '{rule_name}' -Direction Inbound -Protocol UDP -LocalPort {min_port}-{max_port} -Action Allow -Profile Any -Description 'Allow WebRTC for CrawlerAI'
        "
        """
        
        # Execute the command
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode == 0:
            logger.info(f"Successfully opened UDP ports {port_range} in Windows firewall")
            return True
        else:
            logger.error(f"Failed to open firewall ports: {result.stderr}")
            return False
    
    except Exception as e:
        logger.error(f"Error configuring Windows firewall: {e}")
        return False

async def test_webrtc_connectivity(stun_server: str = "stun.l.google.com:19302") -> dict:
    """
    Test WebRTC connectivity by creating a connection and gathering ICE candidates.
    
    Args:
        stun_server: STUN server to use for testing
        
    Returns:
        Dictionary with test results
    """
    try:
        pc = RTCPeerConnection(RTCConfiguration(iceServers=[
            RTCIceServer(urls=[f"stun:{stun_server}"])
        ]))
        
        # Track gathered candidates
        candidates = []
        
        @pc.on("icecandidate")
        def on_ice_candidate(candidate):
            if candidate:
                candidates.append({
                    "type": candidate.type,
                    "foundation": candidate.foundation,
                    "component": candidate.component,
                    "protocol": candidate.protocol,
                    "address": candidate.address,
                    "port": candidate.port,
                    "related_address": candidate.related_address,
                    "related_port": candidate.related_port,
                })
        
        # Create data channel to trigger ICE gathering
        # We need to keep the reference even though we don't use it directly
        # to prevent garbage collection
        _ = pc.createDataChannel("test")
        
        # Create offer to start ICE gathering
        await pc.createOffer()
        await pc.setLocalDescription(await pc.createOffer())
        
        # Wait for ICE gathering to complete or timeout
        try:
            # Wait up to 5 seconds for at least one candidate
            for _ in range(50):  # 5 seconds with 0.1s intervals
                if candidates:
                    break
                await asyncio.sleep(0.1)
        finally:
            # Close connection
            await pc.close()
        
        # Check results
        has_public_candidate = any(c.get("type") == "srflx" for c in candidates)
        has_relay_candidate = any(c.get("type") == "relay" for c in candidates)
        
        return {
            "success": len(candidates) > 0,
            "candidates_count": len(candidates),
            "has_public_candidate": has_public_candidate,
            "has_relay_candidate": has_relay_candidate,
            "candidates": candidates,
        }
    
    except Exception as e:
        logger.error(f"Error testing WebRTC connectivity: {e}")
        return {
            "success": False,
            "error": str(e)
        }