#!/usr/bin/env python3
"""
WebRTC Configuration Script for WSL

This script helps configure the Windows firewall and network settings
to enable WebRTC to work properly when running in WSL.

Usage:
  python wsl_webrtc_fix.py

Run this script on the Windows host (not in WSL) to properly configure
the firewall rules for WebRTC traffic.
"""

import os
import platform
import subprocess
import sys


def is_admin():
    """Check if script is running with admin privileges"""
    if os.name == "posix":
        try:
            return os.geteuid() == 0  # POSIX (Linux/WSL) method
        except AttributeError:
            return False
    else:
        # Windows method
        try:
            subprocess.check_output('net session', shell=True, stderr=subprocess.DEVNULL)
            return True
        except Exception:
            return False

def configure_windows_firewall():
    """Configure Windows firewall for WebRTC in WSL"""
    # Define port ranges for WebRTC
    webrtc_port_range = "40000-60000"

    # Check if we're on Windows
    if platform.system() != "Windows":
        print("This script must be run on Windows, not inside WSL.")
        sys.exit(1)
    
    # Check for admin privileges
    if not is_admin():
        print("This script requires Administrator privileges. Please run it as Administrator.")
        sys.exit(1)
    
    print("Configuring Windows Firewall for WebRTC in WSL...")
    
    # Create firewall rules for WebRTC UDP traffic
    commands = [
        f'netsh advfirewall firewall add rule name="WSL WebRTC UDP In" dir=in action=allow protocol=UDP localport={webrtc_port_range}',
        f'netsh advfirewall firewall add rule name="WSL WebRTC UDP Out" dir=out action=allow protocol=UDP localport={webrtc_port_range}'
    ]
    
    for cmd in commands:
        print(f"Running: {cmd}")
        try:
            subprocess.run(cmd, shell=True, check=True)
            print("✓ Success")
        except subprocess.CalledProcessError as e:
            print(f"✗ Error: {e}")
    
    print("\nFirewall configuration complete.")
    
    # Set environment variable for WebRTC port range
    print("\nSetting up environment variables for WebRTC...")
    
    try:
        # Set environment variable for current session
        os.environ["AIORTC_ICE_PORT_RANGE"] = webrtc_port_range
        
        # Set persistent environment variable using PowerShell
        ps_cmd = f'[Environment]::SetEnvironmentVariable("AIORTC_ICE_PORT_RANGE", "{webrtc_port_range}", "User")'
        subprocess.run(["powershell", "-Command", ps_cmd], check=True)
        print(f"✓ Set AIORTC_ICE_PORT_RANGE={webrtc_port_range}")
    except Exception as e:
        print(f"✗ Error setting environment variable: {e}")
    
    print("\nConfiguration complete!")
    print("\nImportant: When running your app in WSL, make sure to:")
    print("1. Expose your app on 0.0.0.0 instead of localhost")
    print("2. Access it from Windows using the WSL IP address (e.g., 172.x.x.x)")
    print("3. Make sure no Windows firewall popup blocks your application")

if __name__ == "__main__":
    configure_windows_firewall()
