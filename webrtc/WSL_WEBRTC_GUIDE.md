# WebRTC in WSL Configuration Guide

This guide explains how to configure WebRTC for CrawlerAI when running in Windows Subsystem for Linux (WSL).

## Issue Overview

When running the CrawlerAI application in WSL, WebRTC connectivity may not work properly due to networking differences between WSL and Windows. Specifically:

1. UDP traffic needs to pass from WSL to Windows and vice versa
2. WebRTC needs to know the correct IP address to use for ICE candidates
3. Windows Firewall may block the necessary UDP ports

This guide provides solutions for these issues.

## Quick Setup

We've provided two scripts to help you set up WebRTC in WSL:

### 1. For Linux/WSL (Run First)

Run the diagnostic script in WSL:

```bash
chmod +x wsl_webrtc_fix.sh
./wsl_webrtc_fix.sh
```

This script will:
- Check if you're running in WSL
- Determine the correct Windows host IP
- Set the necessary environment variables
- Test UDP connectivity to Windows
- Optionally add environment variables to your shell configuration

### 2. For Windows (Run as Administrator)

Run the PowerShell script as Administrator in Windows:

```powershell
powershell.exe -ExecutionPolicy Bypass -File wsl_webrtc_fix.ps1
```

This script will:
- Configure Windows Firewall to allow UDP traffic for WebRTC
- Create a firewall rule for the port range used by WebRTC
- Show diagnostics about your WSL configuration

## Technical Background

### How WebRTC Works in WSL

WSL (Windows Subsystem for Linux) has a special networking setup where:

1. WSL has its own network interface and IP address (typically 172.x.x.x)
2. Windows host acts as a network gateway for WSL
3. For WebRTC to work properly, we need to:
   - Tell WebRTC to use the Windows host IP for ICE candidates
   - Configure port ranges that match in both WSL and Windows
   - Ensure firewall rules allow UDP traffic on those ports

### Why CDPRawVideoTrack Fails in WSL

The CDPRawVideoTrack uses WebRTC to stream video data. In WSL:
1. The default network configuration confuses WebRTC's ICE candidates
2. Windows Firewall blocks UDP traffic needed for peer connections
3. STUN/TURN servers can't establish proper connections due to incorrect IP bindings

## Manual Configuration

If the scripts don't work for you, here's how to manually configure WebRTC for WSL:

### 1. Set Environment Variables in WSL

```bash
# Get Windows host IP address (appears as nameserver in resolv.conf)
export AIORTC_ICE_IP=$(grep nameserver /etc/resolv.conf | awk '{print $2}')
export AIORTC_ICE_PORT_RANGE="40000-65535"

# Add to your shell config to make permanent
echo "export AIORTC_ICE_IP=\"$AIORTC_ICE_IP\"" >> ~/.bashrc
echo "export AIORTC_ICE_PORT_RANGE=\"40000-65535\"" >> ~/.bashrc
```

### 2. Open Windows Firewall Ports

1. Open Windows Defender Firewall with Advanced Security
2. Select "Inbound Rules" and click "New Rule..."
3. Select "Port" as the rule type
4. Choose "UDP" as the protocol and enter "40000-65535" as the port range
5. Allow the connection
6. Apply the rule to all profiles (Domain, Private, Public)
7. Name the rule "CrawlerAI_WebRTC" and finish

## Troubleshooting

If you're still experiencing issues:

### Diagnostic Endpoints

The application provides several diagnostic endpoints to help troubleshoot issues:

1. **Check WebRTC Connectivity**:
   ```
   GET /api/v1/diagnostics/webrtc-connectivity
   ```
   This tests if WebRTC can establish connections and gather ICE candidates.

2. **Check UDP Ports**:
   ```
   GET /api/v1/diagnostics/check-udp-ports?host=<your_windows_host_ip>&ports=40000,50000,60000
   ```
   Tests if specific UDP ports are open between WSL and Windows.

3. **System Information**:
   ```
   GET /api/v1/diagnostics/info
   ```
   Provides detailed system, network and WebRTC configuration information.

4. **Open Firewall Ports**:
   ```
   POST /api/v1/diagnostics/open-firewall-ports
   ```
   Attempts to automatically configure Windows Firewall (requires administrative privileges).

### Common Problems and Solutions

1. **No WebRTC connection**:
   - Ensure Windows Firewall allows UDP traffic on the specified port range
   - Verify that `AIORTC_ICE_IP` is set to the correct Windows host IP
   - Check that the port range matches in both WSL and Windows configurations

2. **ICE gathering fails**:
   - Ensure public internet access is available for STUN server connections
   - Check if your corporate network allows UDP traffic (many block it)
   - Try using a different STUN/TURN server in the configuration

3. **WebRTC works but video quality is poor**:
   - This could indicate partial connectivity (using TURN fallback)
   - Check that direct UDP connections are possible between WSL and Windows

4. **WSL detection issues**:
   - If the scripts don't correctly detect WSL, manually set the environment variables
   - In rare cases, you may need to modify the scripts to match your specific WSL setup

### Advanced Diagnostics

For more detailed diagnostics, try these commands:

```bash
# Check UDP port connectivity
nc -vuz <windows_host_ip> 40000

# View active network connections
netstat -tuna | grep <port>

# Test STUN server connectivity
python3 -c "
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.sendto(b'\x00\x01\x00\x00\x21\x12\xa4\x42\x00\x00\x00\x00\x00\x00\x00\x00', ('stun.l.google.com', 19302))
data, addr = s.recvfrom(1024)
print(f'Received response from {addr}')
"
```

## Restart and Verification

After making changes:

1. Restart your WSL instance: `wsl.exe --shutdown` (from PowerShell)
2. Launch WSL again and verify environment variables: `echo $AIORTC_ICE_IP`
3. Start the CrawlerAI application and check the logs for WebRTC initialization
4. Use the diagnostic endpoints to verify connectivity

For persistent issues, please contact support with the diagnostic information from both scripts.

2. **ICE gathering fails**: Check if the STUN/TURN servers are reachable from your WSL instance.

3. **Connection works in PowerShell but not WSL**: This is likely a firewall or network configuration issue between WSL and Windows.

## Additional Resources

- [WebRTC Troubleshooting Guide](https://webrtc.org/getting-started/troubleshooting)
- [WSL Networking Documentation](https://docs.microsoft.com/en-us/windows/wsl/networking)
- [Windows Firewall Configuration Guide](https://docs.microsoft.com/en-us/windows/security/threat-protection/windows-firewall/windows-firewall-with-advanced-security)
