#!/bin/bash
# Script to diagnose and configure WebRTC for CrawlerAI in WSL

# ANSI color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color
BOLD='\033[1m'

# Function to check if running in WSL
is_wsl() {
    # Comprehensive WSL detection
    if [ -n "${WSL_DISTRO_NAME}" ] || [ -n "${IS_WSL}" ]; then
        return 0  # Running in WSL (environment variable check)
    elif grep -qE 'Microsoft|microsoft|WSL' /proc/version 2>/dev/null; then
        return 0  # Running in WSL (version check)
    elif grep -qE 'Microsoft|microsoft|WSL' /proc/sys/kernel/osrelease 2>/dev/null; then
        return 0  # Running in WSL2 (kernel check)
    elif uname -r | grep -qE 'microsoft|WSL' 2>/dev/null; then
        return 0  # Another check for WSL2 (kernel release)
    else
        # Extra check for WSL existence
        if [ -e /proc/sys/fs/binfmt_misc/WSLInterop ]; then
            return 0  # WSLInterop exists
        else
            return 1  # Not running in WSL
        fi
    fi
}

# Function to get Windows host IP
get_host_ip() {
    # First try from resolv.conf (most reliable in WSL2)
    local nameserver=$(grep nameserver /etc/resolv.conf 2>/dev/null | head -n 1 | awk '{print $2}')
    if [[ -n "$nameserver" && "$nameserver" != "127.0.0.1" ]]; then
        echo "$nameserver"
        return
    fi
    
    # Then try host.docker.internal
    if command -v host >/dev/null 2>&1; then
        if host host.docker.internal >/dev/null 2>&1; then
            local docker_ip=$(host host.docker.internal 2>/dev/null | grep "has address" | head -n 1 | awk '{print $4}')
            if [[ -n "$docker_ip" ]]; then
                echo "$docker_ip"
                return
            fi
        fi
    fi
    
    # Try default route
    if command -v ip >/dev/null 2>&1; then
        local default_route=$(ip route show default 2>/dev/null | head -n 1 | awk '{print $3}')
        if [[ -n "$default_route" ]]; then
            echo "$default_route"
            return
        fi
    fi
    
    # Try cat /etc/resolv.conf as a fallback
    if [ -f /etc/resolv.conf ]; then
        echo "Trying to read /etc/resolv.conf directly..." >&2
        local ns=$(cat /etc/resolv.conf 2>/dev/null | grep nameserver | head -n 1 | awk '{print $2}')
        if [[ -n "$ns" && "$ns" != "127.0.0.1" ]]; then
            echo "$ns"
            return
        fi
    fi
    
    # Default fallback for WSL2
    echo "172.17.0.1"
}

# Function to check UDP connectivity to Windows host
check_udp_port() {
    local host=$1
    local port=$2
    timeout 1 bash -c "echo -n 'test' > /dev/udp/$host/$port" 2>/dev/null
    return $?
}

# Function to print section header
print_header() {
    echo -e "\n${BLUE}${BOLD}$1${NC}"
    echo -e "${BLUE}$(printf '=%.0s' $(seq 1 ${#1}))${NC}\n"
}

# Main script
echo -e "${BOLD}${CYAN}WebRTC Diagnostic Tool for CrawlerAI in WSL${NC}"
echo -e "This script will help diagnose and configure WebRTC connectivity issues.\n"

# Check if running in WSL
if is_wsl; then
    echo -e "${GREEN}✓ Running in WSL environment${NC}"
else
    echo -e "${RED}✗ Not running in WSL environment${NC}"
    echo -e "This script is intended to be run in WSL. Some checks may not be accurate."
fi

# System information
print_header "System Information"
echo -e "${BOLD}OS:${NC} $(uname -a)"
echo -e "${BOLD}Distribution:${NC} $(lsb_release -ds 2>/dev/null || cat /etc/os-release | grep PRETTY_NAME | cut -d= -f2)"
echo -e "${BOLD}WSL Version:${NC} $(wsl.exe --version 2>/dev/null || echo "Unknown")"

# Network configuration
print_header "Network Configuration"
HOST_IP=$(get_host_ip)
echo -e "${BOLD}Windows Host IP:${NC} $HOST_IP"
echo -e "${BOLD}WSL IP:${NC} $(hostname -I | awk '{print $1}')"
echo -e "${BOLD}Ping Windows Host:${NC}"
if ping -c 1 -W 2 $HOST_IP >/dev/null 2>&1; then
    echo -e "${GREEN}✓ Can ping Windows host${NC}"
else
    echo -e "${RED}✗ Cannot ping Windows host${NC}"
    echo -e "This may indicate network connectivity issues between WSL and Windows."
fi

# WebRTC ports check
print_header "WebRTC UDP Port Check"
echo -e "Testing UDP connectivity to Windows host (sample ports):"

# Define port ranges to test
PORT_RANGE="40000-65535"
MIN_PORT=$(echo $PORT_RANGE | cut -d- -f1)
MAX_PORT=$(echo $PORT_RANGE | cut -d- -f2)
MID_PORT=$(( (MIN_PORT + MAX_PORT) / 2 ))

# Generate test ports - ensure we have 3 distinct ports
TEST_PORTS=($MIN_PORT $MID_PORT $MAX_PORT)
# Remove duplicates
TEST_PORTS=($(printf "%s\n" "${TEST_PORTS[@]}" | sort -n | uniq))

# Add some random ports in the range if we have fewer than 3 ports
while [ ${#TEST_PORTS[@]} -lt 3 ]; do
    RANDOM_PORT=$(( $RANDOM % ($MAX_PORT - $MIN_PORT + 1) + $MIN_PORT ))
    # Only add if not already in the array
    if [[ ! " ${TEST_PORTS[@]} " =~ " ${RANDOM_PORT} " ]]; then
        TEST_PORTS+=($RANDOM_PORT)
    fi
done

echo -e "${BOLD}Windows Host IP:${NC} $HOST_IP"
echo -e "${BOLD}Testing ports:${NC} ${TEST_PORTS[*]}"

PORTS_OK=0
for port in "${TEST_PORTS[@]}"; do
    printf "${BOLD}Port %s:${NC} " "$port"
    
    # We'll try a more reliable UDP test
    (
        # Use a subshell with timeout
        timeout 1 bash -c "echo -n 'test' > /dev/udp/$HOST_IP/$port" 2>/dev/null
        PORT_RESULT=$?
        
        if [ $PORT_RESULT -eq 0 ]; then
            echo -e "${GREEN}✓ Appears open${NC}"
            exit 0
        else
            echo -e "${YELLOW}? May be filtered${NC} (normal for UDP tests)"
            exit 1
        fi
    )
    
    # Check the result from the subshell
    if [ $? -eq 0 ]; then
        PORTS_OK=$((PORTS_OK + 1))
    fi
    
    # Add a slight delay to prevent overwhelming output
    sleep 0.2
done

# Environment variables for WebRTC
print_header "WebRTC Environment Variables"
echo -e "Setting up environment variables for WebRTC in WSL:"

# Set environment variables
export AIORTC_ICE_IP="$HOST_IP"
export AIORTC_ICE_PORT_RANGE="$PORT_RANGE"

echo -e "${BOLD}AIORTC_ICE_IP:${NC} $AIORTC_ICE_IP"
echo -e "${BOLD}AIORTC_ICE_PORT_RANGE:${NC} $AIORTC_ICE_PORT_RANGE"

# Detect the current shell and its config file
SHELL_CONFIG=""
if [ -n "$BASH_VERSION" ]; then
    SHELL_NAME="bash"
    SHELL_CONFIG="$HOME/.bashrc"
elif [ -n "$ZSH_VERSION" ]; then
    SHELL_NAME="zsh"
    SHELL_CONFIG="$HOME/.zshrc"
else
    # Try to guess based on the default shell
    DEFAULT_SHELL=$(basename "$SHELL")
    if [ "$DEFAULT_SHELL" = "bash" ]; then
        SHELL_NAME="bash"
        SHELL_CONFIG="$HOME/.bashrc"
    elif [ "$DEFAULT_SHELL" = "zsh" ]; then
        SHELL_NAME="zsh"
        SHELL_CONFIG="$HOME/.zshrc"
    else
        # Fallback
        SHELL_NAME="unknown"
        SHELL_CONFIG="$HOME/.profile"
    fi
fi

echo -e "\nDetected shell: ${BOLD}$SHELL_NAME${NC}"
echo -e "Configuration file: ${BOLD}$SHELL_CONFIG${NC}"

# Make permanent in shell config if requested
echo -e "\nWould you like to add these environment variables to your $SHELL_NAME configuration? (y/n)"
read -r answer
if [[ "$answer" =~ ^[Yy]$ ]]; then
    # First remove any existing entries to avoid duplicates
    if [ -f "$SHELL_CONFIG" ]; then
        sed -i '/AIORTC_ICE_IP=/d' "$SHELL_CONFIG"
        sed -i '/AIORTC_ICE_PORT_RANGE=/d' "$SHELL_CONFIG"
    fi
    
    # Add the exports
    echo -e "\n# CrawlerAI WebRTC Configuration" >> "$SHELL_CONFIG"
    echo "export AIORTC_ICE_IP=\"$HOST_IP\"" >> "$SHELL_CONFIG"
    echo "export AIORTC_ICE_PORT_RANGE=\"$PORT_RANGE\"" >> "$SHELL_CONFIG"
    
    echo -e "${GREEN}✓ Environment variables added to $SHELL_CONFIG${NC}"
    echo -e "Run this command to apply changes to your current session:"
    echo -e "${CYAN}source $SHELL_CONFIG${NC}"
fi

# Windows firewall configuration
print_header "Windows Firewall Configuration"
echo -e "To ensure WebRTC works correctly, you need to configure the Windows firewall."
echo -e "You can do this by running the 'wsl_webrtc_fix.ps1' script as Administrator in PowerShell."
echo -e "\nLocation: ./wsl_webrtc_fix.ps1"
echo -e "Usage: ${CYAN}powershell.exe -ExecutionPolicy Bypass -File wsl_webrtc_fix.ps1${NC}"

# Additional checks for network configuration
print_header "Additional Network Tests"

# Check if ping works to host
echo -e "${BOLD}Ping test to Windows host:${NC}"
if ping -c 1 -W 2 $HOST_IP >/dev/null 2>&1; then
    echo -e "${GREEN}✓ Ping to Windows host successful${NC}"
else
    echo -e "${YELLOW}! Ping to Windows host failed${NC}"
    echo -e "This could indicate firewall issues or networking problems."
fi

# Check WSL network interface
echo -e "\n${BOLD}WSL Network Interface:${NC}"
ip addr show | grep -E "eth0|[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+" | head -n 2

# Try to detect if Windows Defender Firewall is enabled
echo -e "\n${BOLD}Checking Windows Firewall Status:${NC}"
FIREWALL_CHECK=$(powershell.exe -Command "Get-NetFirewallProfile | Select-Object -Property Name,Enabled" 2>/dev/null)
if [ $? -eq 0 ]; then
    echo -e "$FIREWALL_CHECK"
    if echo "$FIREWALL_CHECK" | grep -q "True"; then
        echo -e "${YELLOW}! Windows Firewall appears to be enabled${NC}"
        echo -e "Make sure to run the PowerShell script to create the necessary firewall rules."
    fi
else
    echo -e "${YELLOW}Unable to check Windows Firewall status${NC}"
fi

# Summary
print_header "Summary"
if [ $PORTS_OK -gt 0 ]; then
    echo -e "${GREEN}✓ Basic UDP connectivity appears to be working${NC}"
    echo -e "WebRTC should work if Windows firewall is properly configured."
else
    echo -e "${YELLOW}! UDP connectivity tests were inconclusive${NC}"
    echo -e "This could be due to Windows firewall blocking the connections."
    echo -e "Please run the PowerShell script as Administrator to configure the firewall."
fi

# Store diagnostics in a file
DIAG_FILE="webrtc_wsl_diagnostics.txt"
{
    echo "=== CrawlerAI WebRTC WSL Diagnostics ==="
    echo "Date: $(date)"
    echo "Host IP: $HOST_IP"
    echo "WSL IP: $(hostname -I | awk '{print $1}')"
    echo "WSL Version: $(wsl.exe --version 2>/dev/null || echo "Unknown")"
    echo "Ports tested: ${TEST_PORTS[*]}"
    echo "Ports OK: $PORTS_OK"
    echo "=== Environment Variables ==="
    echo "AIORTC_ICE_IP: $AIORTC_ICE_IP"
    echo "AIORTC_ICE_PORT_RANGE: $AIORTC_ICE_PORT_RANGE"
    echo "=== System Info ==="
    uname -a
    cat /etc/os-release 2>/dev/null || echo "OS release info not available"
} > "$DIAG_FILE"

echo -e "\n${BOLD}Next Steps:${NC}"
echo -e "1. ${CYAN}Run the Windows PowerShell script as Administrator to configure the firewall:${NC}"
echo -e "   ${CYAN}powershell.exe -ExecutionPolicy Bypass -File wsl_webrtc_fix.ps1${NC}"
echo -e "2. Apply the environment variables to your current session:"
echo -e "   ${CYAN}source $SHELL_CONFIG${NC}"
echo -e "3. Restart your application to use the new environment variables"
echo -e "4. If issues persist, try restarting the WSL instance:"
echo -e "   ${CYAN}wsl.exe --shutdown${NC} (from PowerShell)"

echo -e "\n${BOLD}Diagnostics saved to:${NC} $DIAG_FILE"

exit 0
