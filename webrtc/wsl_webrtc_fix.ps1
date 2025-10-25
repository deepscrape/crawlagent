# PowerShell script to configure Windows Firewall for WebRTC in WSL
# Run this script as Administrator to open the necessary UDP ports
# Fixes WebRTC connectivity issues when running CrawlerAI in WSL

param (
    [string]$PortRange = "40000-65535",
    [string]$RuleName = "CrawlerAI_WebRTC",
    [switch]$Force = $false
)

# Set up colored output
$ESC = [char]27
$Green = "$ESC[32m"
$Yellow = "$ESC[33m"
$Red = "$ESC[31m"
$Blue = "$ESC[34m"
$Bold = "$ESC[1m"
$Reset = "$ESC[0m"

function Write-ColorOutput {
    param (
        [string]$Text,
        [string]$Color = "$Reset"
    )
    Write-Host "$Color$Text$Reset"
}

# Function to check if running as Administrator
function Test-Administrator {
    $currentUser = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())
    return $currentUser.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

# Check if running as admin
if (-not (Test-Administrator)) {
    Write-ColorOutput "This script must be run as Administrator." $Red
    Write-ColorOutput "Please restart PowerShell with elevated privileges and try again." $Yellow
    
    # Offer to restart as admin
    $restart = Read-Host "Would you like to restart this script as Administrator? (y/n)"
    if ($restart -eq "y") {
        $scriptPath = $MyInvocation.MyCommand.Path
        Start-Process powershell.exe -ArgumentList "-ExecutionPolicy Bypass -File `"$scriptPath`"" -Verb RunAs
    }
    exit 1
}

Write-ColorOutput "${Bold}WebRTC Configuration for CrawlerAI in WSL${Reset}" $Blue
Write-ColorOutput "This script will configure Windows Firewall to allow WebRTC traffic from WSL." $Reset
Write-Host

# Parse port range
$portRangeParts = $PortRange -split '-'
if ($portRangeParts.Count -ne 2) {
    Write-ColorOutput "Invalid port range format. Use min-max format (e.g., 40000-65535)" $Red
    exit 1
}

$minPort = [int]$portRangeParts[0]
$maxPort = [int]$portRangeParts[1]

Write-ColorOutput "${Bold}Firewall Configuration${Reset}" $Blue
Write-Host

# Check if rule already exists
$existingRule = Get-NetFirewallRule -DisplayName $RuleName -ErrorAction SilentlyContinue

if ($existingRule) {
    Write-ColorOutput "Firewall rule '$RuleName' already exists." $Yellow
    
    # Get rule details
    $rulePorts = (Get-NetFirewallPortFilter -AssociatedNetFirewallRule $existingRule).LocalPort
    $ruleEnabled = ($existingRule).Enabled
    $ruleAction = (Get-NetFirewallSecurityFilter -AssociatedNetFirewallRule $existingRule).Action
    
    Write-Host "Current rule details:"
    Write-Host "  - Ports: $rulePorts"
    Write-Host "  - Enabled: $ruleEnabled"
    Write-Host "  - Action: $ruleAction"
    
    if ($Force) {
        Write-Host "Removing existing rule to recreate it..."
        Remove-NetFirewallRule -DisplayName $RuleName
    } else {
        $recreate = Read-Host "Would you like to recreate this rule? (y/n)"
        if ($recreate -eq "y") {
            Remove-NetFirewallRule -DisplayName $RuleName
        } else {
            Write-ColorOutput "Using existing rule. If you have connectivity issues, run this script with -Force to recreate the rule." $Yellow
            # Skip to the verification part
            $rule = $existingRule
        }
    }
}

# Create the firewall rule if needed
if (-not $rule) {
    try {
        Write-ColorOutput "Creating firewall rule '$RuleName' for UDP ports $minPort-$maxPort..." $Green
        
        $params = @{
            DisplayName = $RuleName
            Direction = "Inbound"
            Protocol = "UDP"
            LocalPort = "$minPort-$maxPort"
            Action = "Allow"
            Profile = "Any"
            Description = "Allow WebRTC for CrawlerAI in WSL"
            Enabled = "True"
        }
        
        $rule = New-NetFirewallRule @params
        
        Write-ColorOutput "Successfully created firewall rule." $Green
    } catch {
        Write-ColorOutput "Failed to create firewall rule: $_" $Red
        exit 1
    }
}

# Verify the rule
Write-ColorOutput "${Bold}Firewall Rule Verification${Reset}" $Blue
Write-Host

$verifiedRule = Get-NetFirewallRule -DisplayName $RuleName -ErrorAction SilentlyContinue
if ($verifiedRule) {
    Write-ColorOutput "✓ Rule '$RuleName' is active." $Green
    
    # Additional verification
    $ruleEnabled = $verifiedRule.Enabled
    if ($ruleEnabled) {
        Write-ColorOutput "✓ Rule is enabled." $Green
    } else {
        Write-ColorOutput "⚠ Rule is disabled. Enabling it now..." $Yellow
        Set-NetFirewallRule -DisplayName $RuleName -Enabled True
    }
    
    # Check if the firewall profile is active
    $activeProfiles = (Get-NetFirewallProfile | Where-Object { $_.Enabled -eq $true }).Name
    Write-Host "Active firewall profiles: $($activeProfiles -join ', ')"
    
    # WSL connectivity diagnostics
    Write-ColorOutput "${Bold}WSL Connectivity Diagnostics${Reset}" $Blue
    Write-Host
    
    # Check if WSL is installed
    try {
        $wslInstalled = $null -ne (Get-Command wsl.exe -ErrorAction SilentlyContinue)
        if ($wslInstalled) {
            Write-ColorOutput "✓ WSL is installed." $Green
            
            # Display WSL information
            $wslStatus = wsl.exe --status 2>&1
            Write-Host "WSL Status:"
            Write-Host $wslStatus
            
            # Find WSL IP address
            Write-Host "`nWSL IP Address:"
            $wslIP = wsl.exe hostname -I 2>&1
            Write-Host "WSL IP: $wslIP"
            
            # Get Windows host IP as seen from WSL
            $hostIP = wsl.exe bash -c 'grep nameserver /etc/resolv.conf | awk "{print \$2}"' 2>&1
            Write-Host "Windows Host IP (from WSL): $hostIP"
                # Test UDP port binding
            $testPort = $minPort + 100 # Pick a test port in the range
            Write-Host "`nTesting UDP port $testPort..."
            try {
                $udpClient = New-Object System.Net.Sockets.UdpClient $testPort
                Write-ColorOutput "✓ Successfully bound to UDP port $testPort" $Green
                $udpClient.Close()
            } catch {
                Write-ColorOutput "⚠ Could not bind to UDP port $testPort" $Yellow 
            }

        } else {
            Write-ColorOutput "WSL does not appear to be installed." $Yellow
        }
    } catch {
        Write-ColorOutput "Error checking WSL installation: $_" $Yellow
    }
    
    # Display Host IP information
    Write-Host "`nWindows Host IP Addresses:"
    $hostIPs = ipconfig | Select-String "IPv4"
    Write-Host $hostIPs
    
    Write-Host
    Write-ColorOutput "${Bold}Summary${Reset}" $Blue
    Write-ColorOutput "✓ Firewall rule configuration complete." $Green
    Write-ColorOutput "WebRTC should now work properly between WSL and Windows." $Green
    Write-Host
    Write-Host "If you still have connection issues, try the following:"
    Write-Host "1. Restart your WSL instance: wsl.exe --shutdown"
    Write-Host "2. Set the environment variables in WSL:"
    Write-Host "   export AIORTC_ICE_IP=`"$hostIP`""
    Write-Host "   export AIORTC_ICE_PORT_RANGE=`"$PortRange`""
    Write-Host "3. Try running the WebRTC diagnostic endpoint:"
    Write-Host "   GET /api/v1/diagnostics/webrtc-connectivity"
} else {
    Write-ColorOutput "Failed to verify the created rule." $Red
    exit 1
}
