#!/bin/bash
# filepath: d:\Windows\Documents\Programming\Projects\Python\crawlerai\crawlagent\run-server.sh

# Kill any existing Xvfb process
pkill Xvfb || true

# Start Xvfb with proper configuration
Xvfb :99 -screen 0 ${VIDEO_WIDTH:-1280}x${VIDEO_HEIGHT:-720}x24 &
XVFB_PID=$!

# Wait for Xvfb to be ready
echo "Waiting for Xvfb to start..."
for i in {1..10}; do
  if xdpyinfo -display :99 >/dev/null 2>&1; then
    echo "Xvfb started successfully"
    break
  fi
  echo "Waiting for Xvfb... $i"
  sleep 1
done

# Verify Xvfb is running
if ! xdpyinfo -display :99 >/dev/null 2>&1; then
  echo "ERROR: Xvfb failed to start properly!"
  exit 1
fi

# Export display for all processes
export DISPLAY=:99

# Start the application
# exec uvicorn server:app --host 0.0.0.0 --port 8000 --proxy-headers --forwarded-allow-ips="*" --ws websockets