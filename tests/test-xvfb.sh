#!/bin/bash
# test-xvfb.sh

# Start Xvfb
Xvfb :99 -screen 0 1280x720x24 &
XVFB_PID=$!
export DISPLAY=:99

# Wait for Xvfb to start
sleep 2

# Test if Xvfb is working
if xdpyinfo >/dev/null; then
  echo "Xvfb is working!"
  
  # Take a screenshot to verify
  import -window root /tmp/screenshot.png
  echo "Screenshot saved to /tmp/screenshot.png"
else
  echo "Xvfb failed to start!"
fi

# Clean up
kill $XVFB_PID