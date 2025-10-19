import asyncio
import logging
import os
import platform
import signal  # Import signal for SIGTERM/SIGKILL
import subprocess
import time
from collections import deque
from typing import Dict, Optional, Tuple

from fastapi import HTTPException

logger = logging.getLogger("crawlagent")

class VirtualDisplayManager:
    def __init__(self, max_displays: int = 5, start_display_num: int = 99, start_debug_port: int = 9222):
        self.max_displays = max_displays
        self.start_display_num = start_display_num
        self.start_debug_port = start_debug_port
        # Store (debug_port, xvfb_process, last_used_timestamp)
        self.available_displays: deque[Tuple[int, int]] = deque() # (display_num, debug_port)
        # display_num -> (debug_port, xvfb_process, last_used)
        self.in_use_displays: Dict[int, Tuple[int, Optional[subprocess.Popen], float]] = {} 
        self.lock = asyncio.Lock()
        self.is_linux = platform.system() == "Linux"

        # Pre-populate available displays
        for i in range(self.max_displays):
            display_num = self.start_display_num + i
            debug_port = self.start_debug_port + i
            self.available_displays.append((display_num, debug_port))
        logger.info(f"Initialized VirtualDisplayManager with {len(self.available_displays)} available displays.")

    async def _launch_xvfb_and_fluxbox(self, display_num: int) -> subprocess.Popen:
        display_addr = f":{display_num}"
        # Get geometry from xdpyinfo if possible, else fallback
        geometry = "1920x1080x24"
        try:
            xdpyinfo = subprocess.run(
            ["xdpyinfo"], env={**os.environ, "DISPLAY": display_addr},
            capture_output=True, text=True, check=True
            )
            for line in xdpyinfo.stdout.splitlines():
                if "dimensions:" in line:
                    # Example line: dimensions:    1920x1080 pixels (508x285 millimeters)
                    dims = line.split("dimensions:")[1].split()[0]
                    width, height = dims.split("x")
                    geometry = f"{width}x{height}x24"
                    break
        except Exception as e:
            logger.warning(f"Could not get geometry from xdpyinfo: {e}. Using default {geometry}")
        
        # Prefer Xorg with Xdummy if available, else Xvfb
        xserver_bin = "Xorg"
        try:
            # Check if Xdummy is available
            subprocess.run(["which", "Xdummy"], check=True, capture_output=True)
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            xserver_bin = "Xvfb"
            try:
                # Check if Xvfb is available
                subprocess.run(["which", "Xvfb"], check=True, capture_output=True)
            except (subprocess.CalledProcessError, FileNotFoundError):
                raise RuntimeError("Neither Xdummy nor Xvfb found. Cannot launch virtual display.") from e

        # Check if fluxbox is available
        try:
            subprocess.run(["which", "fluxbox"], check=True, capture_output=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            raise RuntimeError("fluxbox not found. Cannot launch virtual display.")

        command = [xserver_bin, display_addr, "-screen", "0", geometry, "-nolisten", "tcp"]
        logger.info(f"Launching X server: {' '.join(command)}")
        # Use os.setsid to create a new session for Xvfb, so it doesn't die with the parent
        xvfb_process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, 
                                        preexec_fn = getattr(os, "setsid", None) if self.is_linux else None)
        await asyncio.sleep(0.5) # Give X server time to start
        
        # Start a minimal window manager (fluxbox)
        fluxbox_command = ["fluxbox"]
        logger.info(f"Launching fluxbox on {display_addr}: {' '.join(fluxbox_command)}")
        # Use os.setsid to create a new session for fluxbox, so it doesn't die with the parent
        preexec_fn = getattr(os, "setsid", None) if self.is_linux else None
        subprocess.Popen(fluxbox_command, env={**os.environ, "DISPLAY": display_addr}, stdout=subprocess.PIPE, 
                         stderr=subprocess.PIPE, preexec_fn=preexec_fn)
        await asyncio.sleep(0.2) # Give WM time to start
        
        return xvfb_process

    async def _kill_xvfb_and_fluxbox(self, xvfb_process: Optional[subprocess.Popen]):
        if xvfb_process:
            try:
                # Terminate the process group to kill Xvfb and its children (like fluxbox)
                if self.is_linux:
                    killpg = getattr(os, "killpg", None)
                    getpgid = getattr(os, "getpgid", None)
                    sigterm = getattr(signal, "SIGTERM", None)
                    if killpg and getpgid and sigterm:
                        killpg(getpgid(xvfb_process.pid), sigterm)
                else:
                    xvfb_process.terminate()
                await asyncio.to_thread(xvfb_process.wait, timeout=5)
            except subprocess.TimeoutExpired:
                if self.is_linux:
                    killpg = getattr(os, "killpg", None)
                    getpgid = getattr(os, "getpgid", None)
                    sigkill = getattr(signal, "SIGKILL", None)
                    if killpg and getpgid and sigkill:
                        killpg(getpgid(xvfb_process.pid), sigkill)
                else:
                    xvfb_process.kill()
                logger.warning(f"Xvfb process {xvfb_process.pid} killed after timeout.")
            except Exception as e:
                logger.error(f"Error closing Xvfb process {xvfb_process.pid}: {e}")

    async def get_display_and_port(self) -> Tuple[int, int, Optional[subprocess.Popen]]:
        async with self.lock:
            if not self.available_displays:
                raise HTTPException(status_code=503, detail="No virtual displays available. Max capacity reached.")
            
            display_num, debug_port = self.available_displays.popleft()
            
            xvfb_process = None
            if self.is_linux:
                xvfb_process = await self._launch_xvfb_and_fluxbox(display_num)
            
            self.in_use_displays[display_num] = (debug_port, xvfb_process, time.time())
            logger.info(f"Allocated display :{display_num} (port {debug_port}). Available: {len(self.available_displays)}, \
                        In Use: {len(self.in_use_displays)}")
            return display_num, debug_port, xvfb_process

    async def release_display_and_port(self, display_num: int):
        async with self.lock:
            if display_num in self.in_use_displays:
                debug_port, xvfb_process, _ = self.in_use_displays.pop(display_num)
                await self._kill_xvfb_and_fluxbox(xvfb_process)
                self.available_displays.append((display_num, debug_port))
                logger.info(f"Released display :{display_num} (port {debug_port}). Available: {len(self.available_displays)}, \
                            In Use: {len(self.in_use_displays)}")
            else:
                logger.warning(f"Attempted to release unknown display :{display_num}")

    async def cleanup_idle_displays(self, idle_timeout: int = 300): # 5 minutes
        # This method is more for future expansion if we want to pre-launch displays
        # For now, displays are launched on demand and killed on release.
        pass

    async def close_all_displays(self):
        async with self.lock:
            for display_num in list(self.in_use_displays.keys()):
                debug_port, xvfb_process, _ = self.in_use_displays.pop(display_num)
                await self._kill_xvfb_and_fluxbox(xvfb_process)
            self.available_displays.clear()
            logger.info("All virtual displays and Xvfb processes closed.")
