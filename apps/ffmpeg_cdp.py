# server_ffmpeg_cdp.py
import asyncio
import logging
import os
import shlex
import subprocess
from typing import Dict

import numpy as np
from aiortc import MediaStreamError, RTCConfiguration, RTCIceServer, RTCPeerConnection, VideoStreamTrack
from av import VideoFrame
from pydantic import BaseModel

from monitoring import manager
from utils import get_wsl2_host_ip
from virtual_display_manager import VirtualDisplayManager  # Import VirtualDisplayManager

logger = logging.getLogger("crawlagent")

# --- CONFIG ---
WIDTH = int(os.getenv("VIDEO_WIDTH", "1280"))
HEIGHT = int(os.getenv("VIDEO_HEIGHT", "720"))
FPS = int(os.getenv("VIDEO_FPS", "30"))
FRAME_BYTES = WIDTH * HEIGHT * 3  # rgb24
FFMPEG_LOGLEVEL = "info"# Optional: default fallback if env vars not set
ICE_IP = os.getenv("AIORTC_ICE_IP", get_wsl2_host_ip())
ICE_PORTS = os.getenv("AIORTC_ICE_PORT_RANGE", "40000-40100")

# Configure ICE servers and port range
RTC_CONFIG = RTCConfiguration(
    iceServers=[
        RTCIceServer(urls=["stun:stun.l.google.com:19302"]),
        # RTCIceServer(urls=["stun:global.stun.twilio.com:3478"]),
        # RTCIceServer(
        #     urls=["turn:global.turn.twilio.com:3478?transport=udp"],
        #     username="9fd980264c6f3c19f68b59dafc27425a8633354695542d094e11210ab33b9cc7",
        #     credential="iMNQsOQiSEx6KlaYt0qEu2pQFcBvKi/NrLMbEx1Yn8k="
        # ),
        # RTCIceServer(
        #     urls=["turn:global.turn.twilio.com:3478?transport=tcp"],
        #     username="9fd980264c6f3c19f68b59dafc27425a8633354695542d094e11210ab33b9cc7",
        #     credential="iMNQsOQiSEx6KlaYt0qEu2pQFcBvKi/NrLMbEx1Yn8k="
        # ),
        # RTCIceServer(
        #     urls=["turn:global.turn.twilio.com:443?transport=tcp"],
        #     username="9fd980264c6f3c19f68b59dafc27425a8633354695542d094e11210ab33b9cc7",
        #     credential="iMNQsOQiSEx6KlaYt0qEu2pQFcBvKi/NrLMbEx1Yn8k="
        # ),
        RTCIceServer(urls=["stun:stun.relay.metered.ca:80"]),
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
    ],
)

print(f"Using ICE IP: {ICE_IP} and port range: {ICE_PORTS}")


# session storage


# ---------- models ----------
class OfferModel(BaseModel):
    sdp: str
    type: str
    session_id: str | None = None

# ---------- Video track: reads raw rgb24 frames from ffmpeg stdout ----------
class FFmpegRawVideoTrack(VideoStreamTrack):
    def __init__(self, stdout, width=WIDTH, height=HEIGHT, fps=FPS):
        super().__init__()
        self.stdout = stdout
        self.width = width
        self.height = height
        self.fps = fps
        self.frame_bytes = width * height * 3
        self._started = False    
    async def recv(self):
        try:
            # pacing based on frame rate handled by next_timestamp() timestamps
            loop = asyncio.get_event_loop()
            data = await loop.run_in_executor(None, self.stdout.read, self.frame_bytes)
            if not data or len(data) < self.frame_bytes:
                logger.warning(f"FFmpegRawVideoTrack: EOF or short read, len(data)={len(data)}")
                raise MediaStreamError
                # EOF -> raise to stop stream
                # raise asyncio.CancelledError("ffmpeg ended or short read")
            logger.debug(f"FFmpegRawVideoTrack: Read {len(data)} bytes from FFmpeg stdout")
            arr = np.frombuffer(data, dtype=np.uint8)
            arr = arr.reshape((self.height, self.width, 3))        # convert to VideoFrame (rgb24)
            frame = VideoFrame.from_ndarray(arr, format="rgb24")
            pts, time_base = await self.next_timestamp()
            frame.pts = pts
            frame.time_base = time_base
            return frame
        except Exception as e:
            logger.error(f"FFmpegRawVideoTrack error: {e}")
            raise MediaStreamError from e

# ---------- helpers ----------
async def start_ffmpeg(display=":99", width=1280, height=720, fps=30):
    """
    Launch FFmpeg to capture X display and output raw rgb24 frames on stdout.
    Caller must provide Xvfb started on display first.
    """
    cmd = (
        f"ffmpeg -hide_banner -loglevel error "
        f"-probesize 32M -analyzeduration 10M -thread_queue_size 1024 -fflags nobuffer -flags low_delay -vsync 0 "
        f"-f x11grab -framerate {fps} -video_size {width}x{height} -i {display} "
        f"-pix_fmt rgb24 -f rawvideo pipe:1"
    )

    proc = subprocess.Popen(
        shlex.split(cmd),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=10**8,  # Prevent blocking due to buffer fullness
    )

    async def log_ffmpeg_stderr():
        if proc.stderr:
            while True:
                line = await asyncio.to_thread(proc.stderr.readline)
                if not line:
                    break
                logger.error(f"FFmpeg STDERR: {line.decode().strip()}")
            logger.info("FFmpeg stderr logging stopped.")
        else:
            logger.warning("FFmpeg process stderr is not available.")

    asyncio.create_task(log_ffmpeg_stderr())
    return proc


async def cleanup_session(session_id: str, pcs: Dict[str, RTCPeerConnection],
                          page_store: Dict[str, Dict], 
                          ffmpeg_procs: Dict[str, subprocess.Popen],
                          virtual_display_manager: VirtualDisplayManager): # Added virtual_display_manager
    logger.warning(f"Initiating cleanup for session {session_id}...") # Changed to warning for emphasis
    pc = pcs.pop(session_id, None)
    if pc:
        try:
            await pc.close()
        except Exception:
            pass
    page_info = page_store.pop(session_id, None)
    if page_info:
        try:
            # Close Playwright browser and context
            if "browser" in page_info and page_info["browser"]:
                await page_info["browser"].close()
            if "playwright" in page_info and page_info["playwright"]:
                await page_info["playwright"].stop()
        except Exception as e:
            logger.error(f"Error closing Playwright browser/context for session {session_id}: {e}")
    
    ff = ffmpeg_procs.pop(session_id, None)
    if ff and ff.poll() is None:
        ff.terminate()
        try:
            ff.wait(timeout=2)
        except subprocess.TimeoutExpired:
            ff.kill()
        except Exception as e:
            logger.error(f"Error terminating ffmpeg process for session {session_id}: {e}")
    
    # Release virtual display resources
    if page_info and "display_num" in page_info and page_info["display_num"] is not None:
        display_num = page_info["display_num"]
        await virtual_display_manager.release_display_and_port(display_num)

    # Get WebSocket from ConnectionManager
    ws = manager.get_websocket_by_session_id(session_id)
    if ws:
        try:
            await ws.close()
        except Exception:
            pass
