# server_ffmpeg_cdp.py
import asyncio
import logging
import os
import platform  # Ensure platform is imported at the top
import shlex
import subprocess
from typing import Dict, Optional

import cv2
import numpy as np
from aiortc import MediaStreamError, MediaStreamTrack, RTCPeerConnection, VideoStreamTrack
from av import VideoFrame
from pydantic import BaseModel

from monitoring import websocket_manager
from utils import configure_ice_servers, get_wsl2_host_ip
from virtual_display_manager import VirtualDisplayManager  # Import VirtualDisplayManager

logger = logging.getLogger("crawlagent")

# --- CONFIG ---
WIDTH = int(os.getenv("VIDEO_WIDTH", "1280"))
HEIGHT = int(os.getenv("VIDEO_HEIGHT", "720"))
FPS = int(os.getenv("VIDEO_FPS", "30"))
FRAME_BYTES = WIDTH * HEIGHT * 3  # rgb24
FFMPEG_LOGLEVEL = "info"  # Optional: default fallback if env vars not set
ICE_IP = os.getenv("AIORTC_ICE_IP", get_wsl2_host_ip())
ICE_PORTS = os.getenv("AIORTC_ICE_PORT_RANGE", "40000-65535")

# Configure ICE servers and port range
# Use the configure_ice_servers function imported from webrtc_utils
RTC_CONFIG = configure_ice_servers(
    # Add additional STUN servers if needed
    stun_servers=["stun:stun.l.google.com:19302", "stun:stun.relay.metered.ca:80"],
    # Default TURN servers are already included in configure_ice_servers
)

print(f"Using ICE IP: {ICE_IP} and port range: {ICE_PORTS}")


# session storage


# ---------- models ----------
class OfferModel(BaseModel):
    sdp: str
    type: str
    session_id: str | None = None

# ---------- Video track: reads raw rgb24 frames from CDP ----------
class CDPRawVideoTrack(VideoStreamTrack):
    def __init__(self, page, session_id: str, width=WIDTH, height=HEIGHT, fps=FPS):
        super().__init__()
        self.page = page
        self.session_id = session_id
        self.width = width
        self.height = height
        self.fps = fps
        self.frame_bytes = width * height * 3
        self._started = False
        self._capture_task = None
        self.queue: asyncio.Queue[VideoFrame] = asyncio.Queue()

    async def start(self):
        self._capture_task = asyncio.create_task(self._capture_frames())

    async def stop(self):
        if self._capture_task:
            self._capture_task.cancel()
            try:
                await self._capture_task
            except asyncio.CancelledError:
                pass

    async def _capture_frames(self):
        try:
            while True:
                # Capture a screenshot using CDP
                screenshot = await self.page.screenshot(type='jpeg', quality=100)
                
                # Convert the screenshot to a VideoFrame
                img = cv2.imdecode(np.frombuffer(screenshot, np.uint8), cv2.IMREAD_COLOR)
                if img is None:
                    logger.error(f"CDPRawVideoTrack: Failed to decode screenshot for session {self.session_id}")
                    continue

                img = cv2.cvtColor(img, cv2.COLOR_BGR2RGB).astype(np.uint8)
                try:
                    frame = VideoFrame.from_ndarray(img, format="rgb24")
                    pts, time_base = await self.next_timestamp()
                    frame.pts = pts
                    frame.time_base = time_base
                    await self.queue.put(frame)
                except Exception as e:
                    logger.error(f"CDPRawVideoTrack: Failed to create VideoFrame: {e} for session {self.session_id}")
                    continue
                
                logger.debug(f"CDPRawVideoTrack: Captured and queued frame for session {self.session_id}")
                await asyncio.sleep(1 / self.fps)  # Limit frame rate
        except asyncio.CancelledError:
            logger.info(f"CDPRawVideoTrack: Frame capture cancelled for session {self.session_id}")
        except Exception as e:
            logger.error(f"CDPRawVideoTrack error: {e} for session {self.session_id}")
            raise MediaStreamError from e

    async def recv(self):
        frame = await self.queue.get()
        return frame

# ---------- Video track: reads raw rgb24 frames from ffmpeg stdout ----------
class FFmpegRawVideoTrack(VideoStreamTrack):

    def __init__(self, stdout, session_id, width=WIDTH, height=HEIGHT, fps=FPS):
        super().__init__()
        self.stdout = stdout
        self.session_id = session_id
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


class VideoTransformTrack(MediaStreamTrack):
    """
    A video stream track that transforms frames from an another track.
    """

    kind = "video"

    def __init__(self, track, transform):
        super().__init__()  # don't forget this!
        self.track = track
        self.transform = transform

    async def recv(self):
        frame = await self.track.recv()

        if self.transform == "cartoon":
            img = frame.to_ndarray(format="bgr24")

            # prepare color
            img_color = cv2.pyrDown(cv2.pyrDown(img))
            for _ in range(6):
                img_color = cv2.bilateralFilter(img_color, 9, 9, 7)
            img_color = cv2.pyrUp(cv2.pyrUp(img_color))

            # prepare edges
            img_edges = cv2.cvtColor(img, cv2.COLOR_RGB2GRAY)
            img_edges = cv2.adaptiveThreshold(
                cv2.medianBlur(img_edges, 7),
                255,
                cv2.ADAPTIVE_THRESH_MEAN_C,
                cv2.THRESH_BINARY,
                9,
                2,
            )
            img_edges = cv2.cvtColor(img_edges, cv2.COLOR_GRAY2RGB)

            # combine color and edges
            img = cv2.bitwise_and(img_color, img_edges)
            img = img.astype(np.uint8) # Ensure correct data type

            # rebuild a VideoFrame, preserving timing information
            new_frame = VideoFrame.from_ndarray(img, format="bgr24")
            new_frame.pts = frame.pts
            new_frame.time_base = frame.time_base
            return new_frame
        elif self.transform == "edges":
            # perform edge detection
            img = frame.to_ndarray(format="bgr24")
            img = cv2.cvtColor(cv2.Canny(img, 100, 200), cv2.COLOR_GRAY2BGR)
            img = img.astype(np.uint8) # Ensure correct data type

            # rebuild a VideoFrame, preserving timing information
            new_frame = VideoFrame.from_ndarray(img, format="bgr24")
            new_frame.pts = frame.pts
            new_frame.time_base = frame.time_base
            return new_frame
        elif self.transform == "rotate":
            # rotate image
            img = frame.to_ndarray(format="bgr24")
            rows, cols, _ = img.shape
            M = cv2.getRotationMatrix2D((cols / 2, rows / 2), frame.time * 45, 1)
            img = cv2.warpAffine(img, M, (cols, rows))
            img = img.astype(np.uint8) # Ensure correct data type

            # rebuild a VideoFrame, preserving timing information
            new_frame = VideoFrame.from_ndarray(img, format="bgr24")
            new_frame.pts = frame.pts
            new_frame.time_base = frame.time_base
            return new_frame
        else:
            return frame


async def get_playwright_window_identifier(page, browser_name=None):
    await page.evaluate("window.document.title = 'PlaywrightCapture'")
    title = await page.title()
    if platform.system() == "Windows":
        # Determine browser window suffix
        if browser_name is None:
            user_agent = await page.evaluate("navigator.userAgent")
            if "Chrome" in user_agent:
                browser_name = "chromium"
            elif "Firefox" in user_agent:
                browser_name = "firefox"
            elif "WebKit" in user_agent or "Safari" in user_agent:
                browser_name = "webkit"
            else:
                browser_name = "chrome"  # Default fallback
        if browser_name == "chrome":
            suffix = " - Google Chrome"
        elif browser_name == "firefox":
            suffix = " - Mozilla Firefox"
        elif browser_name == "webkit":
            suffix = " [WebKit]"
        elif browser_name == "chromium":
            suffix = " - Chromium"
        else:
            suffix = ""
        return f'{title}{suffix}'
    elif platform.system() == "Linux":
        # Use wmctrl to get window ID (Linux only)
        try:
            output = subprocess.check_output(['wmctrl', '-l']).decode()
            for line in output.splitlines():
                if title in line:
                    return line.split()[0]  # window ID
        except Exception as e:
            logger.error(f"wmctrl error: {e}")
        return None
    else:
        logger.error(f"Unsupported platform for window identification: {platform.system()}")
        return None


async def start_ffmpeg(is_windows: bool, display: str | None = ":99", width: int = 1280, 
                       height: int = 720, fps: int = 30, window_title: str | None = None):
    """
    Launch FFmpeg to capture a specific browser window.
    On Windows, uses gdigrab with window title.
    On Linux, uses x11grab with window ID.
    """
    window_id = "window_title"
    logger.debug(f"Starting FFmpeg capture:  window_title = {window_title}")
    if is_windows and window_title:
        cmd = (
            f"ffmpeg -hide_banner -loglevel error "
            f"-probesize 32M -analyzeduration 10M -thread_queue_size 1024 -fflags nobuffer -flags low_delay -vsync 0 "
            f"-f gdigrab -framerate {fps} -video_size {width}x{height} -i title=\"{window_title}\" "
            f"-vf \"scale=trunc(iw/2)*2:trunc(ih/2)*2\" "
            f"-pix_fmt rgb24 -f rawvideo pipe:1"
        )

        # f"ffmpeg -hide_banner -loglevel error "
        #     f"-y "  # Overwrite output files without asking
        #     f"-probesize 32M -analyzeduration 10M -thread_queue_size 1024 -fflags nobuffer -flags low_delay -vsync 0 "
        #     f"-f gdigrab -framerate {fps} -video_size {width}x{height} -i title=\"{window_title}\" "
        #     f"-vf \"scale=trunc(iw/2)*2:trunc(ih/2)*2\" "
        #     f"-pix_fmt yuv420p -c:v libx264 -preset ultrafast"
        #     f"-pix_fmt rgb24 -f rawvideo pipe:1"f"-vf \"scale=trunc(iw/2)*2:trunc(ih/2)*2\" "
        #     f"-pix_fmt yuv420p -c:v libx264 -preset ultrafast"
    elif not is_windows and window_id:
        cmd = (
            f"ffmpeg -hide_banner -loglevel error "
            f"-probesize 32M -analyzeduration 10M -thread_queue_size 1024 -fflags nobuffer -flags low_delay -vsync 0 "
            f"-f x11grab -framerate {fps} -video_size {width}x{height} -i {display}+{window_id} "
            f"-pix_fmt rgb24 -f rawvideo pipe:1"
        )
    else:
        # Fallback to desktop or display capture
        if is_windows:
            cmd = (
                f"ffmpeg -hide_banner -loglevel error "
                f"-probesize 32M -analyzeduration 10M -thread_queue_size 1024 -fflags nobuffer -flags low_delay -vsync 0 "
                f"-f gdigrab -framerate {fps} -video_size {width}x{height} -i desktop "
                f"-pix_fmt rgb24 -f rawvideo pipe:1"
            )
        else:
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
        bufsize=10**8,
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
                          virtual_display_manager: VirtualDisplayManager,
                          ffmpeg_procs: Optional[Dict[str, subprocess.Popen]],
                          ):  # Added virtual_display_manager
    logger.warning(f"Initiating cleanup for session {session_id}...")  # Changed to warning for emphasis
    pc = pcs.pop(session_id, None)
    if pc:
        try:
            await pc.close()
        except Exception:
            pass
    page_info = page_store.pop(session_id, None)
    if page_info:
        try:
            # Stop CDPRawVideoTrack if it exists
            if "video_track" in page_info and page_info["video_track"]:
                await page_info["video_track"].stop()
            # Close Playwright browser and context
            if "browser" in page_info and page_info["browser"]:
                await page_info["browser"].close()
            if "playwright" in page_info and page_info["playwright"]:
                await page_info["playwright"].stop()
        except Exception as e:
            logger.error(f"Error closing Playwright browser/context for session {session_id}: {e}")
    if ffmpeg_procs:
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
    ws = websocket_manager.get_websocket_by_session_id(session_id)
    if ws:
        try:
            await ws.close()
        except Exception:
            pass
