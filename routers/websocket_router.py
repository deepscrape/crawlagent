import asyncio
import json
import logging
import os
import platform  # Import platform
import subprocess
from datetime import datetime
from typing import Annotated, Any, Dict

from aiortc import RTCPeerConnection, RTCSessionDescription

# from aiortc.contrib.media import MediaPlayer
from celery import uuid
from celery.result import AsyncResult
from crawl4ai import AsyncLogger, BrowserConfig, CrawlerRunConfig
from crawl4ai.browser_manager import BrowserManager
from fastapi import (
    APIRouter,
    Cookie,
    Depends,
    HTTPException,
    Query,
    Request,
    WebSocket,
    WebSocketDisconnect,
    WebSocketException,
    status,
)

from apps.ffmpeg_cdp import (
    FPS,
    HEIGHT,
    RTC_CONFIG,
    WIDTH,
    CDPRawVideoTrack,
    FFmpegRawVideoTrack,
    OfferModel,
    cleanup_session,
    get_playwright_window_identifier,
    start_ffmpeg,
)
from auth import get_token_dependency
from celery_app import celery_app
from configure import config, get_websocket_custom_limiter
from enums import CeleryTaskStatus
from firestore import auth
from monitoring import websocket_manager

# from routers.browser_sessions import add_browser_endpoints # No longer needed
from utils import convert_celery_status
from virtual_display_manager import VirtualDisplayManager  # Import VirtualDisplayManager

logger = logging.getLogger("crawlagent")
# logger = AsyncLogger(verbose=True, log_file=None)

# session storage
pcs: Dict[str, RTCPeerConnection] = {}
page_store: Dict[str, Dict] = {}
ffmpeg_procs: Dict[str, subprocess.Popen] = {}

# _socket_client: set[WebSocket]

socket_router = APIRouter()

# def init_websocket_router(socket_client: set[WebSocket]) -> APIRouter:
#     """Inject shared singletons and return the router for mounting."""
#     global _socket_client
#     _socket_client = socket_client
#     return websocket_router

# Combine verification code for token/cookie
verify_token = get_token_dependency(config)

async def get_cookie_or_token(
    # websocket: WebSocket,
    session: Annotated[str | None, Cookie()] = None,
    token: Annotated[str | None, Query()] = None,
):
    """Dependency to get either session cookie or token from query params."""
    if config["security"].get("jwt_enabled", True) is False:
        return "jwt_disabled"

    value = session or token
    if value is None:
        raise WebSocketException(code=status.WS_1008_POLICY_VIOLATION)

    decoded = auth.verify_id_token(token)
        
    # else:
    #     decoded = auth.verify_session_cookie(session, check_revoked=True)

    if not decoded:
        raise WebSocketException(code=status.WS_1008_POLICY_VIOLATION)
    
    return decoded.get("uid", "unknown")

@socket_router.websocket("/events")
async def websocket_endpoint(
    websocket: WebSocket,
    cookie_or_token: Annotated[str, Depends(get_cookie_or_token)],
    custom_limit_check: Annotated[bool, Depends(get_websocket_custom_limiter)]
):
    try:
        # await websocket.accept()
        # _socket_client.add(websocket)
        client_id = cookie_or_token
        connection_id = await websocket_manager.connect(websocket, client_id)
        logger.info(f"Client {client_id} connected with connection_id {connection_id}")
        await websocket_manager.send_personal_message("Hello, this is a server event!", connection_id)
        await websocket_manager.broadcast(f"Client #{client_id} says: Hello, this is a server event!")
        
    except WebSocketDisconnect as e:
        # _socket_client.remove(websocket)
        await websocket_manager.broadcast(f"Client #{client_id} left the chat")
        websocket_manager.disconnect(connection_id)
        logger.info("WebSocket: Client disconnected", str(e))
    except Exception as e:
        logger.error(f"Error handling connection for {client_id}: {str(e)}", exc_info=True)
        websocket_manager.disconnect(connection_id)
    finally:
        # _socket_client.discard(websocket)
        await websocket_manager.broadcast(f"Client #{client_id} left the chat")
        websocket_manager.disconnect(connection_id)
        logger.info("WebSocket: Client disconnected (cleanup)")

@socket_router.websocket("/task/status")
async def websocket_task_status(
    websocket: WebSocket,
    cookie_or_token: Annotated[str, Depends(get_cookie_or_token)],
    custom_limit_check: Annotated[bool, Depends(get_websocket_custom_limiter)]
):
    # await websocket.accept()
    # _socket_client.add(websocket)
    client_id = cookie_or_token
    connection_id = await websocket_manager.connect(websocket, client_id)
    logger.info(f"Client {client_id} connected with connection_id {connection_id}")
    poll_interval = .7
    active_tasks = set()
    tasks = {}

    try:
        while True:
            # Wait for new task_ids or a ping from the client
            try:
                data = await asyncio.wait_for(websocket.receive_json(), timeout=poll_interval)
                websocket_manager.record_message_received(connection_id)
                task_ids = data.get("task_ids", [])
                logger.debug(f"WebSocket: Client subscribed to task IDs: {task_ids[:50]}")
                if not task_ids:
                    await websocket_manager.send_personal_message({"error": "No task IDs provided"}, connection_id)       
                    continue
                # Update the set of tracked tasks
                active_tasks = set(task_ids)
                tasks = {task_id: AsyncResult(task_id, app=celery_app) for task_id in task_ids}
            except asyncio.TimeoutError:
                # No new message, just continue to poll statuses
                pass

            if not active_tasks:
                continue

            status_data = []
            completed_tasks = set()

            for task_id in list(active_tasks):
                task = tasks[task_id]
                task_status = {
                    "task_id": task_id,
                    "status": convert_celery_status(task.status),
                    "timestamp": asyncio.get_event_loop().time(),
                }
                if task.info:
                    task_status["info"] = task.info
                if task.status in [CeleryTaskStatus.SUCCESS, CeleryTaskStatus.FAILURE, CeleryTaskStatus.REVOKED]:
                    task_status["status"] = convert_celery_status(task.status)
                    task_status["result"] = task.result if task.status == CeleryTaskStatus.SUCCESS else None    
                    task_status["error"] = str(task.result) if task.status == CeleryTaskStatus.FAILURE else None
                    task_status["final"] = True
                    completed_tasks.add(task_id)
                status_data.append(task_status)

            active_tasks -= completed_tasks

            await websocket_manager.send_personal_message({"tasks": status_data}, connection_id)

            # --- CLOSE CONNECTION IF ALL TASKS ARE DONE ---
            if not active_tasks:
                logger.debug(f"All tracked tasks completed for client {client_id}. Closing WebSocket.")
                break

    except WebSocketDisconnect:
        # _socket_client.remove(websocket)
        logger.info("WebSocket: Client disconnected from task status tracking")
    except Exception as e:
        logger.error(f"Error in websocket task status: {str(e)}")
        await websocket_manager.send_personal_message({"error": str(e)}, connection_id)
        # await websocket_manager.broadcast(f"Client #{client_id} says: {{"error": str(e)}}")
    finally:
        # _socket_client.discard(websocket)
        websocket_manager.disconnect(connection_id)
        logger.info("WebSocket: Client disconnected from task status tracking (cleanup)")

# ---------- FFmpeg + CDP app ----------

# ---------- /offer endpoint ----------
@socket_router.post("/offer")
async def offer(
        offer: OfferModel, request: Request,
        token: Any = Depends(verify_token)):
    """
    Client sends an SDP offer; server returns SDP answer and session_id.
    Authenticate with Firestore Authorization: Bearer <jwt>.
    
    This endpoint creates a new browser session with WebRTC streaming:
    1. Creates a peer connection for WebRTC
    2. Launches a headful Playwright browser
    3. Starts video streaming using CDPRawVideoTrack
    4. Creates and returns an SDP answer
    """
    user_id = token.get("uid", "unknown") if isinstance(token, dict) else "unknown"
    
    session_id = offer.session_id or str(uuid())
    logger.info(f"Creating browser session {session_id} for user {user_id}")
    offer_desc = RTCSessionDescription(sdp=offer.sdp, type=offer.type)
    pc = RTCPeerConnection(configuration=RTC_CONFIG)

    pcs[session_id] = pc
    browser_manager = None

    # Access VirtualDisplayManager from app.state
    virtual_display_manager: VirtualDisplayManager = request.app.state.virtual_display_manager

    @pc.on("connectionstatechange")
    async def on_conn_state_change():
        logger.info(f"WebRTC connection {session_id} state changed to {pc.connectionState}, iceConnectionState: \
                    {pc.iceConnectionState}, signalingState: {pc.signalingState}")
        if pc.connectionState in ("failed", "closed", "disconnected"):
            logger.warning(f"WebRTC connection {session_id} state is {pc.connectionState}. Initiating cleanup.") 
            await cleanup_session(session_id, pcs, page_store, virtual_display_manager, ffmpeg_procs)

    @pc.on("iceconnectionstatechange")
    def on_ice_state_change():
        logger.info("ICE state: %s", pc.iceConnectionState)
    try:
        is_windows = platform.system() == "Windows"
        
        display_num = None
        debug_port = None
        xvfb_process = None

        if not is_windows:
            display_num, debug_port, xvfb_process = await virtual_display_manager.get_display_and_port()
            display_addr = f":{display_num}"
            if not os.environ.get("DISPLAY") == display_addr:
                logger.warning(f"DISPLAY not set to {display_addr}, setting it now")
                os.environ["DISPLAY"] = display_addr
            logger.info(f"Allocated virtual display {display_addr} and debug port {debug_port} for session {session_id}")
        else:
            debug_port = virtual_display_manager.start_debug_port + len(virtual_display_manager.in_use_displays) 
            logger.info(f"Allocated debug port {debug_port} for session {session_id} on Windows.")

        browser_args = [
            f"--window-size={WIDTH},{HEIGHT}",
        ]
        
        config_extra_args = config["crawler"]["browser"].get("extra_args", [])
        for arg in config_extra_args:
            if arg not in browser_args:
                browser_args.append(arg)
        
        logger.debug(f"Launching Playwright with args: {browser_args}")

        import tempfile
        user_data_dir = tempfile.mkdtemp(prefix="crawl4ai-test-")
        logger.debug(f"Created temporary user data directory: {user_data_dir}")

        # cdp_url = f"ws://localhost:{9222}/devtools/browser/" # Dynamically construct CDP URL
        browser_type = "chromium"  # or "chromium", "firefox"
        browser_config = BrowserConfig(
            headless=True,
            # browser_mode="docker",
            viewport_height=HEIGHT,
            viewport_width=WIDTH,
            browser_type=browser_type,
            browser_mode="cdp",
            # user_data_dir=user_data_dir,
            # extra_args=browser_args,
            # cdp_url=cdp_url # Pass dynamically constructed CDP URL
        )
        
        browser_manager = BrowserManager(browser_config=browser_config, logger=AsyncLogger(verbose=True, log_file=None))
        await browser_manager.start()

        logger.info(f"Requesting page from crawler {session_id}")
        crawler_config = CrawlerRunConfig(session_id=session_id)
        try:
            # pc.addTransceiver("video", direction="sendonly")
            page, _ = await browser_manager.get_page(crawler_config)
       
            if not page:
                raise ValueError("Page initialization returned None")
            start_url = "https://example.com"
            await page.goto(start_url)
            
            title = await page.title()
            logger.info(f"Page title: {title}")
        except Exception as e:
            logger.error(f"Exception in get_page: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Exception in get_page: {e}") from e

        video_track = CDPRawVideoTrack(page, session_id, width=WIDTH, height=HEIGHT, fps=FPS)
        await video_track.start()
        pc.addTrack(video_track)

        page_store[session_id] = {
            "page": page,
            "context": page.context,
            "browser": browser_manager.browser, 
            "playwright": await browser_manager.get_playwright(),
            "user_id": user_id,
            "created_at": datetime.now().isoformat(),
            "display_num": display_num,
            "debug_port": debug_port,
            "xvfb_process": xvfb_process,
            "video_track": video_track
        }
        window_title = await get_playwright_window_identifier(page, browser_name=browser_type)
        # Use allocated display_addr
        ff = await start_ffmpeg(is_windows=is_windows, display=None 
                                if is_windows else display_addr, width=WIDTH, height=HEIGHT, fps=FPS,
                                  window_title=window_title) 
        if not ff.stdout:
            # Pass virtual_display_manager to cleanup_session
            await cleanup_session(session_id, pcs, page_store, virtual_display_manager,ffmpeg_procs)
            raise HTTPException(status_code=500, detail="ffmpeg stdout not available")
        
        ffmpeg_procs[session_id] = ff
        logger.info(f"Started ffmpeg with PID {ff.pid} for session {session_id}")


        # if ff and ff.stdout:
        #     track = FFmpegRawVideoTrack(ff.stdout, session_id=session_id, width=WIDTH, height=HEIGHT, fps=FPS)
        #     pc.addTrack(track)
        #     logger.info(f"Added FFmpegRawVideoTrack to PeerConnection for session {session_id}")
        # else:
        #     logger.error("FFmpeg process or stdout not available.")

        await pc.setRemoteDescription(offer_desc)
        logger.debug(f"Creating SDP answer for session {session_id}")
        answer = await pc.createAnswer()
        logger.debug(f"SDP answer created for session {session_id}: {answer.sdp}")
        await pc.setLocalDescription(answer)
        logger.info(f"Set local description for session {session_id}")

        logger.info(f"Browser session {session_id} ready for user {user_id}")
        return {
            "sdp": pc.localDescription.sdp, 
            "type": pc.localDescription.type, 
            "session_id": session_id,
            "status": "ready"
        }
    except HTTPException as e:
        # If HTTPException is raised, it means VirtualDisplayManager couldn't allocate a display
        # or another specific HTTP error occurred. Re-raise it.
        raise e
    except Exception as e:
        logger.error(f"Error creating browser session: {str(e)}", exc_info=True)
        # Attempt to release display if it was allocated before error
        if display_num is not None:
            await virtual_display_manager.release_display_and_port(display_num)
        raise HTTPException(status_code=500, detail=f"Failed to create browser session: {e}") from e

@socket_router.websocket("/{session_id}")
async def ws_input(
    websocket: WebSocket, session_id: str,
    cookie_or_token: Annotated[str, Depends(get_cookie_or_token)],
    custom_limit_check: Annotated[bool, Depends(get_websocket_custom_limiter)]
    ):
    """
    WebSocket endpoint for input and optional CDP commands.
    Client must include a valid JWT as a query param: ?token=<jwt>
    
    Supports browser interactions:
    - mouse_move: Move the mouse pointer
    - mouse_click: Click at specific coordinates
    - key: Type keyboard input
    - js_eval: Evaluate JavaScript in the browser context
    - cdp: Direct Chrome DevTools Protocol commands
    """
    
    client_id = cookie_or_token
    # Connect and store session_id in the connection metadata
    connection_id = await websocket_manager.connect(websocket, client_id, session_id=session_id)
    logger.info(f"Client {client_id} connected with connection_id {connection_id} for session {session_id}")    

    try:
        while True:
            raw = await websocket.receive_text()
            # raw = await asyncio.wait_for(websocket.receive_json(), timeout=poll_interval)
            websocket_manager.record_message_received(connection_id)
            msg = json.loads(raw)
            typ = msg.get("type")
            page_info = page_store.get(session_id)
            if not page_info:
                await websocket_manager.send_personal_message({"error": "session not found"}, connection_id)
                continue
            page = page_info["page"]

            if typ == "mouse_move":
                x = msg.get("x")
                y = msg.get("y")
                await page.mouse.move(float(x), float(y))
            elif typ == "mouse_click":
                x = msg.get("x")
                y = msg.get("y")
                button = msg.get("button", "left")
                await page.mouse.click(float(x), float(y), button=button)
            elif typ == "key":
                key = msg.get("key")
                await page.keyboard.type(str(key))
            elif typ == "js_eval":
                script = msg.get("script")
                # careful: sanitize or limit allowed scripts in prod
                res = await page.evaluate(script)
                await websocket_manager.send_personal_message({"type": "js_result", "result": res}, connection_id)        
            elif typ == "cdp":
                # direct CDP call via Playwright CDP session
                # msg: {type: 'cdp', method: 'Network.enable', params:{...}}
                method = msg.get("method")
                params = msg.get("params", {})
                cdp = await page.context.new_cdp_session(page)
                res = await cdp.send(method, params)
                await websocket_manager.send_personal_message({"type": "cdp_result", "result": res}, connection_id)
            else:
                await websocket_manager.send_personal_message({"error": "unknown message type"}, connection_id)
    except WebSocketDisconnect:
        logger.info(f"WebSocket: Client {client_id} disconnected from session {session_id}")
    except Exception as e:
        logger.error(f"Error in websocket input: {str(e)}")
        await websocket_manager.send_personal_message({"error": str(e)}, connection_id)
    finally:
        # No need to remove from ws_inputs as we're using the websocket_manager
        websocket_manager.disconnect(connection_id)
        logger.info(f"WebSocket: Client {client_id} disconnected from session {session_id} (cleanup)")

# ---------- Browser Session Management Endpoints ----------
# Add browser session management endpoints
# socket_router = add_browser_endpoints( # Removed as per new architecture
#     socket_router, 
#     verify_token, 
#     page_store, 
#     pcs, 
#     ffmpeg_procs, 
#     logger, 
#     cleanup_session
# )
