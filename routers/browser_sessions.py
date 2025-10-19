# ---------- Browser Session Management Endpoints ----------
from typing import Any, Dict

from fastapi import Depends, HTTPException, Request
from pydantic import BaseModel


class NavigateRequest(BaseModel):
    url: str

class ScreenshotRequest(BaseModel):
    full_page: bool = False
    encoding: str = "base64"  # base64 or binary

def add_browser_endpoints(router, verify_token, page_store, pcs, ffmpeg_procs, logger, cleanup_session):
    """
    Adds browser session management endpoints to a router.
    """

    @router.get("/browser/sessions")
    async def list_browser_sessions(
        request: Request,
        token: Any = Depends(verify_token)  # noqa: B008
    ):
        """
        List active browser sessions for the authenticated user.
        """
        user_id = token.get("uid", "unknown") if isinstance(token, dict) else "unknown"
        
        # Filter sessions belonging to this user
        user_sessions = {}
        for session_id, session_data in page_store.items():
            if session_data.get("user_id") == user_id:
                # Include basic info, exclude heavyweight objects
                try:
                    title = await session_data["page"].title() if "page" in session_data else "No page"
                    url = session_data["page"].url if "page" in session_data else ""
                except:
                    title = "Unknown"
                    url = ""
                
                user_sessions[session_id] = {
                    "created_at": session_data.get("created_at", ""),
                    "title": title,
                    "url": url,
                    "status": "active"
                }
        
        return {"sessions": user_sessions, "count": len(user_sessions)}    
    
    @router.post("/browser/sessions/{session_id}/navigate")
    async def navigate_browser(
        session_id: str,
        data: NavigateRequest,
        token: Any = Depends(verify_token)  # noqa: B008
    ):
        """
        Navigate the browser to a specified URL.
        """
        user_id = token.get("uid", "unknown") if isinstance(token, dict) else "unknown"
        
        # Check if session exists
        if session_id not in page_store:
            raise HTTPException(status_code=404, detail="Session not found")
        
        # Check if user owns this session
        if page_store[session_id].get("user_id") != user_id:
            raise HTTPException(status_code=403, detail="Not authorized to access this session")
        
        # Navigate to URL
        url = data.url
        try:
            page = page_store[session_id].get("page")
            if page:
                await page.goto(url)
                title = await page.title()
                return {
                    "status": "success", 
                    "url": url,
                    "title": title
                }
            else:
                raise HTTPException(status_code=500, detail="Page not available")
        except Exception as e:
            logger.error(f"Error navigating to {url}: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Navigation error: {str(e)}") from e

    @router.get("/browser/sessions/{session_id}/screenshot")
    async def take_screenshot(
        session_id: str,
        full_page: bool = False,
        token: Any = Depends(verify_token)  # noqa: B008
    ):
        """
        Take a screenshot of the current browser view.
        """
        user_id = token.get("uid", "unknown") if isinstance(token, dict) else "unknown"
        
        # Check if session exists
        if session_id not in page_store:
            raise HTTPException(status_code=404, detail="Session not found")
        
        # Check if user owns this session
        if page_store[session_id].get("user_id") != user_id:
            raise HTTPException(status_code=403, detail="Not authorized to access this session")
        
        try:
            page = page_store[session_id].get("page")
            if page:
                # Take screenshot using Playwright
                screenshot = await page.screenshot(full_page=full_page, type="jpeg", quality=80)
                
                # Return base64 encoded screenshot
                import base64
                return {
                    "status": "success",
                    "content_type": "image/jpeg",
                    "data": base64.b64encode(screenshot).decode("utf-8"),
                    "encoding": "base64"
                }
            else:
                raise HTTPException(status_code=500, detail="Page not available")
        except Exception as e:
            logger.error(f"Error taking screenshot for session {session_id}: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Screenshot error: {str(e)}") from e


    @router.post("/browser/sessions/{session_id}/execute")
    async def execute_javascript(
        session_id: str,
        data: Dict,
        token: Any = Depends(verify_token)  # noqa: B008
    ):
        """
        Execute JavaScript in the browser context.
        """
        user_id = token.get("uid", "unknown") if isinstance(token, dict) else "unknown"
        
        # Check if session exists
        if session_id not in page_store:
            raise HTTPException(status_code=404, detail="Session not found")
        
        # Check if user owns this session
        if page_store[session_id].get("user_id") != user_id:
            raise HTTPException(status_code=403, detail="Not authorized to access this session")
        
        # Execute JavaScript
        script = data.get("script")
        if not script:
            raise HTTPException(status_code=400, detail="Script is required")
        
        try:
            page = page_store[session_id].get("page")
            if page:
                result = await page.evaluate(script)
                return {
                    "status": "success",
                    "result": result
                }
            else:
                raise HTTPException(status_code=500, detail="Page not available")
        except Exception as e:
            logger.error(f"Error executing JavaScript for session {session_id}: {str(e)}")
            raise HTTPException(status_code=500, detail=f"JavaScript execution error: {str(e)}") from e

    @router.delete("/browser/sessions/{session_id}")
    async def close_browser_session(
        session_id: str,
        token: Any = Depends(verify_token)  # noqa: B008
    ):
        """
        Close a browser session.
        """
        user_id = token.get("uid", "unknown") if isinstance(token, dict) else "unknown"
        
        # Check if session exists
        if session_id not in page_store:
            raise HTTPException(status_code=404, detail="Session not found")
        
        # Check if user owns this session
        if page_store[session_id].get("user_id") != user_id:
            raise HTTPException(status_code=403, detail="Not authorized to access this session")
        
        # Close the session
        try:
            await cleanup_session(session_id, pcs, page_store, ffmpeg_procs)
            return {"status": "success", "message": f"Session {session_id} closed"}
        except Exception as e:
            logger.error(f"Error closing session {session_id}: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Error closing session: {str(e)}") from e

    return router
