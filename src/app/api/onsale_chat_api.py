"""
OnSale Chat API Router
Handles WebSocket endpoints for onsale email analysis chat feature
"""

import json
import logging
import os
from typing import Dict, Any, Optional
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Depends, HTTPException
from starlette import status

from app.model.onsale_chat import ChatMessage, ChatRole, WebSocketMessage
from app.model.onsale_email_analysis import OnsaleEmailAnalysisItem
from app.service.onsale_chat_handler import OnSaleChatHandler
from app.service.onsale_chat_session_service import OnSaleChatSessionService
from app.db import onsale_email_analysis_db
from app.auth.auth_system import get_current_user_with_roles, _get_current_user
from app.model.user import User
from app.service.email_combined_user_retriever import EmailCombinedUserRetriever

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/onsale-chat", tags=["onsale-chat"])

# Initialize session service
session_service = OnSaleChatSessionService()


class ConnectionManager:
    """Manages WebSocket connections for chat sessions"""
    
    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}
    
    async def connect(self, websocket: WebSocket, session_id: str):
        await websocket.accept()
        self.active_connections[session_id] = websocket
        logger.info(f"WebSocket connected for session: {session_id}")
    
    def disconnect(self, session_id: str):
        if session_id in self.active_connections:
            del self.active_connections[session_id]
            logger.info(f"WebSocket disconnected for session: {session_id}")
    
    async def send_message(self, session_id: str, message: Dict[str, Any]):
        if session_id in self.active_connections:
            try:
                await self.active_connections[session_id].send_text(json.dumps(message))
            except Exception as e:
                logger.error(f"Error sending message to session {session_id}: {e}")
                self.disconnect(session_id)


manager = ConnectionManager()


async def authenticate_websocket_user(websocket: WebSocket) -> Optional[User]:
    """Authenticate WebSocket user using Firebase token"""
    try:
        # Get token from query parameters
        token = websocket.query_params.get("token")
        if not token:
            logger.warning("WebSocket authentication: Missing token")
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Missing authentication token")
            return None
        
        logger.info(f"WebSocket authentication: Received token (first 10 chars): {token[:10]}...")
        
        # Create a mock HTTPAuthorizationCredentials object
        from fastapi.security import HTTPAuthorizationCredentials
        credentials = HTTPAuthorizationCredentials(scheme="Bearer", credentials=token)
        
        # Use the existing authentication function
        user = await _get_current_user(credentials)
        
        logger.info(f"WebSocket authentication successful for user: {user.user_id}")
        return user
        
    except HTTPException as http_exc:
        # Catch HTTPException specifically and close WebSocket with appropriate code
        logger.error(f"WebSocket authentication HTTPException: {http_exc.detail}")
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason=f"Authentication failed: {http_exc.detail}")
        return None
    except Exception as e:
        logger.error(f"WebSocket authentication unexpected error: {e}")
        await websocket.close(code=status.WS_1011_INTERNAL_ERROR, reason=f"Authentication failed due to server error: {e}")
        return None


@router.websocket("/ws/{analysis_id}")
async def websocket_endpoint(websocket: WebSocket, analysis_id: str):
    """WebSocket endpoint for onsale email analysis chat"""
    
    # Authenticate user
    user = await authenticate_websocket_user(websocket)
    if not user:
        return  # Connection already closed by authenticate_websocket_user
    
    # Initialize chat handler
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        await websocket.close(code=status.WS_1011_INTERNAL_ERROR, reason="OpenAI API key not configured")
        return
    
    chat_handler = OnSaleChatHandler(api_key)
    
    # Get analysis data
    try:
        analysis_data = await onsale_email_analysis_db.get_onsale_email_analysis_by_id(analysis_id)
        if not analysis_data:
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Analysis not found")
            return
        
        analysis = OnsaleEmailAnalysisItem(**analysis_data)
    except Exception as e:
        logger.error(f"Error fetching analysis {analysis_id}: {e}")
        await websocket.close(code=status.WS_1011_INTERNAL_ERROR, reason="Error fetching analysis data")
        return
    
    # Use authenticated user ID
    user_id = user.user_id
    
    # Get or create chat session
    session = session_service.get_session(user_id, analysis_id)
    if not session:
        session_service.create_session(user_id, analysis_id)
    
    # Get existing chat history
    chat_history = session_service.get_messages(user_id, analysis_id)
    
    # Initialize WebSocket connection
    session_id = f"chat_{user_id}_{analysis_id}"
    
    # Connect to WebSocket
    await manager.connect(websocket, session_id)
    
    try:
        # Send welcome message only if no existing history
        if not chat_history:
            welcome_message = WebSocketMessage(
                type="message",
                data={
                    "role": "assistant",
                    "content": f"Hello! I'm your AI assistant for the **{analysis.event_name}** event analysis. I have access to all the analysis data including scores, market conditions, risk factors, and recommendations. How can I help you understand this opportunity better?",
                    "timestamp": "now"
                }
            )
            await manager.send_message(session_id, welcome_message.dict())
            
            # Save welcome message to session
            welcome_chat_message = ChatMessage(
                role=ChatRole.ASSISTANT,
                content=welcome_message.data["content"] if welcome_message.data else ""
            )
            session_service.add_message(user_id, analysis_id, welcome_chat_message)
        else:
            # Send existing messages to client
            for msg in chat_history:
                message_data = WebSocketMessage(
                    type="message",
                    data={
                        "role": msg.role.value,
                        "content": msg.content,
                        "timestamp": msg.timestamp.isoformat(),
                        "is_complete": True
                    }
                )
                await manager.send_message(session_id, message_data.dict())
        
        # Handle incoming messages
        while True:
            try:
                # Receive message from client
                data = await websocket.receive_text()
                message_data = json.loads(data)
                
                if message_data.get("type") == "message":
                    user_message = message_data.get("content", "")
                    
                    if not user_message.strip():
                        continue
                    
                    # Add user message to history and save to Redis
                    user_chat_message = ChatMessage(
                        role=ChatRole.USER,
                        content=user_message
                    )
                    session_service.add_message(user_id, analysis_id, user_chat_message)
                    chat_history.append(user_chat_message)
                    
                    # Send user message confirmation
                    user_msg_response = WebSocketMessage(
                        type="message",
                        data={
                            "role": "user",
                            "content": user_message,
                            "timestamp": "now"
                        }
                    )
                    await manager.send_message(session_id, user_msg_response.dict())
                    
                    # Send typing indicator
                    typing_msg = WebSocketMessage(
                        type="typing",
                        data={"is_typing": True}
                    )
                    await manager.send_message(session_id, typing_msg.dict())
                    
                    # Process with AI and stream response
                    full_response = ""
                    async for chunk in chat_handler.process_chat_message_stream(
                        user_message, analysis, chat_history
                    ):
                        if chunk["type"] == "content":
                            full_response += chunk["content"]
                            # Send streaming chunk
                            chunk_msg = WebSocketMessage(
                                type="stream",
                                data={
                                    "role": "assistant",
                                    "content": chunk["content"],
                                    "is_partial": True
                                }
                            )
                            await manager.send_message(session_id, chunk_msg.dict())
                        elif chunk["type"] == "error":
                            error_msg = WebSocketMessage(
                                type="error",
                                message=chunk["message"]
                            )
                            await manager.send_message(session_id, error_msg.dict())
                            break
                    
                    # Send completion message
                    if full_response:
                        # Add assistant message to history and save to Redis
                        assistant_chat_message = ChatMessage(
                            role=ChatRole.ASSISTANT,
                            content=full_response
                        )
                        session_service.add_message(user_id, analysis_id, assistant_chat_message)
                        chat_history.append(assistant_chat_message)
                        
                        # Send completion indicator
                        completion_msg = WebSocketMessage(
                            type="message",
                            data={
                                "role": "assistant",
                                "content": full_response,
                                "timestamp": "now",
                                "is_complete": True
                            }
                        )
                        await manager.send_message(session_id, completion_msg.dict())
                    
                    # Stop typing indicator
                    stop_typing_msg = WebSocketMessage(
                        type="typing",
                        data={"is_typing": False}
                    )
                    await manager.send_message(session_id, stop_typing_msg.dict())
                
                elif message_data.get("type") == "ping":
                    # Handle ping for connection health
                    pong_msg = WebSocketMessage(type="pong")
                    await manager.send_message(session_id, pong_msg.dict())
                
            except WebSocketDisconnect:
                logger.info(f"WebSocket disconnected for session {session_id}")
                break  # Exit the loop on disconnect
            except json.JSONDecodeError:
                logger.error(f"Invalid JSON received from session {session_id}")
                continue
            except Exception as e:
                logger.error(f"Error processing message in session {session_id}: {e}")
                # Only try to send error message if connection is still active
                try:
                    error_msg = WebSocketMessage(
                        type="error",
                        message="An error occurred while processing your message"
                    )
                    await manager.send_message(session_id, error_msg.dict())
                except:
                    # If we can't send the error message, the connection is likely broken
                    logger.info(f"Connection appears to be broken for session {session_id}, breaking loop")
                    break
    
    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected for session {session_id}")
    except Exception as e:
        logger.error(f"Unexpected error in WebSocket session {session_id}: {e}")
    finally:
        try:
            manager.disconnect(session_id)
        except Exception as e:
            logger.error(f"Error disconnecting session {session_id}: {e}")


@router.get("/analysis/{analysis_id}")
async def get_analysis_for_chat(
    analysis_id: str,
    user: User = Depends(get_current_user_with_roles(["user"]))
) -> Dict[str, Any]:
    """Get analysis data for chat initialization"""
    try:
        analysis_data = await onsale_email_analysis_db.get_onsale_email_analysis_by_id(analysis_id)
        if not analysis_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Analysis not found"
            )
        
        # Return minimal data needed for chat
        return {
            "id": analysis_data["id"],
            "event_name": analysis_data["event_name"],
            "performer": analysis_data["performer"],
            "venue_name": analysis_data["venue_name"],
            "event_date": analysis_data["event_date"],
            "opportunity_score": analysis_data["opportunity_score"],
            "overall_opportunity_score": analysis_data["overall_opportunity_score"]
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching analysis {analysis_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error fetching analysis data"
        )


@router.get("/sessions")
async def get_user_active_sessions(
    user: User = Depends(get_current_user_with_roles(["user"]))
) -> Dict[str, Any]:
    """Get user's active chat sessions"""
    try:
        user_id = user.user_id
        active_analysis_ids = session_service.get_user_active_sessions(user_id)
        
        # Get analysis details for each active session
        sessions_data = []
        for analysis_id in active_analysis_ids:
            try:
                analysis_data = await onsale_email_analysis_db.get_onsale_email_analysis_by_id(analysis_id)
                if analysis_data:
                    sessions_data.append({
                        "analysis_id": analysis_id,
                        "event_name": analysis_data["event_name"],
                        "performer": analysis_data["performer"],
                        "venue_name": analysis_data["venue_name"],
                        "event_date": analysis_data["event_date"],
                        "opportunity_score": analysis_data["opportunity_score"],
                        "overall_opportunity_score": analysis_data["overall_opportunity_score"]
                    })
            except Exception as e:
                logger.error(f"Error fetching analysis {analysis_id} for session list: {e}")
                continue
        
        return {
            "sessions": sessions_data,
            "total": len(sessions_data)
        }
    
    except Exception as e:
        logger.error(f"Error getting user active sessions: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error fetching active sessions"
        )


@router.delete("/sessions/{analysis_id}")
async def clear_chat_session(
    analysis_id: str,
    user: User = Depends(get_current_user_with_roles(["user"]))
) -> Dict[str, Any]:
    """Clear a specific chat session"""
    try:
        user_id = user.user_id
        success = session_service.clear_session(user_id, analysis_id)
        
        if success:
            return {"message": "Chat session cleared successfully"}
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Chat session not found"
            )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error clearing chat session: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error clearing chat session"
        )
