"""
OnSale Chat Session Service
Single Responsibility: Manage chat sessions in Redis with 24-hour expiration
"""

import json
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime
from app.cache import redis_client
from app.model.onsale_chat import ChatMessage, ChatRole

logger = logging.getLogger(__name__)


class OnSaleChatSessionService:
    """Single responsibility: Manage chat sessions in Redis"""

    def __init__(self):
        self.redis_client = redis_client
        self.session_ttl = 24 * 60 * 60  # 24 hours in seconds

    def _get_session_key(self, user_id: str, analysis_id: str) -> str:
        """Generate Redis key for chat session"""
        return f"onsale_chat_session:{user_id}:{analysis_id}"

    def _get_user_sessions_key(self, user_id: str) -> str:
        """Generate Redis key for user's active sessions"""
        return f"onsale_chat_user_sessions:{user_id}"

    def create_session(self, user_id: str, analysis_id: str) -> str:
        """Create a new chat session"""
        session_key = self._get_session_key(user_id, analysis_id)
        user_sessions_key = self._get_user_sessions_key(user_id)
        
        # Create session data
        session_data = {
            "user_id": user_id,
            "analysis_id": analysis_id,
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
            "messages": [],
            "is_active": True
        }
        
        try:
            # Store session data
            self.redis_client.setex(
                session_key,
                self.session_ttl,
                json.dumps(session_data)
            )
            
            # Add to user's active sessions
            self.redis_client.sadd(user_sessions_key, analysis_id)
            self.redis_client.expire(user_sessions_key, self.session_ttl)
            
            logger.info(f"Created chat session for user {user_id}, analysis {analysis_id}")
            return session_key
            
        except Exception as e:
            logger.error(f"Error creating chat session: {e}")
            raise

    def get_session(self, user_id: str, analysis_id: str) -> Optional[Dict[str, Any]]:
        """Get existing chat session"""
        session_key = self._get_session_key(user_id, analysis_id)
        
        try:
            session_data = self.redis_client.get(session_key)
            if session_data:
                session = json.loads(session_data)
                # Update last accessed time
                session["updated_at"] = datetime.utcnow().isoformat()
                self.redis_client.setex(
                    session_key,
                    self.session_ttl,
                    json.dumps(session)
                )
                return session
            return None
            
        except Exception as e:
            logger.error(f"Error getting chat session: {e}")
            return None

    def add_message(self, user_id: str, analysis_id: str, message: ChatMessage) -> bool:
        """Add a message to the chat session"""
        session_key = self._get_session_key(user_id, analysis_id)
        
        try:
            session_data = self.redis_client.get(session_key)
            if not session_data:
                return False
            
            session = json.loads(session_data)
            session["messages"].append({
                "role": message.role.value,
                "content": message.content,
                "timestamp": message.timestamp.isoformat()
            })
            session["updated_at"] = datetime.utcnow().isoformat()
            
            # Store updated session
            self.redis_client.setex(
                session_key,
                self.session_ttl,
                json.dumps(session)
            )
            
            logger.info(f"Added message to session for user {user_id}, analysis {analysis_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error adding message to session: {e}")
            return False

    def get_messages(self, user_id: str, analysis_id: str) -> List[ChatMessage]:
        """Get all messages from a chat session"""
        session = self.get_session(user_id, analysis_id)
        if not session:
            return []
        
        messages = []
        for msg_data in session.get("messages", []):
            try:
                message = ChatMessage(
                    role=ChatRole(msg_data["role"]),
                    content=msg_data["content"],
                    timestamp=datetime.fromisoformat(msg_data["timestamp"])
                )
                messages.append(message)
            except Exception as e:
                logger.error(f"Error parsing message: {e}")
                continue
        
        return messages

    def clear_session(self, user_id: str, analysis_id: str) -> bool:
        """Clear a chat session"""
        session_key = self._get_session_key(user_id, analysis_id)
        user_sessions_key = self._get_user_sessions_key(user_id)
        
        try:
            # Remove session data
            self.redis_client.delete(session_key)
            
            # Remove from user's active sessions
            self.redis_client.srem(user_sessions_key, analysis_id)
            
            logger.info(f"Cleared chat session for user {user_id}, analysis {analysis_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error clearing chat session: {e}")
            return False

    def get_user_active_sessions(self, user_id: str) -> List[str]:
        """Get list of analysis IDs that have active sessions for a user"""
        user_sessions_key = self._get_user_sessions_key(user_id)
        
        try:
            analysis_ids = self.redis_client.smembers(user_sessions_key)
            return [str(aid) for aid in analysis_ids]
        except Exception as e:
            logger.error(f"Error getting user active sessions: {e}")
            return []

    def cleanup_expired_sessions(self) -> int:
        """Clean up expired sessions (called periodically)"""
        try:
            # Redis automatically handles expiration, but we can clean up user session sets
            pattern = "onsale_chat_user_sessions:*"
            keys = self.redis_client.keys(pattern)
            
            cleaned_count = 0
            for key in keys:
                user_id = str(key).split(':')[-1]
                analysis_ids = self.get_user_active_sessions(user_id)
                
                # Remove analysis IDs that no longer have valid sessions
                for analysis_id in analysis_ids:
                    session_key = self._get_session_key(user_id, analysis_id)
                    if not self.redis_client.exists(session_key):
                        self.redis_client.srem(key, analysis_id)
                        cleaned_count += 1
            
            logger.info(f"Cleaned up {cleaned_count} expired session references")
            return cleaned_count
            
        except Exception as e:
            logger.error(f"Error cleaning up expired sessions: {e}")
            return 0
