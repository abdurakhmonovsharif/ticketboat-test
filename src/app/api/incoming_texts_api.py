import logging
from typing import Optional

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel

from app.auth.auth_system import get_current_user_with_roles
from app.db import incoming_texts_db
from app.model.user import User

# Configure logging
logger = logging.getLogger("app.api.incoming_texts_api")

router = APIRouter()


class IncomingTextResponse(BaseModel):
    id: str
    event_type: str
    sender: str
    recipient: str
    message: Optional[str] = None
    email: Optional[str] = None
    tags: Optional[list] = None
    created: str
    raw_payload: dict


class IncomingTextsResponse(BaseModel):
    events: list[IncomingTextResponse]
    total: int
    limit: int
    offset: int


@router.get("/incoming-texts", response_model=IncomingTextsResponse)
async def get_incoming_texts(
    limit: int = Query(1000, description="Maximum number of records to return", ge=1, le=10000),
    offset: int = Query(0, description="Number of records to skip for pagination", ge=0),
    event_type: Optional[str] = Query(None, description="Filter by event type"),
    sender: Optional[str] = Query(None, description="Filter by sender"),
    recipient: Optional[str] = Query(None, description="Filter by recipient"),
    start_date: Optional[str] = Query(None, description="Filter by start date (ISO format)"),
    end_date: Optional[str] = Query(None, description="Filter by end date (ISO format)"),
    tag_search: Optional[str] = Query(None, description="Filter by tags. Multiple tags can be specified as comma-separated values."),
    tag_search_logic: str = Query(..., description="Logic to use when filtering by multiple tags. Must be either 'AND' or 'OR'."),
    message: Optional[str] = Query(None, description="Filter by message content (case-insensitive)"),
    user: User = Depends(get_current_user_with_roles(["user"])),
):
    """
    Retrieve incoming texts with pagination and filtering options.
    
    Returns the most recent incoming texts first, with options to filter by various fields
    and control the number of results returned.
    """
    logger = logging.getLogger("app.api.incoming_texts_api")
    logger.info("=== Incoming Texts endpoint called with params: limit=%s, offset=%s ===", limit, offset)
    
    try:
        logger.info(
            "[Incoming Texts] Request received - User: %s, Params: limit=%s, offset=%s, event_type=%s, sender=%s, recipient=%s, start_date=%s, end_date=%s, tag_search=%s, tag_search_logic=%s, message=%s",
            user.email, limit, offset, event_type, sender, recipient, start_date, end_date, tag_search, tag_search_logic, message
        )
        
        result = await incoming_texts_db.get_incoming_texts(
            limit=limit,
            offset=offset,
            event_type=event_type,
            sender=sender,
            recipient=recipient,
            start_date=start_date,
            end_date=end_date,
            tag_search=tag_search,
            tag_search_logic=tag_search_logic,
            message=message,
        )
        
        return result
    except Exception as e:
        logger.error(
            "[Incoming Texts] Error processing request: %s",
            str(e),
            exc_info=True,
            stack_info=True
        )
        raise 