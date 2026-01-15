import json
import logging
import random
from typing import Optional, Dict, Any
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from fastapi import HTTPException
from starlette import status

from app.database import get_pg_database

logger = logging.getLogger(__name__)

async def get_incoming_texts(
    limit: int = 1000,
    offset: int = 0,
    event_type: Optional[str] = None,
    sender: Optional[str] = None,
    recipient: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    tag_search: Optional[str] = None,
    tag_search_logic: str = "OR",
    message: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Retrieve incoming texts with pagination and filtering options.
    
    Args:
        limit: Maximum number of records to return (default: 1000)
        offset: Number of records to skip for pagination (default: 0)
        event_type: Filter by event type
        sender: Filter by sender
        recipient: Filter by recipient
        start_date: Filter by start date (ISO format)
        end_date: Filter by end date (ISO format)
        tag_search: Search string to match against tag keys or values
        tag_search_logic: Logic to use when filtering by multiple tags ('AND' or 'OR')
        message: Filter by message content (case-insensitive partial match)
        
    Returns:
        Dictionary containing the incoming texts and total count
    """
    logger.info("Fetching incoming texts with params: limit=%s, offset=%s, event_type=%s, sender=%s, recipient=%s, start_date=%s, end_date=%s, tag_search=%s, tag_search_logic=%s, message=%s",
                limit, offset, event_type, sender, recipient, start_date, end_date, tag_search, tag_search_logic, message)
    
    db = get_pg_database()
    
    # Build the base query
    base_query = """
    SELECT 
        id, 
        event_type, 
        sender, 
        recipient, 
        message, 
        email, 
        tags,
        created,
        raw_payload
    FROM 
        textchest_webhook_events
    WHERE 
        1=1
    """
    
    # Build the count query
    count_query = """
    SELECT COUNT(*) as total
    FROM textchest_webhook_events
    WHERE 1=1
    """
    
    # Parameters for the queries
    filter_params = {}
    
    # Add filters if provided
    if event_type:
        base_query += " AND event_type = :event_type"
        count_query += " AND event_type = :event_type"
        filter_params["event_type"] = event_type
        
    if sender:
        base_query += " AND sender LIKE :sender"
        count_query += " AND sender LIKE :sender"
        filter_params["sender"] = f"%{sender}%"
        
    if recipient:
        base_query += " AND recipient LIKE :recipient"
        count_query += " AND recipient LIKE :recipient"
        filter_params["recipient"] = f"%{recipient}%"

    if tag_search:
        # Split the tag search string into individual tags
        tags = [tag.strip() for tag in tag_search.split(',') if tag.strip()]
        
        # Build a condition for each tag
        tag_conditions = []
        for i, tag in enumerate(tags):
            param_name = f"tag_{i}"
            # Check if the tag exists in the JSON array with case-insensitive partial matching
            tag_condition = f"EXISTS (SELECT 1 FROM jsonb_array_elements_text(tags) WHERE LOWER(value) LIKE LOWER(:{param_name}))"
            tag_conditions.append(tag_condition)
            filter_params[param_name] = f"%{tag}%"
        
        # Join the conditions with AND or OR based on the specified logic
        logic_operator = " AND " if tag_search_logic.upper() == "AND" else " OR "
        tag_condition = f"({' AND '.join(tag_conditions)})" if tag_search_logic.upper() == "AND" else f"({' OR '.join(tag_conditions)})"
        
        base_query += f" AND {tag_condition}"
        count_query += f" AND {tag_condition}"
    
    # Handle date filters with datetime objects
    if start_date:
        try:
            start_datetime = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
            base_query += " AND created >= :start_date"
            count_query += " AND created >= :start_date"
            filter_params["start_date"] = start_datetime
        except ValueError as e:
            logger.error(f"Invalid start_date format: {start_date}, error: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid start_date format: {start_date}"
            )
        
    if end_date:
        try:
            end_datetime = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
            base_query += " AND created <= :end_date"
            count_query += " AND created <= :end_date"
            filter_params["end_date"] = end_datetime
        except ValueError as e:
            logger.error(f"Invalid end_date format: {end_date}, error: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid end_date format: {end_date}"
            )
    
    if message:
        base_query += " AND message ILIKE :message"
        count_query += " AND message ILIKE :message"
        filter_params["message"] = f"%{message}%"
    
    # Add ordering and pagination to base query only
    base_query += " ORDER BY created DESC LIMIT :limit OFFSET :offset"
    base_params = {**filter_params, "limit": limit, "offset": offset}
    
    try:
        # Random cleanup of old records (1 in 10 chance)
        if random.random() < 0.1:
            cleanup_threshold = datetime.now(timezone.utc) - timedelta(hours=48)
            cleanup_query = """
                DELETE FROM textchest_webhook_events 
                WHERE created < :cleanup_threshold
            """
            logger.info("Performing cleanup of records older than 48 hours")
            await db.execute(query=cleanup_query, values={"cleanup_threshold": cleanup_threshold})
        
        # Get the total count
        logger.debug("Executing count query: %s with params: %s", count_query, filter_params)
        count_result = await db.fetch_one(query=count_query, values=filter_params)
        total_count = count_result["total"] if count_result else 0
        
        # Get the paginated results
        logger.debug("Executing base query: %s with params: %s", base_query, base_params)
        rows = await db.fetch_all(query=base_query, values=base_params)
        
        # Format the results
        events = []
        for row in rows:
            try:
                event = {
                    "id": row["id"],
                    "event_type": row["event_type"],
                    "sender": row["sender"],
                    "recipient": row["recipient"],
                    "message": row["message"],
                    "email": row["email"],
                    "tags": json.loads(row["tags"] or "[]"),
                    "created": row["created"].isoformat(),
                    "raw_payload": json.loads(row["raw_payload"])
                }
                events.append(event)
            except Exception as e:
                logger.error("Error processing row %s: %s", row["id"], str(e), exc_info=True)
                continue
        
        logger.info("Successfully retrieved %d incoming texts out of %d total", len(events), total_count)
        return {
            "events": events,
            "total": total_count,
            "limit": limit,
            "offset": offset
        }
        
    except Exception as e:
        logger.error("Error fetching incoming texts: %s", str(e), exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        ) 


async def get_ticketmaster_otp_message(recipient: str) -> str | None:
    """
    Get message that contains 'ticketmaster' and a 4-8 digit code.

    Args:
        recipient: The recipient phone number to filter messages.

    Returns:
        str: The message string if found, otherwise None.
    """
    now_utc = datetime.now(ZoneInfo("UTC"))
    window_start = now_utc - timedelta(minutes=1)
    window_end = now_utc + timedelta(minutes=1)
    db = get_pg_database()

    query = """
        SELECT message
        FROM textchest_webhook_events
        WHERE recipient = :recipient
          AND created BETWEEN :window_start AND :window_end
          AND message ILIKE '%ticketmaster%'
          AND message ~ '\\m\\d{4,8}\\M'
        ORDER BY created DESC
        LIMIT 1
    """

    values = {
        "recipient": recipient,
        "window_start": window_start,
        "window_end": window_end,
    }

    result = await db.fetch_one(query=query, values=values)
    if result:
        return result["message"]
    return None