import traceback
from typing import Optional, Dict, Any
from datetime import datetime, timezone

from fastapi import HTTPException
from app.database import get_pg_buylist_database
from app.model.ticket_limit import SetTicketLimitRequest, TicketLimitSerializer


async def get_ticket_limit(
    event_code: Optional[str] = None,
    venue_code: Optional[str] = None,
    performer_id: Optional[str] = None
) -> Optional[TicketLimitSerializer]:
    try:
        if not event_code and not venue_code and not performer_id:
            raise HTTPException(
                status_code=400,
                detail="At least one identifier (event_code, venue_code, or performer_id) must be provided"
            )

        # Build query based on provided identifiers
        # For 'show' limits: check event_code
        # For 'run' limits: check venue_code AND performer_id
        conditions = []
        params = {}

        if event_code:
            conditions.append("event_code = :event_code")
            params["event_code"] = event_code

        if venue_code and performer_id:
            conditions.append("(venue_code = :venue_code AND performer_id = :performer_id)")
            params["venue_code"] = venue_code
            params["performer_id"] = performer_id
        elif venue_code:
            conditions.append("venue_code = :venue_code")
            params["venue_code"] = venue_code
        elif performer_id:
            conditions.append("performer_id = :performer_id")
            params["performer_id"] = performer_id

        if not conditions:
            return None

        query = f"""
            SELECT id, event_code, venue_code, performer_id, limit_type, limit_value,
                   created_by, updated_by, created_at, updated_at
            FROM shadows_ticket_limits
            WHERE {' OR '.join(conditions)}
            ORDER BY updated_at DESC
            LIMIT 1
        """

        result = await get_pg_buylist_database().fetch_one(query, params)

        if not result:
            return None

        return TicketLimitSerializer(**dict(result))

    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"An error occurred while getting ticket limit: {str(e)}"
        ) from e


async def set_ticket_limit(
    limit_data: SetTicketLimitRequest,
    user_email: str
) -> TicketLimitSerializer:

    try:
        if limit_data.limit_type == 'show' and not limit_data.event_code:
            raise HTTPException(
                status_code=400,
                detail="event_code is required for 'show' type limits"
            )

        if limit_data.limit_type == 'run' and not limit_data.venue_code:
            raise HTTPException(
                status_code=400,
                detail="venue_code is required for 'run' type limits"
            )

        # For 'run' type limits, event_code should be NULL (applies to all events at venue)
        # For 'show' type limits, event_code is specific to one event
        event_code_value = limit_data.event_code if limit_data.limit_type == 'show' else None

        current_time = datetime.now(timezone.utc).replace(tzinfo=None)

        query = """
            INSERT INTO shadows_ticket_limits (
                event_code, venue_code, performer_id,
                limit_type, limit_value,
                created_by, updated_by, created_at, updated_at
            )
            VALUES (
                :event_code, :venue_code, :performer_id,
                :limit_type, :limit_value,
                :created_by, :updated_by, :created_at, :updated_at
            )
            ON CONFLICT (venue_code, performer_id)
            DO UPDATE SET
                event_code = EXCLUDED.event_code,
                limit_type = EXCLUDED.limit_type,
                limit_value = EXCLUDED.limit_value,
                updated_by = EXCLUDED.updated_by,
                updated_at = EXCLUDED.updated_at
            RETURNING id, event_code, venue_code, performer_id,
                      limit_type, limit_value,
                      created_by, updated_by, created_at, updated_at;
        """

        params = {
            "event_code": event_code_value,
            "venue_code": limit_data.venue_code,
            "performer_id": limit_data.performer_id,
            "limit_type": limit_data.limit_type,
            "limit_value": limit_data.limit_value,
            "created_by": user_email,
            "updated_by": user_email,
            "created_at": current_time,
            "updated_at": current_time,
        }

        result = await get_pg_buylist_database().fetch_one(query, params)

        if not result:
            raise HTTPException(status_code=500, detail="Failed to set ticket limit")

        return TicketLimitSerializer(**dict(result))

    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"An error occurred while setting ticket limit: {str(e)}"
        ) from e

