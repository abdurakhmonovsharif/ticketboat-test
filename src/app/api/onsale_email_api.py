import traceback
from typing import Optional, List

from fastapi import APIRouter, Query, Depends, HTTPException
from starlette import status

from app.db import onsale_email_db
from app.auth.auth_system import get_current_user_with_roles
from app.model.onsale_email import MarkHandledResponse
from app.model.user import User

router = APIRouter(prefix="/onsale-email")


@router.get("/events")
async def get_onsale_email(
        timezone: str = Query(
            default="America/Chicago",
            description="Timezone for date filtering (e.g., 'America/Chicago')",
        ),
        search_term: Optional[str] = Query(
            default=None,
            description="Search term",
        ),
        venue: Optional[List[str]] = Query(
            default=None,
            description="Filter by venue",
        ),
        start_date: Optional[str] = Query(
            default=None,
            description="Start date for filtering",
        ),
        end_date: Optional[str] = Query(
            default=None,
            description="End date for filtering",
        ),
        page_size: int = Query(
            default=50,
            description="Number of results to return per page",

        ),
        page: int = Query(
            default=1,
            description="Page number to return",
        ),
        user: User = Depends(get_current_user_with_roles(["user"])),
) -> dict:
    return await onsale_email_db.get_onsale_email_details(timezone, page, page_size, search_term, venue, start_date,
                                                          end_date)


@router.get("/venues")
async def get_all_venues(
        user: User = Depends(get_current_user_with_roles(["user"])),
) -> dict:
    return await onsale_email_db.get_onsale_email_venues()


@router.post("/mark-handled", response_model=MarkHandledResponse, status_code=status.HTTP_200_OK)
async def mark_handled_emails(
        event_ids: List[str],
        user: User = Depends(get_current_user_with_roles(["user"]))
):
    if not event_ids or any(not isinstance(e, str) or not e.strip() for e in event_ids):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Missing event_id in request."
        )
    try:
        updated_ids, failed_ids = await onsale_email_db.mark_emails_as_handled(event_ids)
        return MarkHandledResponse(handled_events=updated_ids, failed_events=failed_ids)
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while updating emails as handled: {str(e)}"
        )
