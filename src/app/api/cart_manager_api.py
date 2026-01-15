from typing import Optional

from fastapi import APIRouter, Depends, Query, Path
from fastapi.responses import StreamingResponse

from app.auth.auth_system import get_current_user_with_roles
from app.db import cart_manager_pg_db
from app.model.cart_manager import CartManagerUpdateNew, BulkCartManagerUpdate
from app.model.user import User

router = APIRouter(prefix="/cart-manager")


@router.get("/list")
async def get_cart_managers(
        status: Optional[str] = Query(None, regex="^(?i)(approve|decline|pending)$"),
        event_codes: Optional[str] = Query(None),
        time_status: Optional[str] = Query(None),
        captain_emails: Optional[str] = Query(None),
        start_date: Optional[str] = Query(None, description="Start date in YYYY-MM-DD format"),
        end_date: Optional[str] = Query(None, description="End date in YYYY-MM-DD format"),
        timezone: str = Query(
            default="America/Chicago",
            description="Timezone for date filtering (e.g., 'America/Chicago')",
        ),
        page_size: int = Query(
            default=50,
            description="Number of results to return per page",
        ),
        page: int = Query(
            default=1,
            description="Page number to return",
        ),
        view: Optional[str] = Query('cart'),
        company: str = Query(
            default="",
            description="Filter by company name"
        ),
        on_sale_event_code: str = Query(
            default="",
            description="Filter by onsale code"
        ),
        on_sale_link: str = Query(
            default="",
            description="Filter by onsale code"
        ),
        on_sale_venue: str = Query(
            default="",
            description="venue"
        ),
        on_sale_city: str = Query(
            default="",
            description="city"
        ),
        event_date_time_timestamp: str = Query(
            default="",
            description="show event date"
        ),
        search_term: str = Query(
            default="",
            description="search"
        ),
        user: User = Depends(get_current_user_with_roles(["admin", "captain", "dev", "user", "buyer"]))
):
    event_codes_list = event_codes.split(",") if event_codes else []
    time_status_list = time_status.split(",") if time_status else []
    captain_list = captain_emails.split(",") if captain_emails else []

    return (
        await cart_manager_pg_db.get_all_carts(
            status=status,
            event_codes=event_codes_list,
            time_status=time_status_list,
            captain_list=captain_list,
            page=page,
            page_size=page_size,
            start_date=start_date,
            end_date=end_date,
            timezone=timezone,
            view=view,
            company=company,
            on_sale_event_code=on_sale_event_code,
            on_sale_link=on_sale_link,
            on_sale_venue=on_sale_venue,
            on_sale_city=on_sale_city,
            event_date_time_timestamp=event_date_time_timestamp,
            search_term=search_term
        )
    )


@router.get('/carts')
async def get_carts_by_id(
        view: str = Query(..., regex="^(show|tour)$"),
        id: str = Query(..., description="ID for the specific show or tour"),
        status: Optional[str] = Query(None, regex="^(?i)(approve|decline|pending)$"),
        event_codes: Optional[str] = Query(None),
        time_status: Optional[str] = Query(None),
        captain_emails: Optional[str] = Query(None),
        start_date: Optional[str] = Query(None, description="Start date in YYYY-MM-DD format"),
        end_date: Optional[str] = Query(None, description="End date in YYYY-MM-DD format"),
        timezone: str = Query(
            default="America/Chicago",
            description="Timezone for date filtering (e.g., 'America/Chicago')",
        ),
        user: User = Depends(get_current_user_with_roles(["admin", "captain", "dev", "user", "buyer"]))
):
    """
    Retrieve carts for a specific show or tour based on their MD5 ID
    - For shows: MD5(event_name + venue + event_date_time)
    - For tours: MD5(event_name)
    """
    event_codes_list = event_codes.split(",") if event_codes else []
    time_status_list = time_status.split(",") if time_status else []
    captain_list = captain_emails.split(",") if captain_emails else []

    return await cart_manager_pg_db.get_specific_carts(
        id,
        view,
        status,
        event_codes_list,
        time_status_list,
        captain_list,
        start_date,
        end_date,
        timezone
    )


@router.patch("/update_status")
async def update_cart_manager_status(
        update_data: CartManagerUpdateNew,
        user: User = Depends(get_current_user_with_roles(["admin", "captain", "buyer", "user"]))
):
    return await cart_manager_pg_db.update_status(update_data, user)


@router.get("/list/export")
async def export_carts(
        status: Optional[str] = Query(None, regex="^(?i)(approve|decline|pending)$"),
        event_codes: Optional[str] = Query(None),
        time_status: Optional[str] = Query(None),
        captain_emails: Optional[str] = Query(None),
        start_date: Optional[str] = Query(None, description="Start date in YYYY-MM-DD format"),
        end_date: Optional[str] = Query(None, description="End date in YYYY-MM-DD format"),
        timezone: str = Query(
            default="America/Chicago",
            description="Timezone for date filtering (e.g., 'America/Chicago')",
        ),
        view: Optional[str] = Query('cart'),
        user: User = Depends(get_current_user_with_roles(["admin", "captain", "buyer", "user"])),
):
    event_codes_list = event_codes.split(",") if event_codes else []
    time_status_list = time_status.split(",") if event_codes else []
    captain_list = captain_emails.split(",") if captain_emails else []
    result = await cart_manager_pg_db.get_all_carts(
        status=status,
        event_codes=event_codes_list,
        time_status=time_status_list,
        captain_list=captain_list,
        start_date=start_date,
        end_date=end_date,
        timezone=timezone,
        view=view
    )
    csv_data = cart_manager_pg_db.convert_to_csv(result.get("items", []))
    return StreamingResponse(
        iter([csv_data]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=carts_export.csv"}
    )


@router.get("/events")
async def get_events_titles(
        hour: int = Query(1, ge=1),
        user: User = Depends(get_current_user_with_roles(["admin", "captain", "buyer", "user"]))
):
    return await cart_manager_pg_db.get_carts_events_title(hour)


@router.patch("/bulk_update_status")
async def bulk_update_cart_manager_status(
        update_data: BulkCartManagerUpdate,
        user: User = Depends(get_current_user_with_roles(["admin", "captain", "buyer", "user"]))
):
    return await cart_manager_pg_db.bulk_update_status(update_data, user)
