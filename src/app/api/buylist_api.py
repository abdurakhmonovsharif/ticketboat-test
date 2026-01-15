from datetime import date
from typing import Optional, List, Dict
import csv
import io
import traceback

from fastapi import APIRouter, Depends, Query, HTTPException
from fastapi.responses import StreamingResponse

from app.auth.auth_system import get_current_user_with_roles
from app.db.account_suggestion_db import fetch_suggestions_by_ids
from app.db.discount_db import create_discount, get_discounts_by_buylist_id, delete_discount, get_discounts_by_identifiers
from app.db.ticket_limit_db import get_ticket_limit, set_ticket_limit
from app.model.buylist import (
    BuyListItemSerializer,
    SuggestionsRequest,
    SuggestionsResponse,
    UpdateBuyListItemRequest,
    BatchUpdateBuylistRequest,
    SaveErrorReportRequest,
    UnclaimSalesRequest
)
from app.model.discount import CreateDiscountRequest, DiscountSerializer, GetDiscountsResponse
from app.model.ticket_limit import SetTicketLimitRequest, TicketLimitSerializer
from app.model.user import User
from app.db.buylist_db import (
    get_buylist_items,
    get_account_ids,
    get_buyers,
    get_buylist_item,
    grab_items,
    update_item,
    get_exchange_marketplaces,
    update_items_status,
    save_error_report,
    unclaim_sale,
    get_sale_history
)

router = APIRouter(prefix="/buylist")


@router.get("")
async def fetch_buylist_items(
        timezone: str = Query(
            default="America/Chicago",
            description="Timezone for date filtering (e.g., 'America/Chicago')",
        ),
        page_size: int = Query(
            default=10,
            description="Number of results to return per page",
            ge=1
        ),
        page: int = Query(
            default=1,
            description="Page number to return",
            ge=1
        ),
        sort_by: str = Query(
            default="created_at",
            description="Column to sort by",
        ),
        sort_order: str = Query(
            default="desc",
            description="Sort order: 'asc' or 'desc'",
        ),
        is_escalated: Optional[bool] = Query(None),
        account_ids: Optional[List[str]] = Query(None),
        exchange_marketplaces: Optional[List[str]] = Query(None),
        event_name: Optional[str] = Query(None),
        buylist_status: Optional[List[str]] = Query(None),
        venue_city: Optional[str] = Query(None),
        buyers: Optional[List[str]] = Query(None),
        subs: Optional[bool] = Query(None),
        nih: Optional[bool] = Query(None),
        escalated: Optional[bool] = Query(None),
        mismapped: Optional[bool] = Query(None),
        event_start_date: Optional[date] = Query(None),
        event_end_date: Optional[date] = Query(None),
        transaction_start_date: Optional[date] = Query(None),
        transaction_end_date: Optional[date] = Query(None),
        search_term: Optional[str] = Query(None),
        currency_code: Optional[List[str]] = Query(None),
        is_hardstock: Optional[bool] = Query(None),
        user: User = Depends(get_current_user_with_roles(["user", "shadows"])),
):
    return await get_buylist_items(
        timezone=timezone,
        page_size=page_size,
        page=page,
        sort_by=sort_by,
        sort_order=sort_order,
        is_escalated=is_escalated,
        account_ids=account_ids,
        exchange_marketplaces=exchange_marketplaces,
        event_name=event_name,
        buylist_status=buylist_status,
        venue_city=venue_city,
        buyers=buyers,
        subs=subs,
        nih=nih,
        escalated=escalated,
        mismapped=mismapped,
        event_start_date=event_start_date,
        event_end_date=event_end_date,
        transaction_start_date=transaction_start_date,
        transaction_end_date=transaction_end_date,
        search_term=search_term,
        currency_code=currency_code,
        is_hardstock=is_hardstock
    )


@router.get("/available-account-ids")
async def fetch_account_ids(
        is_escalated: Optional[bool] = Query(None),
        exchange_marketplaces: Optional[List[str]] = Query(None),
        event_name: Optional[str] = Query(None),
        buylist_status: Optional[List[str]] = Query(None),
        venue_city: Optional[str] = Query(None),
        buyers: Optional[List[str]] = Query(None),
        subs: Optional[bool] = Query(None),
        nih: Optional[bool] = Query(None),
        escalated: Optional[bool] = Query(None),
        mismapped: Optional[bool] = Query(None),
        search_term: Optional[str] = Query(None),
        currency_code: Optional[List[str]] = Query(None),
        user: User = Depends(get_current_user_with_roles(["user", "shadows"])),
):
    return await get_account_ids(
        is_escalated=is_escalated,
        exchange_marketplaces=exchange_marketplaces,
        event_name=event_name,
        buylist_status=buylist_status,
        venue_city=venue_city,
        buyers=buyers,
        subs=subs,
        nih=nih,
        escalated=escalated,
        mismapped=mismapped,
        search_term=search_term,
        currency_code=currency_code
    )


@router.get("/buyers")
async def fetch_buyers(
        account_ids: Optional[List[str]] = Query(None),
        exchange_marketplaces: Optional[List[str]] = Query(None),
        event_name: Optional[str] = Query(None),
        buylist_status: Optional[List[str]] = Query(None),
        venue_city: Optional[str] = Query(None),
        subs: Optional[bool] = Query(None),
        nih: Optional[bool] = Query(None),
        escalated: Optional[bool] = Query(None),
        mismapped: Optional[bool] = Query(None),
        search_term: Optional[str] = Query(None),
        currency_code: Optional[List[str]] = Query(None),
        user: User = Depends(get_current_user_with_roles(["user", "shadows"])),
):
    return await get_buyers(
        account_ids=account_ids,
        exchange_marketplaces=exchange_marketplaces,
        event_name=event_name,
        buylist_status=buylist_status,
        venue_city=venue_city,
        subs=subs,
        nih=nih,
        escalated=escalated,
        mismapped=mismapped,
        search_term=search_term,
        currency_code=currency_code
    )


@router.get("/exchange-marketplaces")
async def fetch_exchange_marketplaces(
        account_ids: Optional[List[str]] = Query(None),
        exchange_marketplaces: Optional[List[str]] = Query(None),
        event_name: Optional[str] = Query(None),
        buyers: Optional[List[str]] = Query(None),
        buylist_status: Optional[List[str]] = Query(None),
        venue_city: Optional[str] = Query(None),
        subs: Optional[bool] = Query(None),
        nih: Optional[bool] = Query(None),
        escalated: Optional[bool] = Query(None),
        mismapped: Optional[bool] = Query(None),
        search_term: Optional[str] = Query(None),
        currency_code: Optional[List[str]] = Query(None),
        user: User = Depends(get_current_user_with_roles(["user", "shadows"])),
):
    return await get_exchange_marketplaces(
        account_ids=account_ids,
        exchange_marketplaces=exchange_marketplaces,
        event_name=event_name,
        buyers=buyers,
        buylist_status=buylist_status,
        venue_city=venue_city,
        subs=subs,
        nih=nih,
        escalated=escalated,
        mismapped=mismapped,
        search_term=search_term,
        currency_code=currency_code
    )


@router.post("/unclaim")
async def unclaim_sale_api(
        payload: UnclaimSalesRequest,
        user: User = Depends(get_current_user_with_roles(["user", "shadows"]))
):
    return await unclaim_sale(payload.ids, user)


@router.get("/get-sale-history")
async def get_sale_history_api(id: str):
    return await get_sale_history(id)


async def generate_csv_stream(items: List[Dict]):
    """Generator function to stream CSV data in chunks"""
    if not items:
        return
    
    # Create header
    fieldnames = list(items[0].keys())
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    yield output.getvalue()
    output.truncate(0)
    output.seek(0)
    
    # Write rows in chunks to avoid memory issues
    chunk_size = 500
    for i in range(0, len(items), chunk_size):
        chunk = items[i:i + chunk_size]
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writerows(chunk)
        yield output.getvalue()


@router.get("/export")
async def export_buylist_items_csv(
        timezone: str = Query(
            default="America/Chicago",
            description="Timezone for date filtering (e.g., 'America/Chicago')",
        ),
        sort_by: str = Query(
            default="created_at",
            description="Column to sort by",
        ),
        sort_order: str = Query(
            default="desc",
            description="Sort order: 'asc' or 'desc'",
        ),
        is_escalated: Optional[bool] = Query(None),
        account_ids: Optional[List[str]] = Query(None),
        exchange_marketplaces: Optional[List[str]] = Query(None),
        event_name: Optional[str] = Query(None),
        buylist_status: Optional[List[str]] = Query(None),
        venue_city: Optional[str] = Query(None),
        buyers: Optional[List[str]] = Query(None),
        subs: Optional[bool] = Query(None),
        nih: Optional[bool] = Query(None),
        escalated: Optional[bool] = Query(None),
        mismapped: Optional[bool] = Query(None),
        event_start_date: Optional[date] = Query(None),
        event_end_date: Optional[date] = Query(None),
        transaction_start_date: Optional[date] = Query(None),
        transaction_end_date: Optional[date] = Query(None),
        search_term: Optional[str] = Query(None),
        currency_code: Optional[List[str]] = Query(None),
        is_hardstock: Optional[bool] = Query(None),
        user: User = Depends(get_current_user_with_roles(["user", "shadows"])),
):
    """Export buylist items to CSV with filters applied. Limited to 20,000 records."""
    try:
        result = await get_buylist_items(
            page_size=20000,
            page=1,
            sort_by=sort_by,
            sort_order=sort_order,
            timezone=timezone,
            is_escalated=is_escalated,
            account_ids=account_ids,
            exchange_marketplaces=exchange_marketplaces,
            event_name=event_name,
            buylist_status=buylist_status,
            venue_city=venue_city,
            buyers=buyers,
            subs=subs,
            nih=nih,
            escalated=escalated,
            mismapped=mismapped,
            event_start_date=event_start_date,
            event_end_date=event_end_date,
            transaction_start_date=transaction_start_date,
            transaction_end_date=transaction_end_date,
            search_term=search_term,
            currency_code=currency_code,
            is_hardstock=is_hardstock
        )

        items = result.get("items", [])

        # Use streaming response with generator for better memory efficiency
        return StreamingResponse(
            generate_csv_stream(items),
            media_type="text/csv",
            headers={
                "Content-Disposition": "attachment; filename=buylist_export.csv"
            }
        )
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"An error occurred while exporting buylist: {str(e)}")


@router.get("/ticket-limit")
async def get_ticket_limit_endpoint(
        event_code: Optional[str] = Query(None),
        venue_code: Optional[str] = Query(None),
        performer_id: Optional[str] = Query(None),
        user: User = Depends(get_current_user_with_roles(["user", "shadows"]))
):
    """Get ticket limit for an event, venue, or performer. Returns null if no limit is set."""
    limit = await get_ticket_limit(event_code, venue_code, performer_id)
    return limit  # Returns None/null if no limit found, which is fine


@router.post("/ticket-limit", response_model=TicketLimitSerializer)
async def set_ticket_limit_endpoint(
        limit_data: SetTicketLimitRequest,
        user: User = Depends(get_current_user_with_roles(["user", "shadows"]))
):
    """Set or update a ticket limit."""
    return await set_ticket_limit(limit_data, user.email)


@router.get("/{item_id}", response_model=BuyListItemSerializer)
async def fetch_buylist_item(
        item_id: str,
        timezone: str = Query(
            default="America/Chicago",
            description="Timezone for date filtering (e.g., 'America/Chicago')",
        ),
        user: User = Depends(get_current_user_with_roles(["user", "shadows"])),
):
    return await get_buylist_item(item_id, timezone)


@router.patch("/grab", response_model=dict)
async def grab_buylist_items(
        payload: Dict[str, List[str]],
        user: User = Depends(get_current_user_with_roles(["user", "shadows"])),
):
    return await grab_items(payload["item_ids"], user)


@router.patch("/{item_id}", response_model=dict)
async def update_buylist_item(
        item_id: str,
        update_data: UpdateBuyListItemRequest,
        user: User = Depends(get_current_user_with_roles(["user", "shadows"])),
):
    return await update_item(item_id, update_data)


@router.patch("/batch-status-update", response_model=dict)
async def update_buylist_items_batch(
        update_data: BatchUpdateBuylistRequest,
        user: User = Depends(get_current_user_with_roles(["user", "shadows"])),
):
    return await update_items_status(
        update_data.item_ids,
        update_data.buylist_order_status
    )


@router.post("/error-report", response_model=dict)
async def save_error_report_endpoint(
        save_data: SaveErrorReportRequest,
        user: User = Depends(get_current_user_with_roles(["user", "shadows"])),
):
    return await save_error_report(save_data, user.name)


@router.post("/discounts", response_model=DiscountSerializer)
async def create_discount_endpoint(
        discount_data: CreateDiscountRequest,
        user: User = Depends(get_current_user_with_roles(["user", "shadows"]))
):
    """Create a new discount for a buylist item."""
    return await create_discount(discount_data, user.name)


@router.get("/{buylist_id}/discounts", response_model=GetDiscountsResponse)
async def get_discounts_endpoint(
        buylist_id: str,
        user: User = Depends(get_current_user_with_roles(["user", "shadows"]))
):
    """Get all discounts for a specific buylist item."""
    discounts = await get_discounts_by_buylist_id(buylist_id)
    return GetDiscountsResponse(discounts=discounts, total=len(discounts))


@router.get("/discounts/by-identifiers", response_model=GetDiscountsResponse)
async def get_discounts_by_identifiers_endpoint(
        event_code: Optional[str] = Query(None),
        performer_id: Optional[str] = Query(None),
        venue_id: Optional[str] = Query(None),
        user: User = Depends(get_current_user_with_roles(["user", "shadows"]))
):
    """Get all discounts by event_code, performer_id, or venue_id identifiers."""
    discounts = await get_discounts_by_identifiers(event_code, performer_id, venue_id)
    return GetDiscountsResponse(discounts=discounts, total=len(discounts))


@router.delete("/discounts/{discount_id}")
async def delete_discount_endpoint(
        discount_id: str,
        user: User = Depends(get_current_user_with_roles(["user", "shadows"]))
):
    """Delete a discount."""
    return await delete_discount(discount_id)


@router.post(
    "/suggestions",
    dependencies=[Depends(get_current_user_with_roles(["user", "shadows"]))],
    response_model=List[SuggestionsResponse],
)
async def get_account_suggestions(payload: SuggestionsRequest):
    """Get account suggestions for a list of buylist item IDs."""
    suggestions = await fetch_suggestions_by_ids(payload.item_ids)
    return suggestions
