from typing import Dict, Any
from fastapi import APIRouter, Query

from app.db.shadows_ticketmaster_events_db import (
    get_items,
    get_details,
    search
)
from app.model.shadows_ticketmaster import (
    ShadowsTicketmasterEventsResponse,
    ShadowsTicketmasterSeatingResponse,
    ShadowsTicketmasterSearchQuery
)


router = APIRouter(prefix="/shadows-ticketmaster-events")

@router.get("")
async def get_ticketmaster_events(
    page: int = Query(
        default=100
    ),
    page_size: int = Query(
        default=1
    )
):
    items = await get_items(page=page, page_size=page_size)
    return ShadowsTicketmasterEventsResponse(
        items=items
    )

@router.get("/{event_code}")
async def retrieve_ticketmaster_details(
    event_code: str
):
    items = await get_details(event_code=event_code)
    return ShadowsTicketmasterSeatingResponse(
        items=items
    )

@router.post("/search")
async def search_events(payload: ShadowsTicketmasterSearchQuery):
    items = await search(payload)
    return ShadowsTicketmasterEventsResponse(
        items=items
    )
