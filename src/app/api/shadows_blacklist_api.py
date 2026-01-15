from typing import Dict, Any, Optional
from fastapi import APIRouter, Depends, HTTPException, Query

from app.auth.auth_system import get_current_user
from app.db.shadows_blacklist_db import (
    get_snowflake_ticketmaster_data,
    get_snowflake_all_rows_data,
    get_blacklist_items,
    create_blacklist,
    delete_blacklist,
    create_blacklist_log,
    format_sqs_message,
    send_data_to_sqs,
    send_to_snowflake,
    delete_blacklist_snowflake
)
from app.model.shadows_blacklist import ShadowsDeleteBlacklistModel
from app.model.user import User
from app.cache import invalidate_shadows_cache


router = APIRouter(prefix="/blacklist")

@router.get("")
async def get_blacklist(
    page_size: int = Query(default=50, description="Number of results to return per page"),
    page: int = Query(default=1, description="Page number to return"),
    search: Optional[str] = Query(default=None, description="Search by event code, event name, or URL"),
) -> Dict[str, Any]:
    items = await get_blacklist_items(page=page, page_size=page_size, search=search)
    return items

@router.post("/ticketmaster-id")
async def post_blacklist_item_by_id(
    payload: Dict[str, Any],
    user: User = Depends(get_current_user())
):
    ticketmaster_id = payload.get('id')
    market = payload.get('market')

    tm_snowflake_data = await get_snowflake_ticketmaster_data(
        id=ticketmaster_id, 
        event_code='', 
        market=market, 
        user=user
    )

    blacklist_data = await create_blacklist(tm_snowflake_data)
    sqs_message = format_sqs_message(blacklist_data.get("data"))
    await send_data_to_sqs(sqs_message)
    await create_blacklist_log(blacklist_data, 'create', user)
    await send_to_snowflake(tm_snowflake_data)
    
    return blacklist_data

@router.post("/event-code")
async def post_blacklist_item_by_event_code(
    payload: Dict[str, Any],
    user: User = Depends(get_current_user())
):
    ticketmaster_event_code = payload.get('event_code')
    market = payload.get('market')

    tm_snowflake_data = await get_snowflake_ticketmaster_data(
        id='', 
        event_code=ticketmaster_event_code, 
        market=market, 
        user=user
    )

    blacklist_data = await create_blacklist(tm_snowflake_data)
    sqs_message = format_sqs_message(blacklist_data.get("data"))
    await send_data_to_sqs(sqs_message)
    await create_blacklist_log(blacklist_data, 'create', user)
    await send_to_snowflake(tm_snowflake_data)

    return blacklist_data

@router.post("/listing-id")
async def post_blacklist_item_by_listing_id(
    payload: Dict[str, Any],
    user: User = Depends(get_current_user())
):
    listing_id = payload.get('listing_id')
    market = payload.get('market')
    viagogo_event_id = payload.get('viagogo_event_id')
    vivid_event_id = payload.get('vivid_event_id')
    gotickets_event_id = payload.get('gotickets_event_id')
    seatgeek_event_id = payload.get('seatgeek_event_id')

    tm_snowflake_data = await get_snowflake_all_rows_data(
        blacklist_type='listing_id', 
        external_id=listing_id,  # type: ignore
        market=market,  # type: ignore
        user=user, 
        viagogo_event_id=viagogo_event_id, 
        vivid_event_id=vivid_event_id,
        seatgeek_event_id=seatgeek_event_id,
        gotickets_event_id=gotickets_event_id
    )

    blacklist_data = await create_blacklist(tm_snowflake_data)
    sqs_message = format_sqs_message(blacklist_data.get("data"))
    await send_data_to_sqs(sqs_message)
    await create_blacklist_log(blacklist_data, 'create', user)
    await send_to_snowflake(tm_snowflake_data)
    
    return blacklist_data

@router.post("/listing-section")
async def post_blacklist_item_by_listing_section(
    payload: Dict[str, Any],
    user: User = Depends(get_current_user())
):
    section = payload.get('section')
    section_id = payload.get('section_id')
    market = payload.get('market')
    viagogo_event_id = payload.get('viagogo_event_id')
    vivid_event_id = payload.get('vivid_event_id')
    gotickets_event_id = payload.get('gotickets_event_id')
    seatgeek_event_id = payload.get('seatgeek_event_id')

    tm_snowflake_data = await get_snowflake_all_rows_data(
        blacklist_type='listing_section', 
        section=section, # type: ignore
        external_id=section_id,  # type: ignore
        market=market,  # type: ignore
        user=user, 
        viagogo_event_id=viagogo_event_id, 
        vivid_event_id=vivid_event_id,
        seatgeek_event_id=seatgeek_event_id,
        gotickets_event_id=gotickets_event_id
    )

    blacklist_data = await create_blacklist(tm_snowflake_data)
    sqs_message = format_sqs_message(blacklist_data.get("data"))
    await send_data_to_sqs(sqs_message)
    await create_blacklist_log(blacklist_data, 'create', user)
    await send_to_snowflake(tm_snowflake_data)
    
    return blacklist_data

@router.delete("/delete")
async def delete_blacklist_entry(
    payload: ShadowsDeleteBlacklistModel,
    user: User = Depends(get_current_user())
):
    try:
        deleted_data = await delete_blacklist(payload)
        await create_blacklist_log(deleted_data, 'delete', user)
        await delete_blacklist_snowflake(payload)
        return deleted_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed: {e}")
    finally:
        invalidate_shadows_cache(f"blacklist_{payload.event_code}")
