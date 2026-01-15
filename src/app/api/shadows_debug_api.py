from fastapi import APIRouter, Depends, Query
from app.auth.auth_system import get_current_user_with_roles
from app.db.shadows_debug_db import get_viagogo_change_history_stats, get_viagogo_sale_by_order_id, get_viagogo_change_history, get_ticketmaster_seating_history
from app.model.user import User
from app.cache import get_shadows_redis_client
import json
import pickle

router = APIRouter(prefix="/shadows")

@router.get("/viagogo-change-history-stats")
async def get_viagogo_change_history_stats_endpoint(
    user: User = Depends(get_current_user_with_roles(["user"])),
):
    return await get_viagogo_change_history_stats()

@router.get("/viagogo-sale/{order_id}")
async def get_viagogo_sale_endpoint(
    order_id: str,
    user: User = Depends(get_current_user_with_roles(["user"])),
):
    return await get_viagogo_sale_by_order_id(order_id)

@router.get("/viagogo-change-history")
async def get_viagogo_change_history_endpoint(
    location_id: str = Query(...),
    anchor_utc_timestamp: str = Query(...),
    before_hours: int = Query(6),
    after_hours: int = Query(3),
    user: User = Depends(get_current_user_with_roles(["user"])),
):
    return await get_viagogo_change_history(location_id, anchor_utc_timestamp, before_hours, after_hours)

@router.get("/ticketmaster-seating-history")
async def get_ticketmaster_seating_history_endpoint(
    event_code: str = Query(...),
    section: str = Query(...),
    row: str = Query(...),
    anchor_utc_timestamp: str = Query(...),
    before_hours: int = Query(6),
    after_hours: int = Query(3),
    user: User = Depends(get_current_user_with_roles(["user"])),
):
    return await get_ticketmaster_seating_history(event_code, section, row, anchor_utc_timestamp, before_hours, after_hours)

@router.get("/ticketmaster-event-redis")
async def get_ticketmaster_event_redis(
    event_code: str = Query(...),
    section: str = Query(...),
    row: str = Query(...),
    segment: str = Query(...),
    external_id: str = Query(None),
    user: User = Depends(get_current_user_with_roles(["user"])),
):
    redis_client = get_shadows_redis_client()

    # Original Ticketmaster event key
    redis_key = f"ticketmaster_event:{{seating}}:ticketmaster_event#{event_code}/section#{section}/row#{row}"
    value = redis_client.get(redis_key)
    if value is None:
        redis_key = f"ticketmaster_event:{{seating}}:ticketmaster_event#{event_code}/section#{section}/row#GA"
        value = redis_client.get(redis_key)

    # New Viagogo listings key
    viagogo_key = f"viagogo_listingsv5:ticketmaster_event#{event_code}:section#{section}/row#{row}/segment#{segment}/"
    viagogo_value = redis_client.get(viagogo_key)

    # New Viagogo listing key (with literal {viagogo})
    viagogo_listing_value = None
    viagogo_listing_key = None
    if external_id:
        location_id = external_id.split(';')[0]
        viagogo_listing_key = f"viagogo_listing:{{viagogo}}:{location_id}"
        viagogo_listing_value = redis_client.get(viagogo_listing_key)

    def decode_redis_value(val):
        if val is None:
            return None
        try:
            if isinstance(val, bytes):
                try:
                    val_str = val.decode("utf-8")
                    return json.loads(val_str)
                except Exception:
                    return pickle.loads(val)
            elif isinstance(val, str):
                return json.loads(val)
            else:
                return str(val)
        except Exception:
            return str(val)

    result = {
        "ticketmaster": {
            "key": redis_key,
            "value": decode_redis_value(value)
        },
        "viagogo": {
            "key": viagogo_key,
            "value": decode_redis_value(viagogo_value)
        },
    }
    if viagogo_listing_key:
        result["viagogo_listing"] = {
            "key": viagogo_listing_key,
            "value": decode_redis_value(viagogo_listing_value)
        }
    return result
 