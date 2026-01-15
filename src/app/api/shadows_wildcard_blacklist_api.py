from typing import Dict, Any
from fastapi import APIRouter, Depends, Query

from app.auth.auth_system import get_current_user
from app.db.shadows_blacklist_wildcard_db import (
    get_item,
    get_items,
    update_item,
    create_item,
    delete_item
)
from app.model.shadows_blacklist_wildcard import ShadowsWildcardBlacklist, ShadowsWildcardBlacklistChangeLog
from app.model.user import User


router = APIRouter(prefix="/wildcard-blacklist")

@router.get("")
async def list_wildcard_blacklist():
    result = await get_items()
    return result

@router.get("/retrieve")
async def retrieve_wildcard_blacklist(
    payload: Dict[str, Any]
):
    id = payload.get("id")
    result = await get_item(id=str(id))
    return result

@router.post("/create")
async def create_wildcard_blacklist(
    payload: Dict[str, Any],
    user: User = Depends(get_current_user())
):
    event_name_like = payload.get("event_name_like")
    reason = payload.get("reason")
    market = payload.get("market")
    city = payload.get("city")
    result = await create_item(
        event_name_like=str(event_name_like),
        reason=str(reason),
        market=market,
        user=user,
        similarity=str(payload.get("similarity")),
        field=str(payload.get("field")),
        city=str(city) if city is not None else None
    )
    return result

@router.patch("/update")
async def update_wildcard_blacklist(
    payload: Dict[str, Any],
    user: User = Depends(get_current_user())
):
    id = payload.get("id")
    event_name_like = payload.get("event_name_like")
    reason = payload.get("reason")
    result = await update_item(
        str(event_name_like),
        str(reason),
        str(id),
        user,
        str(payload.get("similarity")),
        str(payload.get("field")),
        payload.get("market"),
        str(payload.get("city")) if payload.get("city") is not None else None
    )
    return result

@router.delete("/delete")
async def delete_wildcard_blacklist_item(
    payload: Dict[str, Any],
    user: User = Depends(get_current_user())
):
    id = payload.get("id")
    event_name_like = payload.get("event_name_like")
    result = await delete_item(str(id), str(event_name_like), user)
    return result
