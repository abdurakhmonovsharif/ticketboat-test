from typing import Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Query

from app.auth.auth_system import get_current_user
from app.db.shadows_viagogo_event_mapping_db import (
    get_viagogo_unmapped_events,
    get_viagogo_mapped_events,
    update_viagogo_mapping,
    update_ignore_mapping,
    remove_viagogo_mapping_event
)
from app.model.shadows_viagogo_event_mapping import *
from app.model.user import User

router = APIRouter(prefix="/viagogomapping")

@router.get("")
async def get_viagogo_event_mapping(
    page_size: int = Query(
        default=500,
        description="Number of results to return per page",
        ge=1
    ),
    page: int = Query(
        default=1,
        description="Page number to return",
        ge=1
    ),
):
    items = await get_viagogo_unmapped_events(page=page, page_size=page_size)
    return {
        "count": len(items),
        "items": items
    }

@router.post("/mapped-viagogo-events")
async def search_mapped_events(payload: ShadowsViagogoSearchMappedEventModel):
    items = await get_viagogo_mapped_events(payload)
    return {
        "count": len(items),
        "items": items
    }

@router.post("/update-viagogo-event-id")
async def update_viagogo_event_id(
    payload: ShadowsUpdateEventModel,
    user: User = Depends(get_current_user())
):
    return await update_viagogo_mapping(payload, user)

@router.post("/update-ignore")
async def update_ignore_viagogo_mapping(
    payload: ShadowsUpdateIgnoreModel,
    user: User = Depends(get_current_user())
):
    return await update_ignore_mapping(payload, user)

@router.post("/remove-mapping")
async def remove_viagogo_mapping(
    payload: ShadowsRemoveEventModel,
    user: User = Depends(get_current_user())
):
    return await remove_viagogo_mapping_event(payload, user)
