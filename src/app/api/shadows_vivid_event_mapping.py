from typing import Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Query

from app.auth.auth_system import get_current_user
from app.db.shadows_vivid_event_mapping_db import (
    get_vivid_unmapped_events,
    get_vivid_mapped_events,
    update_vivid_mapping_event,
    update_ignore_mapping,
    remove_vivid_mapping_event
)
from app.model.shadows_vivid_event_mapping import *
from app.model.user import User

router = APIRouter(prefix="/vividmapping")

@router.get("")
async def get_vivid_event_mapping(
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
    items = await get_vivid_unmapped_events(page=page, page_size=page_size)
    return {
        "count": len(items),
        "items": items
    }

@router.post("/mapped-vivid-events")
async def search_mapped_events(payload: ShadowsVividSearchMappedEventModel):
    items = await get_vivid_mapped_events(payload)
    return {
        "count": len(items),
        "items": items
    }

@router.post("/update-vivid-event-id")
async def update_vivid_event_id(
    payload: ShadowsUpdateEventModel,
    user: User = Depends(get_current_user())
):
    return await update_vivid_mapping_event(payload, user)

@router.post("/update-ignore")
async def update_ignore_vivid_mapping(
    payload: ShadowsUpdateIgnoreModel,
    user: User = Depends(get_current_user())
):
    return await update_ignore_mapping(payload, user)

@router.post("/remove-mapping")
async def remove_vivid_mapping(
    payload: ShadowsRemoveEventModel,
    user: User = Depends(get_current_user())
):
    return await remove_vivid_mapping_event(payload, user)
