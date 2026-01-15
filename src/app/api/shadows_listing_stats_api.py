from typing import Dict, Any
from fastapi import APIRouter, Depends, HTTPException

from app.auth.auth_system import get_current_user
from app.db.shadows_listing_stats_db import (
    get_items
)
from app.model.shadows_listings_stats import ShadowsListingResponse
from app.model.user import User


router = APIRouter(prefix="/shadows-listing-stats")

@router.get("")
async def get_listing_stats():
    items = await get_items()
    return ShadowsListingResponse(
        items=items
    )
