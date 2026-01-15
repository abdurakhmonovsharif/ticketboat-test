from typing import Dict, Any
from fastapi import APIRouter, Depends, HTTPException

from app.auth.auth_system import get_current_user_with_roles
from app.db.shadows_listings_db import (
    get_viagogo_listings,
    get_vivid_listings,
    get_gotickets_listings,
    get_seatgeek_listings,
    retrieve_listing_db,
    search_listing_db
)
from app.model.shadows_listings import *
from app.model.user import User


router = APIRouter(prefix="/listings")

@router.get("")
async def get_all_listings(user: User = Depends(get_current_user_with_roles(["user"]))):
    viagogo_listings = await get_viagogo_listings()
    vivid_listings = await get_vivid_listings()
    gotickets_listings = await get_gotickets_listings()
    seatgeek_listings = await get_seatgeek_listings()
    return ShadowsListingsModel (
        viagogo=viagogo_listings,
        vivid=vivid_listings,
        gotickets=gotickets_listings,
        seatgeek=seatgeek_listings
    ).to_items_format()

@router.post("/search")
async def search_listings(
    payload: ShadowsListingSearchModel
):
    return await search_listing_db(payload)

@router.get("/{id}")
async def retrieve_listings(
    id: str,
    market: str, 
    user: User = Depends(get_current_user_with_roles(["user"]))
):
    return await retrieve_listing_db(id=id, market=market)
