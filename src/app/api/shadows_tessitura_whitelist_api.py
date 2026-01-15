from typing import Dict, Any
from fastapi import APIRouter, Query

from app.db.shadows_tessitura_whitelist_db import (
    get_items
)
from app.model.shadows_tessitura_whitelist import ShadowsTessituraWhitelistResponse
from app.model.user import User


router = APIRouter(prefix="/shadows-tessitura-whitelist")

@router.get("")
async def get_whitelist(
    page: int = Query(
        default=1
    ),
    page_size: int = Query(
        default=100
    )
):
    items = await get_items(page, page_size)
    return ShadowsTessituraWhitelistResponse(
        items=items
    )
