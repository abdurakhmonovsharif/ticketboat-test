from typing import Optional

from fastapi import APIRouter, Depends

from app.auth.auth_system import get_current_user_with_roles
from app.db.shadows_offer_types_db import update_offer_type, get_all_offer_types
from app.model.user import User

router = APIRouter(prefix="/shadows-offer-types")


@router.get("")
async def get_all_offer_types_(offer_filter: str,
                               page: int = 1,
                               page_size: int = 100,
                               search_term: Optional[str] = '',
                               verified: Optional[bool] = False,
                               user: User = Depends(get_current_user_with_roles(["user", "shadows"]))):
    return await get_all_offer_types(offer_filter, page, page_size, search_term, verified)


@router.put("")
async def update_offer_type_(
        name: str,
        action: bool,
        user: User = Depends(get_current_user_with_roles(["user", "shadows"]))
):
    return await update_offer_type(name, action)
