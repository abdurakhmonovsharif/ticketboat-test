from fastapi import APIRouter, Depends, HTTPException

from app.auth.auth_system import get_current_user_with_roles
from app.db import favourite_page_db
from app.model.favourite_pages import FavouritePageCreate
from app.model.user import User

router = APIRouter(prefix="/favourite-page")

roles = ["user", "admin", "dev"]


@router.post("")
async def save_user_favourite(
        create_data: FavouritePageCreate,
        user: User = Depends(get_current_user_with_roles(roles))
):
    try:
        result = await favourite_page_db.save_user_favourite(create_data, user)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("")
async def get_user_favourite(
        user: User = Depends(get_current_user_with_roles(roles)),
):
    return await favourite_page_db.get_user_favourite(user)
