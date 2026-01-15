from fastapi import APIRouter, Depends, HTTPException

from app.auth.auth_system import get_current_user_with_roles
from app.db import email_filter_db
from app.model.email_filter import EmailFilterCreate
from app.model.user import User

router = APIRouter(prefix="/email-filter")

roles = ["user", "admin", "dev"]


@router.post("/create")
async def save_user_email_filter(
        create_data: EmailFilterCreate,
        user: User = Depends(get_current_user_with_roles(roles))
):
    try:
        result = await email_filter_db.save_email_filter(create_data, user)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/edit/{filter_id}")
async def update_user_email_filter(
        filter_id: str,
        update_data: EmailFilterCreate,
        user: User = Depends(get_current_user_with_roles(roles))
):
    try:
        result = await email_filter_db.update_email_filter(filter_id, update_data, user)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("")
async def get_user_email_filter(
        user: User = Depends(get_current_user_with_roles(roles)),
):
    try:
        return await email_filter_db.get_email_filter(user)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{filter_id}")
async def delete_user_email_filter(
        filter_id: str,
        user: User = Depends(get_current_user_with_roles(roles)),
):
    try:
        return await email_filter_db.delete_user_email_filter(filter_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed: {e}")
