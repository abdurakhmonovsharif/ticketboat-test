from fastapi import APIRouter, Depends, HTTPException

from app.auth.auth_system import get_current_user_with_roles
from app.db import log_navigation_db
from app.model.log_navigation import LogNavigation
from app.model.user import User

router = APIRouter(prefix="/log-navigation")

roles = ["user", "admin", "captain", "dev", "buyer"]


@router.post("")
async def create_log_navigation(
        create_data: LogNavigation,
        user: User = Depends(get_current_user_with_roles(roles))
):
    try:
        result = await log_navigation_db.create_log_navigation(create_data, user)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("")
async def get_log_navigation(
        user: User = Depends(get_current_user_with_roles(["admin", "dev", "user"])),
):
    return await log_navigation_db.get_popular_log_navigation()


@router.delete("/old-logs")
async def delete_old_logs(
        user: User = Depends(get_current_user_with_roles(["admin", "dev", "user"])),
):
    try:
        await log_navigation_db.delete_old_log_navigation()
        return {"message": "Old logs deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

