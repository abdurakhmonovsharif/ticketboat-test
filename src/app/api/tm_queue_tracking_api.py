from typing import Optional

from fastapi import APIRouter, Depends, Query, HTTPException

from app.auth.auth_system import get_current_user_with_roles
from app.db import tm_queue_tracking_db
from app.model.user import User

router = APIRouter(prefix="/tm-queue")


@router.get("")
async def get_tm_queue_tracking(
        user: User = Depends(get_current_user_with_roles(["admin", "dev", "user"])),
        page_size: int = Query(10, ge=1),
        page: int = Query(0, ge=0),
        search: Optional[str] = Query(None, description="Search term"),
):
    return await tm_queue_tracking_db.get_tm_queue_tracking(page_size=page_size, page=page, search=search)


@router.get("/summary")
async def get_tm_queue_tracking(
        user: User = Depends(get_current_user_with_roles(["admin", "dev", "user"])),
        page_size: int = Query(10, ge=1),
        page: int = Query(0, ge=0),
        search: Optional[str] = Query(None, description="Search term"),
):
    try:
        return await tm_queue_tracking_db.get_tm_queue_summary(page_size=page_size, page=page, search=search)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed: {e}")
