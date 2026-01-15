from typing import Optional

from fastapi import APIRouter, Depends, Query

from app.auth.auth_system import get_current_user_with_roles
from app.db import report_category_db
from app.model.report import CategoryPayload
from app.model.user import User

router = APIRouter(prefix="/report_category")


@router.get("")
async def get_categories(
        timezone: Optional[str] = Query(
            default="America/Chicago",
            description="Timezone for date filtering (e.g., 'America/Chicago')",
        ),
        search_term: Optional[str] = Query(
            default="",
            description="Search term to filter results"
        ),
        page_size: Optional[int] = Query(
            default=50,
            description="Number of results to return per page",
        ),
        page: Optional[int] = Query(
            default=1,
            description="Page number to return",
        ),
        user: User = Depends(get_current_user_with_roles(["user"]))
):
    return await report_category_db.get_categories_by_user_role(user, timezone, search_term, page, page_size)


@router.post("")
async def create_category(
        category_payload: CategoryPayload,
        user: User = Depends(get_current_user_with_roles(["user"]))
):
    return await report_category_db.create_category(category_payload)


@router.put("/{category_id}")
async def update_category(
        category_id: str,
        category_payload: CategoryPayload,
        user: User = Depends(get_current_user_with_roles(["admin"]))
):
    return await report_category_db.update_category(category_id, category_payload)


@router.delete("/{category_id}")
async def delete_category(
        category_id: str,
        user: User = Depends(get_current_user_with_roles(["admin"]))
):
    return await report_category_db.delete_category(category_id)
