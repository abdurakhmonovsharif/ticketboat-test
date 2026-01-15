from typing import Optional, List

from fastapi import APIRouter, Depends, Query

from app.auth.auth_system import get_current_user_with_roles
from app.db import report_db
from app.model.report import ReportPayload
from app.model.user import User

router = APIRouter(prefix="/powerbi_report")


@router.get("")
async def get_reports(
        timezone: Optional[str] = Query(
            default="America/Chicago",
            description="Timezone for date filtering (e.g., 'America/Chicago')",
        ),
        search_term: Optional[str] = Query(
            default="",
            description="Search term to filter results"
        ),
        category_id: Optional[str] = Query(
            default="",
            description="Filter reports by category ID"
        ),
        roles: Optional[List[str]] = Query(
            default=None,
            description="Filter reports by roles"
        ),
        page_size: Optional[int] = Query(
            default=50,
            description="Number of results to return per page",
        ),
        page: Optional[int] = Query(
            default=1,
            description="Page number to return",
        ),
        user: User = Depends(get_current_user_with_roles(["user"])),
):
    return await report_db.get_reports(timezone, search_term, category_id, roles, page, page_size, user.roles)


@router.post("")
async def create_report(
        report_payload: ReportPayload,
        user: User = Depends(get_current_user_with_roles(["user"]))
):
    return await report_db.create_report(report_payload)


@router.put("/{report_id}")
async def update_reports(
        report_id: str,
        report_payload: ReportPayload,
        user: User = Depends(get_current_user_with_roles(["admin"]))
):
    return await report_db.update_report(report_id, report_payload)


@router.delete("/{report_id}")
async def delete_reports(
        report_id: str,
        user: User = Depends(get_current_user_with_roles(["admin"]))
):
    return await report_db.delete_report(report_id)
