from fastapi import APIRouter, Depends, HTTPException

from app.auth.auth_system import get_current_user_with_roles
from app.db import super_priority_event_db
from app.model.super_priority_req import SuperPriorityEventRequest
from app.model.user import User

router = APIRouter(prefix="/super-priority")


@router.post("")
async def create_super_priority_event(
        sp_input: SuperPriorityEventRequest,
        user: User = Depends(get_current_user_with_roles(["admin", "captain"])),
):
    try:
        return await super_priority_event_db.create_super_priority_event(sp_input)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed: {e}")


@router.delete("/{event_code}")
async def delete_super_priority_event(
        event_code: str,
        user: User = Depends(get_current_user_with_roles(["admin", "captain"])),
):
    try:
        return await super_priority_event_db.delete_super_priority_event(event_code)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed: {e}")


@router.get("")
async def get_all_super_priority_list(
        user: User = Depends(get_current_user_with_roles(["admin", "captain"])),
):
    try:
        return await super_priority_event_db.get_all_super_priority_list()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed: {e}")


@router.get("/seats/{event_code}")
async def get_super_priority_event_seats(
        event_code: str,
        user: User = Depends(get_current_user_with_roles(["admin", "captain"])),
):
    try:
        return await super_priority_event_db.get_super_priority_event_seats(event_code)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed: {e}")


@router.get("/listing/{event_code}")
async def get_super_priority_event_listings(
        event_code: str,
        user: User = Depends(get_current_user_with_roles(["admin", "captain"])),
):
    try:
        return await super_priority_event_db.get_super_priority_event_listings(event_code)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed: {e}")
