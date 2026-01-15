from typing import Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Query

from app.auth.auth_system import get_current_user
from app.db.shadows_30day_mapping_db import (
    get_thirty_day_mapped_events,
    get_thirty_day_unmapped_events
)
from app.model.shadows_viagogo_event_mapping import *
from app.model.user import User

router = APIRouter(prefix="/thirtydaymapping")

@router.get("/mapped-events")
async def get_thirtyday_mapped_events(
    start_date: str = Query(
        default=None
    ),
    end_date: str = Query(
        default=None
    ),
    page: int = Query(
        default=500
    ),
    page_size: int = Query(
        default=1
    )
):
    return await get_thirty_day_mapped_events(start_date, end_date, page, page_size)

@router.get("/unmapped-events")
async def get_thirtyday_unmapped_events(
    start_date: str = Query(
        default=None
    ),
    end_date: str = Query(
        default=None
    ),
    page: int = Query(
        default=500
    ),
    page_size: int = Query(
        default=1
    )
):
    return await get_thirty_day_unmapped_events(start_date, end_date, page, page_size)
