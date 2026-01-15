from fastapi import APIRouter, Depends, HTTPException
from typing import List

from app.auth.auth_system import get_current_user_with_roles
from app.db import price_change_monitor_db
from app.model.price_change_monitor import (
    PriceChangeMonitorCreate,
    PriceChangeMonitorUpdate,
    PriceChangeMonitorResponse
)
from app.model.user import User

router = APIRouter(prefix="/price-monitors")

roles = ["user", "admin", "dev"]


@router.post("", response_model=PriceChangeMonitorResponse)
async def create_price_monitor(
    create_data: PriceChangeMonitorCreate,
    user: User = Depends(get_current_user_with_roles(roles))
):
    """Create a new price change monitor"""
    try:
        result = await price_change_monitor_db.create_monitor(create_data, user)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("", response_model=List[PriceChangeMonitorResponse])
async def get_all_price_monitors(
    user: User = Depends(get_current_user_with_roles(roles))
):
    """Get all price change monitors"""
    try:
        return await price_change_monitor_db.get_all_monitors(user)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{monitor_id}", response_model=PriceChangeMonitorResponse)
async def get_price_monitor(
    monitor_id: int,
    user: User = Depends(get_current_user_with_roles(roles))
):
    """Get a specific price change monitor by ID"""
    try:
        result = await price_change_monitor_db.get_monitor_by_id(monitor_id, user)
        if not result:
            raise HTTPException(status_code=404, detail="Monitor not found")
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{monitor_id}", response_model=PriceChangeMonitorResponse)
async def update_price_monitor(
    monitor_id: int,
    update_data: PriceChangeMonitorUpdate,
    user: User = Depends(get_current_user_with_roles(roles))
):
    """Update an existing price change monitor"""
    try:
        result = await price_change_monitor_db.update_monitor(monitor_id, update_data, user)
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{monitor_id}")
async def delete_price_monitor(
    monitor_id: int,
    user: User = Depends(get_current_user_with_roles(roles))
):
    """Delete a price change monitor"""
    try:
        result = await price_change_monitor_db.delete_monitor(monitor_id, user)
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))





