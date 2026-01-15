from fastapi import APIRouter, HTTPException, Depends
from typing import List
from app.db.shadows_config_db import (
    get_all_configs,
    get_configs_by_exchange,
    get_exchanges,
    create_config,
    update_config,
    delete_config,
    delete_exchange,
    get_config_history_by_exchange,
)
from app.model.shadows_config import (
    ShadowsConfig,
    ShadowsConfigCreate,
    ShadowsConfigUpdate,
    Exchange,
)
from app.auth.auth_system import get_current_user_with_roles
from app.model.user import User

router = APIRouter(prefix="/shadows")

@router.get("/configs", response_model=List[ShadowsConfig])
async def get_all_shadows_configs(
    user: User = Depends(get_current_user_with_roles(["admin", "shadows lead"]))
):
    """Get all shadows configurations"""
    try:
        return await get_all_configs()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/configs/exchange/{exchange}", response_model=List[ShadowsConfig])
async def get_configs_for_exchange(
    exchange: str,
    user: User = Depends(get_current_user_with_roles(["admin", "shadows lead"]))
):
    """Get all configurations for a specific exchange"""
    try:
        return await get_configs_by_exchange(exchange)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/exchanges", response_model=List[Exchange])
async def get_all_exchanges(
    user: User = Depends(get_current_user_with_roles(["admin", "shadows lead"]))
):
    """Get list of all exchanges"""
    try:
        return await get_exchanges()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/configs", response_model=ShadowsConfig)
async def create_shadows_config(
    config: ShadowsConfigCreate,
    user: User = Depends(get_current_user_with_roles(["admin", "shadows lead"]))
):
    """Create a new shadows configuration"""
    try:
        return await create_config(config, updated_by=user.email or "Unknown")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/configs/{config_id}", response_model=ShadowsConfig)
async def update_shadows_config(
    config_id: int,
    config_update: ShadowsConfigUpdate,
    user: User = Depends(get_current_user_with_roles(["admin", "shadows lead"]))
):
    """Update a shadows configuration value"""
    try:
        return await update_config(config_id, config_update.config_value, updated_by=user.email or "Unknown")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/configs/{config_id}")
async def delete_shadows_config(
    config_id: int,
    user: User = Depends(get_current_user_with_roles(["admin", "shadows lead"]))
):
    """Delete a shadows configuration"""
    try:
        await delete_config(config_id, updated_by=user.email or "Unknown")
        return {"message": "Config deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/exchanges/{exchange}")
async def delete_shadows_exchange(
    exchange: str,
    user: User = Depends(get_current_user_with_roles(["admin", "shadows lead"]))
):
    """Delete an exchange and all its configurations"""
    try:
        await delete_exchange(exchange, updated_by=user.email or "Unknown")
        return {"message": "Exchange deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/configs/exchange/{exchange}/history")
async def get_config_history_for_exchange(
    exchange: str,
    page: int = 1,
    page_size: int = 20,
    user: User = Depends(get_current_user_with_roles(["admin", "shadows lead"]))
):
    """Get configuration change history for a specific exchange with pagination"""
    try:
        if page < 1:
            raise HTTPException(status_code=400, detail="Page must be >= 1")
        if page_size < 1 or page_size > 100:
            raise HTTPException(status_code=400, detail="Page size must be between 1 and 100")
        
        return await get_config_history_by_exchange(exchange, page, page_size)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

