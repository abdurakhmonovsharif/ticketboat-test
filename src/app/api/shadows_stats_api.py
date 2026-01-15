from fastapi import APIRouter, Depends

from app.auth.auth_system import get_current_user_with_roles
from app.db.shadows_stat_db import get_stats, store_stat_config, get_stats_config, delete_stats_config, \
    update_stat_config
from app.model.shadows_stats import ShadowsStatsConfigModel
from app.model.user import User

router = APIRouter(prefix="/shadows-stat")


@router.get("")
async def get_all_stats(user: User = Depends(get_current_user_with_roles(["user"]))):
    return await get_stats()


@router.post("/config")
async def post_config(
        payload: ShadowsStatsConfigModel,
        user: User = Depends(get_current_user_with_roles(["user"]))
):
    return await store_stat_config(payload)


@router.get("/config")
async def get_all_configs(
        user: User = Depends(get_current_user_with_roles(["user"]))
):
    return await get_stats_config()


@router.delete("/config/{name}/{type}")
async def delete_config(
        name: str,
        type: str,
        user: User = Depends(get_current_user_with_roles(["user"])),

):
    return await delete_stats_config(name,type)


@router.put("/config/{name}")
async def update_config(
        name: str,
        payload: ShadowsStatsConfigModel,
        user: User = Depends(get_current_user_with_roles(["user"])),

):
    return await update_stat_config(payload, name)
