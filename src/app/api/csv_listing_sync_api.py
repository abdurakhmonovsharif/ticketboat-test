from fastapi import APIRouter, Depends

from app.auth.auth_system import get_current_user_with_roles
from app.db import csv_sync_db
from app.model.user import User

router = APIRouter()

roles = ["dev"]


@router.get("/csv_listings")
async def get_vivid_csv_by_account(
        account_id: str,
        marketplace: str,
        user: User = Depends(get_current_user_with_roles(roles)),
):
    return csv_sync_db.get_vivid_csv_by_account_(account_id, marketplace)

@router.get("/csv_accounts")
async def get_vivid_csv_by_account(
        marketplace: str,
        user: User = Depends(get_current_user_with_roles(roles)),
):
    return csv_sync_db.get_csv_accounts(marketplace)

@router.put("/stop_realtime")
async def get_vivid_csv_by_account(
        marketplace: str,
        account_id: str,
        new_value: bool = False,
        user: User = Depends(get_current_user_with_roles(roles)),
):
    return csv_sync_db.stop_realtime(marketplace,account_id,new_value)
