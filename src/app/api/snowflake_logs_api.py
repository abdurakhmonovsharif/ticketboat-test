from fastapi import APIRouter, Depends, Query, HTTPException

from app.auth.auth_system import get_current_user_with_roles
from app.db import snowflake_logs_db
from app.model.user import User


router = APIRouter(prefix="/snowflake-logs")


def _lowercase_keys(record: dict) -> dict:
    return { (k.lower() if isinstance(k, str) else k): v for k, v in record.items() }


@router.get("/export-sale")
async def export_market_logs_by_sale_csv(
        market: str = Query(..., description="One of: viagogo, vivid, seatgeek, gotickets"),
        sale_id: str = Query(..., description="Sale ID in the market-specific sales table"),
        limit: int = Query(100, ge=1, le=1000),
        user: User = Depends(get_current_user_with_roles(["dev", "admin", "shadows", "user"]))
):
    try:
        rows = await snowflake_logs_db.fetch_market_logs_by_sale_id(market, sale_id, limit)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    if not rows:
        return []

    return [_lowercase_keys(dict(r)) for r in rows]
