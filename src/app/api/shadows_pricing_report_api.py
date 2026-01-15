from fastapi import APIRouter
from app.db.shadows_pricing_report_db import (
    get_items
)
from app.model.shadows_pricing_report import ShadowsPricingReportResponse


router = APIRouter(prefix="/shadows-pricing-report")

@router.get("")
async def get_pricing_override():
    items = await get_items()
    return ShadowsPricingReportResponse(
        items=items
    )
