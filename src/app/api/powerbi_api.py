from fastapi import APIRouter, Depends

from app.auth.auth_system import get_current_user_with_roles
from app.model.user import User
from app.service.power_bi_embed_fetcher import PowerBiEmbedFetcher, PowerBiReport

router = APIRouter(prefix="/powerbi")


@router.get("/embed_token/{workspace_id}/{report_id}")
async def recent_purchases(
    workspace_id: str,
    report_id: str,
    user: User = Depends(get_current_user_with_roles(["user"])),
):
    return PowerBiEmbedFetcher().get_embed_token(
        user, PowerBiReport(workspace_id=workspace_id, report_id=report_id)
    )
