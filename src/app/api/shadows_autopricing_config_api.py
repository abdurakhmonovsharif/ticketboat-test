from fastapi import APIRouter, HTTPException, Depends
from typing import List
from app.db.shadows_autopricing_config_db import get_all_autopricing_config, upsert_autopricing_config, get_autopricing_config_history
from app.model.shadows_autopricing_config import AutopricingConfigItem, AutopricingConfigUpdateRequest
from app.auth.auth_system import get_current_user, get_current_user_with_roles
from app.model.user import User

router = APIRouter(prefix="/autopricing-config")

@router.get("", response_model=List[AutopricingConfigItem])
def get_config(user: User = Depends(get_current_user_with_roles(["admin", "shadows lead"]))):
    try:
        return get_all_autopricing_config()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/history")
def get_config_history():
    try:
        return get_autopricing_config_history()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("", response_model=None)
async def update_config(request: AutopricingConfigUpdateRequest, user: User = Depends(get_current_user_with_roles(["admin", "shadows lead"]))):
    try:
        updated_by = user.email if user and user.email else "system"
        await upsert_autopricing_config(request.key, request.value, updated_by)
        return {"message": "Config updated"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 