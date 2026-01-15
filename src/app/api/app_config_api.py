from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from app.auth.auth_system import get_current_user, get_current_user_with_roles
from app.db.app_config_db import (
    get_all_config_values,
    get_config_entry,
    write_config_value,
)
from app.model.user import User

router = APIRouter(prefix="/app-config")


class ConfigValueRequest(BaseModel):
    config_value: str
    description: str | None = None


class ConfigValueResponse(BaseModel):
    config_key: str
    config_value: str
    description: str | None = None


@router.get("", response_model=list[ConfigValueResponse])
async def list_config_values(
    _: User = Depends(get_current_user()),
) -> list[ConfigValueResponse]:
    config_values = await get_all_config_values()
    return [ConfigValueResponse(**config) for config in config_values]


@router.get("/{config_key}", response_model=ConfigValueResponse)
async def get_config(
    config_key: str,
    _: User = Depends(get_current_user()),
) -> ConfigValueResponse:
    entry = await get_config_entry(config_key)
    if entry is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Config value not found for key '{config_key}'",
        )
    return ConfigValueResponse(**entry)


@router.put("/{config_key}", response_model=ConfigValueResponse)
async def upsert_config(
    config_key: str,
    payload: ConfigValueRequest,
    _: User = Depends(get_current_user_with_roles(["admin"])),
) -> ConfigValueResponse:
    await write_config_value(
        config_key, payload.config_value, description=payload.description
    )
    return ConfigValueResponse(
        config_key=config_key,
        config_value=payload.config_value,
        description=payload.description or config_key,
    )
