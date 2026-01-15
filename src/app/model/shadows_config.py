from pydantic import BaseModel
from typing import Optional, Literal
from datetime import datetime

OverrideType = Literal["default", "country", "venue"]

class ShadowsConfig(BaseModel):
    id: Optional[int] = None
    exchange: str
    override_type: OverrideType
    override_value: Optional[str] = None
    config_type: str
    config_value: float
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

class ShadowsConfigCreate(BaseModel):
    exchange: str
    override_type: OverrideType
    override_value: Optional[str] = None
    config_type: str
    config_value: float

class ShadowsConfigUpdate(BaseModel):
    config_value: float

class Exchange(BaseModel):
    name: str

