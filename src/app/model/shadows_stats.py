from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class ShadowsStatsModel(BaseModel):
    name: str
    type: str
    last_updated: str
    config_data: Optional[dict] = None
    stat_data: Optional[dict] = None


class ShadowsStatsConfigModel(BaseModel):
    name: str
    type: str
    data: Optional[dict] = None
