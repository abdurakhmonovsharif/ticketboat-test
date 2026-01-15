from pydantic import BaseModel
from typing import Optional

class AutopricingConfigItem(BaseModel):
    key: str
    value: Optional[str]

class AutopricingConfigUpdateRequest(BaseModel):
    key: str
    value: str
