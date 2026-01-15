from pydantic import BaseModel, field_validator, Field
from typing import List, Optional
from datetime import datetime


class ShadowsListingStats(BaseModel):
    viagogo_account_id: str
    max_listings: int

class ShadowsListingResponse(BaseModel):
    items: List[ShadowsListingStats] = Field(default_factory=list)
