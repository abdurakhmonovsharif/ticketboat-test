from pydantic import BaseModel, field_validator, Field
from typing import List, Optional
from datetime import datetime


class ShadowsPricingReport(BaseModel):
    id: str
    event_name: str
    event_start_date: Optional[str] = None
    venue: Optional[str] = None
    percentage: str
    created_at: datetime

class ShadowsPricingReportResponse(BaseModel):
    items: List[ShadowsPricingReport] = Field(default_factory=list)
