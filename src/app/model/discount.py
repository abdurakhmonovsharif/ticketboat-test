from uuid import UUID
from pydantic import BaseModel, field_validator
from typing import List, Optional
from datetime import datetime


class DiscountSerializer(BaseModel):
    id: str
    buylist_id: Optional[str] = None
    discount_text: str
    discount_type: Optional[str] = None
    event_code: Optional[str] = None
    performer_id: Optional[str] = None
    venue_id: Optional[str] = None
    created_at: datetime
    created_by: str

    @field_validator('id', mode='before')
    @classmethod
    def convert_uuid_to_string(cls, v):
        if isinstance(v, UUID):
            return str(v)
        return v


class CreateDiscountRequest(BaseModel):
    buylist_id: str
    discount_text: str
    discount_type: str  # "event", "performer", or "venue"
    event_code: Optional[str] = None
    performer_id: Optional[str] = None
    venue_id: Optional[str] = None


class GetDiscountsResponse(BaseModel):
    discounts: List[DiscountSerializer]
    total: int
