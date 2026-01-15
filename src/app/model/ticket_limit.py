from uuid import UUID
from pydantic import BaseModel, field_validator
from typing import Optional
from datetime import datetime


class TicketLimitSerializer(BaseModel):
    id: int
    event_code: Optional[str] = None
    venue_code: Optional[str] = None
    performer_id: Optional[str] = None
    limit_type: str  # "show" or "run"
    limit_value: Optional[int] = None  # null means unlimited
    created_by: Optional[str] = None
    updated_by: Optional[str] = None
    created_at: datetime
    updated_at: datetime


class SetTicketLimitRequest(BaseModel):
    event_code: Optional[str] = None
    venue_code: Optional[str] = None
    performer_id: Optional[str] = None
    limit_type: str  # "show" or "run"
    limit_value: Optional[int] = None  # null means unlimited

    @field_validator('limit_type')
    @classmethod
    def validate_limit_type(cls, v):
        if v not in ['show', 'run']:
            raise ValueError('limit_type must be either "show" or "run"')
        return v

    @field_validator('limit_value')
    @classmethod
    def validate_limit_value(cls, v):
        if v is not None and v <= 0:
            raise ValueError('limit_value must be greater than 0 or null for unlimited')
        return v


class GetTicketLimitRequest(BaseModel):
    event_code: Optional[str] = None
    venue_code: Optional[str] = None
    performer_id: Optional[str] = None


