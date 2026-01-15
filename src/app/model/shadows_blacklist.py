from pydantic import BaseModel, field_validator, Field
from typing import Optional, List
from datetime import datetime


class ShadowsBlacklistModel(BaseModel):
    id: str
    event_code: Optional[str]
    event_name: Optional[str]
    start_date: Optional[datetime]
    notes: Optional[str]
    url: Optional[str]
    section: Optional[str]
    expiration_date: Optional[datetime]
    added_by: Optional[str]
    market: Optional[str]
    created_at: datetime = Field(default_factory=datetime.now)
    viagogo_event_id: Optional[str] = None
    vivid_event_id: Optional[str] = None
    seatgeek_event_id: Optional[str] = None
    gotickets_event_id: Optional[str] = None

class ShadowsBlacklistChangeLogModel(BaseModel):
    id: str
    operation: str
    event_code: str
    event_name: Optional[str] = None
    start_date: Optional[datetime] = None
    section: Optional[str] = None
    url: Optional[str] = None
    added_by: str
    market: Optional[str] = None

class ShadowsDeleteBlacklistModel(BaseModel):
    id: str
    event_code: str
    notes: str
    section: Optional[str] = None

class ShadowsBlacklistSQSMessage(BaseModel):
    id: str
    sub_id: str
    seating_section: Optional[str] = ""
    event_blacklisted_at: int
    event_blacklisted_at_str: str
    event_blacklisted_reason: str
    event_blacklisted_expires_at: int
    event_blacklisted_expires_at_str: str
    market: List[str]
    viagogo_event_id: Optional[str] = None
    vivid_event_id: Optional[str] = None
    seatgeek_event_id: Optional[str] = None
    gotickets_event_id: Optional[str] = None
