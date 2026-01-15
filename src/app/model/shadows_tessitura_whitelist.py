from pydantic import BaseModel, field_validator, Field
from typing import List, Optional
from datetime import datetime


class ShadowsTessituraWhitelist(BaseModel):
    id: str
    performance_date: datetime
    display_date: Optional[str] = None
    display_time: Optional[str] = None
    performance_title: str
    action_url: str
    description: Optional[str] = None
    country: str
    venue_name: str
    city: str
    state: str
    is_sold_out: Optional[bool] = None
    visited_at: datetime
    is_whitelisted: bool

class ShadowsTessituraWhitelistResponse(BaseModel):
    items: List[ShadowsTessituraWhitelist] = Field(default_factory=list)

class ShadowsTessituraAddWhitelistRequest(BaseModel):
    tessitura_event_id: str
    tessitura_event_url: str

class ShadowsTessituraAddWhitelistResponse(BaseModel):
    tessitura_event_id: str
    tessitura_event_url: str
    is_whitelisted: bool

class ShadowsTessituraRemoveWhitelistRequest(BaseModel):
    tessitura_event_id: str

class ShadowsTessituraRemoveWhitelistResponse(BaseModel):
    tessitura_event_id: str
    is_whitelisted: bool

class ShadowsTessituraSearchEventsRequest(BaseModel):
    performance_title: Optional[str] = None
    venue_name: Optional[str] = None
    action_url: Optional[str] = None
